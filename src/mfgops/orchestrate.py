"""Pipeline orchestration: stages, dependencies, logging, retries and a quality gate.

Stage order (each runs only if the one before succeeded):

    validate_sources → monitor_row_counts → bronze → profile → silver
        → quality_gate → gold → reconcile

The quality gate is what keeps bad data out of reporting. If more than
``max_error_rate`` of any table's rows fail an error rule, the run stops
before gold is rebuilt, so the report keeps showing the last good data and
the failure is visible in the run log and on the data-quality page.

Every stage writes a row to ``meta_pipeline_runs`` (status, timing, rows,
message). In Fabric, the same function is called from a notebook that a Data
Pipeline schedules. Locally:

    python -m mfgops.orchestrate
"""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone

from mfgops.ingest import profile, validate_sources
from mfgops.lakehouse import Lakehouse, get_spark
from mfgops.pipeline import SOURCE_TABLES, new_run_id, run_bronze, run_gold, run_silver
from mfgops.reconcile import reconcile

RETRYABLE = (OSError, ConnectionError, TimeoutError)

LOG_SCHEMA = (
    "run_id string, stage string, status string, attempt int, started_at timestamp, "
    "finished_at timestamp, seconds double, rows long, message string"
)
COUNTS_SCHEMA = "run_id string, table_name string, row_count long, recorded_at timestamp"


class QualityGateError(RuntimeError):
    pass


class ReconciliationError(RuntimeError):
    pass


class RowCountError(RuntimeError):
    pass


@dataclass
class RunResult:
    run_id: str
    status: str = "running"
    failed_stage: str | None = None
    message: str = ""
    stages: list[dict] = field(default_factory=list)
    outputs: dict = field(default_factory=dict)


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _log(lh: Lakehouse, rec: dict) -> None:
    lh.append(
        lh.spark.createDataFrame(
            [
                (
                    rec["run_id"],
                    rec["stage"],
                    rec["status"],
                    rec["attempt"],
                    rec["started_at"],
                    rec["finished_at"],
                    rec["seconds"],
                    rec["rows"],
                    rec["message"][:1000],
                )
            ],
            LOG_SCHEMA,
        ),
        "meta",
        "pipeline_runs",
    )
    print(
        f"[{rec['stage']:<18}] {rec['status']:<7} {rec['seconds']:6.1f}s  rows={rec['rows']:<8} {rec['message'][:160]}"
    )


def _previous_counts(lh: Lakehouse) -> dict[str, int]:
    try:
        df = lh.read("meta", "source_counts")
    except Exception:
        return {}
    last = df.agg({"recorded_at": "max"}).first()[0]
    if last is None:
        return {}
    return {r["table_name"]: r["row_count"] for r in df.filter(df.recorded_at == last).collect()}


def monitor_row_counts(
    previous: dict[str, int], current: dict[str, int], warn_change: float = 0.30, fail_drop: float = 0.50
) -> list[str]:
    """Compare row counts with the last successful run. Big drops fail, big changes warn."""
    warnings = []
    for t, n in current.items():
        p = previous.get(t)
        if not p:
            continue
        change = (n - p) / p
        if change <= -fail_drop:
            raise RowCountError(f"{t}: row count fell from {p} to {n} ({change:+.0%}), extract probably incomplete")
        if abs(change) >= warn_change:
            warnings.append(f"{t}: {p} → {n} ({change:+.0%})")
    return warnings


def run_pipeline(lh: Lakehouse, max_error_rate: float = 0.02, retries: int = 2, backoff_s: float = 2.0) -> RunResult:
    res = RunResult(run_id=new_run_id())
    ctx: dict = {}

    def stage(name: str, fn: Callable[[], tuple[int, str]]) -> None:
        ctx["current_stage"] = name
        for attempt in range(1, retries + 2):
            start = _now()
            t0 = time.perf_counter()
            try:
                rows, msg = fn()
                rec = dict(
                    run_id=res.run_id,
                    stage=name,
                    status="success",
                    attempt=attempt,
                    started_at=start,
                    finished_at=_now(),
                    seconds=round(time.perf_counter() - t0, 2),
                    rows=int(rows),
                    message=msg,
                )
                _log(lh, rec)
                res.stages.append(rec)
                return
            except RETRYABLE as exc:
                if attempt <= retries:
                    rec = dict(
                        run_id=res.run_id,
                        stage=name,
                        status="retry",
                        attempt=attempt,
                        started_at=start,
                        finished_at=_now(),
                        seconds=round(time.perf_counter() - t0, 2),
                        rows=0,
                        message=f"{type(exc).__name__}: {exc}",
                    )
                    _log(lh, rec)
                    time.sleep(backoff_s * attempt)
                    continue
                raise
            except Exception as exc:
                rec = dict(
                    run_id=res.run_id,
                    stage=name,
                    status="failed",
                    attempt=attempt,
                    started_at=start,
                    finished_at=_now(),
                    seconds=round(time.perf_counter() - t0, 2),
                    rows=0,
                    message=f"{type(exc).__name__}: {exc}",
                )
                _log(lh, rec)
                res.stages.append(rec)
                raise

    def s_validate():
        ctx["counts"] = validate_sources(lh)
        return sum(ctx["counts"].values()), f"{len(ctx['counts'])} extracts match their contracts"

    def s_monitor():
        warnings = monitor_row_counts(_previous_counts(lh), ctx["counts"])
        return len(warnings), ("; ".join(warnings) or "row counts in line with the last successful run")

    def s_bronze():
        counts = run_bronze(lh)
        return sum(counts.values()), "raw extracts landed"

    def s_profile():
        frames = [profile(lh, lh.read("bronze", t), t, res.run_id) for t in SOURCE_TABLES]
        out = frames[0]
        for f in frames[1:]:
            out = out.unionByName(f)
        lh.write(out, "meta", "column_profile")
        return out.count(), "column profile written"

    def s_silver():
        ctx["silver"] = run_silver(lh, res.run_id)
        q = sum(v["quarantined"] for v in ctx["silver"].values())
        return q, f"{q} rows quarantined"

    def s_gate():
        worst = max(ctx["silver"].items(), key=lambda kv: kv[1]["error_rate"])
        over = {t: v["error_rate"] for t, v in ctx["silver"].items() if v["error_rate"] > max_error_rate}
        if over:
            detail = ", ".join(f"{t} {r:.1%}" for t, r in over.items())
            raise QualityGateError(
                f"error rate above {max_error_rate:.0%} in: {detail}. Gold not refreshed, report keeps last good data"
            )
        return 0, f"passed (highest error rate {worst[0]} {worst[1]['error_rate']:.2%}, limit {max_error_rate:.0%})"

    def s_gold():
        res.outputs["gold"] = run_gold(lh)
        return sum(res.outputs["gold"].values()), "star schema refreshed"

    def s_reconcile():
        checks = reconcile(lh)
        res.outputs["reconciliation"] = checks
        failed = [c["check"] for c in checks if not c["passed"]]
        if failed:
            raise ReconciliationError("; ".join(failed))
        return len(checks), f"{len(checks)} of {len(checks)} reconciliation checks passed"

    plan = [
        ("validate_sources", s_validate),
        ("monitor_row_counts", s_monitor),
        ("bronze", s_bronze),
        ("profile", s_profile),
        ("silver", s_silver),
        ("quality_gate", s_gate),
        ("gold", s_gold),
        ("reconcile", s_reconcile),
    ]
    try:
        for name, fn in plan:
            stage(name, fn)
    except Exception as exc:
        res.status = "failed"
        res.failed_stage = ctx.get("current_stage")
        res.message = str(exc)
        return res

    now = _now()
    lh.append(
        lh.spark.createDataFrame([(res.run_id, t, n, now) for t, n in ctx["counts"].items()], COUNTS_SCHEMA),
        "meta",
        "source_counts",
    )
    res.status = "success"
    return res


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--raw", default="data/raw")
    p.add_argument("--lakehouse", default="lakehouse")
    p.add_argument("--max-error-rate", type=float, default=0.02)
    a = p.parse_args()
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    lh = Lakehouse(spark, root=a.lakehouse, raw_path=a.raw)
    res = run_pipeline(lh, a.max_error_rate)
    print(
        json.dumps(
            {"run_id": res.run_id, "status": res.status, "failed_stage": res.failed_stage, "message": res.message},
            indent=1,
        )
    )
    spark.stop()
    raise SystemExit(0 if res.status == "success" else 1)


if __name__ == "__main__":
    main()
