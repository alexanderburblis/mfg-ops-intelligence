"""Pipeline orchestration: stages, dependencies, logging, retries and a quality gate.

Stage order (each runs only if the one before succeeded):

    validate_sources → monitor_row_counts → bronze → profile → silver
        → quality_gate → gold → reconcile

The quality gate is what keeps bad data out of reporting. If more than
``max_error_rate`` of any table's rows fail an error rule, the run stops
before gold is rebuilt, so the report keeps showing the last good data and
the failure is visible in the run log and on the data-quality page.

Every stage writes a row to ``meta_pipeline_runs`` (status, timing, rows,
message) and every run one row to ``meta_run_summary``, which feeds the
run-monitoring page of the report. In Fabric, the same function is called from a notebook that a Data
Pipeline schedules. Locally:

    python -m mfgops.orchestrate                       # full load from data/raw
    python -m mfgops.orchestrate --mode incremental \
        --raw data/increments/2026-11-15               # change extract
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
from mfgops.pipeline import SOURCE_TABLES, new_run_id, run_bronze, run_gold, run_silver, write_watermarks
from mfgops.reconcile import reconcile

RETRYABLE = (OSError, ConnectionError, TimeoutError)

LOG_SCHEMA = (
    "run_id string, stage string, status string, attempt int, started_at timestamp, "
    "finished_at timestamp, seconds double, rows long, message string"
)
COUNTS_SCHEMA = "run_id string, table_name string, row_count long, recorded_at timestamp"
SUMMARY_SCHEMA = (
    "run_id string, load_mode string, status string, failed_stage string, message string, "
    "started_at timestamp, finished_at timestamp, seconds double, rows_received long, rows_quarantined long, "
    "max_error_rate double, rows_inserted long, rows_updated long"
)


class QualityGateError(RuntimeError):
    pass


class ReconciliationError(RuntimeError):
    pass


class RowCountError(RuntimeError):
    pass


@dataclass
class RunResult:
    run_id: str
    mode: str = "full"
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


def run_pipeline(
    lh: Lakehouse,
    max_error_rate: float = 0.02,
    retries: int = 2,
    backoff_s: float = 2.0,
    mode: str = "full",
) -> RunResult:
    """Run every stage in order. ``mode`` is ``full`` or ``incremental`` (see ``pipeline``)."""
    if mode not in ("full", "incremental"):
        raise ValueError("mode must be 'full' or 'incremental'")
    incremental = mode == "incremental"
    res = RunResult(run_id=new_run_id(), mode=mode)
    ctx: dict = {"run_started": _now(), "t0": time.perf_counter()}

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
        ctx["counts"] = validate_sources(lh, incremental=incremental)
        present = sum(1 for n in ctx["counts"].values() if n)
        return sum(ctx["counts"].values()), f"{present} extracts match their contracts ({mode} load)"

    def s_monitor():
        if incremental:
            return 0, "incremental load: volumes vary by design, row-count comparison applies to full loads"
        warnings = monitor_row_counts(_previous_counts(lh), ctx["counts"])
        return len(warnings), ("; ".join(warnings) or "row counts in line with the last successful run")

    def s_bronze():
        counts = run_bronze(lh, res.run_id, incremental=incremental)
        return sum(counts.values()), "raw extracts appended to history" if incremental else "raw extracts landed"

    def s_profile():
        tables = [t for t in SOURCE_TABLES if ctx["counts"].get(t)]
        frames = [
            profile(lh, lh.read("bronze", t).filter(f"_batch_id = '{res.run_id}'"), t, res.run_id) for t in tables
        ]
        out = frames[0]
        for f in frames[1:]:
            out = out.unionByName(f)
        lh.write(out, "meta", "column_profile")
        return out.count(), "column profile written"

    def s_silver():
        ctx["silver"] = run_silver(lh, res.run_id, incremental=incremental)
        q = sum(v["quarantined"] for v in ctx["silver"].values())
        if incremental:
            ins = sum(v.get("inserted", 0) for v in ctx["silver"].values())
            upd = sum(v.get("updated", 0) for v in ctx["silver"].values())
            return q, f"{q} rows quarantined, merged into silver: {ins} inserted, {upd} updated"
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
        _summary(lh, res, ctx)
        return res

    now = _now()
    if not incremental:
        lh.append(
            lh.spark.createDataFrame([(res.run_id, t, n, now) for t, n in ctx["counts"].items()], COUNTS_SCHEMA),
            "meta",
            "source_counts",
        )
    # Watermarks move only when the whole run succeeded, so a failed run is simply repeated.
    write_watermarks(lh, {t: v.get("watermark") for t, v in ctx.get("silver", {}).items()}, res.run_id)
    res.status = "success"
    _summary(lh, res, ctx)
    return res


def _summary(lh: Lakehouse, res: RunResult, ctx: dict) -> None:
    """One row per run in ``meta_run_summary``: the source of the run-monitoring report page."""
    silver = ctx.get("silver", {})
    row = (
        res.run_id,
        res.mode,
        res.status,
        res.failed_stage,
        res.message[:1000],
        ctx["run_started"],
        _now(),
        round(time.perf_counter() - ctx["t0"], 2),
        int(sum(ctx.get("counts", {}).values())),
        int(sum(v["quarantined"] for v in silver.values())),
        float(max([v["error_rate"] for v in silver.values()] or [0.0])),
        int(sum(v.get("inserted", 0) for v in silver.values())),
        int(sum(v.get("updated", 0) for v in silver.values())),
    )
    lh.append(lh.spark.createDataFrame([row], SUMMARY_SCHEMA), "meta", "run_summary")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--raw", default="data/raw")
    p.add_argument("--lakehouse", default="lakehouse")
    p.add_argument("--max-error-rate", type=float, default=0.02)
    p.add_argument("--mode", choices=["full", "incremental"], default="full")
    a = p.parse_args()
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    lh = Lakehouse(spark, root=a.lakehouse, raw_path=a.raw)
    res = run_pipeline(lh, a.max_error_rate, mode=a.mode)
    print(
        json.dumps(
            {
                "run_id": res.run_id,
                "mode": res.mode,
                "status": res.status,
                "failed_stage": res.failed_stage,
                "message": res.message,
            },
            indent=1,
        )
    )
    spark.stop()
    raise SystemExit(0 if res.status == "success" else 1)


if __name__ == "__main__":
    main()
