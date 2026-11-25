"""Source contracts, schema validation and profiling, run before anything is loaded.

A source extract is only loaded if it matches its contract: the file exists,
it has exactly the expected columns (the D365 entity fields from
``source_map`` plus ``ModifiedDateTime``), and it is not empty. A broken extract
fails the load with a clear message instead of silently producing nulls
further down the pipeline.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from mfgops.lakehouse import Lakehouse
from mfgops.source_map import ENTITIES

# Column contract per source extract: the D365-shaped columns plus the change-tracking timestamp.
CONTRACTS: dict[str, list[str]] = {name: e.source_columns for name, e in ENTITIES.items()}


class SourceValidationError(RuntimeError):
    """Raised when one or more source extracts break their contract."""

    def __init__(self, problems: list[str]):
        self.problems = problems
        super().__init__("Source validation failed:\n  - " + "\n  - ".join(problems))


def validate_sources(lh: Lakehouse, incremental: bool = False) -> dict[str, int]:
    """Check every extract against its contract. Return row counts, or raise with every problem found.

    A full load needs every extract and none may be empty. An incremental load
    only receives the entities that changed, so a missing or empty extract
    means "no changes" there, but an extract that is present must still match
    its contract.
    """
    problems, counts = [], {}
    for name, expected in CONTRACTS.items():
        file = ENTITIES[name].file
        try:
            df = lh.read_raw(file)
            df.columns  # noqa: B018  (forces the read)
        except Exception as exc:  # missing file, unreadable file
            if incremental:
                counts[name] = 0
                continue
            problems.append(f"{file}: cannot read source file ({type(exc).__name__})")
            continue
        missing = [c for c in expected if c not in df.columns]
        extra = [c for c in df.columns if c not in expected]
        if missing:
            problems.append(f"{file}: missing columns {missing}")
        if extra:
            problems.append(f"{file}: unexpected columns {extra}")
        n = df.count()
        if n == 0 and not incremental:
            problems.append(f"{file}: file is empty")
        counts[name] = n
    if problems:
        raise SourceValidationError(problems)
    return counts


PROFILE_SCHEMA = (
    "run_id string, profiled_at timestamp, table_name string, column_name string, "
    "row_count long, null_count long, null_pct double, distinct_count long, min_value string, max_value string"
)


def profile(lh: Lakehouse, df: DataFrame, table: str, run_id: str) -> DataFrame:
    """One row per column: nulls, distinct values, min and max (as text). Feeds the data dictionary."""
    cols = [c for c in df.columns if not c.startswith("_")]
    aggs = []
    for c in cols:
        aggs += [
            F.sum(F.col(c).isNull().cast("int")).alias(f"{c}__nulls"),
            F.approx_count_distinct(c).alias(f"{c}__distinct"),
            F.min(c).cast("string").alias(f"{c}__min"),
            F.max(c).cast("string").alias(f"{c}__max"),
        ]
    stats = df.agg(F.count(F.lit(1)).alias("__rows"), *aggs).first()
    n = stats["__rows"]
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [
        (
            run_id,
            now,
            table,
            c,
            n,
            stats[f"{c}__nulls"],
            round(stats[f"{c}__nulls"] / n * 100, 2) if n else 0.0,
            stats[f"{c}__distinct"],
            stats[f"{c}__min"],
            stats[f"{c}__max"],
        )
        for c in cols
    ]
    return lh.spark.createDataFrame(rows, PROFILE_SCHEMA)
