"""Read and write lakehouse tables the same way locally and in Microsoft Fabric.

In Fabric, tables are Delta tables in the attached lakehouse (``saveAsTable``)
and raw files sit under ``Files/raw``. Locally, each table is a Parquet folder
under ``lakehouse/<layer>/<table>``. The transformation code never needs to
know which one it is running on.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def in_fabric() -> bool:
    try:
        import notebookutils  # noqa: F401  (only exists inside Fabric)

        return True
    except ImportError:
        return False


def get_spark(app_name: str = "mfgops") -> SparkSession:
    """Return the active session (Fabric) or build a small local one."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


@dataclass
class Lakehouse:
    spark: SparkSession
    root: str = "lakehouse"
    raw_path: str = "data/raw"
    fabric: bool = False

    @classmethod
    def detect(cls, spark: SparkSession, root: str = "lakehouse", raw_path: str = "data/raw") -> Lakehouse:
        if in_fabric():
            return cls(spark, root="", raw_path="Files/raw", fabric=True)
        return cls(spark, root=root, raw_path=raw_path, fabric=False)

    def read_raw(self, name: str) -> DataFrame:
        """Read ``<raw_path>/<name>.csv`` as text columns."""
        return (
            self.spark.read.option("header", True)
            .option("inferSchema", False)  # bronze keeps everything as text, typing happens in silver
            .csv(f"{self.raw_path}/{name}.csv")
        )

    def _table(self, layer: str, name: str) -> str:
        return f"{layer}_{name}"

    def write(self, df: DataFrame, layer: str, name: str) -> None:
        if self.fabric:
            df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(
                self._table(layer, name)
            )
        else:
            df.write.mode("overwrite").parquet(str(Path(self.root) / layer / name))

    def append(self, df: DataFrame, layer: str, name: str) -> None:
        if self.fabric:
            df.write.mode("append").format("delta").saveAsTable(self._table(layer, name))
        else:
            df.write.mode("append").parquet(str(Path(self.root) / layer / name))

    def read(self, layer: str, name: str) -> DataFrame:
        if self.fabric:
            return self.spark.read.table(self._table(layer, name))
        return self.spark.read.parquet(str(Path(self.root) / layer / name))

    def exists(self, layer: str, name: str) -> bool:
        if self.fabric:
            return self.spark.catalog.tableExists(self._table(layer, name))
        return (Path(self.root) / layer / name).exists()

    def merge(self, df: DataFrame, layer: str, name: str, keys: list[str] | tuple[str, ...]) -> dict[str, int]:
        """Upsert ``df`` into a table by business key: matching rows are replaced, new rows inserted.

        In Fabric this is a Delta ``MERGE``. Locally (plain Parquet, no Delta
        runtime) the same result is produced by keeping the existing rows
        whose key is not in ``df`` and adding ``df``. Returns updated and
        inserted row counts.
        """
        df = df.cache()
        incoming = df.count()
        if not self.exists(layer, name):
            self.write(df, layer, name)
            return {"updated": 0, "inserted": incoming}
        current = self.read(layer, name)
        updated = current.join(df.select(*keys).distinct(), list(keys), "left_semi").count()
        if self.fabric:
            from delta.tables import DeltaTable  # available in every Fabric Spark runtime

            cond = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
            (
                DeltaTable.forName(self.spark, self._table(layer, name))
                .alias("t")
                .merge(df.alias("s"), cond)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            path = Path(self.root) / layer / name
            tmp = path.with_name(name + "__merge_tmp")
            kept = current.join(df.select(*keys).distinct(), list(keys), "left_anti")
            kept.unionByName(df, allowMissingColumns=True).write.mode("overwrite").parquet(str(tmp))
            shutil.rmtree(path)
            tmp.rename(path)
        return {"updated": updated, "inserted": incoming - updated}
