#!/usr/bin/env python3
# Copyright (c) Curt Hagenlocher. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.
"""PySpark + delta-spark interop driver for the EngineeredWood Delta test suite.

Invoked as:  python spark_driver.py <command> <json-args-file> [json-result-file]
Writes a single JSON object to the result file (or stdout if omitted), same contract as
delta_rs_driver.py.

The result must go to a FILE, not stdout: Spark's JVM writes "SUCCESS: The process with PID ...
has been terminated." directly to the inherited stdout descriptor on shutdown, which no
Python-level redirection can prevent, and that trailing text corrupts the JSON.

This is tier 3: the reference implementation. It is the only tier that exercises writer
features, DESCRIBE DETAIL, clustering and OPTIMIZE, and the only one that reads
column-mapped tables written with the legacy minReader=2/minWriter=5 numbering.

COST: every invocation starts a JVM and a fresh SparkSession -- roughly 15-20 seconds before
any work happens. Commands are therefore coarse-grained on purpose: each one does a whole
scenario rather than a single operation, so a test needs exactly one session. Do not split a
command into two just to make it read nicer.
"""
import json
import os
import sys
import traceback

# Spark and py4j print freely; keep anything they emit away from the result channel.
_real_stdout = sys.stdout
sys.stdout = sys.stderr


def _spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("ew-interop")
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.databricks.delta.snapshotPartitions", "1")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _uri(path):
    """Spark wants forward slashes even on Windows."""
    return os.path.abspath(path).replace("\\", "/")


def _rows(df):
    """DataFrame -> sorted list of dicts, for order-independent comparison."""
    rows = [r.asDict(recursive=True) for r in df.collect()]
    return sorted(rows, key=lambda r: json.dumps(r, sort_keys=True, default=str))


def _detail(spark, path):
    """DESCRIBE DETAIL as a plain dict -- tier 3's headline capability."""
    row = spark.sql(f"DESCRIBE DETAIL delta.`{_uri(path)}`").collect()[0].asDict(recursive=True)
    return {
        "format": row.get("format"),
        "partition_columns": list(row.get("partitionColumns") or []),
        "clustering_columns": list(row.get("clusteringColumns") or []),
        "num_files": row.get("numFiles"),
        "min_reader_version": row.get("minReaderVersion"),
        "min_writer_version": row.get("minWriterVersion"),
        "table_features": sorted(row.get("tableFeatures") or []),
        "properties": dict(row.get("properties") or {}),
    }


def cmd_probe(args):
    from importlib.metadata import version
    import pyspark
    spark = _spark()
    try:
        # `delta` has no __version__; the version lives in the delta-spark dist metadata.
        return {"spark": spark.version, "delta_spark": version("delta-spark"),
                "java_home": os.environ.get("JAVA_HOME"), "pyspark": pyspark.__version__}
    finally:
        spark.stop()


def cmd_read(args):
    """Read an EW-written table and report what Spark decoded, plus DESCRIBE DETAIL."""
    spark = _spark()
    try:
        df = spark.read.format("delta").load(_uri(args["path"]))
        return {
            "columns": list(df.columns),
            "row_count": df.count(),
            "rows": _rows(df),
            "detail": _detail(spark, args["path"]),
        }
    finally:
        spark.stop()


def cmd_write(args):
    """Write a table WITH Spark, for the reference -> EW direction.

    Optional `sql` is a list of statements run against the table afterwards (DELETE, UPDATE,
    OPTIMIZE, ALTER ...), each with {path} substituted -- this is how the writer-feature and
    deletion-vector scenarios are driven without a second session.
    """
    spark = _spark()
    try:
        path = _uri(args["path"])
        df = spark.createDataFrame(args["rows"], args["schema"])
        writer = df.write.format("delta")
        for k, v in (args.get("options") or {}).items():
            writer = writer.option(k, v)
        if args.get("partition_by"):
            writer = writer.partitionBy(*args["partition_by"])
        if args.get("cluster_by"):
            writer = writer.clusterBy(*args["cluster_by"])
        writer.mode(args.get("mode", "errorifexists")).save(path)

        for stmt in args.get("sql") or []:
            spark.sql(stmt.format(path=path))

        return {"detail": _detail(spark, args["path"]),
                "rows": _rows(spark.read.format("delta").load(path))}
    finally:
        spark.stop()


def cmd_sql(args):
    """Run statements against an existing EW-written table, then report the result.

    The writer-side half of tier 3: Spark MUTATING a table EngineeredWood created is the only
    way to test that EW's output is not merely readable but writable-through.
    """
    spark = _spark()
    try:
        path = _uri(args["path"])
        results = []
        for stmt in args["sql"]:
            rendered = stmt.format(path=path)
            out = spark.sql(rendered)
            results.append({"sql": rendered,
                            "rows": _rows(out) if out.columns else []})
        df = spark.read.format("delta").load(path)
        return {"statements": results, "rows": _rows(df), "row_count": df.count(),
                "detail": _detail(spark, args["path"])}
    finally:
        spark.stop()


COMMANDS = {
    "probe": cmd_probe,
    "read": cmd_read,
    "write": cmd_write,
    "sql": cmd_sql,
}


def _emit(out_path, obj):
    payload = json.dumps(obj, ensure_ascii=False, default=str)
    if out_path:
        with open(out_path, "w", encoding="utf-8") as fh:
            fh.write(payload)
    else:
        _real_stdout.write(payload)


def main():
    out_path = sys.argv[3] if len(sys.argv) > 3 else None
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        _emit(out_path, {"ok": False,
                         "error": f"unknown command; expected one of {sorted(COMMANDS)}"})
        return 0
    if len(sys.argv) > 2:
        with open(sys.argv[2], "r", encoding="utf-8") as fh:
            args = json.load(fh)
    else:
        args = {}
    try:
        result = COMMANDS[sys.argv[1]](args)
        result.setdefault("ok", True)
    except Exception as exc:
        # Spark stacks are enormous and the useful part is the innermost Delta/Java message.
        result = {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:4000],
                  "traceback": traceback.format_exc()[-4000:]}
    _emit(out_path, result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
