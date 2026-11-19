"""Minimal Spark smoke job: count lines in one gzip archive from MinIO."""

import sys

from pyspark.sql import SparkSession


def main():
    path = sys.argv[1]
    spark = SparkSession.builder.appName("gh-smoke").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    count = spark.read.text(path).count()
    print(f"SMOKE_EVENTS={count}", flush=True)
    spark.stop()
    if count <= 0:
        raise SystemExit(f"No events found in {path}")


if __name__ == "__main__":
    main()
