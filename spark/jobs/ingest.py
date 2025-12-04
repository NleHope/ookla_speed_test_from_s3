#!/usr/bin/env python3
"""
Ingest Ookla Open Data from public S3 (anonymous) → MinIO (credentialed)
Steps:
1. Read from AWS S3 public
2. Reconfigure Spark Hadoop settings to MinIO
3. Write to MinIO
"""

from pyspark.sql import SparkSession
import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://host.minikube.internal:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")

SOURCE_PATH = "s3a://ookla-open-data/parquet/performance/type=mobile/year=2024/quarter=1/"
TARGET_PATH = "s3a://minio-bucket/raw/ookla-mobile-2024q1/"

def create_spark():
    return (
        SparkSession.builder
        .appName("OoklaToMinIO")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .getOrCreate()
    )

def reconfigure_to_minio(spark):
    hconf = spark._jsc.hadoopConfiguration()

    # Remove AWS anonymous provider
    hconf.unset("fs.s3a.aws.credentials.provider")
    hconf.unset("spark.hadoop.fs.s3a.aws.credentials.provider")

    # MinIO endpoint + credentials
    pairs = {
        "fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,

        "fs.s3a.access.key": MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,

        "fs.s3a.secret.key": MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,

        "fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.path.style.access": "true",

        "fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",

        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    }

    for k, v in pairs.items():
        hconf.set(k, v)

    print("✓ Spark Hadoop reconfigured to MinIO")

def main():
    spark = create_spark()
    
    print("[1] Reading Ookla public S3...")
    df = spark.read.parquet(SOURCE_PATH)
    print(f"Rows: {df.count():,}")

    print("[2] Switching to MinIO credentials...")
    reconfigure_to_minio(spark)

    print("[3] Writing to MinIO...")
    df.write.mode("overwrite").parquet(TARGET_PATH)
    print("Done.")

    spark.stop()

if __name__ == "__main__":
    main()
