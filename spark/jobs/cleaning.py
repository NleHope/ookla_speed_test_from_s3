"""
Spark Transform Job: Bronze to Silver Layer
============================================
Transform Ookla speed test data from Bronze bucket to Silver bucket
with data quality checks, cleaning, and enrichment.

ETL Pipeline:
1. Read from Bronze (raw parquet files)
2. Data Quality Checks & Cleaning
3. Enrich with geo-coordinates (quadkey -> lat/lon)
4. Enrich with city information (offline reverse geocoding)
5. Write to Silver bucket (cleaned, validated, enriched data)

Author: Generated from EDA notebook
Date: 2025-12-15
"""
DEMO_MODE = True   # True = only 2019-q1, False = full behavior
DEMO_SUBFOLDER = "2019-q1"

from urllib.parse import urljoin

import sys
import os
from pathlib import Path
from datetime import datetime
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, when, isnan, isnull, trim, length,
    regexp_replace, lit, current_timestamp, to_timestamp,
    pandas_udf, PandasUDFType
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
    IntegerType, TimestampType
)
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def resolve_bronze_path(bronze_path: str) -> str:
    """
    If DEMO_MODE is enabled, force Spark to read only one subfolder (2019-q1).
    Otherwise, keep the original path unchanged.
    """
    if not DEMO_MODE:
        return bronze_path.rstrip("/") + "/"

    # Ensure exactly one demo folder is scanned
    base = bronze_path.rstrip("/") + "/"
    return urljoin(base, DEMO_SUBFOLDER + "/")

class OoklaTransformJob:
    """
    Spark ETL Job to transform Ookla speed test data from Bronze to Silver layer
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.job_start_time = datetime.now()
        logger.info(f"OoklaTransformJob initialized at {self.job_start_time}")
    
    def quadkey_to_latlon_udf(self):
        """
        
        Create UDF to convert Bing Maps quadkey to latitude/longitude
        """
        @udf(returnType=StructType([
            StructField("lat", DoubleType(), False),
            StructField("lon", DoubleType(), False)
        ]))
        def convert_quadkey(quadkey):
            """Convert quadkey to lat/lon coordinates (tile center)"""
            if not quadkey or quadkey == "":
                return (0.0, 0.0)
            
            lat_min, lat_max = -85.05112878, 85.05112878
            lon_min, lon_max = -180.0, 180.0
            
            try:
                for digit in str(quadkey):
                    lat_mid = (lat_min + lat_max) / 2
                    lon_mid = (lon_min + lon_max) / 2
                    
                    if digit == '0':  # Top-left
                        lat_min = lat_mid
                        lon_max = lon_mid
                    elif digit == '1':  # Top-right
                        lat_min = lat_mid
                        lon_min = lon_mid
                    elif digit == '2':  # Bottom-left
                        lat_max = lat_mid
                        lon_max = lon_mid
                    elif digit == '3':  # Bottom-right
                        lat_max = lat_mid
                        lon_min = lon_mid
                    else:
                        # Invalid digit, return default
                        return (0.0, 0.0)
                
                lat = (lat_min + lat_max) / 2
                lon = (lon_min + lon_max) / 2
                return (float(lat), float(lon))
            except Exception as e:
                logger.warning(f"Error converting quadkey {quadkey}: {e}")
                return (0.0, 0.0)
        
        return convert_quadkey
    
    def create_city_lookup_udf(self):
        """
        Create Pandas UDF for offline reverse geocoding (lat/lon -> city)
        Uses reverse_geocoder library for fast, offline geocoding
        """
        try:
            import reverse_geocoder as rg
            
            @pandas_udf(StringType())
            def get_city(lat: pd.Series, lon: pd.Series) -> pd.Series:
                """Batch reverse geocoding using offline database"""
                try:
                    # Combine lat/lon into list of tuples
                    coords = list(zip(lat, lon))
                    
                    # Filter out invalid coordinates (0,0)
                    valid_coords = [(la, lo) if (la != 0.0 and lo != 0.0) else (1.0, 1.0) 
                                   for la, lo in coords]
                    
                    # Batch reverse geocoding
                    results = rg.search(valid_coords)
                    
                    # Format: "City, Region, Country"
                    cities = []
                    for i, r in enumerate(results):
                        if coords[i] == (0.0, 0.0):
                            cities.append("Unknown")
                        else:
                            city_name = r.get('name', 'Unknown')
                            admin1 = r.get('admin1', '')
                            country = r.get('cc', '')
                            cities.append(f"{city_name}, {admin1}, {country}")
                    
                    return pd.Series(cities)
                except Exception as e:
                    logger.error(f"Error in reverse geocoding: {e}")
                    return pd.Series(["Unknown"] * len(lat))
            
            return get_city
        except ImportError:
            logger.warning("reverse_geocoder not available, city lookup will be skipped")
            return None
    
    def validate_schema(self, df):
        """
        Validate that the input DataFrame has the expected schema
        """
        expected_columns = [
            'quadkey', 'tile', 'avg_d_kbps', 'avg_u_kbps', 
            'avg_lat_ms', 'tests', 'devices'
        ]
        
        actual_columns = df.columns
        missing_columns = set(expected_columns) - set(actual_columns)
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        logger.info(f"Schema validation passed. Columns: {actual_columns}")
        return True
    
    def clean_data(self, df):
        """
        Apply data cleaning procedures:
        1. Remove null/empty quadkeys
        2. Remove negative or zero values for speed metrics
        3. Handle outliers (optional)
        4. Trim string fields
        5. Add data quality flags
        """
        logger.info("Starting data cleaning...")
        initial_count = df.count()
        
        # Add processing metadata
        df = df.withColumn("processed_at", current_timestamp())
        df = df.withColumn("data_quality_score", lit(100))
        
        # 1. Clean quadkey (remove null, empty, or invalid)
        df = df.withColumn("quadkey", trim(col("quadkey")))
        df = df.filter(
            (col("quadkey").isNotNull()) & 
            (length(col("quadkey")) > 0) &
            (length(col("quadkey")) <= 30)  # Reasonable quadkey length
        )
        
        # Reduce quality score for suspicious quadkeys
        df = df.withColumn(
            "data_quality_score",
            when(length(col("quadkey")) < 10, col("data_quality_score") - 10)
            .otherwise(col("data_quality_score"))
        )
        
        # 2. Clean tile field
        df = df.withColumn("tile", trim(col("tile")))
        
        # 3. Validate and clean speed metrics (must be positive)
        df = df.filter(
            (col("avg_d_kbps").isNotNull()) & 
            (col("avg_d_kbps") > 0) &
            (col("avg_u_kbps").isNotNull()) & 
            (col("avg_u_kbps") > 0)
        )
        
        # Handle outliers - flag extremely high speeds (> 1Gbps = 1,000,000 kbps)
        df = df.withColumn(
            "is_outlier_download",
            when(col("avg_d_kbps") > 1000000, True).otherwise(False)
        )
        df = df.withColumn(
            "is_outlier_upload",
            when(col("avg_u_kbps") > 500000, True).otherwise(False)
        )
        
        # Reduce quality score for outliers
        df = df.withColumn(
            "data_quality_score",
            when(col("is_outlier_download") | col("is_outlier_upload"), 
                 col("data_quality_score") - 20)
            .otherwise(col("data_quality_score"))
        )
        
        # 4. Validate latency (must be non-negative and reasonable)
        df = df.filter(
            (col("avg_lat_ms").isNotNull()) & 
            (col("avg_lat_ms") >= 0)
        )
        
        # Flag high latency (> 1000ms)
        df = df.withColumn(
            "is_high_latency",
            when(col("avg_lat_ms") > 1000, True).otherwise(False)
        )
        
        # 5. Validate test counts (must be positive)
        df = df.filter(
            (col("tests").isNotNull()) & 
            (col("tests") > 0) &
            (col("devices").isNotNull()) & 
            (col("devices") > 0)
        )
        
        # 6. Add derived metrics
        df = df.withColumn(
            "download_mbps", 
            (col("avg_d_kbps") / 1024.0).cast(DoubleType())
        )
        df = df.withColumn(
            "upload_mbps", 
            (col("avg_u_kbps") / 1024.0).cast(DoubleType())
        )
        df = df.withColumn(
            "tests_per_device",
            (col("tests") / col("devices")).cast(DoubleType())
        )
        
        cleaned_count = df.count()
        removed_count = initial_count - cleaned_count
        removal_percentage = (removed_count / initial_count * 100) if initial_count > 0 else 0
        
        logger.info(f"Data cleaning completed:")
        logger.info(f"  - Initial records: {initial_count:,}")
        logger.info(f"  - Cleaned records: {cleaned_count:,}")
        logger.info(f"  - Removed records: {removed_count:,} ({removal_percentage:.2f}%)")
        
        return df
    
    def enrich_with_coordinates(self, df):
        """
        Enrich data with latitude and longitude from quadkey
        """
        logger.info("Enriching data with geo-coordinates...")
        
        quadkey_udf = self.quadkey_to_latlon_udf()
        
        # Add coordinates
        df = df.withColumn("coords", quadkey_udf(col("quadkey")))
        df = df.withColumn("latitude", col("coords.lat"))
        df = df.withColumn("longitude", col("coords.lon"))
        df = df.drop("coords")
        
        # Validate coordinates
        df = df.filter(
            (col("latitude") != 0.0) | (col("longitude") != 0.0)
        )
        
        logger.info("Geo-coordinates enrichment completed")
        return df
    
    def enrich_with_city(self, df):
        """
        Enrich data with city information using offline reverse geocoding
        """
        logger.info("Enriching data with city information...")
        
        city_udf = self.create_city_lookup_udf()
        
        if city_udf is None:
            logger.warning("Skipping city enrichment - reverse_geocoder not available")
            df = df.withColumn("city", lit("Unknown"))
            df = df.withColumn("city_lookup_success", lit(False))
            return df
        
        # Add city information
        df = df.withColumn("city", city_udf(col("latitude"), col("longitude")))
        df = df.withColumn(
            "city_lookup_success", 
            when(col("city") != "Unknown", True).otherwise(False)
        )
        
        logger.info("City enrichment completed")
        return df
    
    def add_partitioning_columns(self, df):
        """
        Add columns for partitioning the output data
        Extract date from tile or use processing date
        """
        logger.info("Adding partitioning columns...")
        
        # Try to extract year-month from tile field (format: year-month-day_...)
        df = df.withColumn(
            "partition_year",
            when(length(col("tile")) > 4, col("tile").substr(1, 4))
            .otherwise(lit("2019"))
        )
        df = df.withColumn(
            "partition_month",
            when(length(col("tile")) > 7, col("tile").substr(6, 2))
            .otherwise(lit("01"))
        )
        
        return df
    
    def run_transform(self, bronze_path: str, silver_path: str):
        """
        Main transformation pipeline: Bronze -> Silver
        
        Args:
            bronze_path: S3 path to bronze data (input)
            silver_path: S3 path to silver data (output)
        """
        logger.info("="*80)
        logger.info("Starting Bronze to Silver Transformation")
        logger.info("="*80)
        logger.info(f"Input (Bronze): {bronze_path}")
        logger.info(f"Output (Silver): {silver_path}")
        
        try:
            # 1. Read from Bronze
            logger.info("Step 1: Reading data from Bronze layer...")
            df_bronze = (
                self.spark.read
                    .format("parquet")
                    .load(f"s3a://bronze/pump/2019-q1/*.parquet")
            )
            if df_bronze.rdd.isEmpty():
                raise RuntimeError(f"No parquet files found under {bronze_path}")

            logger.info(f"Bronze records loaded: {df_bronze.count():,}")
            df_bronze.printSchema()
            
            # 2. Validate schema
            logger.info("Step 2: Validating schema...")
            self.validate_schema(df_bronze)
            
            # 3. Clean data
            logger.info("Step 3: Cleaning data...")
            df_cleaned = self.clean_data(df_bronze)
            
            # 4. Enrich with coordinates
            logger.info("Step 4: Enriching with geo-coordinates...")
            df_with_coords = self.enrich_with_coordinates(df_cleaned)
            
            # 5. Enrich with city
            logger.info("Step 5: Enriching with city information...")
            df_enriched = self.enrich_with_city(df_with_coords)
            
            # 6. Add partitioning columns
            logger.info("Step 6: Adding partitioning columns...")
            df_final = self.add_partitioning_columns(df_enriched)
            
            # 7. Write to Silver
            logger.info("Step 7: Writing to Silver layer...")
            final_count = df_final.count()
            logger.info(f"Final records to write: {final_count:,}")
            
            # Show sample of transformed data
            logger.info("Sample of transformed data:")
            df_final.select(
                "quadkey", "latitude", "longitude", "city", 
                "download_mbps", "upload_mbps", "avg_lat_ms",
                "data_quality_score"
            ).show(10, truncate=False)
            
            # Write with partitioning
            df_final.write \
                .mode("overwrite") \
                .partitionBy("partition_year", "partition_month") \
                .parquet(silver_path)
            
            logger.info("✅ Data successfully written to Silver layer!")
            
            # 8. Calculate and log statistics
            self.log_statistics(df_bronze, df_final)
            
            logger.info("="*80)
            logger.info("Transformation Completed Successfully!")
            logger.info("="*80)
            
        except Exception as e:
            logger.error(f"❌ Error during transformation: {e}", exc_info=True)
            raise
    
    def log_statistics(self, df_bronze, df_silver):
        """
        Calculate and log transformation statistics
        """
        logger.info("\n" + "="*80)
        logger.info("TRANSFORMATION STATISTICS")
        logger.info("="*80)
        
        bronze_count = df_bronze.count()
        silver_count = df_silver.count()
        
        logger.info(f"Bronze records: {bronze_count:,}")
        logger.info(f"Silver records: {silver_count:,}")
        logger.info(f"Records processed: {silver_count:,}")
        logger.info(f"Records filtered: {bronze_count - silver_count:,}")
        logger.info(f"Data retention rate: {(silver_count/bronze_count*100):.2f}%")
        
        # Quality statistics
        avg_quality = df_silver.agg({"data_quality_score": "avg"}).collect()[0][0]
        logger.info(f"Average data quality score: {avg_quality:.2f}")
        
        # City lookup success rate
        city_success = df_silver.filter(col("city_lookup_success") == True).count()
        city_success_rate = (city_success / silver_count * 100) if silver_count > 0 else 0
        logger.info(f"City lookup success rate: {city_success_rate:.2f}%")
        
        # Speed statistics
        speed_stats = df_silver.agg(
            {"download_mbps": "avg", "upload_mbps": "avg", "avg_lat_ms": "avg"}
        ).collect()[0]
        
        logger.info(f"Average download speed: {speed_stats[0]:.2f} Mbps")
        logger.info(f"Average upload speed: {speed_stats[1]:.2f} Mbps")
        logger.info(f"Average latency: {speed_stats[2]:.2f} ms")
        
        job_duration = (datetime.now() - self.job_start_time).total_seconds()
        logger.info(f"Job duration: {job_duration:.2f} seconds")
        logger.info("="*80 + "\n")


def create_spark_session():
    """
    Create and configure Spark session for K8s execution
    """
    spark = SparkSession.builder \
        .appName("Ookla-Transform-Bronze-to-Silver") \
        .config("spark.jars.packages", ",".join([
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ])) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("INFO")
    
    return spark


def main():
    """
    Main entry point for the Spark job
    """
    # Parse arguments
    if len(sys.argv) < 3:
        print("Usage: cleaning.py <bronze_s3_path> <silver_s3_path>")
        print("Example: cleaning.py s3a://bronze/pump/2019-q1/ s3a://silver/pump/2019-q1/")
        sys.exit(1)
    
    bronze_path = sys.argv[1]
    silver_path = sys.argv[2]
    
    bronze_path = resolve_bronze_path(bronze_path)
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Run transformation
        job = OoklaTransformJob(spark)
        job.run_transform(bronze_path, silver_path)
        
        logger.info("🎉 Job completed successfully!")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"💥 Job failed with error: {e}", exc_info=True)
        sys.exit(1)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
