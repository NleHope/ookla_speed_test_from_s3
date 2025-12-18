# Ookla Transform Job: Bronze → Silver

## Overview

This Spark job transforms Ookla speed test data from the Bronze layer (raw data) to the Silver layer (cleaned, validated, and enriched data) as part of the ETL pipeline.

## Architecture

```
Bronze Bucket (Raw Data)
    ↓
Transform Job (Spark on K8s)
    ├── Data Quality Checks
    ├── Data Cleaning
    ├── Geo-enrichment (Quadkey → Lat/Lon)
    ├── City Enrichment (Reverse Geocoding)
    └── Partitioning
    ↓
Silver Bucket (Cleaned & Enriched Data)
```

## Features

### 1. Data Quality & Cleaning
- **Null/Empty validation**: Remove records with missing quadkeys
- **Range validation**: Filter invalid speed metrics (negative or zero)
- **Outlier detection**: Flag extremely high speeds (>1 Gbps)
- **Latency validation**: Validate reasonable latency values
- **Quality scoring**: Assign data quality scores (0-100)

### 2. Data Enrichment
- **Geo-coordinates**: Convert Bing Maps quadkey to latitude/longitude
- **City lookup**: Offline reverse geocoding (lat/lon → city name)
- **Derived metrics**: Calculate Mbps, tests per device

### 3. Data Processing
- **Partitioning**: Partition by year and month for efficient querying
- **Metadata**: Add processing timestamp and data lineage
- **Statistics**: Log comprehensive transformation statistics

## Schema

### Bronze Schema (Input)
```
quadkey: string          # Bing Maps quadkey
tile: string             # Tile identifier (YYYY-MM-DD_type)
avg_d_kbps: long        # Average download speed (kbps)
avg_u_kbps: long        # Average upload speed (kbps)
avg_lat_ms: long        # Average latency (ms)
tests: long             # Number of tests
devices: long           # Number of devices
```

### Silver Schema (Output)
```
# Original fields
quadkey: string
tile: string
avg_d_kbps: long
avg_u_kbps: long
avg_lat_ms: long
tests: long
devices: long

# Enriched fields
latitude: double              # Latitude coordinate
longitude: double             # Longitude coordinate
city: string                  # City, Region, Country
download_mbps: double         # Download speed in Mbps
upload_mbps: double           # Upload speed in Mbps
tests_per_device: double      # Tests per device ratio

# Quality & metadata fields
data_quality_score: int       # Quality score (0-100)
is_outlier_download: boolean  # Download outlier flag
is_outlier_upload: boolean    # Upload outlier flag
is_high_latency: boolean      # High latency flag
city_lookup_success: boolean  # City lookup success
processed_at: timestamp       # Processing timestamp
partition_year: string        # Partition key: year
partition_month: string       # Partition key: month
```

## Data Cleaning Rules

### 1. Quadkey Validation
- Must not be null or empty
- Length must be between 1-30 characters
- Quality score reduced by 10 for suspicious quadkeys (<10 chars)

### 2. Speed Metrics Validation
- Download speed must be > 0 kbps
- Upload speed must be > 0 kbps
- Outlier flagging:
  - Download > 1,000,000 kbps (1 Gbps)
  - Upload > 500,000 kbps (500 Mbps)
- Quality score reduced by 20 for outliers

### 3. Latency Validation
- Must be non-negative (≥ 0)
- High latency flagged if > 1000ms

### 4. Test Count Validation
- Tests must be > 0
- Devices must be > 0

## Deployment

### Prerequisites
1. Kubernetes cluster with Spark Operator installed
2. MinIO or S3 compatible storage
3. Docker image with required Python packages:
   - pyspark==3.5.1
   - pandas>=2.3.3
   - pyarrow>=4.0.0
   - reverse-geocoder>=1.5.1

### Build Docker Image
```bash
# Build image with dependencies
docker build -f Dockerfile.spark -t spark-s3:latest .

# Tag for minikube
minikube image load spark-s3:latest
```

### Create Silver Bucket
```bash
# Create silver bucket in MinIO
mc mb minio/silver
```

### Deploy Job
```bash
# Apply the Spark job
kubectl apply -f k8s/spark-transform-job.yaml

# Check job status
kubectl get sparkapplications

# View logs
kubectl logs -f ookla-transform-job-driver
```

### Monitor Job
```bash
# Check job progress
kubectl describe sparkapplication ookla-transform-job

# View executor logs
kubectl logs -l spark-role=executor

# Access Spark UI (if enabled)
kubectl port-forward <driver-pod> 4040:4040
```

## Usage

### Command Line
```bash
# Run locally
spark-submit \
  --master local[*] \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  spark/jobs/transform.py \
  s3a://bronze/pump/2019-q1/ \
  s3a://silver/pump/2019-q1/

# Run on K8s (via kubectl)
kubectl apply -f k8s/spark-transform-job.yaml
```

### Python API
```python
from pyspark.sql import SparkSession
from spark.jobs.transform import OoklaTransformJob

# Create Spark session
spark = SparkSession.builder \
    .appName("Ookla-Transform") \
    .getOrCreate()

# Run transformation
job = OoklaTransformJob(spark)
job.run_transform(
    bronze_path="s3a://bronze/pump/2019-q1/",
    silver_path="s3a://silver/pump/2019-q1/"
)
```

## Performance Tuning

### Executor Configuration
```yaml
spark.executor.instances: 4
spark.executor.cores: 2
spark.executor.memory: 2g
spark.driver.memory: 2g
```

### S3A Optimization
```yaml
spark.hadoop.fs.s3a.multipart.size: 104857600  # 100MB
spark.hadoop.fs.s3a.fast.upload: true
spark.hadoop.fs.s3a.connection.maximum: 200
spark.hadoop.fs.s3a.threads.max: 100
```

### Adaptive Query Execution
```yaml
spark.sql.adaptive.enabled: true
spark.sql.adaptive.coalescePartitions.enabled: true
spark.sql.adaptive.skewJoin.enabled: true
```

## Output Statistics

The job logs comprehensive statistics:

```
================================================================================
TRANSFORMATION STATISTICS
================================================================================
Bronze records: 3,231,245
Silver records: 3,195,478
Records processed: 3,195,478
Records filtered: 35,767
Data retention rate: 98.89%
Average data quality score: 92.45
City lookup success rate: 99.87%
Average download speed: 28.45 Mbps
Average upload speed: 12.67 Mbps
Average latency: 45.32 ms
Job duration: 245.67 seconds
================================================================================
```

## Troubleshooting

### Common Issues

1. **Memory Issues**
   - Increase executor memory in `spark-transform-job.yaml`
   - Adjust `spark.memory.fraction` and `spark.memory.storageFraction`

2. **S3A Connection Errors**
   - Verify MinIO endpoint is accessible from K8s pods
   - Check access keys in job configuration
   - Ensure `host.minikube.internal` resolves correctly

3. **City Lookup Failures**
   - Ensure `reverse-geocoder` package is installed
   - Check if reverse-geocoder data files are accessible
   - Job will continue with "Unknown" for failed lookups

4. **Job Failures**
   - Check driver logs: `kubectl logs ookla-transform-job-driver`
   - Check executor logs: `kubectl logs -l spark-role=executor`
   - Review restart policy and retry configuration

## Testing

### Unit Tests
```bash
# Run tests
pytest tests/test_transform.py -v
```

### Integration Tests
```bash
# Test with sample data
spark-submit spark/jobs/transform.py \
  s3a://bronze/pump/2019-q1/2019-01-01_performance_mobile_tiles.parquet \
  s3a://silver/pump/test-output/
```

## Next Steps

1. **Gold Layer**: Create aggregations and analytics tables
2. **Data Quality Dashboard**: Visualize quality scores and statistics
3. **Incremental Processing**: Add support for incremental updates
4. **Data Catalog**: Register tables in Hive Metastore or Glue
5. **Monitoring**: Set up Prometheus metrics and Grafana dashboards

## References

- [Apache Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
- [Spark Operator](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator)
- [Bing Maps Tile System](https://docs.microsoft.com/en-us/bingmaps/articles/bing-maps-tile-system)
- [Reverse Geocoder](https://github.com/thampiman/reverse-geocoder)
