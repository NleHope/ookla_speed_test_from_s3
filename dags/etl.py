"""
Ookla Speed Test ETL Pipeline with Spark on Minikube
=====================================================

This DAG orchestrates the following workflow:
1. Build Spark Docker image with S3 dependencies
2. Load image to Minikube's Docker registry
3. Apply Kubernetes RBAC for Spark
4. Submit Spark job to Minikube cluster
5. Monitor job execution
6. Cleanup resources

Architecture:
- Airflow: Running in container1 (airflow-scheduler/worker)
- MinIO (Data Lake): Running in container2 (accessible via host network)
- Minikube: K8s cluster on host machine
- Spark: Submitted as K8s job to Minikube from Airflow container

Requirements:
- Minikube running on host
- kubectl configured and mounted to Airflow container
- Docker socket mounted to Airflow container
- Network connectivity: Airflow -> Host -> Minikube
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.bash import BashSensor
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG Configuration
dag = DAG(
    'ookla_speed_test_etl',
    default_args=default_args,
    description='ETL pipeline to process Ookla speed test data using Spark on Minikube',
    schedule_interval='@daily',
    catchup=False,
    tags=['spark', 'kubernetes', 'etl', 'ookla'],
)

# Task 1: Build Spark Docker Image
build_spark_image = BashOperator(
    task_id='build_spark_docker_image',
    bash_command="""
    set -e
    echo "Building Spark Docker image with S3 dependencies..."
    cd /opt/airflow
    docker build -f Dockerfile.spark -t spark-s3:latest .
    echo "✓ Image built successfully"
    docker images | grep spark-s3
    """,
    dag=dag,
)

# Task 2: Load Image to Minikube
load_image_to_minikube = BashOperator(
    task_id='load_image_to_minikube',
    bash_command="""
    set -e
    echo "Loading image to Minikube's Docker registry..."
    
    # Save image to tar file
    docker save spark-s3:latest -o /tmp/spark-s3-latest.tar
    
    # Load into Minikube
    minikube image load /tmp/spark-s3-latest.tar
    
    # Verify image is in Minikube
    minikube image ls | grep spark-s3 || echo "Warning: Image not found in Minikube"
    
    # Cleanup
    rm -f /tmp/spark-s3-latest.tar
    
    echo "✓ Image loaded to Minikube successfully"
    """,
    dag=dag,
)

# Task 3: Apply Kubernetes RBAC
apply_k8s_rbac = BashOperator(
    task_id='apply_k8s_rbac',
    bash_command="""
    set -e
    echo "Applying Kubernetes RBAC for Spark..."
    kubectl apply -f /opt/airflow/k8s/spark-rbac.yaml
    echo "✓ RBAC applied successfully"
    
    # Verify service account
    kubectl get serviceaccount spark -n default
    """,
    dag=dag,
)

# Task 4: Submit Spark Job to Minikube
submit_spark_job = BashOperator(
    task_id='submit_spark_job_to_k8s',
    bash_command="""
    set -e
    echo "Submitting Spark job to Minikube cluster..."
    
    # Delete existing job if exists
    kubectl delete sparkapplication ookla-transform-job -n default --ignore-not-found=true
    
    # Wait a bit for cleanup
    sleep 5
    
    # Apply the Spark job
    kubectl apply -f /opt/airflow/k8s/spark-transform-job.yaml
    
    echo "✓ Spark job submitted successfully"
    
    # Show job status
    kubectl get sparkapplication ookla-transform-job -n default
    """,
    dag=dag,
)

# Task 5: Wait for Spark Job Completion
wait_for_spark_job = BashSensor(
    task_id='wait_for_spark_job_completion',
    bash_command="""
    # Check if Spark job completed successfully
    STATUS=$(kubectl get sparkapplication ookla-transform-job -n default -o jsonpath='{.status.applicationState.state}' 2>/dev/null || echo "NOTFOUND")
    
    echo "Current Spark job status: $STATUS"
    
    if [ "$STATUS" = "COMPLETED" ]; then
        echo "✓ Spark job completed successfully!"
        exit 0
    elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "SUBMISSION_FAILED" ]; then
        echo "✗ Spark job failed with status: $STATUS"
        # Show driver logs for debugging
        echo "=== Driver Pod Logs ==="
        kubectl logs -l spark-role=driver,spark-app-name=ookla-transform-job -n default --tail=50 || true
        exit 1
    elif [ "$STATUS" = "NOTFOUND" ]; then
        echo "✗ Spark application not found"
        exit 1
    else
        echo "Job is still running (status: $STATUS)..."
        exit 1
    fi
    """,
    poke_interval=30,  # Check every 30 seconds
    timeout=3600,  # Timeout after 1 hour
    mode='poke',
    dag=dag,
)

# Task 6: Get Spark Job Logs
get_spark_logs = BashOperator(
    task_id='get_spark_job_logs',
    bash_command="""
    set -e
    echo "=== Fetching Spark Driver Logs ==="
    kubectl logs -l spark-role=driver,spark-app-name=ookla-transform-job -n default --tail=100 || echo "No driver logs available"
    
    echo ""
    echo "=== Fetching Spark Executor Logs (sample) ==="
    kubectl logs -l spark-role=executor,spark-app-name=ookla-transform-job -n default --tail=50 --max-log-requests=5 || echo "No executor logs available"
    
    echo ""
    echo "✓ Logs retrieved successfully"
    """,
    trigger_rule='all_done',  # Run even if previous task fails
    dag=dag,
)

# Task 7: Cleanup Spark Job Resources
cleanup_spark_job = BashOperator(
    task_id='cleanup_spark_job',
    bash_command="""
    set -e
    echo "Cleaning up Spark job resources..."
    
    # Delete the Spark application
    kubectl delete sparkapplication ookla-transform-job -n default --ignore-not-found=true
    
    # Wait for pods to terminate
    sleep 5
    
    # Delete any orphaned pods
    kubectl delete pods -l spark-app-name=ookla-transform-job -n default --ignore-not-found=true
    
    echo "✓ Cleanup completed successfully"
    """,
    trigger_rule='all_done',  # Always cleanup, even if job fails
    dag=dag,
)

# Task Dependencies
build_spark_image >> load_image_to_minikube >> apply_k8s_rbac >> submit_spark_job
submit_spark_job >> wait_for_spark_job >> get_spark_logs >> cleanup_spark_job
