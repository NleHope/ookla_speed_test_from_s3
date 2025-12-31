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
from airflow. providers.cncf.kubernetes. operators.pod import KubernetesPodOperator
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
    schedule='@daily',
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
    docker build -f spark/Dockerfile.spark -t spark-s3:latest .
    echo "✓ Image built successfully"
    docker images | grep spark-s3
    """,
    dag=dag,
)

# Task 2: Verify Spark Image (Docker Desktop - no need to load separately)
verify_spark_image = BashOperator(
    task_id='verify_spark_image',
    bash_command="""
    set -e
    echo "Verifying Spark image is available..."
    docker images | grep spark-s3
    echo "✓ Spark image is ready"
    """,
    dag=dag,
)

# Task 3: Apply Kubernetes RBAC
apply_k8s_rbac = BashOperator(
    task_id='apply_k8s_rbac',
    bash_command="""
    set -e
    echo "==================================="
    echo "Applying Kubernetes RBAC for Spark"
    echo "==================================="
    
    # Check if RBAC file exists
    if [ ! -f /opt/airflow/k8s_rbac/spark-rbac.yaml ]; then
        echo "❌ Error: spark-rbac.yaml not found at /opt/airflow/k8s_rbac/spark-rbac.yaml"
        exit 1
    fi
    
    echo "✓ RBAC file found"
    
    # Apply RBAC (idempotent operation)
    echo "Applying RBAC configuration..."
    if kubectl apply -f /opt/airflow/k8s_rbac/spark-rbac.yaml; then
        echo "✓ RBAC applied successfully"
    else
        echo "❌ Failed to apply RBAC"
        exit 1
    fi
    
    # Verify service account
    echo ""
    echo "Verifying Spark service account..."
    if kubectl get serviceaccount spark -n default > /dev/null 2>&1; then
        echo "✓ Spark serviceaccount exists"
        kubectl get serviceaccount spark -n default
    else
        echo "❌ Warning: spark serviceaccount not found"
        exit 1
    fi
    
    # Verify role
    echo ""
    echo "Verifying Spark role..."
    if kubectl get role spark-role -n default > /dev/null 2>&1; then
        echo "✓ Spark role exists"
    else
        echo "❌ Warning: spark-role not found"
    fi
    
    # Verify role binding
    echo ""
    echo "Verifying Spark role binding..."
    if kubectl get rolebinding spark-role-binding -n default > /dev/null 2>&1; then
        echo "✓ Spark role binding exists"
    else
        echo "❌ Warning: spark-role-binding not found"
    fi
    
    echo ""
    echo "==================================="
    echo "✓ RBAC setup completed successfully"
    echo "==================================="
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
    kubectl apply -f /opt/airflow/k8s_rbac/spark-application.yaml
    
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
build_spark_image >> verify_spark_image >> apply_k8s_rbac >> submit_spark_job
submit_spark_job >> wait_for_spark_job >> get_spark_logs >> cleanup_spark_job
