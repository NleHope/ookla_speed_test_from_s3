"""
Utility functions for Airflow DAGs
===================================
Common helper functions used across DAGs for:
- MinIO/S3 operations
- Kubernetes operations
- Data validation
- Notifications

Author: Auto-generated
Date: 2024-12-18
"""

import boto3
from botocore.client import Config
from minio import Minio
from kubernetes import client, config as k8s_config
import logging
from typing import List, Dict, Optional
import os

logger = logging.getLogger(__name__)


class MinIOClient:
    """Helper class for MinIO/S3 operations"""
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool = False):
        """
        Initialize MinIO client.
        
        Args:
            endpoint: MinIO endpoint (e.g., 'localhost:9000')
            access_key: MinIO access key
            secret_key: MinIO secret key
            secure: Use HTTPS if True
        """
        self.endpoint = endpoint
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
    
    def check_bucket_exists(self, bucket_name: str) -> bool:
        """Check if a bucket exists"""
        try:
            return self.client.bucket_exists(bucket_name)
        except Exception as e:
            logger.error(f"Error checking bucket {bucket_name}: {e}")
            return False
    
    def list_objects(self, bucket_name: str, prefix: str = "") -> List[str]:
        """
        List objects in a bucket with optional prefix.
        
        Args:
            bucket_name: Name of the bucket
            prefix: Object prefix to filter
            
        Returns:
            List of object names
        """
        try:
            objects = self.client.list_objects(bucket_name, prefix=prefix, recursive=True)
            return [obj.object_name for obj in objects]
        except Exception as e:
            logger.error(f"Error listing objects in {bucket_name}/{prefix}: {e}")
            return []
    
    def check_path_exists(self, bucket_name: str, prefix: str) -> bool:
        """
        Check if any objects exist with the given prefix.
        
        Args:
            bucket_name: Name of the bucket
            prefix: Object prefix to check
            
        Returns:
            True if objects exist, False otherwise
        """
        objects = self.list_objects(bucket_name, prefix)
        return len(objects) > 0
    
    def get_object_count(self, bucket_name: str, prefix: str = "") -> int:
        """Get count of objects in a bucket/prefix"""
        objects = self.list_objects(bucket_name, prefix)
        return len(objects)


class KubernetesHelper:
    """Helper class for Kubernetes operations"""
    
    def __init__(self, namespace: str = "default"):
        """
        Initialize Kubernetes client.
        
        Args:
            namespace: Kubernetes namespace
        """
        self.namespace = namespace
        try:
            k8s_config.load_incluster_config()
        except:
            k8s_config.load_kube_config()
        
        self.custom_api = client.CustomObjectsApi()
        self.core_api = client.CoreV1Api()
    
    def get_spark_application_status(self, app_name: str) -> Optional[Dict]:
        """
        Get SparkApplication status.
        
        Args:
            app_name: Name of the SparkApplication
            
        Returns:
            Dictionary with status information or None if not found
        """
        try:
            app = self.custom_api.get_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=self.namespace,
                plural="sparkapplications",
                name=app_name
            )
            
            status = app.get('status', {})
            return {
                'state': status.get('applicationState', {}).get('state', 'UNKNOWN'),
                'driver_info': status.get('driverInfo', {}),
                'execution_attempts': status.get('executionAttempts', 0),
                'termination_time': status.get('terminationTime'),
            }
        except Exception as e:
            logger.error(f"Error getting SparkApplication {app_name}: {e}")
            return None
    
    def delete_spark_application(self, app_name: str) -> bool:
        """
        Delete a SparkApplication.
        
        Args:
            app_name: Name of the SparkApplication
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.custom_api.delete_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=self.namespace,
                plural="sparkapplications",
                name=app_name
            )
            logger.info(f"Deleted SparkApplication: {app_name}")
            return True
        except Exception as e:
            logger.error(f"Error deleting SparkApplication {app_name}: {e}")
            return False
    
    def get_pod_logs(self, label_selector: str, tail_lines: int = 100) -> str:
        """
        Get logs from pods matching label selector.
        
        Args:
            label_selector: Kubernetes label selector
            tail_lines: Number of lines to retrieve
            
        Returns:
            Combined logs from matching pods
        """
        try:
            pods = self.core_api.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=label_selector
            )
            
            logs = []
            for pod in pods.items:
                pod_log = self.core_api.read_namespaced_pod_log(
                    name=pod.metadata.name,
                    namespace=self.namespace,
                    tail_lines=tail_lines
                )
                logs.append(f"=== Pod: {pod.metadata.name} ===\n{pod_log}")
            
            return "\n\n".join(logs)
        except Exception as e:
            logger.error(f"Error getting pod logs: {e}")
            return ""


class DataValidator:
    """Helper class for data validation"""
    
    @staticmethod
    def validate_parquet_files(minio_client: MinIOClient, bucket: str, prefix: str) -> Dict:
        """
        Validate parquet files in a bucket/prefix.
        
        Args:
            minio_client: MinIOClient instance
            bucket: Bucket name
            prefix: Object prefix
            
        Returns:
            Dictionary with validation results
        """
        objects = minio_client.list_objects(bucket, prefix)
        parquet_files = [obj for obj in objects if obj.endswith('.parquet')]
        
        return {
            'total_objects': len(objects),
            'parquet_files': len(parquet_files),
            'valid': len(parquet_files) > 0,
            'files': parquet_files[:10]  # First 10 files as sample
        }
    
    @staticmethod
    def check_data_freshness(minio_client: MinIOClient, bucket: str, prefix: str, 
                           max_age_hours: int = 24) -> bool:
        """
        Check if data is fresh (modified within max_age_hours).
        
        Args:
            minio_client: MinIOClient instance
            bucket: Bucket name
            prefix: Object prefix
            max_age_hours: Maximum age in hours
            
        Returns:
            True if data is fresh, False otherwise
        """
        # Implementation would check object modification times
        # Placeholder for now
        return True


def send_slack_notification(webhook_url: str, message: str, success: bool = True):
    """
    Send notification to Slack.
    
    Args:
        webhook_url: Slack webhook URL
        message: Message to send
        success: True for success (green), False for failure (red)
    """
    import requests
    
    color = "good" if success else "danger"
    payload = {
        "attachments": [{
            "color": color,
            "text": message,
            "footer": "Airflow ETL Pipeline",
            "ts": int(datetime.now().timestamp())
        }]
    }
    
    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        logger.info("Slack notification sent successfully")
    except Exception as e:
        logger.error(f"Error sending Slack notification: {e}")


def get_quarter_from_date(date) -> str:
    """
    Get quarter string from date.
    
    Args:
        date: datetime object
        
    Returns:
        Quarter string (e.g., '2019-q1')
    """
    year = date.year
    quarter = (date.month - 1) // 3 + 1
    return f"{year}-q{quarter}"


# Example usage functions for Airflow tasks

def check_bronze_data_task(**context):
    """Airflow task to check bronze data existence"""
    from airflow.models import Variable
    
    # Get configuration
    minio_endpoint = Variable.get("minio_endpoint", "localhost:9000")
    minio_access_key = Variable.get("minio_access_key", "minio_access_key")
    minio_secret_key = Variable.get("minio_secret_key", "minio_secret_key")
    
    # Get quarter from XCom
    quarter = context['task_instance'].xcom_pull(task_ids='calculate_quarter', key='quarter')
    
    # Check data
    minio_client = MinIOClient(minio_endpoint, minio_access_key, minio_secret_key, secure=False)
    
    bronze_bucket = "bronze"
    bronze_prefix = f"pump/{quarter}/"
    
    exists = minio_client.check_path_exists(bronze_bucket, bronze_prefix)
    
    if exists:
        count = minio_client.get_object_count(bronze_bucket, bronze_prefix)
        logger.info(f"Found {count} objects in {bronze_bucket}/{bronze_prefix}")
        return True
    else:
        logger.warning(f"No data found in {bronze_bucket}/{bronze_prefix}")
        return False


def verify_silver_data_task(**context):
    """Airflow task to verify silver data"""
    from airflow.models import Variable
    
    # Get configuration
    minio_endpoint = Variable.get("minio_endpoint", "localhost:9000")
    minio_access_key = Variable.get("minio_access_key", "minio_access_key")
    minio_secret_key = Variable.get("minio_secret_key", "minio_secret_key")
    
    # Get quarter from XCom
    quarter = context['task_instance'].xcom_pull(task_ids='calculate_quarter', key='quarter')
    
    # Validate output
    minio_client = MinIOClient(minio_endpoint, minio_access_key, minio_secret_key, secure=False)
    validator = DataValidator()
    
    silver_bucket = "silver"
    silver_prefix = f"pump/{quarter}/"
    
    validation_result = validator.validate_parquet_files(minio_client, silver_bucket, silver_prefix)
    
    logger.info(f"Validation result: {validation_result}")
    
    if not validation_result['valid']:
        raise ValueError(f"Validation failed: No parquet files found in {silver_bucket}/{silver_prefix}")
    
    return validation_result
