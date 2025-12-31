THIS PROJECT IS STILL WIP

For legitimacy, please check ./imgs folder to see my current works

Project: Data lakehouse for big data processing and streaming data processing

Commit 3

Architecture overview
![Architecture](./imgs/dataplatform_archi.png)


To do: 
- Add create silver and gold bucket if not exist scripts
- Add data govern
- Add data validation logic
- Fix airflow logs not shown on airflow UI
- Fix table format write fails

# Data lakehouse with data processing engine and orchestration on kubernetes

## Prerequisites
- Docker & Docker Compose
- Kubernetes (Docker Desktop/Minikube/Kind)
- Python 3.12+
- Helm 3.x
- kubectl

## Quick Start

### 1. Start Infrastructure
```bash
# Start data lakehouse (MinIO, Trino, Hive Metastore, Kafka, PostgreSQL)
docker compose up -d

# Verify services
docker ps
```

### 2. Setup Kubernetes Cluster
```bash
# For Docker Desktop: Enable Kubernetes in settings

# For Minikube:
minikube start --cpus=4 --memory=8192

# Verify cluster
kubectl cluster-info
```

### 3. Deploy Airflow on Kubernetes
```bash
# Add Airflow Helm repo
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Create namespace
kubectl create namespace airflow

# Install Airflow
helm install dev-release apache-airflow/airflow --namespace airflow

# Wait for pods to be ready
kubectl get pods -n airflow -w

# Access Airflow UI (in separate terminal)
kubectl port-forward svc/dev-release-webserver 8080:8080 --namespace airflow
# Default credentials: admin / admin
```

### 4. Setup Spark on Kubernetes
```bash
# Install Spark Operator
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm repo update

kubectl create namespace spark-operator
helm install spark-operator spark-operator/spark-operator --namespace spark-operator

# Apply RBAC for Spark jobs
kubectl apply -f k8s_rbac/spark-rbac.yaml
kubectl apply -f k8s_rbac/airflow-rbac.yaml
```

### 5. Setup Python Environment (Optional - for local testing)
```bash
python3.12 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 6. Initialize MinIO Buckets
```bash
# Access MinIO UI: http://localhost:9001
# Login: minio_access_key / minio_secret_key
# Create buckets: bronze, silver, gold
```

### 7. Run ETL Pipeline
```bash
# Access Airflow UI: http://localhost:8080
# Enable and trigger DAG: ookla_etl
# Monitor Spark jobs: kubectl get sparkapplications -n airflow
```

## Services Access
- **Airflow UI**: use k9s or cmd to port forward airflow api-server to localhost:8081 (admin/admin)
- **MinIO Console**: http://localhost:9001 (minio_access_key/minio_secret_key)
- **Trino**: http://localhost:8080
- **Kafka**: localhost:9092

## Troubleshooting
```bash
# Check Airflow logs
kubectl logs -n airflow -l component=scheduler

# Check Spark job status
kubectl describe sparkapplication <job-name> -n airflow

# Check pod logs
kubectl logs <pod-name> -n airflow

# Restart Airflow
helm upgrade dev-release apache-airflow/airflow --namespace airflow

# Clean up
docker compose down -v
helm uninstall dev-release -n airflow
kubectl delete namespace airflow spark-operator
```

Description for completed Project:
- Data lakehouse for multi purpose use cases

![Full Architecture](./imgs/full_project_vision.png)



