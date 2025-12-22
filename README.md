Project: modern data platform 
Commit 2
Architecture overview
<img width="1807" height="1040" alt="Screenshot from 2025-12-18 17-55-17" src="https://github.com/user-attachments/assets/cedb8ff5-c44f-485f-8788-442d1fd1b6e0" />


To do: 
- Add data govern
- Add data validation logic
- Add table format

# Local Lakehouse + Spark on Minikube

## Prerequisites
Ensure your system has:
- Docker
- Minikube (local Kubernetes)
- Python 3.12+

Spark will be built into a Docker image and run on Minikube.

---

## Setup Python Environment

Create and activate a virtual environment, then install dependencies:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

## Starting data lakehouse and minikube cluster
```bash
docker compose up -d
minikube start

eval $(minikube docker-env)
docker build -t spark-s3:latest .

kubectl apply -f spark-rbac.yaml

kubectl apply -f spark-transform-job.yaml

Description:
- Data lakehouse for multi purpose use cases



Finished platform architecture:

<img width="2439" height="1194" alt="image" src="https://github.com/user-attachments/assets/08e91ed7-42a9-4a90-9f28-ab5d51eea897" />

