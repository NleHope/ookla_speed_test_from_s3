Project: modern data platform 
Commit 2
Architecture overview
<img width="1807" height="1040" alt="Screenshot from 2025-12-18 17-55-17" src="https://github.com/user-attachments/assets/cedb8ff5-c44f-485f-8788-442d1fd1b6e0" />


To do: 
- Add data govern
- Add data validation logic
- Add table format

How to start:
ensure your system has docker and minikube (k8s for local) and download spark components on minikube

Download python libraries:
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt



Run the docker compose file to start up the lakehouse:
'''docker compose up -d'''

start minikube cluster:
minikube start

build and push docker image into minikube:
eval $(eval docker-env)
docker build -t spark-s3:latest .

Send spark job to k8s
kubectl apply -f spark-rbac.yaml
kubectl apply -f spark-job.yaml



Description:
- Data lakehouse for multi purpose use cases



Finished platform architecture:

<img width="2439" height="1194" alt="image" src="https://github.com/user-attachments/assets/08e91ed7-42a9-4a90-9f28-ab5d51eea897" />

