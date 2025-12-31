helm repo add apache-airflow https://airflow.apache.org
kubectl create namespace airflow
helm install dev-release apache-airflow/airflow --namespace airflow
kubectl port-forward svc/dev-release-webserver 8080:8080 --namespace airflow