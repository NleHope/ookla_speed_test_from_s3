#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cmd="${1:-help}"

case "$cmd" in
	up)
		echo "[run.sh] Build and start services with docker-compose..."
		docker compose up -d --build
		;;
	build)
		echo "[run.sh] Build spark docker image..."
		docker build -t spark-job-image -f Dockerfile.spark .
		;;
	k8s)
		echo "[run.sh] Deploy spark job to Kubernetes (minikube)..."
		eval "$(minikube -p minikube docker-env)" || true
		kubectl apply -f k8s/spark-job.yaml
		kubectl wait --for=condition=complete --timeout=600s job/spark-job
		;;
	ingest)
		echo "[run.sh] Run local ingest job using Python (requires Python and deps)..."
		pip install -r requirements.txt || true
		python3 "$ROOT/spark/jobs/ingest.py"
		;;
	export)
		echo "[run.sh] Export data to datalake using utils/export_data_to_datalake.py"
		python3 "$ROOT/utils/export_data_to_datalake.py"
		;;
	down)
		echo "[run.sh] Stop docker-compose services..."
		docker compose down
		;;
	help|*)
		cat <<EOF
Usage: $0 <command>

Commands:
	up      Build and start services with docker-compose
	build   Build the spark Docker image (Dockerfile.spark)
	k8s     Deploy spark job to Kubernetes (minikube)
	ingest  Run local ingest job using Python script
	export  Run export to datalake script
	down    Stop docker-compose services
	help    Show this message
EOF
		;;
esac
