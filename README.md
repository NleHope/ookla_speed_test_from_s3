Project: data-lake-with-minio (lab 1 cua AIDE 2)
Architecture overview

<img width="2124" height="899" alt="image" src="https://github.com/user-attachments/assets/9641d0bf-86e1-431d-bcae-00ff97f0bcd6" />

To do: 
- Add airflow vao de automate va orchestrate pipeline
- Add Greate expectation va Datahub cho data validation va governance

Mô tả
- Pipeline đơn giản để ingest dữ liệu từ nguồn S3 public vào datalake local (MinIO). Có thể query bằng Trino/Hive và chạy job Spark (k8s hoặc local).

Yêu cầu
- Docker và `docker compose`
- Python 3 và `pip` (nếu chạy job Python local)
- (Tùy chọn) `minikube` và `kubectl` nếu muốn deploy Spark job lên Kubernetes

Hướng dẫn nhanh
1. Cho phép file chạy (nếu cần):

```bash
chmod +x run.sh
```

2. Start services (MinIO, Trino, ... theo `docker-compose.yml`):

```bash
./run.sh up
```

3. Các lệnh hữu ích khác:

- `./run.sh build`   : build image Spark từ `Dockerfile.spark`
- `./run.sh k8s`     : deploy và chạy Spark job trên Minikube/K8s
- `./run.sh ingest`  : chạy job ingest cục bộ bằng Python (`spark/jobs/ingest.py`)
- `./run.sh export`  : chạy `utils/export_data_to_datalake.py` để xuất dữ liệu
- `./run.sh down`    : dừng các service do `docker compose` tạo

Ghi chú
- Sau `./run.sh up`, MinIO thường truy cập được tại `http://localhost:9001` (kiểm tra `docker-compose.yml` để biết thông tin port/credential).
- Nếu dùng Kubernetes, hãy đảm bảo `minikube` đang chạy: `minikube start`.

Liên hệ
- Nếu cần mở rộng hướng dẫn (ví dụ: credential MinIO, cấu hình Trino, ví dụ run Spark), báo tôi để tôi cập nhật thêm.
