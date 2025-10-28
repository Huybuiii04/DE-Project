<img width="3274" height="1221" alt="image" src="https://github.com/user-attachments/assets/4eced3f1-d3a8-429f-bcfd-d641a9716621" />


## 1. Docker Compose 

```bash
# Build custom Airflow image
docker compose build

# Khởi động tất cả services
docker compose up -d

# Kiểm tra trạng thái services
docker compose ps

# Dừng tất cả services
docker compose down

# Xem logs của service cụ thể
docker logs <service-name>
docker logs -f <service-name>  # Follow logs real-time

# Restart service cụ thể
docker compose restart <service-name>
```

## 2. Cassandra - Setup Database

```bash
# Tạo keyspace
docker exec -it cassandra_db cqlsh -e "CREATE KEYSPACE IF NOT EXISTS spark_streams WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"

# Tạo table
docker exec -it cassandra_db cqlsh -e "CREATE TABLE IF NOT EXISTS spark_streams.created_users (id UUID PRIMARY KEY, first_name TEXT, last_name TEXT, gender TEXT, address TEXT, post_code TEXT, email TEXT, username TEXT, dob TEXT, registered_date TEXT, phone TEXT, picture TEXT);"

# Kiểm tra keyspaces
docker exec -it cassandra_db cqlsh -e "DESCRIBE KEYSPACES;"

# Xem cấu trúc table
docker exec -it cassandra_db cqlsh -e "DESCRIBE TABLE spark_streams.created_users;"

# Query data
docker exec -it cassandra_db cqlsh -e "SELECT COUNT(*) FROM spark_streams.created_users;"
docker exec -it cassandra_db cqlsh -e "SELECT * FROM spark_streams.created_users LIMIT 10;"

# Connect vào CQL shell interactive
docker exec -it cassandra_db cqlsh
```

## 3. Kafka - Kiểm tra Messages

```bash
# List tất cả topics
docker exec -it broker kafka-topics --bootstrap-server localhost:9092 --list

# Xem thông tin topic
docker exec -it broker kafka-topics --bootstrap-server localhost:9092 --describe --topic users_created

# Consume messages từ đầu
docker exec -it broker kafka-console-consumer --bootstrap-server localhost:9092 --topic users_created --from-beginning --max-messages 10

# Consume messages real-time
docker exec -it broker kafka-console-consumer --bootstrap-server localhost:9092 --topic users_created

# Delete topic (nếu cần)
docker exec -it broker kafka-topics --bootstrap-server localhost:9092 --delete --topic users_created
```

## 4. Airflow - DAG Management

### Access Airflow UI
- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

```bash
# Xem logs của Airflow services
docker logs airflow-webserver
docker logs airflow-scheduler
docker logs airflow-init

# Trigger DAG từ command line
docker exec -it airflow-webserver airflow dags trigger user_automation

# List tất cả DAGs
docker exec -it airflow-webserver airflow dags list

# Test task cụ thể
docker exec -it airflow-webserver airflow tasks test user_automation stream_data_from_api 2025-10-27
```

## 5. Spark - Job Submission

```bash
# Kiểm tra Spark Master UI
# URL: http://localhost:9090

# Kiểm tra Spark Worker
# URL: http://localhost:8082

# Build Spark streaming image
docker compose build spark-streaming

# Start Spark streaming job
docker compose up -d spark-streaming

# Xem logs của Spark streaming
docker logs spark-streaming
docker logs -f spark-streaming

# Copy file vào Spark container (nếu cần)
docker cp spark_stream.py spark-master:/opt/spark_stream.py

# Submit Spark job manually (nếu không dùng docker-compose service)
docker exec spark-master /opt/spark/bin/spark-submit \
  --master local[*] \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/spark/spark_stream.py
```

## 6. Kafka Control Center

- URL: http://localhost:9021
- Xem topics, messages, consumer groups qua Web UI

## 7. Schema Registry

- URL: http://localhost:8081
- API endpoint để manage schemas

```bash
# List tất cả schemas
curl http://localhost:8081/subjects

# Get schema versions
curl http://localhost:8081/subjects/<subject-name>/versions
```

## 8. Troubleshooting Commands

```bash
# Kiểm tra Docker networks
docker network ls
docker network inspect e2e-data-engineering_confluent

# Kiểm tra container health
docker inspect <container-name> | grep -i health

# Remove tất cả containers và volumes (CẢNH BÁO: Xóa data)
docker compose down -v

# Rebuild từ đầu
docker compose down
docker compose build --no-cache
docker compose up -d

# Xem resource usage
docker stats

# Clean up Docker system
docker system prune -a
```

## 9. Complete Pipeline Workflow

### Bước 1: Khởi động services
```bash
docker compose up -d
```

### Bước 2: Setup Cassandra (chỉ cần 1 lần)
```bash
docker exec -it cassandra_db cqlsh -e "CREATE KEYSPACE IF NOT EXISTS spark_streams WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
docker exec -it cassandra_db cqlsh -e "CREATE TABLE IF NOT EXISTS spark_streams.created_users (id UUID PRIMARY KEY, first_name TEXT, last_name TEXT, gender TEXT, address TEXT, post_code TEXT, email TEXT, username TEXT, dob TEXT, registered_date TEXT, phone TEXT, picture TEXT);"
```

### Bước 3: Trigger Airflow DAG
- Truy cập http://localhost:8080
- Login với admin/admin
- Bật DAG `user_automation`
- Trigger DAG để stream data vào Kafka

### Bước 4: Start Spark Streaming
```bash
docker compose up -d spark-streaming
```

### Bước 5: Verify data trong Cassandra
```bash
docker exec -it cassandra_db cqlsh -e "SELECT COUNT(*) FROM spark_streams.created_users;"
docker exec -it cassandra_db cqlsh -e "SELECT * FROM spark_streams.created_users LIMIT 5;"
```

## 10. Services Ports

| Service | Port | URL |
|---------|------|-----|
| Airflow Webserver | 8080 | http://localhost:8080 |
| Kafka Broker | 9092 | localhost:9092 |
| Kafka Control Center | 9021 | http://localhost:9021 |
| Schema Registry | 8081 | http://localhost:8081 |
| Spark Master UI | 9090 | http://localhost:9090 |
| Spark Worker UI | 8082 | http://localhost:8082 |
| Cassandra | 9042 | localhost:9042 |
| Zookeeper | 2181 | localhost:2181 |
| PostgreSQL (Airflow) | 5432 | localhost:5432 |

## 11. Environment Variables

Tất cả environment variables được define trong `docker-compose.yml`:

- **Airflow Secret Key**: `e2e-data-engineering-secret-key-2025`
- **Airflow DB**: `postgresql+psycopg2://airflow:airflow@postgres:5432/airflow`
- **Kafka Broker**: `broker:29092` (internal), `localhost:9092` (external)
- **Cassandra Cluster**: `test-cluster`

## 12. File Structure

```
e2e-data-engineering/
├── dags/
│   └── kafka_stream.py          # Airflow DAG
├── logs/                         # Airflow logs (shared volume)
├── script/
│   └── entrypoint.sh            # Legacy (không sử dụng)
├── docker-compose.yml           # Main orchestration file
├── Dockerfile                   # Custom Airflow image
├── Dockerfile.spark             # Custom Spark image
├── requirements.txt             # Python dependencies
├── spark_stream.py              # Spark streaming job
└── COMMANDS.md                  # This file
```

## 13. Development Tips

```bash
# Edit DAG code
# File sẽ tự động sync vì mount volume ./dags:/opt/airflow/dags

# Edit Spark streaming code
# Cần restart service:
docker compose restart spark-streaming

# Rebuild Airflow image sau khi thay đổi requirements.txt
docker compose build webserver scheduler
docker compose up -d webserver scheduler

# Rebuild Spark image sau khi thay đổi Dockerfile.spark
docker compose build spark-streaming
docker compose up -d spark-streaming
```

## 14. Monitoring & Health Checks

```bash
# Check Airflow health
curl http://localhost:8080/health

# Check all services status
docker compose ps

# Check logs for errors
docker compose logs | grep -i error
docker compose logs | grep -i exception

# Monitor resource usage
docker stats --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"
```

## 15. Backup & Restore

```bash
# Backup Cassandra data
docker exec cassandra_db nodetool snapshot spark_streams

# Export data từ Cassandra
docker exec -it cassandra_db cqlsh -e "COPY spark_streams.created_users TO '/tmp/users.csv' WITH HEADER = TRUE;"
docker cp cassandra_db:/tmp/users.csv ./backup/

# Backup Airflow metadata
docker exec postgres pg_dump -U airflow airflow > airflow_backup.sql
```
"# DE-Project" 
