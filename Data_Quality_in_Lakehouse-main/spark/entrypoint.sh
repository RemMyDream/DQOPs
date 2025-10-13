#!/bin/bash
set -e

# ----------------------
# Tạo thư mục cần thiết
# ----------------------
mkdir -p /opt/spark/logs /opt/spark/work /opt/spark/spark-warehouse
chmod -R 755 /opt/spark/logs /opt/spark/work /opt/spark/spark-warehouse

# ----------------------
# Kiểm tra command đầu vào
# ----------------------
case "$1" in
  master)
    echo "Starting Spark Master..."
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
      --host 0.0.0.0 \
      --port 7077 \
      --webui-port 8080
    ;;

  worker)
    if [ -z "$SPARK_MASTER_URL" ]; then
      echo "ERROR: SPARK_MASTER_URL environment variable is not set!"
      exit 1
    fi
    
    # Đợi Master sẵn sàng
    echo "Waiting for Spark Master at $SPARK_MASTER_URL..."
    MASTER_HOST=$(echo "$SPARK_MASTER_URL" | sed 's|spark://||' | cut -d: -f1)
    until nc -z "$MASTER_HOST" 7077 2>/dev/null; do
      echo "Master not ready, waiting..."
      sleep 2
    done
    
    echo "Starting Spark Worker connecting to $SPARK_MASTER_URL..."
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker "$SPARK_MASTER_URL"
    ;;

  thrift)
    if [ -z "$SPARK_MASTER_URL" ]; then
      echo "ERROR: SPARK_MASTER_URL environment variable is not set!"
      exit 1
    fi
    
    # Đợi Master sẵn sàng
    MASTER_HOST=$(echo "$SPARK_MASTER_URL" | sed 's|spark://||' | cut -d: -f1)
    until nc -z "$MASTER_HOST" 7077 2>/dev/null; do
      echo "Master not ready, waiting..."
      sleep 2
    done
    
    echo "Starting Spark Thrift Server connecting to $SPARK_MASTER_URL..."
    exec /opt/spark/sbin/start-thriftserver.sh \
      --master "$SPARK_MASTER_URL" \
      --conf spark.cores.max=2 \
      --conf spark.sql.warehouse.dir=/opt/spark/spark-warehouse \
      --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
      --hiveconf hive.server2.thrift.port=10000
    ;;

  *)
    echo "Usage: $0 {master|worker|thrift}"
    exit 1
    ;;
esac