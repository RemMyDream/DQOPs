#!/bin/bash
SPARK_WORKLOAD=$1
echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

MASTER_URL="${SPARK_MASTER:-spark://spark-master:7077}"

unset SPARK_MASTER

if [ "$SPARK_WORKLOAD" = "master" ]; then
    start-master.sh
elif [ "$SPARK_WORKLOAD" = "worker" ]; then
    start-worker.sh "$MASTER_URL"
elif [ "$SPARK_WORKLOAD" = "history" ]; then
    start-history-server.sh
elif [ "$SPARK_WORKLOAD" = "driver" ] || [ "$SPARK_WORKLOAD" = "executor" ]; then
    shift
    exec /opt/spark/bin/spark-class "$@"
else
    exec "$@"
fi