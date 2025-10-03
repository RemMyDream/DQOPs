#!/bin/bash
SPARK_WORKLOAD=$1
echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

if [ "$SPARK_WORKLOAD" = "master" ]; 
then
    start-master.sh
elif [ "$SPARK_WORKLOAD" = "worker" ]; 
then
    start-worker.sh spark://spark-master:7077
elif [ "$SPARK_WORKLOAD" = "history" ]; 
then
    start-history-server.sh
else
    echo "Unknown SPARK_WORKLOAD: $SPARK_WORKLOAD"
    exit 1
fi