FROM apache/spark:3.5.1

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      postgresql-client \
      libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

RUN curl -L -o ${SPARK_HOME}/jars/postgresql-42.6.0.jar \
      https://jdbc.postgresql.org/download/postgresql-42.6.0.jar && \
    curl -L -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -L -o ${SPARK_HOME}/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o ${SPARK_HOME}/jars/kafka-clients-3.4.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -L -o ${SPARK_HOME}/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o ${SPARK_HOME}/jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -L -o ${SPARK_HOME}/jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar && \
    curl -L -o ${SPARK_HOME}/jars/iceberg-aws-bundle-1.7.1.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar

COPY conf/spark-defaults.conf ${SPARK_HOME}/conf/

USER spark

