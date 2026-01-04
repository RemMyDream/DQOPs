FROM apache/airflow:2.9.0-python3.10

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        cargo \
        libssl-dev \
        libffi-dev \
        python3-dev \
        procps \
        dos2unix \
    && rm -rf /var/lib/apt/lists/*

# Install Java 11
RUN curl -L -o openjdk-11.tar.gz \
    "https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.20%2B8/OpenJDK11U-jdk_x64_linux_hotspot_11.0.20_8.tar.gz" && \
    tar -xzf openjdk-11.tar.gz -C /opt/ && \
    rm openjdk-11.tar.gz && \
    mv /opt/jdk-11.0.20+8 /opt/java-11

# Set JAVA_HOME
ENV JAVA_HOME=/opt/java-11
ENV AIRFLOW_HOME=/opt/airflow

# Install Spark 3.5.1
ENV SPARK_VERSION=3.5.1
ENV SPARK_HOME=/opt/spark
ENV HADOOP_VERSION=3

RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o spark.tgz \
    && tar -xzf spark.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm spark.tgz

# Download Iceberg JARs
RUN curl -L -o ${SPARK_HOME}/jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar && \
    curl -L -o ${SPARK_HOME}/jars/iceberg-aws-bundle-1.7.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar

RUN curl -L -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Set environment variables for Spark 3.5.1
ENV PATH=${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${JAVA_HOME}/bin:/home/airflow/.local/bin:${PATH}
ENV PYTHONPATH=${AIRFLOW_HOME}:${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip

RUN mkdir -p ${AIRFLOW_HOME}
WORKDIR ${AIRFLOW_HOME}

COPY airflow/requirements.txt ${AIRFLOW_HOME}/requirements.txt
COPY airflow/entrypoint.sh ${AIRFLOW_HOME}/entrypoint.sh

RUN dos2unix ${AIRFLOW_HOME}/entrypoint.sh && \
    chmod +x ${AIRFLOW_HOME}/entrypoint.sh

# Create directories for all code while still root
RUN mkdir -p ${AIRFLOW_HOME}/dags \
    ${AIRFLOW_HOME}/scripts \
    /opt/spark/apps \
    /opt/spark/data \
    /opt/spark/utils && \
    chown -R airflow:root /opt/spark/apps /opt/spark/data /opt/spark/utils

USER airflow
RUN pip3 install -r ./requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.10.txt"

# Copy all code to the image
COPY airflow/dags/ ${AIRFLOW_HOME}/dags/
COPY airflow/scripts/ ${AIRFLOW_HOME}/scripts/
COPY spark/spark_apps/ /opt/spark/apps/
COPY utils/ /opt/spark/utils/

ENTRYPOINT ["/opt/airflow/entrypoint.sh"]