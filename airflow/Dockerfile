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

# Set environment variables for Spark 3.5.1
ENV PATH=${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${JAVA_HOME}/bin:/home/airflow/.local/bin:${PATH}
ENV PYTHONPATH=${AIRFLOW_HOME}:${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip

RUN mkdir -p ${AIRFLOW_HOME}
WORKDIR ${AIRFLOW_HOME}

COPY requirements.txt  ${AIRFLOW_HOME}/requirements.txt
COPY entrypoint.sh ${AIRFLOW_HOME}/entrypoint.sh
RUN chmod +x ${AIRFLOW_HOME}/entrypoint.sh

USER airflow
RUN pip3 install -r ./requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.10.txt"

ENTRYPOINT ["/opt/airflow/entrypoint.sh"]
