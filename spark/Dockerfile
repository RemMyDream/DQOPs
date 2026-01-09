FROM python:3.10

RUN apt-get update && apt-get install -y --no-install-recommends \
      sudo \
      bash \
      curl \
      vim \
      unzip \
      rsync \
      build-essential \
      ssh \
      wget \
      dnsutils \
      postgresql-client \
      libpq-dev \
      libxml2-dev \
      libxslt1-dev \
      zlib1g-dev \
      libssl-dev \
      libffi-dev \
      python3-dev \
      git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN curl -L -o openjdk-11.tar.gz \
    "https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.20%2B8/OpenJDK11U-jdk_x64_linux_hotspot_11.0.20_8.tar.gz" && \
    tar -xzf openjdk-11.tar.gz -C /opt/ && \
    rm openjdk-11.tar.gz && \
    mv /opt/jdk-11.0.20+8 /opt/java-11

ENV JAVA_HOME=/opt/java-11
ENV PATH=$JAVA_HOME/bin:$PATH
RUN java -version

ENV SPARK_HOME=/opt/spark
ENV HADOOP_HOME=/opt/hadoop
RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

RUN curl -L -o spark-3.5.1-bin-hadoop3.tgz \
    https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar xvzf spark-3.5.1-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 && \
    rm spark-3.5.1-bin-hadoop3.tgz

ENV PATH=${SPARK_HOME}/sbin:${SPARK_HOME}/bin:${PATH}
ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:/opt/spark

COPY requirements.txt .
RUN pip3 install -r requirements.txt

RUN mkdir -p ${SPARK_HOME}/jars

RUN curl -L -o ${SPARK_HOME}/jars/postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

RUN curl -L -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

RUN curl -L -o ${SPARK_HOME}/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o ${SPARK_HOME}/jars/kafka-clients-3.4.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -L -o ${SPARK_HOME}/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o ${SPARK_HOME}/jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

RUN curl -L -o ${SPARK_HOME}/jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar

RUN curl -L -o ${SPARK_HOME}/jars/iceberg-aws-bundle-1.7.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar

ENV SPARK_CLASSPATH=${SPARK_HOME}/jars/postgresql-42.6.0.jar:${SPARK_HOME}/jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar:${SPARK_HOME}/jars/iceberg-aws-bundle-1.7.1.jar

COPY conf/spark-defaults.conf "$SPARK_HOME/conf"

RUN chmod u+x ${SPARK_HOME}/sbin/* && chmod u+x ${SPARK_HOME}/bin/*

COPY entrypoint.sh .
RUN sed -i 's/\r$//' entrypoint.sh && chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]