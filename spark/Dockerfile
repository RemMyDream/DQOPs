FROM python:3.10

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      bash \
      curl \
      vim \
      unzip \
      rsync \
      build-essential \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Java 11
RUN curl -L -o openjdk-11.tar.gz \
    "https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.20%2B8/OpenJDK11U-jdk_x64_linux_hotspot_11.0.20_8.tar.gz" && \
    tar -xzf openjdk-11.tar.gz -C /opt/ && \
    rm openjdk-11.tar.gz && \
    mv /opt/jdk-11.0.20+8 /opt/java-11

# Set JAVA_HOME
ENV JAVA_HOME=/opt/java-11
ENV PATH=$JAVA_HOME/bin:$PATH

RUN java -version

ENV SPARK_HOME=/opt/spark
ENV HADOOP_HOME=/opt/hadoop

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# Download Spark 3.5.1
RUN curl https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -o spark-3.5.1-bin-hadoop3.tgz \
 && tar xvzf spark-3.5.1-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-3.5.1-bin-hadoop3.tgz

# Install python deps
COPY ./requirements.txt .
RUN pip3 install -r requirements.txt

ENV PATH=/opt/spark/sbin:/opt/spark/bin:${PATH}

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

# Download Hadoop AWS jars compatible with Spark 3.5.1
RUN curl -L -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# KAFKA CONNECTOR - Updated for Spark 3.5.1
RUN curl -L -o ${SPARK_HOME}/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o ${SPARK_HOME}/jars/kafka-clients-3.4.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -L -o ${SPARK_HOME}/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o ${SPARK_HOME}/jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

COPY entrypoint.sh .

ENTRYPOINT ["./entrypoint.sh"]