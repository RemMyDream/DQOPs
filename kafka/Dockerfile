FROM debezium/connect:2.6.0.Final

USER root

# Install Debezium IBM i connector
RUN mkdir -p /kafka/connect/debezium-connector-ibmi && \
    curl -sSL https://repo1.maven.org/maven2/io/debezium/debezium-connector-ibmi/2.6.0.Final/debezium-connector-ibmi-2.6.0.Final-plugin.tar.gz \
    -o /tmp/debezium-ibmi.tar.gz && \
    tar -xzf /tmp/debezium-ibmi.tar.gz -C /kafka/connect/debezium-connector-ibmi --strip-components=1 && \
    rm /tmp/debezium-ibmi.tar.gz

# Install Confluent Avro Converter and dependencies
RUN mkdir -p /kafka/connect/confluent-avro && \
    curl -sSL https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/7.6.0/kafka-connect-avro-converter-7.6.0.jar \
    -o /kafka/connect/confluent-avro/kafka-connect-avro-converter-7.6.0.jar && \
    curl -sSL https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-data/7.6.0/kafka-connect-avro-data-7.6.0.jar \
    -o /kafka/connect/confluent-avro/kafka-connect-avro-data-7.6.0.jar && \
    curl -sSL https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/7.6.0/kafka-avro-serializer-7.6.0.jar \
    -o /kafka/connect/confluent-avro/kafka-avro-serializer-7.6.0.jar && \
    curl -sSL https://packages.confluent.io/maven/io/confluent/kafka-schema-serializer/7.6.0/kafka-schema-serializer-7.6.0.jar \
    -o /kafka/connect/confluent-avro/kafka-schema-serializer-7.6.0.jar && \
    curl -sSL https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/7.6.0/kafka-schema-registry-client-7.6.0.jar \
    -o /kafka/connect/confluent-avro/kafka-schema-registry-client-7.6.0.jar && \
    curl -sSL https://packages.confluent.io/maven/io/confluent/kafka-schema-converter/7.6.0/kafka-schema-converter-7.6.0.jar \
    -o /kafka/connect/confluent-avro/kafka-schema-converter-7.6.0.jar && \
    curl -sSL https://packages.confluent.io/maven/io/confluent/common-config/7.6.0/common-config-7.6.0.jar \
    -o /kafka/connect/confluent-avro/common-config-7.6.0.jar && \
    curl -sSL https://packages.confluent.io/maven/io/confluent/common-utils/7.6.0/common-utils-7.6.0.jar \
    -o /kafka/connect/confluent-avro/common-utils-7.6.0.jar

# Install Apache Avro
RUN curl -sSL https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.3/avro-1.11.3.jar \
    -o /kafka/connect/confluent-avro/avro-1.11.3.jar

# Install Guava and its dependencies
RUN curl -sSL https://repo1.maven.org/maven2/com/google/guava/guava/31.1-jre/guava-31.1-jre.jar \
    -o /kafka/connect/confluent-avro/guava-31.1-jre.jar && \
    curl -sSL https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar \
    -o /kafka/connect/confluent-avro/failureaccess-1.0.1.jar

# Install additional dependencies
RUN curl -sSL https://repo1.maven.org/maven2/org/apache/commons/commons-compress/1.21/commons-compress-1.21.jar \
    -o /kafka/connect/confluent-avro/commons-compress-1.21.jar && \
    curl -sSL https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.15.2/jackson-core-2.15.2.jar \
    -o /kafka/connect/confluent-avro/jackson-core-2.15.2.jar && \
    curl -sSL https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.15.2/jackson-databind-2.15.2.jar \
    -o /kafka/connect/confluent-avro/jackson-databind-2.15.2.jar && \
    curl -sSL https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.15.2/jackson-annotations-2.15.2.jar \
    -o /kafka/connect/confluent-avro/jackson-annotations-2.15.2.jar

RUN chown -R kafka:kafka /kafka/connect

USER kafka