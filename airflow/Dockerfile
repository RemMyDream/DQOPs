FROM apache/airflow:2.9.0-python3.10

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV AIRFLOW_HOME=/opt/airflow
ENV PATH=/home/airflow/.local/bin:${PATH}

WORKDIR ${AIRFLOW_HOME}

COPY requirements.txt .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

USER airflow
RUN pip install -r requirements.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.10.txt"

ENTRYPOINT ["/opt/airflow/entrypoint.sh"]
