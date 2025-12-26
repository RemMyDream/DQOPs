FROM apache/superset:2.1.3

USER root

COPY ./requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ./init.sh /init.sh
COPY config.py /app/
ENV SUPERSET_CONFIG_PATH=/app/config.py

USER superset
ENTRYPOINT [ "/init.sh" ]