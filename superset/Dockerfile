FROM apache/superset:3.1.0

USER root

COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./init.sh /init.sh
RUN sed -i 's/\r$//' /init.sh && chmod +x /init.sh

COPY config.py /app/
ENV SUPERSET_CONFIG_PATH=/app/config.py

USER superset
ENTRYPOINT [ "/bin/bash", "/init.sh" ]