FROM apache/superset:latest

USER root

COPY ./requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

ENV ADMIN_USERNAME $ADMIN_USERNAME
ENV ADMIN_EMAIL $ADMIN_EMAIL
ENV ADMIN_PASSWORD $ADMIN_PASSWORD

COPY ./init.sh /init.sh

COPY config.py /app/
ENV SUPERSET_CONFIG_PATH /app/config.py

USER superset
ENTRYPOINT [ "/init.sh" ]