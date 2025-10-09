import json
import os

BASE_CONFIG = {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "source_data",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.autocreate.mode": "filtered",
    "include.schema.changes": "false",
     "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
}

DATABASES = {
    "data_source": ["public.orders"],
    "db2": ["public.devices", "public.users"],
    "db3": ["public.products", "public.categories"]
}

OUTPUT_DIR = "cdc_configs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

for db_name, tables in DATABASES.items():
    config = BASE_CONFIG.copy()
    config.update({
        "database.dbname": db_name,
        "database.server.name": db_name,
        "table.include.list": ",".join(tables),
        "topic.prefix": db_name, 
        "slot.name": db_name
    })
    connector = {
        "name": f"{db_name}-cdc",
        "config": config
    }

    path = os.path.join(OUTPUT_DIR, f"{db_name}-cdc.json")
    with open(path, "w") as f:
        json.dump(connector, f, indent=2)

    print(f"Created: {path}")

# Run: curl.exe -i -X POST -H "Accept: application/json" -H "Content-Type: application/json" http://localhost:8083/connectors/ -d "@cdc_configs/data_source-cdc.json"