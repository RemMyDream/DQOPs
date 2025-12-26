from sqlalchemy import create_engine

# Thay username, password, host, port, database
uri = "clickhouse+http://admin:admin@host:8123/default"

engine = create_engine(uri)

try:
    with engine.connect() as conn:
        version = conn.execute("SELECT version()").fetchone()
        print("Connected OK. Version:", version[0])
except Exception as e:
    print("Connection failed:", e)
