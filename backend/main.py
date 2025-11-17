import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "../")) 
from utils.postgresql_client import PostgresSQLClient
from fastapi import FastAPI
from pydantic import BaseModel
from spark.spark_apps.postgres_connection import read_data_from_postgre

def load_cfg(cfg_file):
    cfg = None
    with open(cfg_file, "r") as f:
        try:
            cfg = yaml.safe_load(f)
        except yaml.YAMLError as e:
            print(e)
    return cfg

def run(table_cfg: dict):
    # Load system config
    cfg_file = "/opt/spark/utils/sys_conf/config.yaml" 
    cfg = load_cfg(cfg_file)

    # Configure spark
    app_name = cfg['spark']['app_name']
    # Configure data_source
    host = cfg['data_source']['host']
    port = cfg['data_source']['port']
    # Configure database
    database_name = table_cfg['database']
    schema_name = table_cfg['schema']
    table_name = table_cfg['table']
    user = table_cfg['user']
    password = table_cfg['password']
    # Configure lakehouse
    lakehouse_cfg = cfg['lakehouse']
    access_key = lakehouse_cfg['root_user']
    secret_key = lakehouse_cfg['root_password']
    bucket_name = database_name
    minio_endpoint = f"http://{lakehouse_cfg['endpoint']}"

        
    # Create minio client
    client = create_minio_connection(access_key=access_key, secret_key=secret_key, endpoint=lakehouse_cfg['endpoint'])
    if client:
        logger.info("Minio client exists")
    else:
        logger.info("Do not exist minio client")
    
    # Create bucket
    create_bucket(client=client, bucket_name=bucket_name)
    
    spark = create_spark_connection(app_name=app_name,
                                    access_key=access_key, 
                                    secret_key=secret_key, 
                                    endpoint=minio_endpoint)

    pc = PostgresSQLClient(database=database_name, user=user, password=password, host=host, port=port)
    
    # primary_keys = get_primary_keys(pc, schema=schema_name, table_name=table_name)
    # if not primary_keys:
    #     primary_keys = detect_primary_keys(pc, schema_name, table_name)
    # print("Primary keys detected:", primary_keys)
    
    primary_keys = ['datarow_id']
    
    if spark:
        df = read_data_from_postgre(spark=spark,
                                    pc=pc,
                                    schema=schema_name,
                                    table_name=table_name,
                                    primary_keys=primary_keys)
    
        if df: 
            layer = 'bronze'
            path = f"s3a://{bucket_name}/{layer}/{schema_name}/{table_name}"
            
            ingest_to_bronze(df, path, layer, table_name, spark, primary_keys)
            logger.info(f"Successfully load data from Postgre: {database_name}.{schema_name}.{table_name} to Minio: {layer}.{table_name}")
        else:
            logger.info("DataFrame does not exist")
    else:
        logger.info("Error when load data!")
    
    spark.sql("SHOW TABLES IN bronze").show(truncate=False)
    
    query = f"SELECT * FROM `bronze`.`{table_name}`"
    res = spark.sql(query)
    res.show(5)          # hiển thị 5 dòng đầu
    print(res.count())   # số lượng row
    res.printSchema()    # in schema cột

    spark.stop()

table_cfg = {
    'database': "bkhcn",
    'schema': "public",
    'table': "01_bct_bao_cao_thuc_hien_ke_hoach_san_xuat_kinh_doanh_1008476",
    'user': "postgres",
    'password': "postgres"
}


def run_test():
    # Load system config
    cfg_file = "/opt/spark/utils/sys_conf/config.yaml" 
    cfg = load_cfg(cfg_file)

    # Configure spark
    app_name = cfg['spark']['app_name']
    dwh_cfg = cfg['dwh']
    access_key = dwh_cfg['root_user']
    secret_key = dwh_cfg['root_password']
    spark_endpoint = f"http://{dwh_cfg['endpoint']}"
    bucket_name = dwh_cfg['bucket_name']
    
    spark = create_spark_connection(app_name = app_name,
                                    access_key=access_key, 
                                    secret_key=secret_key, 
                                    endpoint=spark_endpoint)
    
    layer = 'bronze'
    data_source = 'data_source'
    table_name = 'orders'
    path = str(f"s3a://{bucket_name}/{layer}/{data_source}/{table_name}")

    pc = psql_client(database="data_source", host = "data_source")

    with pc.engine.connect() as conn:
        df = pd.read_sql_query(f"""
            SELECT 
                column_name, 
                data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}' AND table_schema = 'public'
            ORDER BY ordinal_position;
        """, con=conn)

    project_root = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../"))

    dialects_folder = os.path.join(project_root, "data_quality/sensors")

    template_folder = os.path.join(project_root, "data_quality/sensors/column/accepted_values/text_found_in_set_percent")

    env = Environment(loader = FileSystemLoader([template_folder, dialects_folder]),
                  trim_blocks=True,
                  lstrip_blocks=True)
    
    template = env.get_template("spark.sql.jinja2")

    context = dict()

    context['target_table'] = dict()
    context['target_table']['schema_name'] = 'bronze'
    context['target_table']['table_name'] = 'data_source_orders'

    context['table'] = dict()
    # context['table']['filter'] = 'is_deleted = FALSE'
    context['table']['timestamp_columns'] = dict()
    context['table']['timestamp_columns']['event_timestamp_column'] = 'created_at'
    context['table']['timestamp_columns']['ingestion_timestamp_column'] = None
    context['table']['columns'] = dict()
    for index, row in df.iterrows():
        column_name = row.loc['column_name']
        data_type = row.loc['data_type']
        context['table']['columns'][column_name] = dict()
        context['table']['columns'][column_name]['type_snapshot'] = dict()

        context['table']['columns'][column_name]['type_snapshot']['column_type'] = data_type
        context['table']['columns'][column_name]['sql_expression'] = None


    context['column_name'] = 'status'

    context['error_sampling'] = dict()
    context['error_sampling']['samples_limit'] = 5
    context['error_sampling']['total_samples_limit'] = 1000
    context['error_sampling']['id_columns'] = ['order_id']

    context['data_groupings'] = dict()
    # context['data_groupings']['status'] = dict()
    # context['data_groupings']['status']['source'] = 'column_value'
    # context['data_groupings']['status']['column'] = 'status'

    context['time_series'] = dict()
    # context['time_series']['mode'] = None
    # context['time_series']['timestamp_column'] = l
    # context['time_series']['time_gradient'] = None

    context['time_window_filter'] = dict()
    # context['time_window_filter']['daily_partitioning_recent_days'] = 30
    # context['time_window_filter']['daily_partitioning_include_today'] = False

    context['parameters'] = dict()
    # context['parameters']['filter'] = 'num_of_item >= 2'
    context['parameters']['expected_values'] = ['Shipped', 'Processing', 'Returned']
    # context['parameters']['sql_expression'] = "100.0 * SUM(CASE WHEN {alias}.email LIKE '%@%' THEN 1 ELSE 0 END) / COUNT(*)"
    # context['parameters']['referenced_column']=
    # context['parameters']['referenced_table']=
    # min_date
    # max_date
    # percentile_value
    # context['parameters']['sql_condition'] = "email LIKE '%@%'"

    context['additional_filters'] = []
    
    sql = template.render(**context)
    print("=== Generated SQL ===\n")
    print(sql)
    spark.sql(sql.strip()).show()
    print("\n=== End of SQL ===")
    
    spark.stop()

run(table_cfg = table_cfg)