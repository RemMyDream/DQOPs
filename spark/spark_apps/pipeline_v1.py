from processor.raw_stooq_to_gold import main as gold_main
from processor.build_ml_features import main as build_ml_main
from ingestion.gdelt_stooq_to_bronze_v1 import main as bronze_main
from ingestion.csv_to_layer import ingest_csv
from processor.stooq_to_silver_v1 import main as silver_stooq_main

# if __name__ == "__main__":
#     ingest_csv(
#         csv_path="/opt/spark/data/gdelt.csv",
#         table_name="gdelt_gkg",
#         catalog="bronze"
#     )

#     ingest_csv(
#         csv_path="/opt/spark/data/sentiment.csv",
#         table_name="sentiment_features",
#         catalog="gold"
#     )

    # Gọi main() từ các module
    # bronze_main()
silver_stooq_main()
gold_main()
build_ml_main()
