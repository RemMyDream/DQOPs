"""
Stock Price Prediction Inference Script
Loads trained model from MLflow and makes predictions
"""
import os
import argparse
import logging
import joblib
import json
import s3fs
import numpy as np
import pandas as pd
import mlflow
import mlflow.keras
from datetime import datetime
from typing import Dict, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants matching trainer.py
FEATURE_COLUMNS = [
    'MA_20', 'MA_200', 'EMA_26',
    'MACD', 'MACD_signal',
    'RSI', 'Stoch_K', 'ROC',
    'BB_width', 'ATR',
    'OBV', 'VWAP',
    'Return', 'Price_Change_Pct',
    'High_Low_Range', 'Open_Close_Range',
    'sentiment_sum', 'polarity_ratio'
]
TARGET_COLUMN = 'next_day_return'
NON_SENTIMENT_FEATURES = [col for col in FEATURE_COLUMNS 
                          if col not in ['sentiment_sum', 'polarity_ratio']]

def load_data_from_minio(
    stock: str,
    look_back: int,
    folder_path: str = "gold/ml_features/data",
    minio_endpoint: str = "http://minio-service.default.svc.cluster.local:9000",
) -> pd.DataFrame:
    """
    Loads data from MinIO.
    Returns enough history to cover the look_back window.
    """
    logger.info(f"Connecting to MinIO at {minio_endpoint}...")
    
    try:
        fs = s3fs.S3FileSystem(
            key=os.environ.get("AWS_ACCESS_KEY_ID"),
            secret=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            client_kwargs={"endpoint_url": minio_endpoint}
        )
        
        # Find parquet files
        parquet_files = [f for f in fs.ls(folder_path) if f.endswith('.parquet')]
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in {folder_path}")
            
        logger.info(f"Reading {parquet_files[0]}...")
        df = pd.read_parquet(f"s3://{parquet_files[0]}", filesystem=fs)
        
        # Filter for stock
        df = df[df['ticker'] == stock].copy()
        
        if df.empty:
            raise ValueError(f"No data found for stock: {stock}")
        
        # Sort by date
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').set_index('date')
            
        # Clean data
        df = df.drop(columns=['ticker', 'ingestion_timestamp'], errors='ignore')
        df = df.dropna()
        
        if len(df) < look_back:
            raise ValueError(
                f"Insufficient data. Need {look_back} rows, got {len(df)}"
            )
            
        # Return recent data with buffer
        return df.tail(look_back + 10)
        
    except Exception as e:
        logger.error(f"Failed to load data from MinIO: {e}")
        raise

def get_model_and_artifacts(
    model_name: str,
    stage: str = "Production"
):
    """
    Downloads model and scalers from MLflow Model Registry.
    """
    logger.info(f"Loading model '{model_name}' from stage '{stage}'...")
    
    try:
        # Test MLflow connection
        mlflow.get_tracking_uri()
        logger.info(f"MLflow tracking URI: {mlflow.get_tracking_uri()}")
        
        model_uri = f"models:/{model_name}/{stage}"
        
        # Load Model
        model = mlflow.keras.load_model(model_uri)
        logger.info("Model loaded successfully.")
        
        # Get run_id for artifacts
        client = mlflow.tracking.MlflowClient()
        latest_versions = client.get_latest_versions(model_name, stages=[stage])
        
        if not latest_versions:
            raise ValueError(f"No model found for {model_name} in stage {stage}")
            
        run_id = latest_versions[0].run_id
        logger.info(f"Associated Run ID: {run_id}")
        
        # Download Scalers
        logger.info("Downloading scalers...")
        local_dir = mlflow.artifacts.download_artifacts(
            run_id=run_id,
            artifact_path="tmp_artifacts"
        )
        
        x_scaler = joblib.load(os.path.join(local_dir, "x_scaler.pkl"))
        y_scaler = joblib.load(os.path.join(local_dir, "y_scaler.pkl"))
        
        return model, x_scaler, y_scaler
        
    except Exception as e:
        logger.error(f"Failed to load model or artifacts: {e}")
        raise

def save_prediction_to_minio(
    result: Dict,
    fs: s3fs.S3FileSystem,
    output_path: str = "gold/predictions"
):
    """
    Saves prediction result to MinIO as JSON.
    """
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        stock = result['stock']
        filename = f"{output_path}/{stock}_prediction_{timestamp}.json"
        
        with fs.open(filename, 'w') as f:
            json.dump(result, f, indent=2)
        
        logger.info(f"Prediction saved to: {filename}")
        
    except Exception as e:
        logger.error(f"Failed to save prediction to MinIO: {e}")
        # Don't raise - prediction already printed to stdout

def predict(
    stock: str,
    look_back: int,
    get_sentiment: bool,
    model_name: str,
    minio_endpoint: str,
    save_to_minio: bool = False
):
    """
    Main prediction function.
    """
    try:
        # 1. Load Resources
        logger.info("=" * 60)
        logger.info(f"Starting prediction for {stock}")
        logger.info("=" * 60)
        
        model, x_scaler, y_scaler = get_model_and_artifacts(model_name)
        
        # 2. Load Data
        df = load_data_from_minio(
            stock,
            look_back,
            minio_endpoint=minio_endpoint
        )
        
        # 3. Preprocess Features (Match trainer.py exactly)
        feature_cols = (
            FEATURE_COLUMNS.copy()
            if get_sentiment
            else NON_SENTIMENT_FEATURES.copy()
        )
        
        # Ensure columns exist
        available_cols = [col for col in feature_cols if col in df.columns]
        
        if len(available_cols) != len(feature_cols):
            missing = set(feature_cols) - set(available_cols)
            logger.warning(f"Missing columns: {missing}")
            logger.info("Using available columns only")
        
        # Get exact last 'look_back' rows
        last_sequence_df = df[available_cols].tail(look_back)
        
        if len(last_sequence_df) != look_back:
            raise ValueError(
                f"Not enough data. Required: {look_back}, Got: {len(last_sequence_df)}"
            )
        
        # 4. Scale Data
        X_raw = last_sequence_df.values
        X_scaled = x_scaler.transform(X_raw)
        
        # Reshape for GRU: (1, look_back, n_features)
        X_input = X_scaled.reshape(1, look_back, X_scaled.shape[1])
        
        # 5. Inference
        logger.info("Running prediction...")
        pred_scaled = model.predict(X_input, verbose=0)
        
        # 6. Inverse Transform
        pred_actual = y_scaler.inverse_transform(pred_scaled)[0][0]
        
        # 7. Build Result
        result = {
            "timestamp": datetime.now().isoformat(),
            "stock": stock,
            "last_data_date": str(last_sequence_df.index[-1]),
            "predicted_next_day_return": float(pred_actual),
            "predicted_return_pct": float(pred_actual * 100),
            "prediction_direction": "UP" if pred_actual > 0 else "DOWN",
            "model_name": model_name,
            "look_back_window": look_back,
            "features_used": len(available_cols)
        }
        
        # 8. Output Result
        logger.info("=" * 60)
        logger.info("PREDICTION RESULT")
        logger.info("=" * 60)
        print(json.dumps(result, indent=2))
        logger.info("=" * 60)
        
        # 9. Optionally save to MinIO
        if save_to_minio:
            fs = s3fs.S3FileSystem(
                key=os.environ.get("AWS_ACCESS_KEY_ID"),
                secret=os.environ.get("AWS_SECRET_ACCESS_KEY"),
                client_kwargs={"endpoint_url": minio_endpoint}
            )
            save_prediction_to_minio(result, fs)
        
        return result
        
    except Exception as e:
        logger.error(f"Prediction failed: {e}", exc_info=True)
        # Return error result
        error_result = {
            "timestamp": datetime.now().isoformat(),
            "stock": stock,
            "error": str(e),
            "status": "failed"
        }
        print(json.dumps(error_result, indent=2))
        raise

def main():
    parser = argparse.ArgumentParser(
        description='Run stock price prediction inference'
    )
    parser.add_argument(
        "--stock",
        type=str,
        required=True,
        help="Stock ticker (e.g., NVDA, AAPL)"
    )
    parser.add_argument(
        "--model-name",
        type=str,
        default="stock-gru-model",
        help="MLflow model name"
    )
    parser.add_argument(
        "--look-back",
        type=int,
        default=45,
        help="Look back window size"
    )
    parser.add_argument(
        "--minio-endpoint",
        type=str,
        default="http://minio-service.default.svc.cluster.local:9000",
        help="MinIO endpoint URL"
    )
    parser.add_argument(
        "--no-sentiment",
        action="store_true",
        help="Exclude sentiment features"
    )
    parser.add_argument(
        "--save-to-minio",
        action="store_true",
        help="Save prediction results to MinIO"
    )
    
    args = parser.parse_args()
    
    # Setup MLflow Tracking URI
    if "MLFLOW_TRACKING_URI" not in os.environ:
        logger.warning(
            "MLFLOW_TRACKING_URI not set. "
            "Defaulting to http://mlflow-service.default.svc.cluster.local:5000"
        )
        os.environ["MLFLOW_TRACKING_URI"] = (
            "http://mlflow-service.default.svc.cluster.local:5000"
        )
    
    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    
    # Run prediction
    predict(
        stock=args.stock,
        look_back=args.look_back,
        get_sentiment=not args.no_sentiment,
        model_name=args.model_name,
        minio_endpoint=args.minio_endpoint,
        save_to_minio=args.save_to_minio
    )

if __name__ == "__main__":
    main()