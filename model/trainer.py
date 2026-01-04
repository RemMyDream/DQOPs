import os
import warnings
warnings.filterwarnings('ignore')
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

import logging
import argparse
import joblib
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

import s3fs
import numpy as np
import pandas as pd
from scipy import stats
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

import tensorflow as tf
from tensorflow.keras import Sequential
from tensorflow.keras.callbacks import EarlyStopping, Callback
from tensorflow.keras.layers import GRU, Dense, Dropout, BatchNormalization

import keras_tuner as kt
import mlflow
import mlflow.keras

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logging.getLogger('tensorflow').setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

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
    folder_path: str = "gold/ml_features/data",
    minio_endpoint: str = "http://localhost:9010",
) -> pd.DataFrame: 
    fs = s3fs.S3FileSystem(
        key=os.environ.get("AWS_ACCESS_KEY_ID"),
        secret=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        client_kwargs={"endpoint_url": minio_endpoint}
    )    
    parquet_files = [f for f in fs.ls(folder_path) if f.endswith('.parquet')]
    df = pd.read_parquet(f"s3://{parquet_files[0]}", filesystem=fs)
    df = df[df['ticker'] == stock].copy()
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').set_index('date')
    df = df.drop(columns=['ticker', 'ingestion_timestamp'], errors='ignore')
    df = df.dropna()
    return df

class StockData:    
    def __init__(
        self,
        data: pd.DataFrame,
        look_back: int = 30,
        rebalancing_frequency: int = 1,
        get_sentiment: bool = True,
        target_col: str = TARGET_COLUMN
    ):
        self.look_back = look_back
        self.rebalancing_frequency = rebalancing_frequency
        self.target_col = target_col
        
        if get_sentiment:
            self.feature_cols = FEATURE_COLUMNS.copy()
        else:
            self.feature_cols = NON_SENTIMENT_FEATURES.copy()
        
        available_cols = [col for col in self.feature_cols if col in data.columns]
        self.feature_cols = available_cols
        self.data = data[self.feature_cols + [target_col]].copy()
        
        if isinstance(self.data.index, pd.DatetimeIndex):
            self.dates = self.data.index.copy()
            self.data = self.data.reset_index(drop=True)
        else:
            self.dates = pd.RangeIndex(len(self.data))
        
        self.X = self.data[self.feature_cols].values
        self.y = self.data[target_col].values.reshape(-1, 1)
        self.x_scaler: Optional[MinMaxScaler] = None
        self.y_scaler: Optional[MinMaxScaler] = None
        
        logger.info(f"StockData initialized: {len(self.X)} samples, {len(self.feature_cols)} features")
    
    def create_sequences_from_raw(
        self, 
        X: np.ndarray, 
        y: np.ndarray, 
        fit_scaler: bool = True
    ) -> Tuple[np.ndarray, np.ndarray]:
        if fit_scaler:
            self.x_scaler = MinMaxScaler()
            self.y_scaler = MinMaxScaler()
            X_scaled = self.x_scaler.fit_transform(X)
            y_scaled = self.y_scaler.fit_transform(y)
        else:
            if self.x_scaler is None:
                raise ValueError("Scaler not fitted. Call with fit_scaler=True first.")
            X_scaled = self.x_scaler.transform(X)
            y_scaled = self.y_scaler.transform(y)
        
        X_seq, y_seq = self._create_sequences(X_scaled, y_scaled)
        return X_seq, y_seq
    
    def _create_sequences(
        self, 
        X: np.ndarray, 
        y: np.ndarray
    ) -> Tuple[np.ndarray, np.ndarray]:
        input_data = []
        target_data = []
        
        for i in range(0, len(X) - self.look_back, self.rebalancing_frequency):
            input_seq = X[i:i + self.look_back]
            target_value = y[i + self.look_back]
            input_data.append(input_seq)
            target_data.append(target_value)
        
        return np.array(input_data), np.array(target_data)
    
    def create_walk_forward_folds(
        self,
        initial_train_samples: int,
        test_window: Optional[int] = None,
        step_size: Optional[int] = None,
        anchored: bool = True,
        min_train_size: Optional[int] = None
    ) -> List[Dict]:
        n_samples = len(self.X)
        
        if test_window is None:
            test_window = self.rebalancing_frequency
        if step_size is None:
            step_size = test_window
        if min_train_size is None:
            min_train_size = self.look_back + 1
        
        folds = []
        fold_num = 0
        current_test_start = initial_train_samples
        current_train_start = 0
        
        while current_test_start < n_samples:
            test_start = current_test_start
            test_end = min(current_test_start + test_window, n_samples)
            
            if anchored:
                train_start = 0
            else:
                train_start = current_train_start
            train_end = test_start
            
            if train_end - train_start < self.look_back + 1:
                current_test_start += step_size
                if not anchored:
                    current_train_start += step_size
                continue
            
            if train_end - train_start < min_train_size:
                current_test_start += step_size
                if not anchored:
                    current_train_start += step_size
                continue
            
            if (test_end - test_start) < 1:
                break

            fold = {
                'fold': fold_num,
                'train_idx': (train_start, train_end),
                'test_idx': (test_start, test_end),
                'train': (self.X[train_start:train_end].copy(),
                         self.y[train_start:train_end].copy()),
                'test': (self.X[test_start:test_end].copy(),
                        self.y[test_start:test_end].copy()),
                'train_dates': self.dates[train_start:train_end],
                'test_dates': self.dates[test_start:test_end]
            }
            folds.append(fold)
            
            fold_num += 1
            current_test_start += step_size
            if not anchored:
                current_train_start += step_size
        
        logger.info(f"Created {len(folds)} walk-forward folds")
        return folds
    
    def inverse_transform(self, data: np.ndarray, scaler: str = 'y') -> np.ndarray:
        if scaler == 'y' and self.y_scaler is not None:
            return self.y_scaler.inverse_transform(data)
        elif scaler == 'x' and self.x_scaler is not None:
            return self.x_scaler.inverse_transform(data)
        else:
            raise ValueError(f"Scaler '{scaler}' not fitted.")
    
    def get_info(self) -> Dict[str, Any]:
        return {
            'total_samples': len(self.X),
            'n_features': len(self.feature_cols),
            'feature_cols': self.feature_cols,
            'look_back': self.look_back,
            'target_col': self.target_col,
            'date_range': (str(self.dates.min()), str(self.dates.max()))
        }

class GRUHyperModel(kt.HyperModel):
    def __init__(self, look_back: int, input_size: int):
        self.look_back = look_back
        self.input_size = input_size
    
    def build(self, hp) -> Sequential:
        from tensorflow.keras.layers import Input
        
        model = Sequential()
        model.add(Input(shape=(self.look_back, self.input_size)))
        
        num_gru_layers = hp.Int("num_gru_layers", 1, 3)
        optimizer_choice = hp.Choice('optimizer', ['adam', 'rmsprop'])
        learning_rate = hp.Float('learning_rate', min_value=1e-4, max_value=1e-2, sampling='log')
        use_batch_norm = hp.Boolean("batch_norm", default=True)
        
        for i in range(num_gru_layers):
            units = hp.Int(f"gru_units_{i}", min_value=32, max_value=200, step=32)
            dropout_rate = hp.Float(f"dropout_{i}", min_value=0.1, max_value=0.4, step=0.1)
            
            model.add(GRU(units=units, return_sequences=(i < num_gru_layers - 1)))
            
            if use_batch_norm:
                model.add(BatchNormalization())
            model.add(Dropout(dropout_rate))
        
        dense_units = hp.Int("dense_units", min_value=16, max_value=64, step=16)
        model.add(Dense(dense_units, activation='relu'))
        model.add(Dropout(0.2))
        model.add(Dense(1))
        
        if optimizer_choice == 'adam':
            optimizer = tf.keras.optimizers.Adam(learning_rate=learning_rate)
        else:
            optimizer = tf.keras.optimizers.RMSprop(learning_rate=learning_rate)
        
        model.compile(optimizer=optimizer, loss=tf.keras.losses.Huber(), metrics=['mae'])
        return model

class MLflowCallback(Callback):
    def on_epoch_end(self, epoch, logs=None):
        if logs:
            for key, value in logs.items():
                mlflow.log_metric(key, value, step=epoch)

class StockModel:
    def __init__(
        self,
        stock_data: StockData,
        folds: List[Dict],
        experiment_name: str = "stock-prediction",
        run_name: Optional[str] = None
    ):
        self.stock_data = stock_data
        self.folds = folds
        self.experiment_name = experiment_name
        self.run_name = run_name or f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.best_hps = None
        self.latest_model = None
        
        fold = folds[0]
        X_tmp, _ = stock_data.create_sequences_from_raw(
             fold['train'][0], fold['train'][1], fit_scaler=True
        )
        self.look_back = stock_data.look_back
        self.input_size = X_tmp.shape[2]
        self.feature_names = stock_data.feature_cols
        
        self.hypermodel = GRUHyperModel(
            look_back=self.look_back,
            input_size=self.input_size
        )
        
        logger.info(f"StockModel initialized: look_back={self.look_back}, input_size={self.input_size}")
    
    def tune_hyperparameters(
        self,
        max_trials: int = 50,
        epochs: int = 50,
        patience: int = 10,
        verbose: int = 1
    ) -> Dict:
        fold = self.folds[-1] 
        logger.info(f"Tuning hyperparameters on fold {fold['fold']} (most recent data)")
        
        X_train, y_train = self.stock_data.create_sequences_from_raw(
            fold['train'][0], fold['train'][1], fit_scaler=True
        )
        
        tuner_dir = f'tuner_dir_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        tuner = kt.BayesianOptimization(
            self.hypermodel,
            objective='val_loss',
            max_trials=max_trials,
            num_initial_points=min(10, max_trials // 3),
            directory=tuner_dir,
            project_name=f'{self.experiment_name}_gru_bayes'
        )
        
        early_stop = EarlyStopping(
            monitor='val_loss',
            patience=patience,
            restore_best_weights=True,
            min_delta=1e-5
        )
        
        tuner.search(
            X_train, y_train,
            epochs=epochs,
            validation_split=0.2,
            callbacks=[early_stop],
            verbose=verbose
        )
        
        self.best_hps = tuner.get_best_hyperparameters(num_trials=1)[0]
        
        best_params = {
            'num_gru_layers': self.best_hps.get('num_gru_layers'),
            'optimizer': self.best_hps.get('optimizer'),
            'learning_rate': self.best_hps.get('learning_rate'),
            'batch_norm': self.best_hps.get('batch_norm'),
            'dense_units': self.best_hps.get('dense_units'),
        }
        
        for i in range(self.best_hps.get('num_gru_layers')):
            best_params[f'gru_units_{i}'] = self.best_hps.get(f'gru_units_{i}')
            best_params[f'dropout_{i}'] = self.best_hps.get(f'dropout_{i}')
        
        logger.info(f"Best hyperparameters: {best_params}")
        return best_params
    
    def _build_model(self) -> Sequential:
        if self.best_hps is None:
            raise ValueError("Run tune_hyperparameters() first.")
        return self.hypermodel.build(self.best_hps)

    def _calculate_all_metrics(
        self,
        actuals: np.ndarray,
        predictions: np.ndarray
    ) -> Dict[str, float]:
        metrics = {
            'mse': mean_squared_error(actuals, predictions),
            'rmse': np.sqrt(mean_squared_error(actuals, predictions)),
            'mae': mean_absolute_error(actuals, predictions),
            'r2': r2_score(actuals, predictions)
        }
        
        y_dir = np.sign(np.diff(actuals))
        pred_dir = np.sign(np.diff(predictions))
        metrics['direction_accuracy'] = float(np.mean(y_dir == pred_dir))
        
        ic, _ = stats.spearmanr(actuals, predictions)
        metrics['ic'] = float(ic)
        
        pearson, _ = stats.pearsonr(actuals, predictions)
        metrics['pearson'] = float(pearson)
        
        strategy_returns = np.where(
            pred_dir == y_dir,
            np.abs(np.diff(actuals)),
            -np.abs(np.diff(actuals))
        )
        
        if len(strategy_returns) > 0 and np.std(strategy_returns) > 0:
            sharpe = np.sqrt(252) * np.mean(strategy_returns) / np.std(strategy_returns)
        else:
            sharpe = 0.0
        metrics['sharpe'] = float(sharpe)
        
        return metrics

    def train_with_mlflow(
        self,
        max_trials: int = 30,
        epochs: int = 100,
        patience: int = 15,
        batch_size: int = 32,
        register_model: bool = True,
        model_name: str = "stock-gru-model"
    ) -> Dict:
        mlflow.set_experiment(self.experiment_name)
        
        with mlflow.start_run(run_name=self.run_name) as parent_run:
            logger.info(f"Parent Run ID: {parent_run.info.run_id}")
            
            # Log global parameters
            data_info = self.stock_data.get_info()
            mlflow.log_params({
                **data_info,
                'training_epochs': epochs,
                'training_batch_size': batch_size,
                'training_patience': patience,
                'n_folds': len(self.folds)
            })
            
            # Hyperparameter tuning
            logger.info("Phase 1: Hyperparameter Tuning")
            best_params = self.tune_hyperparameters(
                max_trials=max_trials, 
                epochs=50,
                patience=10
            )
            mlflow.log_params({f"hp_{k}": v for k, v in best_params.items()})

            # Walk-forward validation
            all_predictions = []
            all_actuals = []
            fold_metrics = []
            
            logger.info("Phase 2: Walk-Forward Validation")
            
            for fold in self.folds:
                fold_num = fold['fold']
                
                with mlflow.start_run(run_name=f"Fold_{fold_num}", nested=True):
                    logger.info(f"  Training Fold {fold_num}")
                    
                    mlflow.set_tag("fold_index", fold_num)
                    mlflow.set_tag("parent_run_id", parent_run.info.run_id)
                    
                    X_train, y_train = self.stock_data.create_sequences_from_raw(
                        fold['train'][0], fold['train'][1], fit_scaler=True
                    )
                    X_test, y_test = self.stock_data.create_sequences_from_raw(
                        fold['test'][0], fold['test'][1], fit_scaler=False
                    )

                    if len(X_train) == 0 or len(X_test) == 0:
                        logger.warning(f"Fold {fold_num} has empty data. Skipping.")
                        continue

                    model = self._build_model()
                    
                    history = model.fit(
                        X_train, y_train,
                        validation_data=(X_test, y_test),
                        epochs=epochs,
                        batch_size=batch_size,
                        callbacks=[
                            EarlyStopping(monitor='val_loss', patience=patience, restore_best_weights=True),
                            MLflowCallback()
                        ],
                        verbose=0
                    )
                    
                    predictions = model.predict(X_test, verbose=0)
                    predictions_inv = self.stock_data.inverse_transform(predictions)
                    y_test_inv = self.stock_data.inverse_transform(y_test)
                    
                    mse = mean_squared_error(y_test_inv, predictions_inv)
                    
                    mlflow.log_metric("mse", mse)
                    mlflow.log_metric("epochs_trained", len(history.history['loss']))
                    
                    all_predictions.extend(predictions_inv.flatten())
                    all_actuals.extend(y_test_inv.flatten())
                    fold_metrics.append({'fold': fold_num, 'mse': mse})
                    
                    self.latest_model = model
                    tf.keras.backend.clear_session()
            
            # Calculate aggregate metrics
            all_predictions = np.array(all_predictions)
            all_actuals = np.array(all_actuals)
            
            if len(all_predictions) > 0:
                metrics = self._calculate_all_metrics(all_actuals, all_predictions)
                logger.info(f"Overall MSE: {metrics['mse']:.6f}")
                
                for k, v in metrics.items():
                    if v is not None:
                        mlflow.log_metric(f"avg_{k}", v)
            
            # Log scalers
            os.makedirs('tmp_artifacts', exist_ok=True)
            joblib.dump(self.stock_data.x_scaler, 'tmp_artifacts/x_scaler.pkl')
            joblib.dump(self.stock_data.y_scaler, 'tmp_artifacts/y_scaler.pkl')
            mlflow.log_artifact('tmp_artifacts/x_scaler.pkl')
            mlflow.log_artifact('tmp_artifacts/y_scaler.pkl')

            # Log model
            if self.latest_model:
                try:
                    local_model_path = "tmp_model.keras"
                    if os.path.exists(local_model_path):
                        os.remove(local_model_path)
                    
                    self.latest_model.save(local_model_path)
                    mlflow.log_artifact(local_model_path, artifact_path="model")
                    logger.info("Model logged successfully")
                    
                    if register_model:
                        model_uri = f"runs:/{parent_run.info.run_id}/model/tmp_model.keras"
                        try:
                            result = mlflow.register_model(model_uri, model_name)
                            logger.info(f"Model registered as '{model_name}' version {result.version}")
                        except Exception as e:
                            if "Successfully registered" in str(e):
                                logger.info(f"Model registered as '{model_name}'")
                            else:
                                logger.warning(f"Registration issue: {e}")
                    
                    if os.path.exists(local_model_path):
                        os.remove(local_model_path)
                        
                except Exception as e:
                    logger.error(f"Failed to log model: {e}")

            return {
                'run_id': parent_run.info.run_id,
                'metrics': metrics if len(all_predictions) > 0 else {},
                'best_params': best_params
            }

def main():
    parser = argparse.ArgumentParser(description='Train Stock Prediction Model')
    
    parser.add_argument('--stock', type=str, default='NVDA', help='Stock ticker')
    parser.add_argument('--minio-endpoint', type=str, default=None, help='MinIO endpoint')
    parser.add_argument('--look-back', type=int, default=45, help='Look back window')
    parser.add_argument('--get-sentiment', action='store_true', help='Include sentiment features')
    parser.add_argument('--initial-train-samples', type=int, default=800)
    parser.add_argument('--test-window', type=int, default=60)
    parser.add_argument('--step-size', type=int, default=60)
    parser.add_argument('--anchored', action='store_true', help='Use anchored window')
    parser.add_argument('--max-trials', type=int, default=10, help='Max hyperparameter trials')
    parser.add_argument('--epochs', type=int, default=50, help='Training epochs')
    parser.add_argument('--batch-size', type=int, default=32)
    parser.add_argument('--patience', type=int, default=10)
    parser.add_argument('--mlflow-tracking-uri', type=str, default='http://localhost:5000')
    parser.add_argument('--mlflow-experiment', type=str, default='stock-prediction')
    parser.add_argument('--register-model', action='store_true', help='Register model')
    parser.add_argument('--model-name', type=str, default='stock-gru-model')
    
    args = parser.parse_args()
    
    mlflow.set_tracking_uri(args.mlflow_tracking_uri)
    logger.info(f"MLflow Tracking URI: {args.mlflow_tracking_uri}")
    
    try:
        data = load_data_from_minio(stock=args.stock)
        
        stock_data = StockData(
            data=data,
            look_back=args.look_back,
            get_sentiment=args.get_sentiment,
            target_col=TARGET_COLUMN
        )
        
        folds = stock_data.create_walk_forward_folds(
            initial_train_samples=args.initial_train_samples,
            test_window=args.test_window,
            step_size=args.step_size,
            anchored=args.anchored
        )
        
        if not folds:
            logger.error("No folds created. Check data size or initial_train_samples.")
            return

        model = StockModel(
            stock_data=stock_data,
            folds=folds,
            experiment_name=args.mlflow_experiment,
            run_name=f"{args.stock}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        
        results = model.train_with_mlflow(
            max_trials=args.max_trials,
            epochs=args.epochs,
            patience=args.patience,
            batch_size=args.batch_size,
            register_model=args.register_model,
            model_name=args.model_name
        )
        
        print("\n" + "=" * 60)
        print("TRAINING SUMMARY")
        print("=" * 60)
        print(f"Stock: {args.stock}")
        print(f"Run ID: {results['run_id']}")
        print(f"\nMetrics:")
        for key, value in results.get('metrics', {}).items():
            print(f"  {key}: {value:.6f}")
        print("=" * 60)
        
        return results
        
    except Exception as e:
        logger.error(f"Training failed: {e}")
        raise

if __name__ == "__main__":
    main()