
#!/usr/bin/env python3
"""
Churn Prediction Model Training
Production-ready code with MLflow integration
"""
import os
import warnings
warnings.filterwarnings('ignore')

import logging
import argparse
import joblib
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

import s3fs
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.metrics import (
    roc_auc_score, precision_score, recall_score,
    f1_score, average_precision_score, classification_report,
    confusion_matrix
)
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from imblearn.over_sampling import ADASYN
from imblearn.pipeline import Pipeline
import optuna

import mlflow
import mlflow.sklearn

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
THRESHOLDS = [0.3, 0.4, 0.5, 0.6, 0.7]


def load_data_from_minio(
    folder_path: str,
    minio_endpoint: str = "http://minio-svc:9000"
) -> pd.DataFrame:
    """Load parquet files from MinIO/Iceberg storage."""
    fs = s3fs.S3FileSystem(
        key=os.environ.get("AWS_ACCESS_KEY_ID"),
        secret=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        client_kwargs={"endpoint_url": minio_endpoint}
    )
    
    parquet_files = [f for f in fs.ls(folder_path) if f.endswith('.parquet')]
    
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {folder_path}")
    
    logger.info(f"Found {len(parquet_files)} parquet files in {folder_path}")
    
    # Read all parquet files
    dfs = []
    for pf in parquet_files:
        df = pd.read_parquet(f"s3://{pf}", filesystem=fs)
        dfs.append(df)
    
    df = pd.concat(dfs, ignore_index=True)
    
    # Clean up columns
    df = df.drop(columns=['ingestion_timestamp'], errors='ignore')
    df = df.dropna()
    
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    return df


class ChurnDataset:
    """Dataset handler for churn prediction."""
    
    def __init__(
        self,
        features_path: str,
        labels_path: str,
        minio_endpoint: str = "http://minio-svc:9000",
        test_size: float = 0.2,
        random_state: int = 42
    ):
        self.minio_endpoint = minio_endpoint
        self.test_size = test_size
        self.random_state = random_state
        
        # Load data
        logger.info("Loading features...")
        self.features = load_data_from_minio(features_path, minio_endpoint)
        
        logger.info("Loading labels...")
        self.labels = load_data_from_minio(labels_path, minio_endpoint)
        
        # Ensure alignment
        if len(self.features) != len(self.labels):
            raise ValueError(
                f"Features ({len(self.features)}) and labels ({len(self.labels)}) count mismatch"
            )
        
        # Get label column name
        self.label_col = self.labels.columns[0]
        self.y = self.labels[self.label_col].values.ravel()
        self.X = self.features.values
        self.feature_names = list(self.features.columns)
        
        logger.info(f"Dataset: {len(self.X)} samples, {len(self.feature_names)} features")
        logger.info(f"Label distribution: {np.bincount(self.y.astype(int))}")
    
    def split(self) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """Split data into train/test sets."""
        X_train, X_test, y_train, y_test = train_test_split(
            self.X, self.y,
            test_size=self.test_size,
            random_state=self.random_state,
            stratify=self.y
        )
        
        logger.info(f"Train set: {len(X_train)}, Test set: {len(X_test)}")
        return X_train, X_test, y_train, y_test
    
    def get_info(self) -> Dict[str, Any]:
        """Get dataset information."""
        return {
            'n_samples': len(self.X),
            'n_features': len(self.feature_names),
            'feature_names': str(self.feature_names),
            'label_col': self.label_col,
            'test_size': self.test_size
        }


class ChurnModelTrainer:
    """Trainer for churn prediction models with MLflow integration."""
    
    MODEL_CONFIGS = {
        'xgb': {
            'class': XGBClassifier,
            'default_params': {
                'random_state': 42,
                'eval_metric': 'aucpr',
                'use_label_encoder': False
            }
        },
        'lightgbm': {
            'class': LGBMClassifier,
            'default_params': {
                'random_state': 42,
                'objective': 'binary',
                'metric': 'average_precision',
                'verbose': -1
            }
        }
    }
    
    def __init__(
        self,
        dataset: ChurnDataset,
        experiment_name: str = "churn-prediction",
        run_name: Optional[str] = None
    ):
        self.dataset = dataset
        self.experiment_name = experiment_name
        self.run_name = run_name or f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Split data
        self.X_train, self.X_test, self.y_train, self.y_test = dataset.split()
        
        # Storage for trained models
        self.trained_models: Dict[str, Pipeline] = {}
        self.best_params: Dict[str, Dict] = {}
        self.metrics_history: Dict[str, Dict] = {}
    
    def _create_hyperparams(self, model_name: str, trial: optuna.Trial) -> Dict:
        """Create hyperparameters for Optuna trial."""
        if model_name == "xgb":
            return {
                'n_estimators': trial.suggest_int('n_estimators', 50, 500),
                'max_depth': trial.suggest_int('max_depth', 3, 15),
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
                'subsample': trial.suggest_float('subsample', 0.5, 1.0),
                'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
                'gamma': trial.suggest_float('gamma', 0, 5),
                'min_child_weight': trial.suggest_int('min_child_weight', 1, 10),
            }
        elif model_name == "lightgbm":
            return {
                'n_estimators': trial.suggest_int('n_estimators', 50, 500),
                'max_depth': trial.suggest_int('max_depth', 3, 15),
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
                'subsample': trial.suggest_float('subsample', 0.5, 1.0),
                'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
                'min_child_samples': trial.suggest_int('min_child_samples', 1, 50),
                'reg_alpha': trial.suggest_float('reg_alpha', 0.0, 5.0),
                'reg_lambda': trial.suggest_float('reg_lambda', 0.0, 5.0),
            }
        else:
            raise ValueError(f"Unknown model: {model_name}")
    
    def _calculate_metrics(
        self,
        y_true: np.ndarray,
        y_proba: np.ndarray,
        threshold: float = 0.5
    ) -> Dict[str, float]:
        """Calculate classification metrics."""
        y_pred = (y_proba >= threshold).astype(int)
        
        return {
            'roc_auc': roc_auc_score(y_true, y_proba),
            'avg_precision': average_precision_score(y_true, y_proba),
            'precision': precision_score(y_true, y_pred, zero_division=0),
            'recall': recall_score(y_true, y_pred, zero_division=0),
            'f1': f1_score(y_true, y_pred, zero_division=0)
        }
    
    def _train_single_model(
        self,
        model_name: str,
        n_trials: int = 15,
        n_splits: int = 5,
        thresholds: List[float] = THRESHOLDS
    ) -> Tuple[Pipeline, Dict, Dict]:
        """Train a single model with hyperparameter tuning."""
        
        model_config = self.MODEL_CONFIGS[model_name]
        model_cls = model_config['class']
        default_params = model_config['default_params']
        
        # Metrics storage
        metrics_history = {
            'train': {t: {'roc_auc': [], 'precision': [], 'recall': [], 'f1': []} 
                        for t in thresholds},
            'val': {t: {'roc_auc': [], 'precision': [], 'recall': [], 'f1': []} 
                    for t in thresholds}
        }
        
        logger.info(f"Starting Optuna optimization for {model_name} ({n_trials} trials)")
        
        def objective(trial):
            params = {**default_params, **self._create_hyperparams(model_name, trial)}
            skf = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
            scores = []
            
            for fold_idx, (train_idx, val_idx) in enumerate(skf.split(self.X_train, self.y_train)):
                X_tr, X_val = self.X_train[train_idx], self.X_train[val_idx]
                y_tr, y_val = self.y_train[train_idx], self.y_train[val_idx]
                
                # Pipeline: ADASYN + Model
                pipeline = Pipeline([
                    ('adasyn', ADASYN(random_state=42)),
                    ('clf', model_cls(**params))
                ])
                
                pipeline.fit(X_tr, y_tr)
                
                # Get predictions
                y_train_proba = pipeline.predict_proba(X_tr)[:, 1]
                y_val_proba = pipeline.predict_proba(X_val)[:, 1]
                
                # Calculate metrics for each threshold
                for threshold in thresholds:
                    y_train_pred = (y_train_proba >= threshold).astype(int)
                    y_val_pred = (y_val_proba >= threshold).astype(int)
                    
                    # Training metrics
                    metrics_history['train'][threshold]['roc_auc'].append(
                        roc_auc_score(y_tr, y_train_proba)
                    )
                    metrics_history['train'][threshold]['precision'].append(
                        precision_score(y_tr, y_train_pred, zero_division=0)
                    )
                    metrics_history['train'][threshold]['recall'].append(
                        recall_score(y_tr, y_train_pred, zero_division=0)
                    )
                    metrics_history['train'][threshold]['f1'].append(
                        f1_score(y_tr, y_train_pred, zero_division=0)
                    )
                    
                    # Validation metrics
                    metrics_history['val'][threshold]['roc_auc'].append(
                        roc_auc_score(y_val, y_val_proba)
                    )
                    metrics_history['val'][threshold]['precision'].append(
                        precision_score(y_val, y_val_pred, zero_division=0)
                    )
                    metrics_history['val'][threshold]['recall'].append(
                        recall_score(y_val, y_val_pred, zero_division=0)
                    )
                    metrics_history['val'][threshold]['f1'].append(
                        f1_score(y_val, y_val_pred, zero_division=0)
                    )
                
                scores.append(average_precision_score(y_val, y_val_proba))
            
            return np.mean(scores)
        
        # Optuna optimization
        optuna.logging.set_verbosity(optuna.logging.WARNING)
        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=n_trials, show_progress_bar=True)
        
        best_params = {**default_params, **study.best_params}
        logger.info(f"Best Average Precision: {study.best_value:.4f}")
        
        # Train final model
        final_pipeline = Pipeline([
            ('adasyn', ADASYN(random_state=42)),
            ('clf', model_cls(**best_params))
        ])
        final_pipeline.fit(self.X_train, self.y_train)
        
        return final_pipeline, best_params, metrics_history
    
    def _evaluate_on_test(
        self,
        model_name: str,
        pipeline: Pipeline,
        threshold: float = 0.5
    ) -> Dict[str, float]:
        """Evaluate model on test set."""
        y_proba = pipeline.predict_proba(self.X_test)[:, 1]
        y_pred = (y_proba >= threshold).astype(int)
        
        metrics = self._calculate_metrics(self.y_test, y_proba, threshold)
        
        # Log confusion matrix
        cm = confusion_matrix(self.y_test, y_pred)
        logger.info(f"\n{model_name} Confusion Matrix (threshold={threshold}):\n{cm}")
        
        # Log classification report
        report = classification_report(self.y_test, y_pred, digits=4)
        logger.info(f"\n{model_name} Classification Report:\n{report}")
        
        return metrics
    
    def train_with_mlflow(
        self,
        models: List[str] = ['xgb', 'lightgbm'],
        n_trials: int = 15,
        n_splits: int = 5,
        thresholds: List[float] = THRESHOLDS,
        register_models: bool = True,
        best_threshold: float = 0.5
    ) -> Dict:
        """Train models with MLflow tracking."""
        
        mlflow.set_experiment(self.experiment_name)
        
        with mlflow.start_run(run_name=self.run_name) as parent_run:
            logger.info(f"MLflow Parent Run ID: {parent_run.info.run_id}")
            
            # Log dataset info
            data_info = self.dataset.get_info()
            mlflow.log_params({
                'n_samples': data_info['n_samples'],
                'n_features': data_info['n_features'],
                'test_size': data_info['test_size'],
                'n_trials': n_trials,
                'n_splits': n_splits,
                'best_threshold': best_threshold
            })
            
            results = {}
            
            for model_name in models:
                logger.info(f"Training {model_name.upper()}")
                
                with mlflow.start_run(run_name=f"{model_name}_training", nested=True):
                    mlflow.set_tag("model_type", model_name)
                    
                    # Train model
                    pipeline, best_params, metrics_history = self._train_single_model(
                        model_name=model_name,
                        n_trials=n_trials,
                        n_splits=n_splits,
                        thresholds=thresholds
                    )
                    
                    # Log parameters
                    for k, v in best_params.items():
                        mlflow.log_param(f"best_{k}", v)
                    
                    # Evaluate on test set
                    test_metrics = self._evaluate_on_test(
                        model_name, pipeline, best_threshold
                    )
                    
                    # Log test metrics
                    for metric_name, value in test_metrics.items():
                        mlflow.log_metric(f"test_{metric_name}", value)
                    
                    # Log model
                    model_info = mlflow.sklearn.log_model(
                        pipeline,
                        artifact_path=f"{model_name}_model",
                        registered_model_name=f"churn-{model_name}" if register_models else None
                    )
                    
                    logger.info(f"Model {model_name} logged to MLflow")
                    
                    # Store results
                    self.trained_models[model_name] = pipeline
                    self.best_params[model_name] = best_params
                    self.metrics_history[model_name] = metrics_history
                    
                    results[model_name] = {
                        'best_params': best_params,
                        'test_metrics': test_metrics
                    }
            
            # Find best model
            best_model_name = max(results.keys(), key=lambda x: results[x]['test_metrics']['f1'])
            mlflow.log_param('best_model', best_model_name)
            mlflow.log_metric('best_model_f1', results[best_model_name]['test_metrics']['f1'])
            
            logger.info(f"Best Model: {best_model_name}")
            
            return {
                'run_id': parent_run.info.run_id,
                'results': results,
                'best_model': best_model_name
            }


def main():
    parser = argparse.ArgumentParser(description='Churn Prediction Model Training')
    
    # Data paths
    parser.add_argument('--features-path', type=str, 
                        default='gold/data_source/churn_features/data',
                        help='Path to features data in MinIO')
    parser.add_argument('--labels-path', type=str,
                        default='gold/data_source/churn_label/data',
                        help='Path to labels data in MinIO')
    parser.add_argument('--minio-endpoint', type=str,
                        default='http://minio-svc:9000',
                        help='MinIO endpoint URL')
    
    # Training params
    parser.add_argument('--models', type=str, nargs='+',
                        default=['xgb', 'lightgbm'],
                        choices=['xgb', 'lightgbm'],
                        help='Models to train')
    parser.add_argument('--n-trials', type=int, default=15,
                        help='Number of Optuna trials')
    parser.add_argument('--n-splits', type=int, default=5,
                        help='Number of CV folds')
    parser.add_argument('--test-size', type=float, default=0.2,
                        help='Test set size')
    parser.add_argument('--best-threshold', type=float, default=0.5,
                        help='Best threshold for final evaluation')
    
    # MLflow params
    parser.add_argument('--mlflow-tracking-uri', type=str,
                        default='http://mlflow-svc:5000',
                        help='MLflow tracking server URI')
    parser.add_argument('--mlflow-experiment', type=str,
                        default='churn-prediction',
                        help='MLflow experiment name')
    parser.add_argument('--register-models', action='store_true',
                        help='Register models to MLflow Model Registry')
    parser.add_argument('--run-name', type=str, default=None,
                        help='MLflow run name')
    
    args = parser.parse_args()
    
    # Set MLflow tracking URI
    mlflow.set_tracking_uri(args.mlflow_tracking_uri)
    logger.info(f"MLflow Tracking URI: {args.mlflow_tracking_uri}")
    logger.info(f"MLflow Experiment: {args.mlflow_experiment}")
    
    try:
        # Load dataset
        dataset = ChurnDataset(
            features_path=args.features_path,
            labels_path=args.labels_path,
            minio_endpoint=args.minio_endpoint,
            test_size=args.test_size
        )
        
        # Initialize trainer
        trainer = ChurnModelTrainer(
            dataset=dataset,
            experiment_name=args.mlflow_experiment,
            run_name=args.run_name
        )
        
        # Train models
        results = trainer.train_with_mlflow(
            models=args.models,
            n_trials=args.n_trials,
            n_splits=args.n_splits,
            register_models=args.register_models,
            best_threshold=args.best_threshold
        )
        
        # Print final summary
        print("\n" + "="*60)
        print("TRAINING COMPLETE")
        print("="*60)
        print(f"MLflow Run ID: {results['run_id']}")
        print(f"Best Model: {results['best_model']}")
        print(f"Experiment: {args.mlflow_experiment}")
        print("="*60)
        
        return results
        
    except Exception as e:
        logger.error(f"Training failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()
