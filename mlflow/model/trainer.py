#!/usr/bin/env python3
"""
Churn Prediction Model Training with Comprehensive MLflow Logging
=================================================================
This script trains churn prediction models (XGBoost, LightGBM) with:
- Hyperparameter optimization via Optuna
- ADASYN oversampling for imbalanced data
- Comprehensive MLflow logging for Superset visualization
- Cross-validation metrics tracking across multiple thresholds

MLflow Tables for Superset:
- experiments: experiment metadata
- runs: run metadata and summary metrics
- params: hyperparameters (key-value)
- metrics: all logged metrics with step/timestamp
- tags: run tags
"""

import os
import warnings
warnings.filterwarnings('ignore')

import logging
import argparse
import json
import tempfile
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

import s3fs
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for containers
import matplotlib.pyplot as plt

from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.metrics import (
    roc_auc_score, precision_score, recall_score,
    f1_score, average_precision_score, classification_report,
    confusion_matrix, roc_curve, precision_recall_curve,
    log_loss, brier_score_loss, auc
)
from sklearn.calibration import calibration_curve
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from imblearn.over_sampling import ADASYN
from imblearn.pipeline import Pipeline
import optuna

import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature

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
    
    dfs = []
    for pf in parquet_files:
        df = pd.read_parquet(f"s3://{pf}", filesystem=fs)
        dfs.append(df)
    
    df = pd.concat(dfs, ignore_index=True)
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
        
        logger.info("Loading features...")
        self.features = load_data_from_minio(features_path, minio_endpoint)
        
        logger.info("Loading labels...")
        self.labels = load_data_from_minio(labels_path, minio_endpoint)
        
        if len(self.features) != len(self.labels):
            raise ValueError(
                f"Features ({len(self.features)}) and labels ({len(self.labels)}) count mismatch"
            )
        
        self.label_col = self.labels.columns[0]
        self.y = self.labels[self.label_col].values.ravel()
        self.X = self.features.values
        self.feature_names = list(self.features.columns)
        
        # Calculate class distribution
        unique, counts = np.unique(self.y, return_counts=True)
        self.class_distribution = dict(zip(unique.astype(int), counts))
        self.imbalance_ratio = counts.max() / counts.min() if len(counts) > 1 else 1.0
        
        logger.info(f"Dataset: {len(self.X)} samples, {len(self.feature_names)} features")
        logger.info(f"Label distribution: {self.class_distribution}")
        logger.info(f"Imbalance ratio: {self.imbalance_ratio:.2f}")
    
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
        """Get dataset information for MLflow logging."""
        return {
            'n_samples': len(self.X),
            'n_features': len(self.feature_names),
            'feature_names': self.feature_names,
            'label_col': self.label_col,
            'test_size': self.test_size,
            'class_distribution': self.class_distribution,
            'imbalance_ratio': self.imbalance_ratio,
            'n_positive': int(self.class_distribution.get(1, 0)),
            'n_negative': int(self.class_distribution.get(0, 0)),
            'positive_rate': float(self.y.mean())
        }


class ChurnModelTrainer:
    """Trainer for churn prediction models with comprehensive MLflow logging."""
    
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
        
        self.X_train, self.X_test, self.y_train, self.y_test = dataset.split()
        
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
                'scale_pos_weight': trial.suggest_float('scale_pos_weight', 1.0, 10.0),
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
                'num_leaves': trial.suggest_int('num_leaves', 20, 100),
            }
        else:
            raise ValueError(f"Unknown model: {model_name}")
    
    def _calculate_comprehensive_metrics(
        self,
        y_true: np.ndarray,
        y_proba: np.ndarray,
        threshold: float = 0.5
    ) -> Dict[str, float]:
        """Calculate comprehensive classification metrics."""
        y_pred = (y_proba >= threshold).astype(int)
        
        # Basic metrics
        metrics = {
            'roc_auc': roc_auc_score(y_true, y_proba),
            'avg_precision': average_precision_score(y_true, y_proba),
            'precision': precision_score(y_true, y_pred, zero_division=0),
            'recall': recall_score(y_true, y_pred, zero_division=0),
            'f1': f1_score(y_true, y_pred, zero_division=0),
            'log_loss': log_loss(y_true, y_proba),
            'brier_score': brier_score_loss(y_true, y_proba),
        }
        
        # Confusion matrix metrics
        tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()
        metrics.update({
            'true_positives': int(tp),
            'true_negatives': int(tn),
            'false_positives': int(fp),
            'false_negatives': int(fn),
            'specificity': tn / (tn + fp) if (tn + fp) > 0 else 0,
            'npv': tn / (tn + fn) if (tn + fn) > 0 else 0,  # Negative Predictive Value
            'fpr': fp / (fp + tn) if (fp + tn) > 0 else 0,  # False Positive Rate
            'fnr': fn / (fn + tp) if (fn + tp) > 0 else 0,  # False Negative Rate
        })
        
        # Balanced accuracy
        metrics['balanced_accuracy'] = (metrics['recall'] + metrics['specificity']) / 2
        
        # Matthews Correlation Coefficient
        denom = np.sqrt((tp + fp) * (tp + fn) * (tn + fp) * (tn + fn))
        metrics['mcc'] = (tp * tn - fp * fn) / denom if denom > 0 else 0
        
        return metrics
    
    def _log_cv_metrics_per_step(
        self,
        metrics_history: Dict,
        thresholds: List[float],
        prefix: str = ""
    ):
        """Log CV metrics with step for time-series visualization in Superset."""
        for split in ['train', 'val']:
            for threshold in thresholds:
                thresh_str = str(threshold).replace('.', '_')
                for metric_name in ['roc_auc', 'precision', 'recall', 'f1']:
                    values = metrics_history[split][threshold][metric_name]
                    for step, value in enumerate(values):
                        mlflow.log_metric(
                            f"{prefix}{split}_{metric_name}_thresh_{thresh_str}",
                            value,
                            step=step
                        )
    
    def _log_summary_metrics(
        self,
        metrics_history: Dict,
        thresholds: List[float],
        prefix: str = ""
    ):
        """Log summary statistics (mean, std) for each threshold."""
        for split in ['train', 'val']:
            for threshold in thresholds:
                thresh_str = str(threshold).replace('.', '_')
                for metric_name in ['roc_auc', 'precision', 'recall', 'f1']:
                    values = metrics_history[split][threshold][metric_name]
                    mlflow.log_metric(
                        f"{prefix}{split}_{metric_name}_thresh_{thresh_str}_mean",
                        np.mean(values)
                    )
                    mlflow.log_metric(
                        f"{prefix}{split}_{metric_name}_thresh_{thresh_str}_std",
                        np.std(values)
                    )
    
    def _log_trial_metrics_per_step(
        self,
        trial_metrics_history: Dict,
        thresholds: List[float],
        prefix: str = "trial_"
    ):
        """Log trial/epoch metrics with step for time-series visualization in Superset."""
        for split in ['train', 'val']:
            for threshold in thresholds:
                thresh_str = str(threshold).replace('.', '_')
                for metric_name in ['roc_auc', 'precision', 'recall', 'f1']:
                    values = trial_metrics_history[split][threshold][metric_name]
                    for step, value in enumerate(values):
                        mlflow.log_metric(
                            f"{prefix}{split}_{metric_name}_thresh_{thresh_str}",
                            value,
                            step=step
                        )
    
    def _create_and_log_plots(
        self,
        model_name: str,
        pipeline: Pipeline,
        metrics_history: Dict,
        thresholds: List[float],
        trial_metrics_history: Dict = None
    ):
        """Create and log visualization plots to MLflow."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # 1. Training metrics plot across FOLDS (original)
            self._plot_training_metrics(metrics_history, thresholds, model_name, tmpdir)
            
            # 2. NEW: Training metrics plot across TRIALS/EPOCHS (like the image you showed)
            if trial_metrics_history:
                self._plot_trial_metrics(trial_metrics_history, thresholds, model_name, tmpdir)
            
            # 3. ROC Curve
            self._plot_roc_curve(pipeline, model_name, tmpdir)
            
            # 4. Precision-Recall Curve
            self._plot_pr_curve(pipeline, model_name, tmpdir)
            
            # 5. Confusion Matrix for multiple thresholds
            self._plot_confusion_matrices(pipeline, thresholds, model_name, tmpdir)
            
            # 6. Feature Importance
            self._plot_feature_importance(pipeline, model_name, tmpdir)
            
            # 7. Calibration Curve
            self._plot_calibration_curve(pipeline, model_name, tmpdir)
            
            # 8. Threshold analysis plot
            self._plot_threshold_analysis(pipeline, thresholds, model_name, tmpdir)
            
            # Log all plots
            mlflow.log_artifacts(tmpdir, artifact_path="plots")
    
    def _plot_trial_metrics(
        self,
        trial_metrics_history: Dict,
        thresholds: List[float],
        model_name: str,
        save_dir: str
    ):
        """
        Plot training metrics across Optuna trials/epochs.
        This creates the 8-subplot figure (4 metrics x 2 splits) with 5 threshold lines each.
        """
        metrics_names = ['roc_auc', 'precision', 'recall', 'f1']
        metrics_labels = ['ROC-AUC', 'Precision', 'Recall', 'F1-Score']
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
        
        fig, axes = plt.subplots(2, 4, figsize=(20, 10))
        fig.suptitle(f'{model_name.upper()} - Training Metrics Across Trials', 
                     fontsize=16, fontweight='bold')
        
        for col_idx, (metric, label) in enumerate(zip(metrics_names, metrics_labels)):
            # Training metrics (top row)
            ax_train = axes[0, col_idx]
            for thresh_idx, threshold in enumerate(thresholds):
                values = trial_metrics_history['train'][threshold][metric]
                epochs = range(len(values))
                ax_train.plot(epochs, values, linewidth=1.5, 
                             label=f'threshold={threshold}',
                             color=colors[thresh_idx], alpha=0.9)
            
            ax_train.set_title(f'Training {label}', fontsize=12, fontweight='bold')
            ax_train.set_xlabel('Epochs', fontsize=10)
            ax_train.set_ylabel(label, fontsize=10)
            ax_train.legend(loc='best', fontsize=8)
            ax_train.grid(True, alpha=0.3)
            ax_train.set_ylim([0, 1.05])
            
            # Validation metrics (bottom row)
            ax_val = axes[1, col_idx]
            for thresh_idx, threshold in enumerate(thresholds):
                values = trial_metrics_history['val'][threshold][metric]
                epochs = range(len(values))
                ax_val.plot(epochs, values, linewidth=1.5,
                           label=f'threshold={threshold}', 
                           color=colors[thresh_idx], alpha=0.9)
            
            ax_val.set_title(f'Validation {label}', fontsize=12, fontweight='bold')
            ax_val.set_xlabel('Epochs', fontsize=10)
            ax_val.set_ylabel(label, fontsize=10)
            ax_val.legend(loc='best', fontsize=8)
            ax_val.grid(True, alpha=0.3)
            ax_val.set_ylim([0, 1.05])
        
        plt.tight_layout()
        plt.savefig(f'{save_dir}/{model_name}_trial_metrics.png', dpi=150, bbox_inches='tight')
        plt.close()
        
        # Also save as CSV for Superset (if needed to query)
        trial_data = []
        n_trials = len(trial_metrics_history['train'][thresholds[0]]['roc_auc'])
        for epoch in range(n_trials):
            for threshold in thresholds:
                row = {
                    'epoch': epoch,
                    'threshold': threshold,
                    'train_roc_auc': trial_metrics_history['train'][threshold]['roc_auc'][epoch],
                    'train_precision': trial_metrics_history['train'][threshold]['precision'][epoch],
                    'train_recall': trial_metrics_history['train'][threshold]['recall'][epoch],
                    'train_f1': trial_metrics_history['train'][threshold]['f1'][epoch],
                    'val_roc_auc': trial_metrics_history['val'][threshold]['roc_auc'][epoch],
                    'val_precision': trial_metrics_history['val'][threshold]['precision'][epoch],
                    'val_recall': trial_metrics_history['val'][threshold]['recall'][epoch],
                    'val_f1': trial_metrics_history['val'][threshold]['f1'][epoch],
                }
                trial_data.append(row)
        
        trial_df = pd.DataFrame(trial_data)
        trial_df.to_csv(f'{save_dir}/{model_name}_trial_metrics.csv', index=False)
        
        logger.info(f"Saved trial metrics plot and CSV for {model_name}")
    
    def _plot_training_metrics(
        self,
        metrics_history: Dict,
        thresholds: List[float],
        model_name: str,
        save_dir: str
    ):
        """Plot training metrics across folds."""
        metrics_names = ['roc_auc', 'precision', 'recall', 'f1']
        metrics_labels = ['ROC-AUC', 'Precision', 'Recall', 'F1-Score']
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
        
        fig, axes = plt.subplots(2, 4, figsize=(20, 10))
        fig.suptitle(f'{model_name.upper()} - Training Metrics Across Folds', 
                     fontsize=16, fontweight='bold')
        
        for col_idx, (metric, label) in enumerate(zip(metrics_names, metrics_labels)):
            # Training metrics (top row)
            ax_train = axes[0, col_idx]
            for thresh_idx, threshold in enumerate(thresholds):
                values = metrics_history['train'][threshold][metric]
                folds = range(1, len(values) + 1)
                ax_train.plot(folds, values, marker='o', linewidth=2, 
                             label=f'threshold={threshold}', markersize=4,  
                             color=colors[thresh_idx], alpha=0.8)
            
            ax_train.set_title(f'Training {label}', fontsize=12, fontweight='bold')
            ax_train.set_xlabel('Fold', fontsize=10)
            ax_train.set_ylabel(label, fontsize=10)
            ax_train.legend(loc='best', fontsize=8)
            ax_train.grid(True, alpha=0.3)
            ax_train.set_ylim([0, 1.05])
            
            # Validation metrics (bottom row)
            ax_val = axes[1, col_idx]
            for thresh_idx, threshold in enumerate(thresholds):
                values = metrics_history['val'][threshold][metric]
                folds = range(1, len(values) + 1)
                ax_val.plot(folds, values, marker='s', linewidth=2, markersize=4,
                           label=f'threshold={threshold}', 
                           color=colors[thresh_idx], alpha=0.8)
            
            ax_val.set_title(f'Validation {label}', fontsize=12, fontweight='bold')
            ax_val.set_xlabel('Fold', fontsize=10)
            ax_val.set_ylabel(label, fontsize=10)
            ax_val.legend(loc='best', fontsize=8)
            ax_val.grid(True, alpha=0.3)
            ax_val.set_ylim([0, 1.05])
        
        plt.tight_layout()
        plt.savefig(f'{save_dir}/{model_name}_training_metrics.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _plot_roc_curve(self, pipeline: Pipeline, model_name: str, save_dir: str):
        """Plot ROC curve."""
        y_proba = pipeline.predict_proba(self.X_test)[:, 1]
        fpr, tpr, _ = roc_curve(self.y_test, y_proba)
        roc_auc = auc(fpr, tpr)
        
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.plot(fpr, tpr, color='darkorange', lw=2, label=f'ROC curve (AUC = {roc_auc:.4f})')
        ax.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--', label='Random')
        ax.set_xlim([0.0, 1.0])
        ax.set_ylim([0.0, 1.05])
        ax.set_xlabel('False Positive Rate', fontsize=12)
        ax.set_ylabel('True Positive Rate', fontsize=12)
        ax.set_title(f'{model_name.upper()} - ROC Curve', fontsize=14, fontweight='bold')
        ax.legend(loc='lower right', fontsize=10)
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f'{save_dir}/{model_name}_roc_curve.png', dpi=150, bbox_inches='tight')
        plt.close()
        
        # Log ROC curve data as CSV for Superset
        roc_df = pd.DataFrame({'fpr': fpr, 'tpr': tpr})
        roc_df.to_csv(f'{save_dir}/{model_name}_roc_data.csv', index=False)
    
    def _plot_pr_curve(self, pipeline: Pipeline, model_name: str, save_dir: str):
        """Plot Precision-Recall curve."""
        y_proba = pipeline.predict_proba(self.X_test)[:, 1]
        precision, recall, thresholds = precision_recall_curve(self.y_test, y_proba)
        avg_precision = average_precision_score(self.y_test, y_proba)
        
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.plot(recall, precision, color='green', lw=2, 
                label=f'PR curve (AP = {avg_precision:.4f})')
        ax.axhline(y=self.y_test.mean(), color='navy', linestyle='--', 
                   label=f'Baseline (prevalence = {self.y_test.mean():.4f})')
        ax.set_xlim([0.0, 1.0])
        ax.set_ylim([0.0, 1.05])
        ax.set_xlabel('Recall', fontsize=12)
        ax.set_ylabel('Precision', fontsize=12)
        ax.set_title(f'{model_name.upper()} - Precision-Recall Curve', fontsize=14, fontweight='bold')
        ax.legend(loc='best', fontsize=10)
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f'{save_dir}/{model_name}_pr_curve.png', dpi=150, bbox_inches='tight')
        plt.close()
        
        # Log PR curve data as CSV
        pr_df = pd.DataFrame({
            'precision': precision[:-1], 
            'recall': recall[:-1], 
            'threshold': thresholds
        })
        pr_df.to_csv(f'{save_dir}/{model_name}_pr_data.csv', index=False)
    
    def _plot_confusion_matrices(
        self, 
        pipeline: Pipeline, 
        thresholds: List[float], 
        model_name: str, 
        save_dir: str
    ):
        """Plot confusion matrices for multiple thresholds."""
        y_proba = pipeline.predict_proba(self.X_test)[:, 1]
        n_thresholds = len(thresholds)
        
        fig, axes = plt.subplots(1, n_thresholds, figsize=(4*n_thresholds, 4))
        if n_thresholds == 1:
            axes = [axes]
        
        for ax, threshold in zip(axes, thresholds):
            y_pred = (y_proba >= threshold).astype(int)
            cm = confusion_matrix(self.y_test, y_pred)
            
            im = ax.imshow(cm, interpolation='nearest', cmap=plt.cm.Blues)
            ax.set_title(f'Threshold = {threshold}', fontsize=12, fontweight='bold')
            
            # Add text annotations
            thresh_color = cm.max() / 2.
            for i in range(cm.shape[0]):
                for j in range(cm.shape[1]):
                    ax.text(j, i, format(cm[i, j], 'd'),
                           ha="center", va="center",
                           color="white" if cm[i, j] > thresh_color else "black")
            
            ax.set_xlabel('Predicted', fontsize=10)
            ax.set_ylabel('Actual', fontsize=10)
            ax.set_xticks([0, 1])
            ax.set_yticks([0, 1])
            ax.set_xticklabels(['Negative', 'Positive'])
            ax.set_yticklabels(['Negative', 'Positive'])
        
        fig.suptitle(f'{model_name.upper()} - Confusion Matrices', fontsize=14, fontweight='bold')
        plt.tight_layout()
        plt.savefig(f'{save_dir}/{model_name}_confusion_matrices.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _plot_feature_importance(self, pipeline: Pipeline, model_name: str, save_dir: str):
        """Plot feature importance."""
        clf = pipeline.named_steps['clf']
        
        if hasattr(clf, 'feature_importances_'):
            importances = clf.feature_importances_
            indices = np.argsort(importances)[::-1][:20]  # Top 20
            
            fig, ax = plt.subplots(figsize=(10, 8))
            ax.barh(range(len(indices)), importances[indices], align='center')
            ax.set_yticks(range(len(indices)))
            ax.set_yticklabels([self.dataset.feature_names[i] for i in indices])
            ax.invert_yaxis()
            ax.set_xlabel('Feature Importance', fontsize=12)
            ax.set_title(f'{model_name.upper()} - Top 20 Feature Importances', 
                        fontsize=14, fontweight='bold')
            ax.grid(True, alpha=0.3, axis='x')
            
            plt.tight_layout()
            plt.savefig(f'{save_dir}/{model_name}_feature_importance.png', dpi=150, bbox_inches='tight')
            plt.close()
            
            # Log feature importance as CSV
            fi_df = pd.DataFrame({
                'feature': self.dataset.feature_names,
                'importance': importances
            }).sort_values('importance', ascending=False)
            fi_df.to_csv(f'{save_dir}/{model_name}_feature_importance.csv', index=False)
    
    def _plot_calibration_curve(self, pipeline: Pipeline, model_name: str, save_dir: str):
        """Plot calibration curve."""
        y_proba = pipeline.predict_proba(self.X_test)[:, 1]
        
        fig, ax = plt.subplots(figsize=(8, 6))
        
        fraction_of_positives, mean_predicted_value = calibration_curve(
            self.y_test, y_proba, n_bins=10, strategy='uniform'
        )
        
        ax.plot(mean_predicted_value, fraction_of_positives, "s-", 
                label=f'{model_name.upper()}', color='darkorange', lw=2)
        ax.plot([0, 1], [0, 1], "k--", label="Perfectly calibrated")
        
        ax.set_xlabel('Mean Predicted Probability', fontsize=12)
        ax.set_ylabel('Fraction of Positives', fontsize=12)
        ax.set_title(f'{model_name.upper()} - Calibration Curve', fontsize=14, fontweight='bold')
        ax.legend(loc='best', fontsize=10)
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f'{save_dir}/{model_name}_calibration_curve.png', dpi=150, bbox_inches='tight')
        plt.close()
        
        # Log calibration data
        cal_df = pd.DataFrame({
            'mean_predicted': mean_predicted_value,
            'fraction_positive': fraction_of_positives
        })
        cal_df.to_csv(f'{save_dir}/{model_name}_calibration_data.csv', index=False)
    
    def _plot_threshold_analysis(
        self, 
        pipeline: Pipeline, 
        thresholds: List[float], 
        model_name: str, 
        save_dir: str
    ):
        """Plot metrics vs threshold analysis."""
        y_proba = pipeline.predict_proba(self.X_test)[:, 1]
        
        # Compute metrics for fine-grained thresholds
        fine_thresholds = np.linspace(0.1, 0.9, 50)
        precision_list = []
        recall_list = []
        f1_list = []
        specificity_list = []
        
        for thresh in fine_thresholds:
            y_pred = (y_proba >= thresh).astype(int)
            precision_list.append(precision_score(self.y_test, y_pred, zero_division=0))
            recall_list.append(recall_score(self.y_test, y_pred, zero_division=0))
            f1_list.append(f1_score(self.y_test, y_pred, zero_division=0))
            
            tn, fp, fn, tp = confusion_matrix(self.y_test, y_pred).ravel()
            specificity_list.append(tn / (tn + fp) if (tn + fp) > 0 else 0)
        
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(fine_thresholds, precision_list, label='Precision', lw=2)
        ax.plot(fine_thresholds, recall_list, label='Recall', lw=2)
        ax.plot(fine_thresholds, f1_list, label='F1-Score', lw=2)
        ax.plot(fine_thresholds, specificity_list, label='Specificity', lw=2, linestyle='--')
        
        # Mark evaluated thresholds
        for thresh in thresholds:
            ax.axvline(x=thresh, color='gray', linestyle=':', alpha=0.5)
        
        ax.set_xlabel('Threshold', fontsize=12)
        ax.set_ylabel('Score', fontsize=12)
        ax.set_title(f'{model_name.upper()} - Threshold Analysis', fontsize=14, fontweight='bold')
        ax.legend(loc='best', fontsize=10)
        ax.grid(True, alpha=0.3)
        ax.set_xlim([0.1, 0.9])
        ax.set_ylim([0, 1.05])
        
        plt.tight_layout()
        plt.savefig(f'{save_dir}/{model_name}_threshold_analysis.png', dpi=150, bbox_inches='tight')
        plt.close()
        
        # Log threshold analysis data
        thresh_df = pd.DataFrame({
            'threshold': fine_thresholds,
            'precision': precision_list,
            'recall': recall_list,
            'f1': f1_list,
            'specificity': specificity_list
        })
        thresh_df.to_csv(f'{save_dir}/{model_name}_threshold_analysis.csv', index=False)
    
    def _train_single_model(
        self,
        model_name: str,
        n_trials: int = 15,
        n_splits: int = 5,
        thresholds: List[float] = THRESHOLDS
    ) -> Tuple[Pipeline, Dict, Dict, List, Any, Dict]:
        """Train a single model with hyperparameter tuning and comprehensive logging."""
        
        model_config = self.MODEL_CONFIGS[model_name]
        model_cls = model_config['class']
        default_params = model_config['default_params']
        
        # Metrics storage per fold (for fold-level analysis)
        metrics_history = {
            'train': {t: {'roc_auc': [], 'precision': [], 'recall': [], 'f1': [], 
                         'avg_precision': [], 'log_loss': []} for t in thresholds},
            'val': {t: {'roc_auc': [], 'precision': [], 'recall': [], 'f1': [],
                       'avg_precision': [], 'log_loss': []} for t in thresholds}
        }
        
        # NEW: Metrics storage per trial/epoch (for the plot you showed)
        # This tracks metrics across ALL trials (epochs) - accumulated from all folds
        trial_metrics_history = {
            'train': {t: {'roc_auc': [], 'precision': [], 'recall': [], 'f1': []} 
                      for t in thresholds},
            'val': {t: {'roc_auc': [], 'precision': [], 'recall': [], 'f1': []} 
                    for t in thresholds}
        }
        
        # Track Optuna trials for logging
        trial_results = []
        
        logger.info(f"Starting Optuna optimization for {model_name} ({n_trials} trials)")
        
        def objective(trial):
            params = {**default_params, **self._create_hyperparams(model_name, trial)}
            skf = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
            scores = []
            fold_metrics = []
            
            # Temporary storage for this trial's fold metrics
            trial_train_metrics = {t: {'roc_auc': [], 'precision': [], 'recall': [], 'f1': []} 
                                   for t in thresholds}
            trial_val_metrics = {t: {'roc_auc': [], 'precision': [], 'recall': [], 'f1': []} 
                                 for t in thresholds}
            
            for fold_idx, (train_idx, val_idx) in enumerate(skf.split(self.X_train, self.y_train)):
                X_tr, X_val = self.X_train[train_idx], self.X_train[val_idx]
                y_tr, y_val = self.y_train[train_idx], self.y_train[val_idx]
                
                pipeline = Pipeline([
                    ('adasyn', ADASYN(random_state=42)),
                    ('clf', model_cls(**params))
                ])
                
                pipeline.fit(X_tr, y_tr)
                
                y_train_proba = pipeline.predict_proba(X_tr)[:, 1]
                y_val_proba = pipeline.predict_proba(X_val)[:, 1]
                
                # Calculate and store metrics for each threshold
                for threshold in thresholds:
                    y_train_pred = (y_train_proba >= threshold).astype(int)
                    y_val_pred = (y_val_proba >= threshold).astype(int)
                    
                    # Training metrics - per fold
                    train_roc_auc = roc_auc_score(y_tr, y_train_proba)
                    train_precision = precision_score(y_tr, y_train_pred, zero_division=0)
                    train_recall = recall_score(y_tr, y_train_pred, zero_division=0)
                    train_f1 = f1_score(y_tr, y_train_pred, zero_division=0)
                    
                    metrics_history['train'][threshold]['roc_auc'].append(train_roc_auc)
                    metrics_history['train'][threshold]['precision'].append(train_precision)
                    metrics_history['train'][threshold]['recall'].append(train_recall)
                    metrics_history['train'][threshold]['f1'].append(train_f1)
                    metrics_history['train'][threshold]['avg_precision'].append(
                        average_precision_score(y_tr, y_train_proba))
                    metrics_history['train'][threshold]['log_loss'].append(
                        log_loss(y_tr, y_train_proba))
                    
                    # Store for trial-level aggregation
                    trial_train_metrics[threshold]['roc_auc'].append(train_roc_auc)
                    trial_train_metrics[threshold]['precision'].append(train_precision)
                    trial_train_metrics[threshold]['recall'].append(train_recall)
                    trial_train_metrics[threshold]['f1'].append(train_f1)
                    
                    # Validation metrics - per fold
                    val_roc_auc = roc_auc_score(y_val, y_val_proba)
                    val_precision = precision_score(y_val, y_val_pred, zero_division=0)
                    val_recall = recall_score(y_val, y_val_pred, zero_division=0)
                    val_f1 = f1_score(y_val, y_val_pred, zero_division=0)
                    
                    metrics_history['val'][threshold]['roc_auc'].append(val_roc_auc)
                    metrics_history['val'][threshold]['precision'].append(val_precision)
                    metrics_history['val'][threshold]['recall'].append(val_recall)
                    metrics_history['val'][threshold]['f1'].append(val_f1)
                    metrics_history['val'][threshold]['avg_precision'].append(
                        average_precision_score(y_val, y_val_proba))
                    metrics_history['val'][threshold]['log_loss'].append(
                        log_loss(y_val, y_val_proba))
                    
                    # Store for trial-level aggregation
                    trial_val_metrics[threshold]['roc_auc'].append(val_roc_auc)
                    trial_val_metrics[threshold]['precision'].append(val_precision)
                    trial_val_metrics[threshold]['recall'].append(val_recall)
                    trial_val_metrics[threshold]['f1'].append(val_f1)
                
                val_score = average_precision_score(y_val, y_val_proba)
                scores.append(val_score)
                fold_metrics.append({
                    'fold': fold_idx,
                    'val_avg_precision': val_score,
                    'val_roc_auc': roc_auc_score(y_val, y_val_proba)
                })
            
            # Aggregate fold metrics to trial-level (mean across folds for this trial)
            for threshold in thresholds:
                for metric in ['roc_auc', 'precision', 'recall', 'f1']:
                    trial_metrics_history['train'][threshold][metric].append(
                        np.mean(trial_train_metrics[threshold][metric])
                    )
                    trial_metrics_history['val'][threshold][metric].append(
                        np.mean(trial_val_metrics[threshold][metric])
                    )
            
            mean_score = np.mean(scores)
            
            # Store trial results
            trial_results.append({
                'trial_number': trial.number,
                'params': params.copy(),
                'mean_score': mean_score,
                'std_score': np.std(scores),
                'fold_metrics': fold_metrics
            })
            
            return mean_score
        
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
        
        return final_pipeline, best_params, metrics_history, trial_results, study, trial_metrics_history
    
    def _evaluate_on_test(
        self,
        model_name: str,
        pipeline: Pipeline,
        thresholds: List[float] = THRESHOLDS
    ) -> Dict[str, Dict[str, float]]:
        """Evaluate model on test set for all thresholds."""
        y_proba = pipeline.predict_proba(self.X_test)[:, 1]
        
        results = {}
        for threshold in thresholds:
            metrics = self._calculate_comprehensive_metrics(self.y_test, y_proba, threshold)
            results[threshold] = metrics
            
            # Log confusion matrix
            y_pred = (y_proba >= threshold).astype(int)
            cm = confusion_matrix(self.y_test, y_pred)
            logger.info(f"\n{model_name} Confusion Matrix (threshold={threshold}):\n{cm}")
        
        # Log classification report for default threshold
        y_pred = (y_proba >= 0.5).astype(int)
        report = classification_report(self.y_test, y_pred, digits=4)
        logger.info(f"\n{model_name} Classification Report (threshold=0.5):\n{report}")
        
        return results
    
    def train_with_mlflow(
        self,
        models: List[str] = ['xgb', 'lightgbm'],
        n_trials: int = 15,
        n_splits: int = 5,
        thresholds: List[float] = THRESHOLDS,
        register_models: bool = True,
        best_threshold: float = 0.5
    ) -> Dict:
        """Train models with comprehensive MLflow tracking for Superset."""
        
        mlflow.set_experiment(self.experiment_name)
        
        with mlflow.start_run(run_name=self.run_name) as parent_run:
            logger.info(f"MLflow Parent Run ID: {parent_run.info.run_id}")
            
            # Log dataset info as params
            data_info = self.dataset.get_info()
            mlflow.log_params({
                'dataset_n_samples': data_info['n_samples'],
                'dataset_n_features': data_info['n_features'],
                'dataset_test_size': data_info['test_size'],
                'dataset_imbalance_ratio': round(data_info['imbalance_ratio'], 4),
                'dataset_positive_rate': round(data_info['positive_rate'], 4),
                'dataset_n_positive': data_info['n_positive'],
                'dataset_n_negative': data_info['n_negative'],
                'training_n_trials': n_trials,
                'training_n_splits': n_splits,
                'training_thresholds': str(thresholds),
                'training_best_threshold': best_threshold,
                'training_timestamp': datetime.now().isoformat()
            })
            
            # Log dataset metrics
            mlflow.log_metrics({
                'dataset_samples': data_info['n_samples'],
                'dataset_features': data_info['n_features'],
                'dataset_positive_rate': data_info['positive_rate'],
                'dataset_imbalance_ratio': data_info['imbalance_ratio']
            })
            
            # Set tags for filtering in Superset
            mlflow.set_tags({
                'pipeline_type': 'churn_prediction',
                'data_source': 'minio_iceberg',
                'resampling_method': 'ADASYN',
                'cv_method': 'StratifiedKFold',
                'optimization_method': 'Optuna'
            })
            
            # Log feature names as artifact
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump({
                    'feature_names': data_info['feature_names'],
                    'n_features': data_info['n_features']
                }, f, indent=2)
                mlflow.log_artifact(f.name, artifact_path="data_info")
            
            results = {}
            
            for model_name in models:
                logger.info(f"\n{'='*60}")
                logger.info(f"Training {model_name.upper()}")
                logger.info(f"{'='*60}")
                
                with mlflow.start_run(run_name=f"{model_name}_training", nested=True) as child_run:
                    # Set model-specific tags
                    mlflow.set_tags({
                        'model_type': model_name,
                        'model_class': self.MODEL_CONFIGS[model_name]['class'].__name__,
                        'parent_run_id': parent_run.info.run_id
                    })
                    
                    # Train model
                    pipeline, best_params, metrics_history, trial_results, study, trial_metrics_history = self._train_single_model(
                        model_name=model_name,
                        n_trials=n_trials,
                        n_splits=n_splits,
                        thresholds=thresholds
                    )
                    
                    # Log best hyperparameters
                    for k, v in best_params.items():
                        if isinstance(v, (int, float, str, bool)):
                            mlflow.log_param(f"best_{k}", v)
                    
                    # Log Optuna study metrics
                    mlflow.log_metrics({
                        'optuna_best_value': study.best_value,
                        'optuna_n_trials': len(study.trials),
                        'optuna_best_trial': study.best_trial.number
                    })
                    
                    # Log CV metrics per step (for time-series plots in Superset)
                    self._log_cv_metrics_per_step(metrics_history, thresholds)
                    
                    # Log summary metrics (mean, std)
                    self._log_summary_metrics(metrics_history, thresholds)
                    
                    # NEW: Log trial/epoch metrics per step for Superset
                    self._log_trial_metrics_per_step(trial_metrics_history, thresholds)
                    
                    # Evaluate on test set for all thresholds
                    test_results = self._evaluate_on_test(model_name, pipeline, thresholds)
                    
                    # Log test metrics for each threshold
                    for threshold in thresholds:
                        thresh_str = str(threshold).replace('.', '_')
                        for metric_name, value in test_results[threshold].items():
                            if isinstance(value, (int, float)):
                                mlflow.log_metric(f"test_{metric_name}_thresh_{thresh_str}", value)
                    
                    # Log test metrics for best threshold separately (for easy comparison)
                    best_test_metrics = test_results[best_threshold]
                    for metric_name, value in best_test_metrics.items():
                        if isinstance(value, (int, float)):
                            mlflow.log_metric(f"test_{metric_name}", value)
                    
                    # Create and log plots (including trial metrics plot)
                    self._create_and_log_plots(
                        model_name, pipeline, metrics_history, thresholds, trial_metrics_history
                    )
                    
                    # Log Optuna trial results as artifact
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                        # Clean trial results for JSON serialization
                        clean_trials = []
                        for tr in trial_results:
                            clean_trial = {
                                'trial_number': tr['trial_number'],
                                'mean_score': tr['mean_score'],
                                'std_score': tr['std_score'],
                                'fold_metrics': tr['fold_metrics']
                            }
                            # Filter serializable params
                            clean_params = {}
                            for k, v in tr['params'].items():
                                if isinstance(v, (int, float, str, bool)):
                                    clean_params[k] = v
                            clean_trial['params'] = clean_params
                            clean_trials.append(clean_trial)
                        
                        json.dump(clean_trials, f, indent=2)
                        mlflow.log_artifact(f.name, artifact_path="optuna_trials")
                    
                    # Log model with signature
                    signature = infer_signature(self.X_test, pipeline.predict_proba(self.X_test))
                    model_info = mlflow.sklearn.log_model(
                        pipeline,
                        artifact_path=f"{model_name}_model",
                        signature=signature,
                        registered_model_name=f"churn-{model_name}" if register_models else None
                    )
                    
                    logger.info(f"Model {model_name} logged to MLflow")
                    
                    # Store results
                    self.trained_models[model_name] = pipeline
                    self.best_params[model_name] = best_params
                    self.metrics_history[model_name] = metrics_history
                    
                    results[model_name] = {
                        'run_id': child_run.info.run_id,
                        'best_params': best_params,
                        'test_metrics': {str(k): v for k, v in test_results.items()},
                        'cv_best_score': study.best_value
                    }
            
            # Find and log best model
            best_model_name = max(
                results.keys(), 
                key=lambda x: results[x]['test_metrics'][str(best_threshold)]['f1']
            )
            
            mlflow.log_param('best_model', best_model_name)
            mlflow.log_metrics({
                'best_model_f1': results[best_model_name]['test_metrics'][str(best_threshold)]['f1'],
                'best_model_roc_auc': results[best_model_name]['test_metrics'][str(best_threshold)]['roc_auc'],
                'best_model_avg_precision': results[best_model_name]['test_metrics'][str(best_threshold)]['avg_precision']
            })
            
            # Log comparison summary as artifact
            summary = {
                'experiment_name': self.experiment_name,
                'run_name': self.run_name,
                'parent_run_id': parent_run.info.run_id,
                'best_model': best_model_name,
                'best_threshold': best_threshold,
                'models_trained': models,
                'results_summary': {
                    model: {
                        'run_id': res['run_id'],
                        'cv_best_score': res['cv_best_score'],
                        'test_f1': res['test_metrics'][str(best_threshold)]['f1'],
                        'test_roc_auc': res['test_metrics'][str(best_threshold)]['roc_auc']
                    }
                    for model, res in results.items()
                }
            }
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump(summary, f, indent=2)
                mlflow.log_artifact(f.name, artifact_path="summary")
            
            logger.info(f"\nBest Model: {best_model_name}")
            
            return {
                'run_id': parent_run.info.run_id,
                'results': results,
                'best_model': best_model_name
            }


def print_metrics_summary(metrics_history: Dict, thresholds: List[float]):
    """Print summary statistics for each threshold."""
    print("\n" + "="*80)
    print("METRICS SUMMARY (Mean ± Std across folds)")
    print("="*80)
    
    for split in ['train', 'val']:
        print(f"\n{split.upper()} SET:")
        print("-" * 80)
        print(f"{'Threshold':<12} {'ROC-AUC':<18} {'Precision':<18} {'Recall':<18} {'F1-Score':<18}")
        print("-" * 80)
        
        for threshold in thresholds:
            roc_auc = metrics_history[split][threshold]['roc_auc']
            precision = metrics_history[split][threshold]['precision']
            recall = metrics_history[split][threshold]['recall']
            f1 = metrics_history[split][threshold]['f1']
            
            print(f"{threshold:<12.1f} "
                  f"{np.mean(roc_auc):.4f}±{np.std(roc_auc):.4f}    "
                  f"{np.mean(precision):.4f}±{np.std(precision):.4f}    "
                  f"{np.mean(recall):.4f}±{np.std(recall):.4f}    "
                  f"{np.mean(f1):.4f}±{np.std(f1):.4f}")
    
    print("="*80 + "\n")


def main():
    parser = argparse.ArgumentParser(description='Churn Prediction Model Training with MLflow')
    
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
    parser.add_argument('--thresholds', type=float, nargs='+',
                        default=[0.3, 0.4, 0.5, 0.6, 0.7],
                        help='Thresholds to evaluate')
    
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
            thresholds=args.thresholds,
            register_models=args.register_models,
            best_threshold=args.best_threshold
        )
        
        # Print summary for each model
        for model_name in args.models:
            if model_name in trainer.metrics_history:
                print(f"\n{model_name.upper()} Model:")
                print_metrics_summary(trainer.metrics_history[model_name], args.thresholds)
        
        # Print final summary
        print("\n" + "="*60)
        print("TRAINING COMPLETE")
        print("="*60)
        print(f"MLflow Run ID: {results['run_id']}")
        print(f"Best Model: {results['best_model']}")
        print(f"Experiment: {args.mlflow_experiment}")
        print(f"Tracking URI: {args.mlflow_tracking_uri}")
        print("="*60)
        
        return results
        
    except Exception as e:
        logger.error(f"Training failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()