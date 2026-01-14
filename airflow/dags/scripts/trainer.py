from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from imblearn.over_sampling import ADASYN
import optuna
from sklearn.metrics import (
    roc_auc_score, precision_score, recall_score, 
    f1_score, average_precision_score
)
from imblearn.pipeline import Pipeline
from imblearn.over_sampling import ADASYN
from imblearn.pipeline import Pipeline
import numpy as np
import pandas as pd
import os
import s3fs
import mlflow
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_data_from_minio(
    folder_path: str,
    minio_endpoint: str = "http://minio-svc:9000"
) -> pd.DataFrame: 
    fs = s3fs.S3FileSystem(
        key=os.environ.get("AWS_ACCESS_KEY_ID"),
        secret=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        client_kwargs={"endpoint_url": minio_endpoint}
    )    
    parquet_files = [f for f in fs.ls(folder_path) if f.endswith('.parquet')]
    df = pd.read_parquet(f"s3://{parquet_files[0]}", filesystem=fs)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').set_index('date')
    df = df.drop(columns=['ticker', 'ingestion_timestamp'], errors='ignore')
    df = df.dropna()
    return df

churn_features = load_data_from_minio("gold/data_source/churn_features/data")
churn_label = load_data_from_minio("gold/data_source/churn_label/data")

x_train_val, x_test, y_train_val, y_test = train_test_split(churn_features, churn_label, test_size=0.2, random_state=42)

def create_params(model_name, trial):
    if model_name == "xgb":
        params = {
            'n_estimators': trial.suggest_int('n_estimators', 50, 500),
            'max_depth': trial.suggest_int('max_depth', 3, 15),
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
            'subsample': trial.suggest_float('subsample', 0.5, 1.0),
            'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
            'gamma': trial.suggest_float('gamma', 0, 5),
            'min_child_weight': trial.suggest_int('min_child_weight', 1, 10),
            'random_state': 42,
            'eval_metric': 'aucpr',
        }
    elif model_name == "lightgbm":
        params = {
            'n_estimators': trial.suggest_int('n_estimators', 50, 500),
            'max_depth': trial.suggest_int('max_depth', 3, 15),
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
            'subsample': trial.suggest_float('subsample', 0.5, 1.0),
            'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
            'min_child_samples': trial.suggest_int('min_child_samples', 1, 50),
            'reg_alpha': trial.suggest_float('reg_alpha', 0.0, 5.0),
            'reg_lambda': trial.suggest_float('reg_lambda', 0.0, 5.0),
            'random_state': 42,
            'objective': 'binary',
            'metric': 'average_precision'
        }
    else:
        raise ValueError("model_name must be 'xgb' or 'lightgbm'")
    return params

def print_metrics_summary(metrics_history, thresholds):
    """
    Print summary statistics for each threshold
    """
    print("\n" + "="*80)
    print("METRICS SUMMARY (Mean ± Std across epochs)")
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

def build_model(model_name: str, X_train_val, y_train_val, n_trials, n_splits=5, 
                thresholds=[0.3, 0.4, 0.5, 0.6, 0.7]):
    """
    Build model with hyperparameter tuning and comprehensive metrics tracking
    
    Parameters:
    -----------
    model_name : str
        'xgb' or 'lightgbm'
    X_train_val : DataFrame
        Training features
    y_train_val : Series
        Training labels
    n_trials : int
        Number of Optuna trials
    n_splits : int
        Number of CV folds
    thresholds : list
        Classification thresholds to evaluate
    
    Returns:
    --------
    best_pipeline : Pipeline
        Trained pipeline with best parameters
    metrics_history : dict
        Training and validation metrics for all thresholds
    """
    
    if model_name == "xgb":
        model_cls = XGBClassifier
    elif model_name == "lightgbm":
        model_cls = LGBMClassifier
    else:
        raise ValueError("model_name must be 'xgb' or 'lightgbm'")
    
    # Initialize metrics storage
    metrics_history = {
        'train': {t: {'roc_auc': [], 'precision': [], 'recall': [], 'f1': []} 
                  for t in thresholds},
        'val': {t: {'roc_auc': [], 'precision': [], 'recall': [], 'f1': []} 
                for t in thresholds}
    }
    
    def objective(trial):
        params = create_params(model_name, trial)
        skf = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
        scores = []

        for fold_idx, (train_idx, val_idx) in enumerate(skf.split(X_train_val, y_train_val)):
            X_tr, X_val = X_train_val.iloc[train_idx], X_train_val.iloc[val_idx]
            y_tr, y_val = y_train_val.iloc[train_idx], y_train_val.iloc[val_idx]

            # Pipeline: ADASYN + model
            pipeline = Pipeline([
                ('adasyn', ADASYN(random_state=42)),
                ('clf', model_cls(**params))
            ])

            pipeline.fit(X_tr, y_tr)
            
            # Get probability predictions
            y_train_proba = pipeline.predict_proba(X_tr)[:, 1]
            y_val_proba = pipeline.predict_proba(X_val)[:, 1]
            
            # Calculate metrics for each threshold
            for threshold in thresholds:
                # Training metrics
                y_train_pred = (y_train_proba >= threshold).astype(int)
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
                y_val_pred = (y_val_proba >= threshold).astype(int)
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
            
            # Use average precision for optimization
            scores.append(average_precision_score(y_val, y_val_proba))

        return np.mean(scores)

    # Optuna study
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=n_trials, show_progress_bar=True)

    print("Best hyperparameters:", study.best_params)
    print(f"Best Average Precision Score: {study.best_value:.4f}")

    # Train final model with best params
    best_params = study.best_params.copy()
    best_pipeline = Pipeline([
        ('adasyn', ADASYN(random_state=42)),
        ('clf', model_cls(**best_params))
    ])
    best_pipeline.fit(X_train_val, y_train_val)

    # Plot metrics
    # plot_metrics(metrics_history, thresholds, model_name)
    
    # Print summary statistics
    print_metrics_summary(metrics_history, thresholds)

    return best_pipeline, metrics_history

# Train XGBoost model
xgb_pipeline, xgb_metrics = build_model(
    model_name='xgb',
    X_train_val=x_train_val,
    y_train_val=y_train_val,
    n_trials=15,
    n_splits=5,
    thresholds=[0.3, 0.4, 0.5, 0.6, 0.7]
)

# # Train LightGBM model
# lightgbm_pipeline, lightgbm_metrics = build_model(
#     model_name='lightgbm',
#     X_train_val=x_train_val,
#     y_train_val=y_train_val,
#     n_trials=15,
#     n_splits=5,
#     thresholds=[0.3, 0.4, 0.5, 0.6, 0.7]
# )



# from sklearn.ensemble import StackingClassifier
# from sklearn.linear_model import LogisticRegression

# xgb_model = xgb_pipeline.named_steps['clf']
# lightgbm_model = lightgbm_pipeline.named_steps['clf']
# # features stacking
# X_meta_train = np.column_stack([
#     xgb_model.predict_proba(x_train_val)[:, 1],
#     lightgbm_model.predict_proba(x_train_val)[:, 1]
# ])

# # Meta model
# meta_model = LogisticRegression()
# meta_model.fit(X_meta_train, y_train_val)
    
# X_meta_test = np.column_stack([
#     xgb_model.predict_proba(x_test)[:, 1],
#     lightgbm_model.predict_proba(x_test)[:, 1]
# ])
# y_pred = meta_model.predict(X_meta_test)
# print(classification_report(y_test, y_pred, digits=4))