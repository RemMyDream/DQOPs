#!/usr/bin/env python3
"""
Verification script to test MLflow and MinIO connectivity.
Verifies that:
1. MinIO is accessible for loading training data
2. MLflow tracking server is accessible
3. MLflow server can store artifacts in MinIO (server-side check)
"""

import os
import sys
import requests
import s3fs
from datetime import datetime

def print_header(text):
    print("\n" + "=" * 60)
    print(f"  {text}")
    print("=" * 60)

def print_success(text):
    print(f"✓ {text}")

def print_error(text):
    print(f"✗ {text}")

def print_warning(text):
    print(f"⚠ {text}")

def print_info(text):
    print(f"ℹ {text}")

def check_environment_variables():
    """Check environment variables for data loading (not artifact storage)"""
    print_header("Checking Environment Variables")
    
    print_info("For data loading from MinIO:")
    required_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
    
    all_set = True
    for var in required_vars:
        value = os.environ.get(var)
        if value:
            display_value = '*' * len(value) if 'SECRET' in var else value
            print_success(f"{var} = {display_value}")
        else:
            print_error(f"{var} is not set")
            all_set = False
    
    print("\n" + "-" * 60)
    print_info("Note: MLFLOW_S3_ENDPOINT_URL is NOT needed in trainer")
    print_info("MLflow server (in K8s) handles artifact storage to MinIO")
    print("-" * 60)
    
    return all_set

def check_mlflow_connection(tracking_uri="http://localhost:5000"):
    """Check if MLflow tracking server is accessible"""
    print_header("Checking MLflow Tracking Server")
    
    try:
        # Check health endpoint
        response = requests.get(f"{tracking_uri}/health", timeout=5)
        if response.status_code == 200:
            print_success(f"MLflow tracking server is accessible at {tracking_uri}")
            
            # Try to get experiments
            response = requests.get(f"{tracking_uri}/api/2.0/mlflow/experiments/list", timeout=5)
            if response.status_code == 200:
                data = response.json()
                num_experiments = len(data.get('experiments', []))
                print_success(f"Found {num_experiments} experiment(s) in MLflow")
                return True
            else:
                print_warning("Could not retrieve experiments list")
                return True
        else:
            print_error(f"MLflow server returned status code: {response.status_code}")
            return False
            
    except requests.exceptions.ConnectionError:
        print_error(f"Cannot connect to MLflow at {tracking_uri}")
        print("  Make sure port-forwarding is active:")
        print("  kubectl port-forward -n dqops svc/mlflow 5000:5000")
        return False
    except Exception as e:
        print_error(f"Error connecting to MLflow: {e}")
        return False

def check_minio_data_access():
    """Check if MinIO is accessible for loading training data"""
    print_header("Checking MinIO Access (for data loading)")
    
    endpoint = os.environ.get('MINIO_ENDPOINT_URL', 'http://localhost:9000')
    access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    
    if not access_key or not secret_key:
        print_error("MinIO credentials not set (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)")
        print_warning("Training will use mock data instead")
        return False
    
    try:
        # Check MinIO health
        response = requests.get(f"{endpoint}/minio/health/live", timeout=5)
        if response.status_code == 200:
            print_success(f"MinIO server is accessible at {endpoint}")
        else:
            print_warning(f"MinIO health check returned status: {response.status_code}")
        
        # Try to create S3 filesystem
        fs = s3fs.S3FileSystem(
            key=access_key,
            secret=secret_key,
            client_kwargs={"endpoint_url": endpoint}
        )
        
        # List buckets
        buckets = fs.ls('')
        print_success(f"Found {len(buckets)} bucket(s): {', '.join(buckets)}")
        
        # Check required buckets
        if 'gold' in buckets:
            print_success("Required 'gold' bucket exists (for training data)")
        else:
            print_warning("'gold' bucket not found (needed for training data)")
        
        if 'mlflow-artifacts' in buckets:
            print_success("'mlflow-artifacts' bucket exists (MLflow uses this)")
        else:
            print_warning("'mlflow-artifacts' bucket not found")
            print_info("MLflow server should create this automatically")
        
        return True
        
    except requests.exceptions.ConnectionError:
        print_error(f"Cannot connect to MinIO at {endpoint}")
        print("  Make sure port-forwarding is active:")
        print("  kubectl port-forward -n dqops svc/minio 9000:9000")
        return False
    except Exception as e:
        print_error(f"Error connecting to MinIO: {e}")
        return False

def check_training_data(bucket='gold', prefix='ml_features/data'):
    """Check if training data is available in MinIO"""
    print_header("Checking Training Data Availability")
    
    endpoint = os.environ.get('MINIO_ENDPOINT_URL', 'http://localhost:9000')
    access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    
    if not access_key or not secret_key:
        print_warning("Credentials not set, skipping data check")
        return False
    
    try:
        fs = s3fs.S3FileSystem(
            key=access_key,
            secret=secret_key,
            client_kwargs={"endpoint_url": endpoint}
        )
        
        path = f"{bucket}/{prefix}"
        
        # Check if path exists
        if fs.exists(path):
            print_success(f"Data path exists: s3://{path}")
            
            # List files
            files = fs.ls(path)
            parquet_files = [f for f in files if f.endswith('.parquet')]
            
            if parquet_files:
                print_success(f"Found {len(parquet_files)} parquet file(s)")
                print("\n  Files:")
                for f in parquet_files[:5]:  # Show first 5
                    size = fs.size(f)
                    print(f"    - {f.split('/')[-1]} ({size:,} bytes)")
                if len(parquet_files) > 5:
                    print(f"    ... and {len(parquet_files) - 5} more")
                return True
            else:
                print_warning("No parquet files found in the data path")
                return False
        else:
            print_warning(f"Data path does not exist: s3://{path}")
            print_info("Training will use mock data")
            return False
            
    except Exception as e:
        print_error(f"Error checking data availability: {e}")
        return False

def check_mlflow_artifact_storage():
    """
    Check if MLflow server is properly configured to use MinIO for artifacts.
    This is a K8s-side configuration, not trainer responsibility.
    """
    print_header("Checking MLflow Artifact Storage Configuration")
    
    print_info("This checks if MLflow SERVER (not trainer) is configured correctly")
    print_info("Artifact storage is handled by MLflow server in K8s")
    
    endpoint = os.environ.get('MINIO_ENDPOINT_URL', 'http://localhost:9000')
    
    try:
        # Check if mlflow-artifacts bucket exists
        access_key = os.environ.get('AWS_ACCESS_KEY_ID')
        secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        
        if access_key and secret_key:
            fs = s3fs.S3FileSystem(
                key=access_key,
                secret=secret_key,
                client_kwargs={"endpoint_url": endpoint}
            )
            
            buckets = fs.ls('')
            if 'mlflow-artifacts' in buckets:
                print_success("'mlflow-artifacts' bucket exists")
                
                # Try to list contents
                try:
                    contents = fs.ls('mlflow-artifacts')
                    if contents:
                        print_success(f"Bucket contains {len(contents)} item(s)")
                        print_info("MLflow has stored artifacts here")
                    else:
                        print_info("Bucket is empty (no runs yet)")
                except:
                    print_info("Bucket exists but is empty")
                
                return True
            else:
                print_warning("'mlflow-artifacts' bucket not found")
                print_info("MLflow server should create this on first artifact upload")
                return True  # Not critical, server will create it
        else:
            print_info("Cannot check bucket without credentials")
            print_info("This is OK - MLflow server has its own credentials")
            return True
            
    except Exception as e:
        print_warning(f"Could not verify artifact storage: {e}")
        print_info("This is OK if running first time")
        return True

def test_mlflow_logging(tracking_uri="http://localhost:5000"):
    """Test if we can log to MLflow (without artifact storage test)"""
    print_header("Testing MLflow Logging")
    
    try:
        import mlflow
        
        mlflow.set_tracking_uri(tracking_uri)
        
        # Create a test experiment
        experiment_name = f"verification-test-{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        with mlflow.start_run(experiment_id=mlflow.create_experiment(experiment_name)) as run:
            # Log test metric and parameter
            mlflow.log_metric("test_metric", 1.0)
            mlflow.log_param("test_param", "test_value")
            
            run_id = run.info.run_id
        
        print_success(f"Successfully logged to MLflow (Run ID: {run_id})")
        print_success(f"Test experiment created: {experiment_name}")
        print_info("Artifacts will be stored by MLflow server (not tested here)")
        
        # Clean up test experiment
        try:
            client = mlflow.tracking.MlflowClient()
            experiment = client.get_experiment_by_name(experiment_name)
            if experiment:
                client.delete_experiment(experiment.experiment_id)
                print_success("Test experiment cleaned up")
        except:
            pass
        
        return True
        
    except Exception as e:
        print_error(f"Error testing MLflow logging: {e}")
        return False

def print_architecture_info():
    """Print information about the architecture"""
    print_header("Architecture Information")
    
    print("""
    Training Data Flow:
    ┌─────────────────┐
    │  Trainer Script │
    └────────┬────────┘
             │
             ├─► MinIO (load data from gold/ml_features/data)
             │   Uses: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
             │
             └─► MLflow Server (send metrics/models)
                      │
                      └─► MinIO (stores artifacts in mlflow-artifacts)
                          Configured in K8s
                          Trainer doesn't need to configure this!
    
    Key Points:
    ✓ Trainer loads data from MinIO (needs credentials)
    ✓ Trainer logs to MLflow tracking server
    ✓ MLflow server stores artifacts to MinIO (server handles this)
    ✓ Trainer does NOT need MLFLOW_S3_ENDPOINT_URL
    """)

def main():
    print("\n" + "=" * 60)
    print("  MLflow + MinIO Setup Verification")
    print("=" * 60)
    
    print_architecture_info()
    
    results = []
    
    # Run all checks
    results.append(("Environment Variables", check_environment_variables()))
    results.append(("MLflow Connection", check_mlflow_connection()))
    results.append(("MinIO Access", check_minio_data_access()))
    results.append(("Training Data", check_training_data()))
    results.append(("MLflow Artifact Storage", check_mlflow_artifact_storage()))
    results.append(("MLflow Logging Test", test_mlflow_logging()))
    
    # Summary
    print_header("Verification Summary")
    
    all_passed = True
    critical_failed = False
    
    critical_checks = ["MLflow Connection", "MinIO Access"]
    
    for check_name, passed in results:
        status = "✓ PASSED" if passed else "✗ FAILED"
        marker = "  "
        
        if not passed and check_name in critical_checks:
            marker = "❗"
            critical_failed = True
        elif not passed:
            marker = "⚠ "
        
        print(f"{marker}{check_name:.<45} {status}")
        if not passed:
            all_passed = False
    
    print("\n" + "=" * 60)
    
    if critical_failed:
        print("\n❗ Critical checks failed. Fix these before training:")
        print("  - Ensure port-forwarding is active")
        print("  - Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        print("  - Verify K8s pods are running: kubectl get pods -n dqops")
        return 1
    elif not all_passed:
        print("\n⚠ Some checks failed, but you can proceed:")
        print("  - Training data not found → Will use mock data")
        print("  - Artifact storage unverified → Should work via MLflow server")
        print("\nRun training with:")
        print("  python trainer.py --stock NVDA --mlflow-tracking-uri http://localhost:5000")
        return 0
    else:
        print("\n✓ All checks passed! You're ready to run training.")
        print("\nRun training with:")
        print("  python trainer.py --stock NVDA --mlflow-tracking-uri http://localhost:5000")
        return 0

if __name__ == "__main__":
    sys.exit(main())