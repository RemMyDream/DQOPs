#!/usr/bin/env python3
"""
Health Check Script
Monitors the status of all services in the data pipeline
"""

import subprocess
import requests
import psycopg2
from sqlalchemy import create_engine, text
import time
from datetime import datetime


class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'


def print_header(text):
    print(f"\n{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"{Colors.BLUE}{text.center(60)}{Colors.END}")
    print(f"{Colors.BLUE}{'='*60}{Colors.END}\n")


def print_status(service, status, message=""):
    status_color = Colors.GREEN if status == "OK" else Colors.RED if status == "FAIL" else Colors.YELLOW
    status_symbol = "✓" if status == "OK" else "✗" if status == "FAIL" else "⚠"
    
    print(f"{status_color}{status_symbol}{Colors.END} {service:<25} {status_color}[{status}]{Colors.END} {message}")


def check_docker_container(container_name):
    """Check if a Docker container is running"""
    try:
        result = subprocess.run(
            ['docker', 'ps', '--filter', f'name={container_name}', '--format', '{{.Names}}'],
            capture_output=True,
            text=True
        )
        return container_name in result.stdout
    except Exception:
        return False


def check_postgres():
    """Check PostgreSQL connection"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="sourcedb",
            user="postgres",
            password="postgres",
            connect_timeout=3
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        cursor.close()
        conn.close()
        return True, "Connected"
    except Exception as e:
        return False, str(e)[:50]


def check_minio():
    """Check MinIO connection"""
    try:
        response = requests.get("http://localhost:9000/minio/health/live", timeout=3)
        return response.status_code == 200, f"Status: {response.status_code}"
    except Exception as e:
        return False, str(e)[:50]


def check_spark():
    """Check Spark Master"""
    try:
        response = requests.get("http://localhost:8080", timeout=3)
        return response.status_code == 200, f"Status: {response.status_code}"
    except Exception as e:
        return False, str(e)[:50]


def check_openmetadata():
    """Check OpenMetadata"""
    try:
        response = requests.get("http://localhost:8585/api/v1/health", timeout=3)
        return response.status_code == 200, f"Status: {response.status_code}"
    except Exception as e:
        return False, str(e)[:50]


def check_elasticsearch():
    """Check Elasticsearch"""
    try:
        response = requests.get("http://localhost:9200/_cluster/health", timeout=3)
        if response.status_code == 200:
            health = response.json()
            return True, f"Status: {health.get('status', 'unknown')}"
        return False, f"Status: {response.status_code}"
    except Exception as e:
        return False, str(e)[:50]


def check_data_in_postgres():
    """Check if data exists in PostgreSQL tables"""
    try:
        engine = create_engine("postgresql://postgres:postgres@localhost:5432/sourcedb")
        
        with engine.connect() as conn:
            tables = ['gdelt_articles', 'finnhub_stock_prices', 'finnhub_company_news']
            counts = {}
            
            for table in tables:
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    counts[table] = result.scalar()
                except Exception:
                    counts[table] = 0
            
            return counts
    except Exception as e:
        return None


def check_bronze_layer():
    """Check if Bronze layer data exists"""
    try:
        # Try to query MinIO through Spark
        # This is a simplified check
        result = subprocess.run(
            ['docker', 'exec', 'spark-master', 'ls', '/opt/bitnami/spark'],
            capture_output=True,
            text=True
        )
        return result.returncode == 0
    except Exception:
        return False


def main():
    print_header("DATA PIPELINE HEALTH CHECK")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Check Docker containers
    print_header("Container Status")
    containers = [
        ('postgres-source', 'PostgreSQL'),
        ('minio', 'MinIO'),
        ('spark-master', 'Spark Master'),
        ('spark-worker-1', 'Spark Worker 1'),
        ('spark-worker-2', 'Spark Worker 2'),
        ('openmetadata-server', 'OpenMetadata'),
        ('elasticsearch', 'Elasticsearch')
    ]
    
    for container_name, display_name in containers:
        is_running = check_docker_container(container_name)
        status = "OK" if is_running else "FAIL"
        message = "Running" if is_running else "Not running"
        print_status(display_name, status, message)
    
    # Check service connectivity
    print_header("Service Connectivity")
    
    # PostgreSQL
    pg_ok, pg_msg = check_postgres()
    print_status("PostgreSQL", "OK" if pg_ok else "FAIL", pg_msg)
    
    # MinIO
    minio_ok, minio_msg = check_minio()
    print_status("MinIO", "OK" if minio_ok else "FAIL", minio_msg)
    
    # Spark
    spark_ok, spark_msg = check_spark()
    print_status("Spark Master", "OK" if spark_ok else "FAIL", spark_msg)
    
    # OpenMetadata
    om_ok, om_msg = check_openmetadata()
    print_status("OpenMetadata", "OK" if om_ok else "FAIL", om_msg)
    
    # Elasticsearch
    es_ok, es_msg = check_elasticsearch()
    print_status("Elasticsearch", "OK" if es_ok else "FAIL", es_msg)
    
    # Check data
    print_header("Data Status")
    
    counts = check_data_in_postgres()
    if counts:
        for table, count in counts.items():
            status = "OK" if count > 0 else "WARN"
            print_status(f"{table}", status, f"{count} records")
    else:
        print_status("PostgreSQL Tables", "FAIL", "Cannot query tables")
    
    # Summary
    print_header("Summary")
    
    all_containers_ok = all(check_docker_container(c[0]) for c in containers)
    all_services_ok = pg_ok and minio_ok and spark_ok
    
    if all_containers_ok and all_services_ok:
        print(f"{Colors.GREEN}✓ All critical services are operational{Colors.END}")
    else:
        print(f"{Colors.RED}✗ Some services are not operational{Colors.END}")
        print(f"\n{Colors.YELLOW}Troubleshooting steps:{Colors.END}")
        print("1. Check Docker logs: docker-compose logs [service-name]")
        print("2. Restart services: docker-compose restart")
        print("3. Rebuild containers: docker-compose down && docker-compose up -d")
    
    print()
    print_header("Quick Links")
    print(f"PostgreSQL:         localhost:5432")
    print(f"MinIO Console:      http://localhost:9001")
    print(f"Spark UI:           http://localhost:8080")
    print(f"OpenMetadata:       http://localhost:8585")
    print(f"Elasticsearch:      http://localhost:9200")
    print()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nHealth check interrupted by user")
    except Exception as e:
        print(f"\n{Colors.RED}Error running health check: {str(e)}{Colors.END}")