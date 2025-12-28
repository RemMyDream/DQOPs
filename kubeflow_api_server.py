#!/usr/bin/env python3
"""
Kubeflow Pipelines API Server
A lightweight API server for Kubeflow Pipelines in Docker Compose
"""

import os
import time
from flask import Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy import create_engine, text
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configuration from environment
DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'kubeflow_db')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Initialize database connection
def init_database():
    """Initialize database connection and create tables if needed"""
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            # Test connection
            conn.execute(text('SELECT 1'))
            logger.info("Database connection successful")
            
            # Create basic tables for Kubeflow Pipelines metadata
            # This is a simplified version - full Kubeflow Pipelines uses more complex schema
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS experiments (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    id SERIAL PRIMARY KEY,
                    experiment_id INTEGER REFERENCES experiments(id),
                    name VARCHAR(255) NOT NULL,
                    status VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.commit()
            logger.info("Database tables initialized")
        return engine
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        return None

# Wait for database to be ready
def wait_for_database(max_retries=30, delay=2):
    """Wait for database to be available"""
    for i in range(max_retries):
        try:
            engine = create_engine(DATABASE_URL)
            with engine.connect() as conn:
                conn.execute(text('SELECT 1'))
                logger.info("Database is ready")
                return engine
        except Exception as e:
            logger.warning(f"Database not ready yet (attempt {i+1}/{max_retries}): {e}")
            time.sleep(delay)
    logger.error("Database connection failed after all retries")
    return None

# API Routes
@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "service": "kubeflow-pipelines-api"}), 200

@app.route('/apis/v1beta1/experiments', methods=['GET'])
def list_experiments():
    """List all experiments"""
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT id, name, description, created_at FROM experiments"))
            experiments = [
                {
                    "id": str(row[0]),
                    "name": row[1],
                    "description": row[2] or "",
                    "created_at": str(row[3])
                }
                for row in result
            ]
            return jsonify({"experiments": experiments}), 200
    except Exception as e:
        logger.error(f"Error listing experiments: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/apis/v1beta1/experiments', methods=['POST'])
def create_experiment():
    """Create a new experiment"""
    try:
        data = request.get_json()
        name = data.get('name', 'default')
        description = data.get('description', '')
        
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            result = conn.execute(
                text("INSERT INTO experiments (name, description) VALUES (:name, :description) RETURNING id"),
                {"name": name, "description": description}
            )
            experiment_id = result.fetchone()[0]
            conn.commit()
            
            return jsonify({
                "id": str(experiment_id),
                "name": name,
                "description": description
            }), 201
    except Exception as e:
        logger.error(f"Error creating experiment: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/apis/v1beta1/runs', methods=['GET'])
def list_runs():
    """List all pipeline runs"""
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT r.id, r.name, r.status, r.created_at, e.name as experiment_name
                FROM pipeline_runs r
                LEFT JOIN experiments e ON r.experiment_id = e.id
            """))
            runs = [
                {
                    "id": str(row[0]),
                    "name": row[1],
                    "status": row[2] or "UNKNOWN",
                    "created_at": str(row[3]),
                    "experiment_name": row[4] or "default"
                }
                for row in result
            ]
            return jsonify({"runs": runs}), 200
    except Exception as e:
        logger.error(f"Error listing runs: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    logger.info("Starting Kubeflow Pipelines API Server...")
    
    # Wait for database
    engine = wait_for_database()
    if engine:
        init_database()
        logger.info("Starting Flask server on port 8888")
        app.run(host='0.0.0.0', port=8888, debug=False)
    else:
        logger.error("Failed to connect to database. Exiting.")
        exit(1)
