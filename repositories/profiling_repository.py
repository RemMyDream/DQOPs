from typing import List, Optional, Dict, Any
from domain.entity.postgres_client import PostgresConnectionClient
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProfilingRepository(PostgresConnectionClient):
    """Repository for profiling_results CRUD"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def init_table(self):
        """Create profiling_results table if not exists"""
        logger.info("Initializing profiling_results table")
        query = """
            CREATE TABLE IF NOT EXISTS profiling_results (
                result_id SERIAL PRIMARY KEY,
                profile_run_id VARCHAR(100) NOT NULL,
                schema_name VARCHAR(100) NOT NULL,
                table_name VARCHAR(100) NOT NULL,
                column_name VARCHAR(100),
                dimension VARCHAR(50) NOT NULL,
                metric_name VARCHAR(100) NOT NULL,
                actual_value DOUBLE PRECISION,
                executed_sql TEXT,
                execution_time_ms INTEGER,
                error_message TEXT,
                column_type VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_profiling_run_id
                ON profiling_results(profile_run_id);
            CREATE INDEX IF NOT EXISTS idx_profiling_table
                ON profiling_results(schema_name, table_name);
            CREATE INDEX IF NOT EXISTS idx_profiling_dimension
                ON profiling_results(dimension);
            CREATE INDEX IF NOT EXISTS idx_profiling_created
                ON profiling_results(created_at DESC);
        """
        self.execute_query(query)
        logger.info("profiling_results table initialization completed")

    def get_results_by_run_id(self, run_id: str) -> List[Dict[str, Any]]:
        """Get all results for a specific profiling run"""
        query = """
            SELECT * FROM profiling_results
            WHERE profile_run_id = :run_id
            ORDER BY schema_name, table_name, column_name, dimension
        """
        return self.execute_query(query, {"run_id": run_id}) or []

    def get_results_by_table(
        self, schema_name: str, table_name: str, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get recent results for a specific table"""
        query = """
            SELECT * FROM profiling_results
            WHERE schema_name = :schema_name AND table_name = :table_name
            ORDER BY created_at DESC
            LIMIT :limit
        """
        return self.execute_query(query, {
            "schema_name": schema_name,
            "table_name": table_name,
            "limit": limit
        }) or []

    def get_latest_run_id(self, schema_name: str, table_name: str) -> Optional[str]:
        """Get the most recent run_id for a table"""
        query = """
            SELECT profile_run_id FROM profiling_results
            WHERE schema_name = :schema_name AND table_name = :table_name
            ORDER BY created_at DESC LIMIT 1
        """
        result = self.execute_query(query, {
            "schema_name": schema_name,
            "table_name": table_name
        })
        return result[0] if result else None

    def get_run_history(
        self, schema_name: str, table_name: str, limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get distinct run summaries for a table"""
        query = """
            SELECT profile_run_id,
                   MIN(created_at) as started_at,
                   COUNT(*) as total_metrics,
                   COUNT(CASE WHEN error_message IS NOT NULL THEN 1 END) as error_count
            FROM profiling_results
            WHERE schema_name = :schema_name AND table_name = :table_name
            GROUP BY profile_run_id
            ORDER BY MIN(created_at) DESC
            LIMIT :limit
        """
        return self.execute_query(query, {
            "schema_name": schema_name,
            "table_name": table_name,
            "limit": limit
        }) or []
