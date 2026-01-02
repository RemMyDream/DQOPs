import sys
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pandas as pd
import finnhub
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from utils.helpers import create_logger, load_cfg

@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def connection_url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def validate(self) -> None:
        required = [self.host, self.port, self.database, self.user, self.password]
        if not all(required):
            raise ValueError("Missing required database configuration fields")

@dataclass
class StockQuote:
    """Stock quote data model."""
    symbol: str
    current_price: float
    high: float
    low: float
    open: float
    previous_close: float
    change: float
    percent_change: float
    timestamp: datetime

    def to_dict(self) -> dict:
        return {
            'symbol': self.symbol,
            'current_price': self.current_price,
            'high': self.high,
            'low': self.low,
            'open': self.open,
            'previous_close': self.previous_close,
            'change': self.change,
            'percent_change': self.percent_change,
            'timestamp': self.timestamp
        }

class DatabaseClient:
    """PostgreSQL database client."""

    def __init__(self, config: DatabaseConfig):
        self._config = config
        self._engine = None  
        self._logger = create_logger(name=self.__class__.__name__)

    @property
    def engine(self):  
        if self._engine is None:
            self._config.validate()
            self._engine = create_engine(self._config.connection_url)
        return self._engine

    def insert_dataframe(self, df: pd.DataFrame, table_name: str) -> int:
        try:
            df.to_sql(table_name, self.engine, if_exists='append', index=False, method='multi')
            self._logger.info(f"Inserted {len(df)} records into {table_name}")
            return len(df)
        except SQLAlchemyError as e:
            self._logger.error(f"Database error: {e}")
            raise


class StockDataProvider(ABC):
    """Abstract base class for stock data providers."""

    @abstractmethod
    def fetch_quote(self, symbol: str) -> Optional[StockQuote]:
        pass

    @abstractmethod
    def fetch_quotes(self, symbols: list[str]) -> list[StockQuote]:
        pass


class FinnhubProvider(StockDataProvider):
    """Finnhub API client for fetching stock data."""

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("Finnhub API key is required")
        self._client = finnhub.Client(api_key=api_key)
        self._logger = create_logger(name=self.__class__.__name__)

    def fetch_quote(self, symbol: str) -> Optional[StockQuote]:
        try:
            self._logger.info(f"Fetching quote for {symbol}")
            quote = self._client.quote(symbol)

            if not quote or not quote.get('c'):
                self._logger.warning(f"No quote data available for {symbol}")
                return None

            timestamp = datetime.fromtimestamp(
                quote.get('t', datetime.now().timestamp())
            )

            stock_quote = StockQuote(
                symbol=symbol,
                current_price=quote.get('c'),
                high=quote.get('h'),
                low=quote.get('l'),
                open=quote.get('o'),
                previous_close=quote.get('pc'),
                change=quote.get('d'),
                percent_change=quote.get('dp'),
                timestamp=timestamp
            )

            self._logger.info(f"Fetched {symbol}: ${stock_quote.current_price}")
            return stock_quote

        except Exception as e:
            self._logger.error(f"Error fetching {symbol}: {e}")
            return None

    def fetch_quotes(self, symbols: list[str]) -> list[StockQuote]:
        quotes = []
        for symbol in symbols:
            quote = self.fetch_quote(symbol)
            if quote:
                quotes.append(quote)
        return quotes


class StockIngestionService:
    """Service for ingesting stock data into database."""

    DEFAULT_SYMBOLS = ['MSFT', 'NVDA']
    TABLE_NAME = 'finnhub_stock_prices'

    def __init__(self, provider: StockDataProvider, db_client: DatabaseClient):
        self._provider = provider
        self._db_client = db_client
        self._logger = create_logger(name=self.__class__.__name__)

    def ingest(self, symbols: Optional[list[str]] = None) -> int:
        symbols = symbols or self.DEFAULT_SYMBOLS
        self._logger.info(f"Starting ingestion for: {symbols}")

        quotes = self._provider.fetch_quotes(symbols)

        if not quotes:
            self._logger.warning("No quotes to ingest")
            return 0

        df = pd.DataFrame([q.to_dict() for q in quotes])
        count = self._db_client.insert_dataframe(df, self.TABLE_NAME)

        self._logger.info(f"Ingestion completed: {count} records")
        return count


class AppConfig:
    """Application configuration loader."""

    def __init__(self, config_path: str = "utils/config.yaml"):
        self._config = load_cfg(config_path)

    @property
    def database_config(self) -> DatabaseConfig:
        source = self._config['data_source']
        return DatabaseConfig(
            host=source['host'],
            port=source['port'],
            database=source['database'],
            user=source['username'],
            password=source['password']
        )

    @property
    def finnhub_api_key(self) -> str:
        return self._config['finnhub']['api_key']


def create_ingestion_service(config_path: str = "utils/config.yaml") -> StockIngestionService:
    app_config = AppConfig(config_path)

    db_client = DatabaseClient(app_config.database_config)
    provider = FinnhubProvider(app_config.finnhub_api_key)

    return StockIngestionService(provider, db_client)


def main():
    logger = create_logger(name="Finnhub2Postgres")
    logger.info("Starting stock data ingestion pipeline")

    try:
        service = create_ingestion_service()
        count = service.ingest(symbols=['MSFT', 'NVDA'])
        logger.info(f"Pipeline completed: {count} records ingested")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()