import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import finnhub
from pandas_datareader import data as pdr
import gdelt
import logging

from dotenv import load_dotenv
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'sourcedb')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')

if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, FINNHUB_API_KEY]):
    logger.error("One or more required environment variables are missing.")
    raise EnvironmentError("Missing environment variables for database or Finnhub API.")

# Create database connection
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)


def ingest_gdelt_events(days=1, max_records=None):
    """
    Ingest GDELT Events data from the last M days
    
    Args:
        days: Number of days to fetch data for (default: 1)
        max_records: Maximum number of records to fetch per day
    """
    try:
        logger.info(f"Starting GDELT Events ingestion for the last {days} days")
        
        gd = gdelt.gdelt(version=2)
        
        all_events = []
        
        # Fetch events for each day in the range
        for i in range(days):
            date = (datetime.now() - timedelta(days=i)).strftime('%Y %m %d')
            logger.info(f"Fetching GDELT events for {date}")
            
            try:
                results = gd.Search(date, table='events', coverage=True)
                
                if results is not None and not results.empty:
                    
                    if max_records is not None:
                        df = results.head(max_records)
                    else:
                        df = results

                    all_events.append(df)
                    logger.info(f"Fetched {len(df)} events for {date}")
                else:
                    logger.warning(f"No GDELT events found for {date}")
            except Exception as e:
                logger.error(f"Error fetching events for {date}: {str(e)}")
                continue
        
        if not all_events:
            logger.warning("No GDELT events found for the specified period")
            return
        
        
        combined_df = pd.concat(all_events, ignore_index=True)
        combined_df.columns = [col.lower() for col in combined_df.columns]
        
        combined_df.to_sql('gdelt_events', engine, if_exists='append', index=False, method='multi')
        
        logger.info(f"Successfully ingested {len(combined_df)} GDELT events from the last {days} days")
        
    except Exception as e:
        logger.error(f"Error ingesting GDELT events: {str(e)}")
        raise


def ingest_gdelt_gkg(days=1, max_records=None):
    """
    Ingest GDELT GKG (Global Knowledge Graph) data from the last M days
    
    Args:
        days: Number of days to fetch data for (default: 1)
        max_records: Maximum number of records to fetch per day
    """
    try:
        logger.info(f"Starting GDELT GKG ingestion for the last {days} days")
        
        gd = gdelt.gdelt(version=2)
        
        all_gkg = []
        
        # Fetch GKG data for each day in the range
        for i in range(days):
            date = (datetime.now() - timedelta(days=i)).strftime('%Y %m %d')
            logger.info(f"Fetching GDELT GKG data for {date}")
            
            try:
                results = gd.Search(date, table='gkg', coverage=True)
                
                if results is not None and not results.empty:
                    # Limit records per day
                    if max_records is not None:
                        df = results.head(max_records)
                    else:
                        df = results

                    all_gkg.append(df)
                    logger.info(f"Fetched {len(df)} GKG records for {date}")
                else:
                    logger.warning(f"No GDELT GKG data found for {date}")
            except Exception as e:
                logger.error(f"Error fetching GKG data for {date}: {str(e)}")
                continue
        
        if not all_gkg:
            logger.warning("No GDELT GKG data found for the specified period")
            return
        
        
        combined_df = pd.concat(all_gkg, ignore_index=True)
        combined_df.columns = [col.lower() for col in combined_df.columns]
        
        combined_df.to_sql('gdelt_gkg', engine, if_exists='append', index=False, method='multi')
        
        logger.info(f"Successfully ingested {len(combined_df)} GDELT GKG records from the last {days} days")
        
    except Exception as e:
        logger.error(f"Error ingesting GDELT GKG data: {str(e)}")
        raise


def ingest_finnhub_stock_data(symbols=['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']):
    """
    Ingest Finnhub stock price data using real-time quotes
    
    Args:
        symbols: List of stock symbols to fetch
    """
    try:
        logger.info(f"Starting Finnhub stock data ingestion for symbols: {symbols}")
        
        finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)
        
        records = []
        
        for symbol in symbols:
            try:
                logger.info(f"Fetching current quote for {symbol}")
                
                # Get real-time quote
                quote = finnhub_client.quote(symbol)
                
                if quote and quote.get('c'):  # 'c' is current price
                    record = {
                        'symbol': symbol,
                        'current_price': quote.get('c'),
                        'high': quote.get('h'),
                        'low': quote.get('l'),
                        'open': quote.get('o'),
                        'previous_close': quote.get('pc'),
                        'change': quote.get('d'),
                        'percent_change': quote.get('dp'),
                        'timestamp': datetime.fromtimestamp(quote.get('t', datetime.now().timestamp()))
                    }
                    records.append(record)
                    logger.info(f"Fetched quote for {symbol}: ${quote.get('c')}")
                else:
                    logger.warning(f"No quote data available for {symbol}")
                        
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {str(e)}")
                continue
        
        # Convert to DataFrame and insert
        if records:
            df = pd.DataFrame(records)
            df.to_sql('finnhub_stock_prices', engine, if_exists='append', index=False, method='multi')
            logger.info(f"Successfully ingested {len(df)} stock price records")
        else:
            logger.warning("No Finnhub stock data to ingest")
            
    except Exception as e:
        logger.error(f"Error ingesting Finnhub stock data: {str(e)}")
        raise


def ingest_stooq_stock_data(symbols=['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN'], days=30):
    """
    Ingest historical stock price data from Stooq
    
    Args:
        symbols: List of stock symbols to fetch
        days: Number of days of historical data to fetch (default: 30)
    """
    try:
        logger.info(f"Starting Stooq stock data ingestion for symbols: {symbols}")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        all_data = []
        
        for symbol in symbols:
            try:
                logger.info(f"Fetching historical data for {symbol} from {start_date.date()} to {end_date.date()}")
                
                # Fetch historical data from Stooq
                df = pdr.DataReader(symbol, 'stooq', start_date, end_date)
                
                if not df.empty:
                    # Reset index to make Date a column
                    df = df.reset_index()
                    
                    # Add symbol column
                    df['symbol'] = symbol
                    
                    # Rename columns to lowercase and more descriptive names
                    df.columns = [col.lower() for col in df.columns]
                    df = df.rename(columns={'date': 'timestamp'})
                    
                    all_data.append(df)
                    logger.info(f"Fetched {len(df)} records for {symbol}")
                else:
                    logger.warning(f"No data available for {symbol}")
                        
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {str(e)}")
                continue
        
        # Convert to DataFrame and insert
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df.to_sql('stooq_stock_prices', engine, if_exists='append', index=False, method='multi')
            logger.info(f"Successfully ingested {len(combined_df)} stock price records from Stooq")
        else:
            logger.warning("No Stooq stock data to ingest")
            
    except Exception as e:
        logger.error(f"Error ingesting Stooq stock data: {str(e)}")
        raise
    

def main():
    """Main ingestion pipeline"""
    logger.info("Starting data ingestion pipeline")
    

    ingest_gdelt_events(days=30,max_records=100)
    ingest_gdelt_gkg(days=30,max_records=100)
    ingest_finnhub_stock_data(symbols=['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'META'])
    ingest_stooq_stock_data(symbols=['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'META'], days=30)
    
    logger.info("Data ingestion pipeline completed successfully")


if __name__ == "__main__":
    main()