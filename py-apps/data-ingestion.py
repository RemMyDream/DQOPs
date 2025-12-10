import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import finnhub
import gdelt
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)`s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')

if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, FINNHUB_API_KEY]):
    logger.error("One or more required environment variables are missing.")
    raise EnvironmentError("Missing environment variables for database or Finnhub API.")

# Create database connection
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)


def create_tables():
    """Create tables if they don't exist"""
    with engine.connect() as conn:
        # GDELT Events table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS gdelt_events (
                id SERIAL PRIMARY KEY,
                globaleventid BIGINT,
                sqldate INTEGER,
                monthyear INTEGER,
                year INTEGER,
                fractiondate NUMERIC,
                actor1code TEXT,
                actor1name TEXT,
                actor1countrycode TEXT,
                actor2code TEXT,
                actor2name TEXT,
                actor2countrycode TEXT,
                isrootevent INTEGER,
                eventcode TEXT,
                eventbasecode TEXT,
                eventrootcode TEXT,
                quadclass INTEGER,
                goldsteinscale NUMERIC,
                nummentions INTEGER,
                numsources INTEGER,
                numarticles INTEGER,
                avgtone NUMERIC,
                actor1geo_type INTEGER,
                actor1geo_fullname TEXT,
                actor1geo_countrycode TEXT,
                actor1geo_lat NUMERIC,
                actor1geo_long NUMERIC,
                actor2geo_type INTEGER,
                actor2geo_fullname TEXT,
                actor2geo_countrycode TEXT,
                actor2geo_lat NUMERIC,
                actor2geo_long NUMERIC,
                actiongeo_type INTEGER,
                actiongeo_fullname TEXT,
                actiongeo_countrycode TEXT,
                actiongeo_lat NUMERIC,
                actiongeo_long NUMERIC,
                dateadded BIGINT,
                sourceurl TEXT,
                ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # GDELT GKG table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS gdelt_gkg (
                id SERIAL PRIMARY KEY,
                gkgrecordid TEXT,
                date BIGINT,
                sourcecollectionidentifier INTEGER,
                sourcecommonname TEXT,
                documentidentifier TEXT,
                counts TEXT,
                v2counts TEXT,
                themes TEXT,
                v2themes TEXT,
                locations TEXT,
                v2locations TEXT,
                persons TEXT,
                v2persons TEXT,
                organizations TEXT,
                v2organizations TEXT,
                v2tone TEXT,
                dates TEXT,
                gcam TEXT,
                sharingimage TEXT,
                relatedimages TEXT,
                socialimageembeds TEXT,
                socialvideoembeds TEXT,
                quotations TEXT,
                allnames TEXT,
                amounts TEXT,
                translationinfo TEXT,
                extras TEXT,
                ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Finnhub stock prices table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS finnhub_stock_prices (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10),
                current_price NUMERIC,
                change NUMERIC,
                percent_change NUMERIC,
                high NUMERIC,
                low NUMERIC,
                open NUMERIC,
                previous_close NUMERIC,
                timestamp TIMESTAMP,
                ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        conn.commit()
        logger.info("Tables created successfully")


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
        
        # Combine all dataframes
        combined_df = pd.concat(all_events, ignore_index=True)
        
        # Insert into PostgreSQL
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
        
        # Combine all dataframes
        combined_df = pd.concat(all_gkg, ignore_index=True)
        
        # Insert into PostgreSQL
        combined_df.to_sql('gdelt_gkg', engine, if_exists='append', index=False, method='multi')
        
        logger.info(f"Successfully ingested {len(combined_df)} GDELT GKG records from the last {days} days")
        
    except Exception as e:
        logger.error(f"Error ingesting GDELT GKG data: {str(e)}")
        raise


def ingest_finnhub_stock_data(symbols=['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']):
    """
    Ingest Finnhub stock price data
    
    Args:
        symbols: List of stock symbols to fetch
    """
    try:
        logger.info(f"Starting Finnhub stock data ingestion for symbols: {symbols}")
        
        finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)
        
        records = []
        
        for symbol in symbols:
            try:
                # Get quote data
                quote = finnhub_client.quote(symbol)
                
                record = {
                    'symbol': symbol,
                    'current_price': quote.get('c'),
                    'change': quote.get('d'),
                    'percent_change': quote.get('dp'),
                    'high': quote.get('h'),
                    'low': quote.get('l'),
                    'open': quote.get('o'),
                    'previous_close': quote.get('pc'),
                    'timestamp': datetime.fromtimestamp(quote.get('t', datetime.now().timestamp()))
                }
                
                records.append(record)
                logger.info(f"Fetched data for {symbol}")
                
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
    

def main():
    """Main ingestion pipeline"""
    logger.info("Starting data ingestion pipeline")
    
    create_tables()

    ingest_gdelt_events()
    ingest_gdelt_gkg()
    ingest_finnhub_stock_data(symbols=['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'META'])
    
    logger.info("Data ingestion pipeline completed successfully")


if __name__ == "__main__":
    main()