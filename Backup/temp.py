import os
import requests
import pandas as pd
from io import StringIO
import warnings
from datetime import datetime, timedelta
import logging
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, Date, MetaData, inspect, UniqueConstraint, TEXT
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
import talib
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
load_dotenv()

logging.basicConfig(
    filename='../stock_downloader_date_range.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_engine():
    db_url = os.getenv('DATABASE_URL')
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    return create_engine(db_url, pool_pre_ping=True, pool_recycle=3600)

class CandlePatternRecognizer:
    def __init__(self):
        self.pattern_names = [
            'Hammer', 'InvertedHammer', 'DragonflyDoji', 'PiercingLine',
            'BullishMarubozu', 'BullishSpinningTop', 'HangingMan', 'ShootingStar',
            'GravestoneDoji', 'BearishSpinningTop', 'Doji', 'LongLeggedDoji'
        ]
        self.patterns = {
            'Hammer': talib.CDLHAMMER,
            'InvertedHammer': talib.CDLINVERTEDHAMMER,
            'DragonflyDoji': talib.CDLDRAGONFLYDOJI,
            'PiercingLine': talib.CDLPIERCING,
            'BullishMarubozu': talib.CDLMARUBOZU,
            'BullishSpinningTop': talib.CDLSPINNINGTOP,
            'HangingMan': talib.CDLHANGINGMAN,
            'ShootingStar': talib.CDLSHOOTINGSTAR,
            'GravestoneDoji': talib.CDLGRAVESTONEDOJI,
            'BearishSpinningTop': talib.CDLSPINNINGTOP,
            'Doji': talib.CDLDOJI,
            'LongLeggedDoji': talib.CDLLONGLEGGEDDOJI,
        }

    def apply_and_encode_patterns(self, df):
        df.columns = df.columns.str.strip()
        for name in self.pattern_names:
            df[name] = (self.patterns[name](df['Open'], df['High'], df['Low'], df['Close']) != 0).astype(int)
        df['pattern_value'] = df[self.pattern_names].astype(str).agg(''.join, axis=1).apply(lambda x: int(x, 2))
        df['matched_patterns'] = df[self.pattern_names].apply(
            lambda row: [pattern for pattern, val in row.items() if val == 1], axis=1
        )
        return df

class StockDatabaseManager:
    def __init__(self):
        self.engine = get_engine()
        self.metadata = MetaData()
        self.inspector = inspect(self.engine)
        self.pattern_recognizer = CandlePatternRecognizer()
        self.existing_tables_cache = set()

    def __del__(self):
        if hasattr(self, 'engine'):
            self.engine.dispose()

    def check_table_exists(self, table_name):
        if table_name in self.existing_tables_cache:
            return True
        exists = self.inspector.has_table(table_name)
        if exists:
            self.existing_tables_cache.add(table_name)
        return exists

    def create_indexes(self, table_name):
        try:
            with self.engine.connect() as conn:
                index_queries = [
                    f'CREATE INDEX IF NOT EXISTS idx_{table_name}_symbol ON {table_name} (symbol);',
                    f'CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name} (date);',
                    f'CREATE INDEX IF NOT EXISTS idx_{table_name}_pattern_value ON {table_name} (pattern_value);',
                    f'CREATE INDEX IF NOT EXISTS idx_{table_name}_symbol_date ON {table_name} (symbol, date);',
                    f'CREATE INDEX IF NOT EXISTS idx_{table_name}_symbol_pattern ON {table_name} (symbol, pattern_value);',
                    f'CREATE INDEX IF NOT EXISTS idx_{table_name}_date_pattern ON {table_name} (date, pattern_value);',
                    f'CREATE INDEX IF NOT EXISTS idx_{table_name}_symbol_date_pattern ON {table_name} (symbol, date, pattern_value);'
                ]
                for q in index_queries:
                    conn.execute(text(q))
        except Exception as e:
            logging.error(f"⚠️ Failed to create indexes on {table_name}: {e}")

    def create_table(self, table_name):
        if self.check_table_exists(table_name):
            return True
        try:
            columns = [
                Column('id', Integer, primary_key=True),
                Column('symbol', String(20), index=True),
                Column('date', Date, index=True),
                Column('open', Float),
                Column('high', Float),
                Column('low', Float),
                Column('close', Float),
                Column('volume', Float),
                Column('prev_close', Float),
                Column('avg_price', Float),
                Column('pattern_value', Integer, index=True),
                Column('matched_patterns', TEXT),
                UniqueConstraint('symbol', 'date', name=f'uq_{table_name}')
            ]
            Table(table_name, self.metadata, *columns)
            self.metadata.create_all(self.engine)
            self.create_indexes(table_name)
            self.existing_tables_cache.add(table_name)
            return True
        except Exception as e:
            logging.error(f"⚠️ Error creating table {table_name}: {e}")
            return False

    def insert_data(self, symbol, data):
        table_name = f"stock_{symbol.lower()}"
        self.create_table(table_name)
        try:
            row = data.iloc[0]
            insert_data = {
                'symbol': symbol,
                'date': datetime.strptime(row['Date'], '%d-%m-%Y').date(),
                'open': float(row['Open']),
                'high': float(row['High']),
                'low': float(row['Low']),
                'close': float(row['Close']),
                'volume': float(row['Volume']),
                'prev_close': float(row['prev_close']),
                'avg_price': float(row['avg_price']),
                'pattern_value': int(row['pattern_value']),
                'matched_patterns': str(row['matched_patterns']),
            }
            keys = ', '.join(insert_data.keys())
            values = ', '.join([f':{k}' for k in insert_data])
            query = f"""
                INSERT INTO {table_name} ({keys}) VALUES ({values})
                ON CONFLICT (symbol, date) DO NOTHING
            """
            with self.engine.begin() as conn:
                conn.execute(text(query), insert_data)
            return True
        except Exception as e:
            logging.error(f"⚠️ Insert failed for {symbol}: {e}")
            return False

    def bulk_insert_common(self, df):
        table_name = "common_stock_data"
        self.create_table(table_name)
        # Only keep rows where at least one pattern detected
        pattern_cols = self.pattern_recognizer.pattern_names
        df_patterns = df[df[pattern_cols].any(axis=1)]
        if df_patterns.empty:
            return
        # Prepare records for bulk insert
        records = []
        df_patterns.reset_index(drop=True, inplace=True)
        for _, row in df_patterns.iterrows():
            record = {
                'symbol': row['Symbol'],
                'date': datetime.strptime(row['Date'], '%d-%m-%Y').date(),
                'open': float(row['Open']),
                'high': float(row['High']),
                'low': float(row['Low']),
                'close': float(row['Close']),
                'volume': float(row['Volume']),
                'prev_close': float(row['prev_close']),
                'avg_price': float(row['avg_price']),
                'pattern_value': int(row['pattern_value']),
                'matched_patterns': str(row['matched_patterns']),
            }
            records.append(record)
        try:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            stmt = pg_insert(table).values(records)
            stmt = stmt.on_conflict_do_nothing(index_elements=['symbol', 'date'])
            with self.engine.begin() as conn:
                conn.execute(stmt)
            logging.info(f"✅ Bulk inserted {len(records)} records into {table_name}")
        except Exception as e:
            logging.error(f"⚠️ Bulk insert failed for {table_name}: {e}")

class StockDataDownloader:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.pattern_recognizer = db_manager.pattern_recognizer

    def is_weekend(self, date_str):
        return datetime.strptime(date_str, "%d%m%Y").weekday() >= 5

    def download_csv(self, date_str):
        url = f'https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv'
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return StringIO(response.content.decode('utf-8'))
            return None
        except:
            return None

    def clean_bhavcopy(self, df):
        df.columns = df.columns.str.strip()
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].str.strip()
        df.drop(columns=['LAST_PRICE', 'TURNOVER_LACS','NO_OF_TRADES', 'DELIV_QTY', 'DELIV_PER'], inplace=True, errors='ignore')
        df.rename(columns={
            'SYMBOL': 'Symbol', 'DATE1': 'Date', 'OPEN_PRICE': 'Open',
            'HIGH_PRICE': 'High', 'LOW_PRICE': 'Low', 'CLOSE_PRICE': 'Close',
            'PREV_CLOSE': 'prev_close', 'AVG_PRICE': 'avg_price',
            'TTL_TRD_QNTY': 'Volume'
        }, inplace=True)
        df['Date'] = pd.to_datetime(df['Date'], format='%d-%b-%Y').dt.strftime('%d-%m-%Y')
        df = df[df['SERIES']=='EQ']
        df.reset_index(drop=True, inplace=True)
        return df

    def process_single_date(self, date_str):
        if self.is_weekend(date_str): return
        csv_file = self.download_csv(date_str)
        if not csv_file: return

        try:
            df = pd.read_csv(csv_file)
            df = self.clean_bhavcopy(df)
            df = self.pattern_recognizer.apply_and_encode_patterns(df)
            # Bulk insert for common table (only rows with pattern)
            self.db_manager.bulk_insert_common(df)
            # Insert all rows into per-company tables (no row dropped)
            for symbol, group in df.groupby("Symbol"):
                self.db_manager.insert_data(symbol, group)
        except Exception as e:
            logging.error(f"⚠️ Error processing {date_str}: {e}")

    def process_date_range(self, start_date, end_date):
        start = datetime.strptime(start_date, "%d%m%Y")
        end = datetime.strptime(end_date, "%d%m%Y")
        current = start
        while current <= end:
            print(current)
            self.process_single_date(current.strftime("%d%m%Y"))
            current += timedelta(days=1)

if __name__ == "__main__":
    db_manager = StockDatabaseManager()
    downloader = StockDataDownloader(db_manager)
    downloader.process_date_range("01012025", datetime.now().strftime("%d%m%Y"))
