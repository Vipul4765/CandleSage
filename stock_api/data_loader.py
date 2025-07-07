import pandas as pd
from psycopg2.extras import execute_batch
from datetime import datetime
from database import get_db_connection


class StockDataLoader:
    def load_companies(self, companies_df: pd.DataFrame):
        """Bulk load companies into database"""
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Get existing symbols for conflict check
                cur.execute("SELECT symbol FROM companies")
                existing = {row[0] for row in cur.fetchall()}

                # Prepare new companies
                new_companies = [
                    (row['symbol'], row.get('full_name'))
                    for _, row in companies_df.iterrows()
                    if row['symbol'] not in existing
                ]

                # Bulk insert
                execute_batch(cur,
                              "INSERT INTO companies (symbol, full_name) VALUES (%s, %s)",
                              new_companies,
                              page_size=1000
                              )
            conn.commit()
        finally:
            conn.close()

    def load_prices(self, prices_df: pd.DataFrame):
        """Bulk load stock prices with pattern detection"""
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Get company ID mapping
                cur.execute("SELECT id, symbol FROM companies")
                company_map = {row[1]: row[0] for row in cur.fetchall()}

                # Prepare price data
                price_data = []
                for _, row in prices_df.iterrows():
                    if row['symbol'] in company_map:
                        price_data.append((
                            company_map[row['symbol']],
                            row['date'],
                            row['prev_close'],
                            row['open'],
                            row['high'],
                            row['low'],
                            row['close'],
                            row['avg_price'],
                            row['volume']
                        ))

                # Bulk insert prices
                execute_batch(cur,
                              """INSERT INTO stock_prices 
                              (company_id, date, prev_close, open, high, low, close, avg_price, volume)
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                              ON CONFLICT (company_id, date) DO UPDATE SET
                              prev_close = EXCLUDED.prev_close,
                              open = EXCLUDED.open,
                              high = EXCLUDED.high,
                              low = EXCLUDED.low,
                              close = EXCLUDED.close,
                              avg_price = EXCLUDED.avg_price,
                              volume = EXCLUDED.volume""",
                              price_data,
                              page_size=1000
                              )
            conn.commit()
        finally:
            conn.close()