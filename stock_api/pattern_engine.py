import talib
import numpy as np
from typing import Dict, List
from datetime import date
from psycopg2.extras import execute_batch
from database import get_db_connection


class PatternEngine:
    def __init__(self):
        self.patterns = {
            1: ('Hammer', talib.CDLHAMMER),
            2: ('Inverted Hammer', talib.CDLINVERTEDHAMMER),
            3: ('Hanging Man', talib.CDLHANGINGMAN),
            4: ('Shooting Star', talib.CDLSHOOTINGSTAR),
            5: ('Doji', talib.CDLDOJI),
            6: ('Bullish Marubozu', lambda o, h, l, c: talib.CDLMARUBOZU(o, h, l, c) & (c > o)),
            7: ('Bearish Marubozu', lambda o, h, l, c: talib.CDLMARUBOZU(o, h, l, c) & (c < o))
        }

    def detect_patterns(self, ohlc_data: Dict[str, np.array]) -> Dict[int, np.array]:
        """Detect patterns in OHLC data"""
        results = {}
        for pattern_id, (name, func) in self.patterns.items():
            results[pattern_id] = func(
                ohlc_data['open'],
                ohlc_data['high'],
                ohlc_data['low'],
                ohlc_data['close']
            )
        return results

    def save_patterns_to_db(self, patterns: Dict[int, np.array], company_id: int, dates: List[date]):
        """Bulk save detected patterns to database"""
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Prepare batch data
                pattern_data = []
                for i, day_date in enumerate(dates):
                    for pattern_id, pattern_results in patterns.items():
                        if pattern_results[i] != 0:
                            pattern_data.append((
                                company_id,
                                day_date,
                                pattern_id,
                                abs(pattern_results[i])  # Confidence score
                            ))

                # Bulk insert
                execute_batch(cur,
                              """INSERT INTO stock_price_patterns 
                              (stock_price_id, date, pattern_id, confidence)
                              SELECT sp.id, %s, %s, %s
                              FROM stock_prices sp
                              WHERE sp.company_id = %s AND sp.date = %s
                              ON CONFLICT DO NOTHING""",
                              pattern_data,
                              page_size=1000
                              )
            conn.commit()
        finally:
            conn.close()