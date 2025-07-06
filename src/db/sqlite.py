import sqlite3
from .base import DBInterface
from ..queries import *

class SQLiteDB(DBInterface):
    def __init__(self, db_path='tick_data.db'):
        self.db_path = db_path
        self.conn = None

    def get_connection(self):
        if not self.conn:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
        return self.conn

    def create_tables(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(ORDER_HISTORY_TABLE_SQLITE)
        cursor.execute(ACTIVE_ORDERS_TABLE_SQLITE)
        conn.commit()

    def create_indexes(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(IDX_HISTORY_TIMESTAMP_SQLITE)
        cursor.execute(IDX_HISTORY_ORDER_ID_SQLITE)
        cursor.execute(IDX_ACTIVE_SYMBOL_OPERATION_SQLITE)
        cursor.execute(IDX_ACTIVE_SYMBOL_TIMESTAMP_SQLITE)
        conn.commit()

    def clear_tables(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM active_orders')
        cursor.execute('DELETE FROM order_history')
        conn.commit()

    def insert_history_batch(self, batch):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.executemany(INSERT_HISTORY_BATCH_SQLITE, batch)
        conn.commit()

    def insert_active_order(self, order_id, symbol, operation, price, volume, timestamp):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(INSERT_ACTIVE_ORDER_SQLITE, (order_id, symbol, operation, price, volume, volume, timestamp))
        conn.commit()

    def process_trade(self, order_id, trade_volume):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(GET_ACTIVE_ORDER_SQLITE, (order_id,))
        result = cursor.fetchone()
        if result:
            remaining_volume = result[0]
            new_remaining = remaining_volume - trade_volume
            if new_remaining <= 0:
                self.delete_active_order(order_id)
            else:
                cursor.execute(UPDATE_ACTIVE_ORDER_VOLUME_SQLITE, (new_remaining, order_id))
                conn.commit()

    def delete_active_order(self, order_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(DELETE_ACTIVE_ORDER_SQLITE, (order_id,))
        conn.commit()

    def get_active_orders_count(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM active_orders')
        return cursor.fetchone()[0]

    def get_active_orders_sample(self, limit=10):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(SAMPLE_ACTIVE_ORDERS_SQLITE, (limit,))
        return cursor.fetchall()

    def get_symbols_summary(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(SYMBOLS_SUMMARY_SQLITE)
        return cursor.fetchall()

    def get_best_prices(self, symbol: str, timestamp: int = None):
        if timestamp is None:
            timestamp = 99999999999999999
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(BEST_PRICES_QUERY_SQLITE, (symbol, timestamp))
        best_buy = cursor.fetchone()
        cursor.execute(BEST_PRICES_QUERY_SQLITE_SELL, (symbol, timestamp))
        best_sell = cursor.fetchone()
        return {
            'symbol': symbol,
            'timestamp': timestamp,
            'max_buy_price': dict(best_buy) if best_buy else None,
            'min_sell_price': dict(best_sell) if best_sell else None
        }

    def close(self):
        if self.conn:
            self.conn.close() 