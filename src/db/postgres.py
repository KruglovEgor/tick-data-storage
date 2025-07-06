import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2.pool import SimpleConnectionPool
from .base import DBInterface
from ..queries import *

class PostgresDB(DBInterface):
    def __init__(self, host='localhost', port=5432, database='tick_data', user='tick_user', password='tick_pass', min_conn=1, max_conn=10):
        self.connection_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        self.pool = SimpleConnectionPool(min_conn, max_conn, **self.connection_params)

    def get_connection(self):
        return self.pool.getconn()

    def return_connection(self, conn):
        self.pool.putconn(conn)

    def create_tables(self):
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(ORDER_HISTORY_TABLE)
                cursor.execute(ACTIVE_ORDERS_TABLE)
                conn.commit()
        finally:
            self.return_connection(conn)

    def clear_tables(self):
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute('DELETE FROM active_orders')
                cursor.execute('DELETE FROM order_history')
                conn.commit()
        finally:
            self.return_connection(conn)

    def insert_history_batch(self, batch):
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                execute_values(cursor, INSERT_HISTORY_BATCH, batch, template=None, page_size=1000)
                conn.commit()
        finally:
            self.return_connection(conn)

    def insert_active_order(self, order_id, symbol, operation, price, volume, timestamp):
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(INSERT_ACTIVE_ORDER, (order_id, symbol, operation, price, volume, volume, timestamp))
                conn.commit()
        finally:
            self.return_connection(conn)

    def process_trade(self, order_id, trade_volume):
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(GET_ACTIVE_ORDER, (order_id,))
                result = cursor.fetchone()
                if result:
                    remaining_volume = result[0]
                    new_remaining = remaining_volume - trade_volume
                    if new_remaining <= 0:
                        self.delete_active_order(order_id)
                    else:
                        cursor.execute(UPDATE_ACTIVE_ORDER_VOLUME, (new_remaining, order_id))
                        conn.commit()
        finally:
            self.return_connection(conn)

    def delete_active_order(self, order_id):
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(DELETE_ACTIVE_ORDER, (order_id,))
                conn.commit()
        finally:
            self.return_connection(conn)

    def get_active_orders_count(self):
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute('SELECT COUNT(*) FROM active_orders')
                return cursor.fetchone()[0]
        finally:
            self.return_connection(conn)

    def get_active_orders_sample(self, limit=10):
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(SAMPLE_ACTIVE_ORDERS, (limit,))
                return cursor.fetchall()
        finally:
            self.return_connection(conn)

    def get_symbols_summary(self):
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(SYMBOLS_SUMMARY)
                return cursor.fetchall()
        finally:
            self.return_connection(conn)

    def get_best_prices(self, symbol: str, timestamp: int = None):
        if timestamp is None:
            timestamp = 99999999999999999
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(BEST_PRICES_QUERY, (symbol, timestamp, symbol, timestamp))
                results = cursor.fetchall()
                best_buy = None
                best_sell = None
                for row in results:
                    if row['operation'] == 'BUY':
                        best_buy = dict(row)
                    elif row['operation'] == 'SELL':
                        best_sell = dict(row)
                return {
                    'symbol': symbol,
                    'timestamp': timestamp,
                    'max_buy_price': best_buy,
                    'min_sell_price': best_sell
                }
        finally:
            self.return_connection(conn)

    def close(self):
        self.pool.closeall()

    def create_indexes(self):
        pass 