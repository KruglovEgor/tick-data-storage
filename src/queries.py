# PostgreSQL queries
ORDER_HISTORY_TABLE = '''
    CREATE TABLE IF NOT EXISTS order_history (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(20) NOT NULL,
        operation CHAR(1) NOT NULL,
        timestamp BIGINT NOT NULL,
        order_id BIGINT NOT NULL,
        action_type INTEGER NOT NULL,
        price DECIMAL(15,5) NOT NULL,
        volume INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
    )
'''

ACTIVE_ORDERS_TABLE = '''
    CREATE TABLE IF NOT EXISTS active_orders (
        order_id BIGINT PRIMARY KEY,
        symbol VARCHAR(20) NOT NULL,
        operation CHAR(1) NOT NULL,
        price DECIMAL(15,5) NOT NULL,
        original_volume INTEGER NOT NULL,
        remaining_volume INTEGER NOT NULL,
        timestamp BIGINT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
    )
'''

INSERT_HISTORY_BATCH = '''
    INSERT INTO order_history (symbol, operation, timestamp, order_id, action_type, price, volume)
    VALUES %s
'''

INSERT_ACTIVE_ORDER = '''
    INSERT INTO active_orders (order_id, symbol, operation, price, original_volume, remaining_volume, timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (order_id) DO NOTHING
'''

GET_ACTIVE_ORDER = 'SELECT remaining_volume FROM active_orders WHERE order_id = %s'
UPDATE_ACTIVE_ORDER_VOLUME = 'UPDATE active_orders SET remaining_volume = %s WHERE order_id = %s'
DELETE_ACTIVE_ORDER = 'DELETE FROM active_orders WHERE order_id = %s'

SAMPLE_ACTIVE_ORDERS = '''
    SELECT order_id, symbol, operation, price, original_volume, remaining_volume, timestamp
    FROM active_orders 
    ORDER BY timestamp DESC 
    LIMIT %s
'''

SYMBOLS_SUMMARY = '''
    SELECT 
        symbol,
        operation,
        COUNT(*) as orders_count,
        SUM(remaining_volume) as total_volume,
        MIN(price) as min_price,
        MAX(price) as max_price,
        AVG(price) as avg_price
    FROM active_orders 
    GROUP BY symbol, operation
    ORDER BY symbol, operation
'''

BEST_PRICES_QUERY = '''
    WITH best_buy AS (
        SELECT order_id, price, remaining_volume
        FROM active_orders 
        WHERE symbol = %s 
            AND operation = 'B' 
            AND remaining_volume > 0
            AND timestamp <= %s
        ORDER BY price DESC
        LIMIT 1
    ),
    best_sell AS (
        SELECT order_id, price, remaining_volume
        FROM active_orders 
        WHERE symbol = %s 
            AND operation = 'S' 
            AND remaining_volume > 0
            AND timestamp <= %s
        ORDER BY price ASC
        LIMIT 1
    )
    SELECT 'BUY' as operation, order_id, price, remaining_volume FROM best_buy
    UNION ALL
    SELECT 'SELL' as operation, order_id, price, remaining_volume FROM best_sell
'''

# SQLite queries
ORDER_HISTORY_TABLE_SQLITE = '''
    CREATE TABLE IF NOT EXISTS order_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        operation TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        order_id INTEGER NOT NULL,
        action_type INTEGER NOT NULL,
        price REAL NOT NULL,
        volume INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
'''

ACTIVE_ORDERS_TABLE_SQLITE = '''
    CREATE TABLE IF NOT EXISTS active_orders (
        order_id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL,
        operation TEXT NOT NULL,
        price REAL NOT NULL,
        original_volume INTEGER NOT NULL,
        remaining_volume INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
'''

INSERT_HISTORY_BATCH_SQLITE = '''
    INSERT INTO order_history (symbol, operation, timestamp, order_id, action_type, price, volume)
    VALUES (?, ?, ?, ?, ?, ?, ?)
'''

INSERT_ACTIVE_ORDER_SQLITE = '''
    INSERT OR IGNORE INTO active_orders (order_id, symbol, operation, price, original_volume, remaining_volume, timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?)
'''

GET_ACTIVE_ORDER_SQLITE = 'SELECT remaining_volume FROM active_orders WHERE order_id = ?'
UPDATE_ACTIVE_ORDER_VOLUME_SQLITE = 'UPDATE active_orders SET remaining_volume = ? WHERE order_id = ?'
DELETE_ACTIVE_ORDER_SQLITE = 'DELETE FROM active_orders WHERE order_id = ?'

SAMPLE_ACTIVE_ORDERS_SQLITE = '''
    SELECT order_id, symbol, operation, price, original_volume, remaining_volume, timestamp
    FROM active_orders 
    ORDER BY timestamp DESC 
    LIMIT ?
'''

SYMBOLS_SUMMARY_SQLITE = '''
    SELECT 
        symbol,
        operation,
        COUNT(*) as orders_count,
        SUM(remaining_volume) as total_volume,
        MIN(price) as min_price,
        MAX(price) as max_price,
        AVG(price) as avg_price
    FROM active_orders 
    GROUP BY symbol, operation
    ORDER BY symbol, operation
'''

BEST_PRICES_QUERY_SQLITE = '''
    SELECT 'BUY' as operation, order_id, price, remaining_volume
    FROM active_orders 
    WHERE symbol = ? 
        AND operation = 'B' 
        AND remaining_volume > 0
        AND timestamp <= ?
    ORDER BY price DESC
    LIMIT 1
'''

BEST_PRICES_QUERY_SQLITE_SELL = '''
    SELECT 'SELL' as operation, order_id, price, remaining_volume
    FROM active_orders 
    WHERE symbol = ? 
        AND operation = 'S' 
        AND remaining_volume > 0
        AND timestamp <= ?
    ORDER BY price ASC
    LIMIT 1
'''

GET_ACTIVE_ORDERS_COUNT = """
SELECT COUNT(*) FROM active_orders
"""

IDX_HISTORY_ORDER_ID_SQLITE = 'CREATE INDEX IF NOT EXISTS idx_history_order_id ON order_history(order_id)' 