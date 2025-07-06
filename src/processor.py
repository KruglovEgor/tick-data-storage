import pandas as pd
import logging
from .db import DBInterface

logger = logging.getLogger(__name__)

class TickDataProcessor:
    def __init__(self, db: DBInterface):
        self.db = db
    
    def process_csv_file(self, csv_file: str, limit: int = None, batch_size: int = 1000):
        logger.info(f"Processing file: {csv_file}")
        
        self.db.clear_tables()
        
        history_batch = []
        processed_count = 0
        
        # Определяем колонки для разных форматов
        sample_df = pd.read_csv(csv_file, comment='#', nrows=1)
        if len(sample_df.columns) == 10:
            columns = ['symbol', 'system', 'type', 'moment', 'id', 'action', 'price', 'volume', 'id_deal', 'price_deal']
        elif len(sample_df.columns) == 7:
            columns = ['symbol', 'type', 'moment', 'id', 'action', 'price', 'volume']
        else:
            raise ValueError(f"Unexpected number of columns: {len(sample_df.columns)}")
        
        # Читаем CSV порциями для экономии памяти
        for chunk in pd.read_csv(csv_file, comment='#', chunksize=batch_size, names=columns, skiprows=1):
            if limit and processed_count >= limit:
                break
            
            for _, row in chunk.iterrows():
                if limit and processed_count >= limit:
                    break
                    
                order_id = row['id']
                action_type = row['action']
                symbol = row['symbol']
                operation = row['type']
                price = row['price']
                volume = row['volume']
                timestamp = row['moment']
                
                history_batch.append((symbol, operation, timestamp, order_id, action_type, price, volume))
                
                if action_type == 1:
                    self.db.insert_active_order(order_id, symbol, operation, price, volume, timestamp)
                elif action_type == 2:
                    self.db.process_trade(order_id, volume)
                elif action_type == 0:
                    self.db.delete_active_order(order_id)
                
                processed_count += 1
                
                if len(history_batch) >= batch_size:
                    self.db.insert_history_batch(history_batch)
                    history_batch = []
                    logger.info(f"Processed: {processed_count}")
            
            if limit and processed_count >= limit:
                break
        
        if history_batch:
            self.db.insert_history_batch(history_batch)
        
        active_count = self.db.get_active_orders_count()
        logger.info(f"Processing completed. Processed: {processed_count}, Active orders: {active_count}")
    
    def get_best_prices(self, symbol: str, timestamp: int = None):
        return self.db.get_best_prices(symbol, timestamp)
    
    def print_analysis(self):
        """Вывод анализа активных заявок"""
        logger.info("=== Анализ активных заявок ===")
        
        total_count = self.db.get_active_orders_count()
        logger.info(f"Всего активных заявок: {total_count}")
        
        if total_count > 0:
            sample = self.db.get_active_orders_sample(5)
            logger.info("Образец активных заявок:")
            for order in sample:
                logger.info(f"  ID: {order['order_id']}, {order['symbol']} {order['operation']}, "
                          f"Цена: {order['price']}, Объем: {order['remaining_volume']}/{order['original_volume']}")
            
            summary = self.db.get_symbols_summary()
            logger.info("Сводка по инструментам:")
            for row in summary:
                op_name = "ПОКУПКА" if row['operation'] == 'B' else "ПРОДАЖА"
                logger.info(f"  {row['symbol']} {op_name}: {row['orders_count']} заявок, "
                          f"объем: {row['total_volume']}, цена: {row['min_price']}-{row['max_price']}") 