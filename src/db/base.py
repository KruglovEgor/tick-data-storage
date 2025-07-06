from abc import ABC, abstractmethod

class DBInterface(ABC):
    @abstractmethod
    def create_tables(self):
        pass

    @abstractmethod
    def create_indexes(self):
        pass

    @abstractmethod
    def clear_tables(self):
        pass

    @abstractmethod
    def insert_history_batch(self, batch):
        pass

    @abstractmethod
    def insert_active_order(self, order_id, symbol, operation, price, volume, timestamp):
        pass

    @abstractmethod
    def process_trade(self, order_id, trade_volume):
        pass

    @abstractmethod
    def delete_active_order(self, order_id):
        pass

    @abstractmethod
    def get_active_orders_count(self):
        pass

    @abstractmethod
    def get_active_orders_sample(self, limit=10):
        pass

    @abstractmethod
    def get_symbols_summary(self):
        pass

    @abstractmethod
    def get_best_prices(self, symbol: str, timestamp: int = None):
        pass

    @abstractmethod
    def close(self):
        pass 