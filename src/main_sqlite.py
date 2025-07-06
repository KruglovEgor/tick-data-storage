#!/usr/bin/env python3

import logging
import os
from src.db import SQLiteDB
from src.processor import TickDataProcessor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_sample_data():
    logger.info("=== Testing with sample data (SQLite) ===")
    
    test_data = """symbol,type,moment,id,action,price,volume
SiH1,B,20210201100207662,100,1,75951,50
RIH1,S,20210201100207662,101,1,137100,4
RIH1,S,20210201100207663,102,1,137100,3
EuH1,B,20210201100207663,103,1,92186,30
RIH1,B,20210201100207863,104,1,137105,10
RIH1,S,20210201100207863,101,2,137100,4
RIH1,B,20210201100207863,104,2,137105,4
RIH1,S,20210201100207863,102,2,137100,3
RIH1,B,20210201100207863,104,2,137105,3
EuH1,B,20210201100307663,103,0,92186,30
RIH1,B,20210201100447863,104,0,137105,3
SiH1,S,20210201100537761,105,1,79500,100"""
    
    with open("test_data.csv", "w") as f:
        f.write(test_data)
    
    db = SQLiteDB()
    db.create_tables()
    
    processor = TickDataProcessor(db)
    processor.process_csv_file("test_data.csv")
    
    sih1_prices = processor.get_best_prices("SiH1", 20210201100537761)
    rih1_prices = processor.get_best_prices("RIH1", 20210201100537761)
    
    logger.info(f"SiH1 prices:")
    if sih1_prices and sih1_prices['max_buy_price']:
        logger.info(f"  Max buy price: {sih1_prices['max_buy_price']['price']}")
    if sih1_prices and sih1_prices['min_sell_price']:
        logger.info(f"  Min sell price: {sih1_prices['min_sell_price']['price']}")
    
    logger.info(f"RIH1 prices:")
    if rih1_prices and rih1_prices['max_buy_price']:
        logger.info(f"  Max buy price: {rih1_prices['max_buy_price']['price']}")
    if rih1_prices and rih1_prices['min_sell_price']:
        logger.info(f"  Min sell price: {rih1_prices['min_sell_price']['price']}")
    
    processor.print_analysis()
    
    db.close()
    os.remove("test_data.csv")

def process_real_data():
    logger.info("=== Processing real data (50000 records) ===")
    
    csv_file = "resources/20241001_fut_ord_50k.csv"
    if not os.path.exists(csv_file):
        logger.error(f"File not found: {csv_file}")
        return
    
    db = SQLiteDB()
    db.create_tables()
    
    processor = TickDataProcessor(db)
    processor.process_csv_file(csv_file, limit=50000)
    
    processor.print_analysis()
    
    db.close()

if __name__ == "__main__":
    test_sample_data()
    process_real_data() 