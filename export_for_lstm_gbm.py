import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from database import DATABASE_URL
import json
from datetime import datetime, timedelta
import argparse
import os
from config import TOP_CRYPTO_SYMBOLS


class DatasetExporter:
    """Экспортер готового датасета для LSTM+LightGBM"""
    
    def __init__(self):
        self.engine = create_engine(DATABASE_URL)
        
    def calculate_market_pressure(self, bids, asks, depth=100):
        """Вычисление market pressure"""
        if not bids or not asks:
            return {}
        
        buy_pressure = 0
        sell_pressure = 0
        mid_price = (bids[0]['price'] + asks[0]['price']) / 2
        
        for i, bid in enumerate(bids[:depth]):
            weight = 1.0 / (i + 1)
            distance = (mid_price - bid['price']) / mid_price
            buy_pressure += bid['volume'] * weight * (1 - distance)
        
        for i, ask in enumerate(asks[:depth]):
            weight = 1.0 / (i + 1)
            distance = (ask['price'] - mid_price) / mid_price
            sell_pressure += ask['volume'] * weight * (1 - distance)
        
        total_pressure = buy_pressure + sell_pressure
        net_pressure = buy_pressure - sell_pressure
        pressure_ratio = buy_pressure / sell_pressure if sell_pressure > 0 else 1.0
        
        return {
            'buy_pressure': buy_pressure,
            'sell_pressure': sell_pressure,
            'net_pressure': net_pressure,
            'pressure_ratio': pressure_ratio,
            'total_pressure': total_pressure
        }
    
    def extract_features(self, bids, asks, depth=100):
        """Извлечение признаков для LSTM+LightGBM"""
        features = {}
        
        if not bids or not asks:
            return features
        
        # Базовые цены
        best_bid = bids[0]['price']
        best_ask = asks[0]['price']
        mid_price = (best_bid + best_ask) / 2
        spread = best_ask - best_bid
        spread_percent = (spread / mid_price) * 100
        
        features['mid_price'] = mid_price
        features['spread'] = spread
        features['spread_percent'] = spread_percent
        
        # Market Pressure
        pressure = self.calculate_market_pressure(bids, asks, depth)
        features.update(pressure)
        
        # Все 100 уровней для LSTM
        bid_prices = []
        bid_volumes = []
        ask_prices = []
        ask_volumes = []
        
        for i in range(depth):
            if i < len(bids):
                bid_prices.append(bids[i]['price'])
                bid_volumes.append(bids[i]['volume'])
            else:
                bid_prices.append(0.0)
                bid_volumes.append(0.0)
                
            if i < len(asks):
                ask_prices.append(asks[i]['price'])
                ask_volumes.append(asks[i]['volume'])
            else:
                ask_prices.append(0.0)
                ask_volumes.append(0.0)
        
        # Нормализация цен относительно mid_price
        bid_prices_norm = [(p - mid_price) / mid_price for p in bid_prices]
        ask_prices_norm = [(p - mid_price) / mid_price for p in ask_prices]
        
        # Добавляем нормализованные цены и объемы для LSTM
        for i in range(depth):
            features[f'bid_price_norm_{i+1}'] = bid_prices_norm[i]
            features[f'ask_price_norm_{i+1}'] = ask_prices_norm[i]
            features[f'bid_volume_{i+1}'] = bid_volumes[i]
            features[f'ask_volume_{i+1}'] = ask_volumes[i]
        
        # Статистические метрики для LightGBM
        total_bid_volume = sum(bid_volumes)
        total_ask_volume = sum(ask_volumes)
        volume_imbalance = (total_bid_volume - total_ask_volume) / (total_bid_volume + total_ask_volume)
        
        features['total_bid_volume'] = total_bid_volume
        features['total_ask_volume'] = total_ask_volume
        features['volume_imbalance'] = volume_imbalance
        
        # Основные статистики
        features['bid_price_mean'] = np.mean(bid_prices)
        features['ask_price_mean'] = np.mean(ask_prices)
        features['bid_volume_mean'] = np.mean(bid_volumes)
        features['ask_volume_mean'] = np.mean(ask_volumes)
        
        features['bid_price_std'] = np.std(bid_prices)
        features['ask_price_std'] = np.std(ask_prices)
        features['bid_volume_std'] = np.std(bid_volumes)
        features['ask_volume_std'] = np.std(ask_volumes)
        
        # Концентрация объемов
        features['bid_volume_concentration'] = np.sum(np.array(bid_volumes[:10])) / total_bid_volume if total_bid_volume > 0 else 0
        features['ask_volume_concentration'] = np.sum(np.array(ask_volumes[:10])) / total_ask_volume if total_ask_volume > 0 else 0
        
        return features
    
    def export_dataset(self, symbol: str, start_time: datetime, end_time: datetime, output_dir: str):
        """Экспорт готового датасета"""
        query = text("""
            SELECT timestamp, bids, asks
            FROM orderbook_entries 
            WHERE symbol = :symbol 
            AND timestamp >= :start_time 
            AND timestamp <= :end_time
            ORDER BY timestamp ASC
        """)
        
        print(f"Экспорт датасета для {symbol}...")
        
        data_rows = []
        
        with self.engine.connect() as conn:
            result = conn.execute(query, {
                "symbol": symbol, 
                "start_time": start_time, 
                "end_time": end_time
            })
            
            for row in result:
                bids = json.loads(row.bids)
                asks = json.loads(row.asks)
                
                features = self.extract_features(bids, asks)
                features['timestamp'] = row.timestamp
                features['symbol'] = symbol
                data_rows.append(features)
        
        if data_rows:
            df = pd.DataFrame(data_rows)
            
            # Создаем директорию если не существует
            os.makedirs(output_dir, exist_ok=True)
            
            # Сохраняем полный датасет
            dataset_file = f"{output_dir}/{symbol}_dataset_{start_time.strftime('%Y%m%d')}_{end_time.strftime('%Y%m%d')}.csv"
            df.to_csv(dataset_file, index=False)
            
            # Создаем матрицу для LSTM (только числовые признаки)
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            lstm_matrix = df[numeric_cols].values
            
            # Сохраняем матрицу
            matrix_file = f"{output_dir}/{symbol}_lstm_matrix_{start_time.strftime('%Y%m%d')}_{end_time.strftime('%Y%m%d')}.npy"
            np.save(matrix_file, lstm_matrix)
            
            print(f"Датасет сохранен:")
            print(f"  - Полный датасет: {dataset_file} ({len(df)} записей)")
            print(f"  - LSTM матрица: {matrix_file} (форма: {lstm_matrix.shape})")
            
            return df
        else:
            print("Данные не найдены")
            return None
    
    def export_all_symbols(self, symbols: list, hours: int = 24, output_dir: str = "dataset"):
        """Экспорт датасетов для всех символов"""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        
        print(f"Экспорт датасетов для LSTM+LightGBM")
        print(f"Период: {start_time} - {end_time}")
        print(f"Символы: {symbols}")
        print("=" * 60)
        
        all_datasets = []
        
        for symbol in symbols:
            df = self.export_dataset(symbol, start_time, end_time, output_dir)
            if df is not None:
                all_datasets.append(df)
        
        # Создаем объединенный датасет
        if all_datasets:
            combined_dataset = pd.concat(all_datasets, ignore_index=True)
            combined_file = f"{output_dir}/all_symbols_dataset_{hours}h.csv"
            combined_dataset.to_csv(combined_file, index=False)
            print(f"\nОбъединенный датасет: {combined_file} ({len(combined_dataset)} записей)")
        
        print(f"\nЭкспорт завершен. Датасеты сохранены в {output_dir}/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Экспорт готового датасета для LSTM+LightGBM")
    parser.add_argument("--symbol", default="BTCUSDT", help="Торговая пара")
    parser.add_argument("--hours", type=int, default=24, help="Количество часов для экспорта")
    parser.add_argument("--output-dir", default="dataset", help="Выходная директория")
    parser.add_argument("--all-symbols", action="store_true", help="Экспорт всех символов")
    
    args = parser.parse_args()
    
    exporter = DatasetExporter()
    
    if args.all_symbols:
        exporter.export_all_symbols(TOP_CRYPTO_SYMBOLS, args.hours, args.output_dir)
    else:
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=args.hours)
        exporter.export_dataset(args.symbol, start_time, end_time, args.output_dir) 