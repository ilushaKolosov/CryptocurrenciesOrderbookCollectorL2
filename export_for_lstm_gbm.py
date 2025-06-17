import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from database import DATABASE_URL
import json
from datetime import datetime, timedelta
import argparse
import os
from config import TOP_CRYPTO_SYMBOLS


class OptimizedDatasetExporter:
    """Оптимизированный экспортер готового датасета для LSTM+LightGBM (~50-60 фичей) с multi-task таргетами"""
    
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
    
    def find_orderbook_wall_level(self, volumes, threshold=0.15):
        """Находит ближайший уровень с крупной стеной (относительно mid_price)"""
        total = sum(volumes)
        for i, v in enumerate(volumes):
            if v > threshold * total:
                return i + 1  # уровень (1-индексация)
        return np.nan  # если стены нет

    def extract_optimized_features(self, bids, asks, depth=100):
        """Оптимизированное извлечение признаков (вместо 400 фичей - ~50)"""
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
        
        # 🎯 ОПТИМИЗАЦИЯ: Вместо 100 уровней - ключевые уровни
        # Топ-10 уровней (самые важные)
        for i in range(10):
            if i < len(bids):
                features[f'bid_price_norm_{i+1}'] = (bids[i]['price'] - mid_price) / mid_price
                features[f'bid_volume_{i+1}'] = bids[i]['volume']
            else:
                features[f'bid_price_norm_{i+1}'] = 0.0
                features[f'bid_volume_{i+1}'] = 0.0
                
            if i < len(asks):
                features[f'ask_price_norm_{i+1}'] = (asks[i]['price'] - mid_price) / mid_price
                features[f'ask_volume_{i+1}'] = asks[i]['volume']
            else:
                features[f'ask_price_norm_{i+1}'] = 0.0
                features[f'ask_volume_{i+1}'] = 0.0
        
        # 🎯 ОПТИМИЗАЦИЯ: Ключевые уровни (25, 50, 75, 100)
        key_levels = [25, 50, 75, 100]
        for level in key_levels:
            if level <= len(bids):
                features[f'bid_price_norm_level_{level}'] = (bids[level-1]['price'] - mid_price) / mid_price
                features[f'bid_volume_level_{level}'] = bids[level-1]['volume']
            else:
                features[f'bid_price_norm_level_{level}'] = 0.0
                features[f'bid_volume_level_{level}'] = 0.0
                
            if level <= len(asks):
                features[f'ask_price_norm_level_{level}'] = (asks[level-1]['price'] - mid_price) / mid_price
                features[f'ask_volume_level_{level}'] = asks[level-1]['volume']
            else:
                features[f'ask_price_norm_level_{level}'] = 0.0
                features[f'ask_volume_level_{level}'] = 0.0
        
        # 🎯 ОПТИМИЗАЦИЯ: Агрегированные метрики вместо всех уровней
        bid_prices = [b['price'] for b in bids[:depth]]
        bid_volumes = [b['volume'] for b in bids[:depth]]
        ask_prices = [a['price'] for a in asks[:depth]]
        ask_volumes = [a['volume'] for a in asks[:depth]]
        
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
        
        # 🎯 ОПТИМИЗАЦИЯ: Дополнительные агрегированные метрики
        # Квантили цен и объемов
        features['bid_price_q25'] = np.percentile(bid_prices, 25)
        features['bid_price_q75'] = np.percentile(bid_prices, 75)
        features['ask_price_q25'] = np.percentile(ask_prices, 25)
        features['ask_price_q75'] = np.percentile(ask_prices, 75)
        
        features['bid_volume_q25'] = np.percentile(bid_volumes, 25)
        features['bid_volume_q75'] = np.percentile(bid_volumes, 75)
        features['ask_volume_q25'] = np.percentile(ask_volumes, 25)
        features['ask_volume_q75'] = np.percentile(ask_volumes, 75)
        
        # Соотношения объемов на разных уровнях
        features['volume_ratio_top10'] = sum(bid_volumes[:10]) / sum(ask_volumes[:10]) if sum(ask_volumes[:10]) > 0 else 1.0
        features['volume_ratio_top25'] = sum(bid_volumes[:25]) / sum(ask_volumes[:25]) if sum(ask_volumes[:25]) > 0 else 1.0
        features['volume_ratio_top50'] = sum(bid_volumes[:50]) / sum(ask_volumes[:50]) if sum(ask_volumes[:50]) > 0 else 1.0
        
        return features
    
    def add_targets(self, df, future_shift=30, wall_threshold=0.15):
        """Добавляет multi-task таргеты: классификация (рост/падение) и регрессия (уровень для лимитного ордера)"""
        # Классификация: рост/падение future_mid_price
        df['future_mid_price'] = df['mid_price'].shift(-future_shift)
        df['target_updown'] = (df['future_mid_price'] > df['mid_price']).astype(int)
        # Регрессия: ближайший уровень крупной bid/ask-стены
        bid_cols = [f'bid_volume_{i+1}' for i in range(10)]
        ask_cols = [f'ask_volume_{i+1}' for i in range(10)]
        df['bid_wall_level'] = df[bid_cols].apply(lambda row: self.find_orderbook_wall_level(row.values, wall_threshold), axis=1)
        df['ask_wall_level'] = df[ask_cols].apply(lambda row: self.find_orderbook_wall_level(row.values, wall_threshold), axis=1)
        return df

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
        
        print(f"📊 Экспорт оптимизированного датасета для {symbol}...")
        
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
                
                features = self.extract_optimized_features(bids, asks)
                features['timestamp'] = row.timestamp
                features['symbol'] = symbol
                data_rows.append(features)
        
        if data_rows:
            df = pd.DataFrame(data_rows)
            
            # Добавляем multi-task targets
            df = self.add_targets(df, future_shift=30, wall_threshold=0.15)
            
            # Создаем директорию если не существует
            os.makedirs(output_dir, exist_ok=True)
            
            # Сохраняем полный датасет
            dataset_file = f"{output_dir}/{symbol}_optimized_dataset_{start_time.strftime('%Y%m%d')}_{end_time.strftime('%Y%m%d')}.csv"
            df.to_csv(dataset_file, index=False)
            
            # Создаем матрицу для LSTM (только числовые признаки)
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            lstm_matrix = df[numeric_cols].values
            
            # Сохраняем матрицу
            matrix_file = f"{output_dir}/{symbol}_optimized_lstm_matrix_{start_time.strftime('%Y%m%d')}_{end_time.strftime('%Y%m%d')}.npy"
            np.save(matrix_file, lstm_matrix)
            
            print(f"✅ Оптимизированный датасет сохранен:")
            print(f"  📁 Полный датасет: {dataset_file} ({len(df)} записей, {len(df.columns)} фичей)")
            print(f"  🧠 LSTM матрица: {matrix_file} (форма: {lstm_matrix.shape})")
            
            return df
        else:
            print("❌ Данные не найдены")
            return None
    
    def export_all_symbols(self, symbols: list, hours: int = 24, output_dir: str = "dataset"):
        """Экспорт датасетов для всех символов"""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        
        print(f"🚀 Экспорт ОПТИМИЗИРОВАННЫХ датасетов для LSTM+LightGBM")
        print(f"📅 Период: {start_time} - {end_time}")
        print(f"🎯 Символы: {symbols}")
        print(f"📊 Вместо 439+ фичей - ~50-60 фичей")
        print("=" * 60)
        
        all_datasets = []
        
        for symbol in symbols:
            df = self.export_dataset(symbol, start_time, end_time, output_dir)
            if df is not None:
                all_datasets.append(df)
                print(f"✅ {symbol}: {len(df)} записей, {len(df.columns)} фичей")
            else:
                print(f"❌ {symbol}: данные не найдены")
        
        if all_datasets:
            # Объединяем все датасеты
            combined_df = pd.concat(all_datasets, ignore_index=True)
            combined_file = f"{output_dir}/combined_optimized_dataset_{start_time.strftime('%Y%m%d')}_{end_time.strftime('%Y%m%d')}.csv"
            combined_df.to_csv(combined_file, index=False)
            
            print("=" * 60)
            print(f"🎉 ОБЪЕДИНЕННЫЙ датасет сохранен: {combined_file}")
            print(f"📊 Общая статистика:")
            print(f"   - Записей: {len(combined_df)}")
            print(f"   - Фичей: {len(combined_df.columns)}")
            print(f"   - Символов: {len(combined_df['symbol'].unique())}")
            print(f"   - Период: {combined_df['timestamp'].min()} - {combined_df['timestamp'].max()}")
        
        return all_datasets


def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(description='Экспорт оптимизированных датасетов для LSTM+LightGBM')
    parser.add_argument('--hours', type=int, default=24, help='Количество часов для экспорта (по умолчанию: 24)')
    parser.add_argument('--output', type=str, default='dataset', help='Директория для сохранения (по умолчанию: dataset)')
    parser.add_argument('--symbols', nargs='+', default=TOP_CRYPTO_SYMBOLS, help='Символы для экспорта')
    
    args = parser.parse_args()
    
    exporter = OptimizedDatasetExporter()
    exporter.export_all_symbols(args.symbols, args.hours, args.output)


if __name__ == "__main__":
    main() 