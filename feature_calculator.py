import asyncio
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from database import DATABASE_URL, create_tables
import json
from datetime import datetime, timedelta
from config import TOP_CRYPTO_SYMBOLS, FEATURE_CALCULATION_INTERVAL
import time
import sqlite3
import os


class OptimizedFeatureCalculator:
    """Оптимизированный сервис для вычисления признаков из orderbook (~50-60 фичей) с multi-task таргетами"""
    
    def __init__(self):
        self.engine = create_engine(DATABASE_URL)
        self.is_running = False
        self.last_calculation_time = {}
        
    def find_orderbook_wall_level(self, volumes, threshold=0.15):
        """Находит ближайший уровень с крупной стеной (относительно mid_price)"""
        total = sum(volumes)
        for i, v in enumerate(volumes):
            if v > threshold * total:
                return i + 1  # уровень (1-индексация)
        return 1.0  # значение по умолчанию если стены нет
        
    def calculate_market_pressure(self, bids, asks, depth=100):
        """Вычисление market pressure (давления рынка)"""
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
    
    def calculate_optimized_features(self, bids, asks, depth=100):
        """Оптимизированное вычисление признаков (вместо 400 фичей - ~50)"""
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
        pressure_features = self.calculate_market_pressure(bids, asks, depth)
        features.update(pressure_features)
        
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

    def add_targets_and_walls(self, features: dict, symbol: str, future_shift: int = 30, wall_threshold: float = 0.15):
        """Добавляет multi-task таргеты и уровни стен"""
        try:
            # Получаем будущую цену для таргета
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(seconds=future_shift + 10)
            
            query = text("""
                SELECT timestamp, bids, asks
                FROM orderbook_entries 
                WHERE symbol = :symbol 
                AND timestamp >= :start_time 
                AND timestamp <= :end_time
                ORDER BY timestamp ASC
                LIMIT 10
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query, {
                    "symbol": symbol, 
                    "start_time": start_time, 
                    "end_time": end_time
                })
                
                future_prices = []
                for row in result:
                    bids = json.loads(row.bids)
                    asks = json.loads(row.asks)
                    if bids and asks:
                        future_mid_price = (bids[0]['price'] + asks[0]['price']) / 2
                        future_prices.append(future_mid_price)
            
            # Добавляем future_mid_price и target_updown
            if future_prices:
                features['future_mid_price'] = future_prices[0] if len(future_prices) > 0 else features['mid_price']
                features['target_updown'] = 1 if features['future_mid_price'] > features['mid_price'] else 0
            else:
                features['future_mid_price'] = features['mid_price']
                features['target_updown'] = 0
            
            # Добавляем уровни стен
            bid_volumes = [features.get(f'bid_volume_{i+1}', 0) for i in range(10)]
            ask_volumes = [features.get(f'ask_volume_{i+1}', 0) for i in range(10)]
            
            features['bid_wall_level'] = self.find_orderbook_wall_level(bid_volumes, wall_threshold)
            features['ask_wall_level'] = self.find_orderbook_wall_level(ask_volumes, wall_threshold)
            
        except Exception as e:
            print(f"⚠️ Ошибка при добавлении таргетов для {symbol}: {e}")
            features['future_mid_price'] = features['mid_price']
            features['target_updown'] = 0
            features['bid_wall_level'] = 1.0  # Значение по умолчанию вместо np.nan
            features['ask_wall_level'] = 1.0  # Значение по умолчанию вместо np.nan
        
        return features
    
    def calculate_time_series_features(self, symbol: str, current_features: dict, window_seconds: int = 30):
        """Вычисление временных рядов признаков"""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(seconds=window_seconds)
            
            query = text("""
                SELECT timestamp, bids, asks
                FROM orderbook_entries 
                WHERE symbol = :symbol 
                AND timestamp >= :start_time 
                AND timestamp <= :end_time
                ORDER BY timestamp DESC
                LIMIT 60
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query, {
                    "symbol": symbol, 
                    "start_time": start_time, 
                    "end_time": end_time
                })
                
                historical_data = []
                for row in result:
                    bids = json.loads(row.bids)
                    asks = json.loads(row.asks)
                    if bids and asks:
                        pressure = self.calculate_market_pressure(bids, asks)
                        
                        historical_data.append({
                            'timestamp': row.timestamp,
                            'mid_price': (bids[0]['price'] + asks[0]['price']) / 2,
                            'spread': asks[0]['price'] - bids[0]['price'],
                            'volume_imbalance': (sum(b['volume'] for b in bids[:10]) - sum(a['volume'] for a in asks[:10])) / 
                                              (sum(b['volume'] for b in bids[:10]) + sum(a['volume'] for a in asks[:10])),
                            'buy_pressure': pressure.get('buy_pressure', 0),
                            'sell_pressure': pressure.get('sell_pressure', 0),
                            'net_pressure': pressure.get('net_pressure', 0)
                        })
            
            if len(historical_data) > 1:
                historical_data.sort(key=lambda x: x['timestamp'])
                
                mid_prices = [d['mid_price'] for d in historical_data]
                spreads = [d['spread'] for d in historical_data]
                volume_imbalances = [d['volume_imbalance'] for d in historical_data]
                buy_pressures = [d['buy_pressure'] for d in historical_data]
                sell_pressures = [d['sell_pressure'] for d in historical_data]
                net_pressures = [d['net_pressure'] for d in historical_data]
                
                # Изменения цены
                if len(mid_prices) > 1:
                    current_features['price_change_1sec'] = (mid_prices[-1] - mid_prices[-2]) / mid_prices[-2] * 100
                    current_features['price_change_5sec'] = (mid_prices[-1] - mid_prices[-5]) / mid_prices[-5] * 100 if len(mid_prices) >= 5 else 0
                    current_features['price_change_30sec'] = (mid_prices[-1] - mid_prices[0]) / mid_prices[0] * 100
                    current_features['price_volatility_30sec'] = np.std(mid_prices) / np.mean(mid_prices) * 100
                
                # Изменения спреда
                if len(spreads) > 1:
                    current_features['spread_change_1sec'] = spreads[-1] - spreads[-2]
                    current_features['spread_change_5sec'] = spreads[-1] - spreads[-5] if len(spreads) >= 5 else 0
                    current_features['spread_volatility_30sec'] = np.std(spreads)
                
                # Изменения дисбаланса объемов
                if len(volume_imbalances) > 1:
                    current_features['volume_imbalance_change_1sec'] = volume_imbalances[-1] - volume_imbalances[-2]
                    current_features['volume_imbalance_change_5sec'] = volume_imbalances[-1] - volume_imbalances[-5] if len(volume_imbalances) >= 5 else 0
                    current_features['volume_imbalance_volatility_30sec'] = np.std(volume_imbalances)
                
                # Изменения market pressure
                if len(buy_pressures) > 1:
                    current_features['buy_pressure_change_1sec'] = buy_pressures[-1] - buy_pressures[-2]
                    current_features['sell_pressure_change_1sec'] = sell_pressures[-1] - sell_pressures[-2]
                    current_features['net_pressure_change_1sec'] = net_pressures[-1] - net_pressures[-2]
                    current_features['buy_pressure_volatility_30sec'] = np.std(buy_pressures)
                    current_features['sell_pressure_volatility_30sec'] = np.std(sell_pressures)
                    current_features['net_pressure_volatility_30sec'] = np.std(net_pressures)
            
        except Exception as e:
            print(f"Ошибка вычисления временных рядов для {symbol}: {e}")
        
        return current_features
    
    async def process_symbol_features(self, symbol: str):
        """Обработка признаков для одного символа"""
        try:
            query = text("""
                SELECT timestamp, bids, asks
                FROM orderbook_entries 
                WHERE symbol = :symbol 
                ORDER BY timestamp DESC 
                LIMIT 1
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query, {"symbol": symbol})
                row = result.fetchone()
                
                if row:
                    bids = json.loads(row.bids)
                    asks = json.loads(row.asks)
                    
                    # Используем оптимизированную функцию
                    features = self.calculate_optimized_features(bids, asks)
                    
                    features['timestamp'] = row.timestamp
                    features['symbol'] = symbol
                    
                    features = self.calculate_time_series_features(symbol, features)
                    
                    features = self.add_targets_and_walls(features, symbol)
                    
                    await self.save_features_to_csv(features, symbol)
                    
                    print(f"✅ Оптимизированные признаки для {symbol}: {len(features)} признаков")
                    
        except Exception as e:
            print(f"Ошибка обработки признаков для {symbol}: {e}")
    
    async def save_features_to_csv(self, features: dict, symbol: str):
        """Сохранение признаков в CSV файл"""
        try:
            output_dir = "dataset"
            os.makedirs(output_dir, exist_ok=True)
            filename = f"{output_dir}/dataset_{symbol}_{datetime.utcnow().strftime('%Y%m%d')}.csv"
            
            df = pd.DataFrame([features])
            
            try:
                existing_df = pd.read_csv(filename)
                updated_df = pd.concat([existing_df, df], ignore_index=True)
                print(f"📁 Обновлен существующий файл: {filename} ({len(updated_df)} строк)")
            except FileNotFoundError:
                updated_df = df
                print(f"🆕 Создан новый файл: {filename} ({len(updated_df)} строк)")
            
            updated_df.to_csv(filename, index=False)
            
        except Exception as e:
            print(f"❌ Ошибка сохранения признаков для {symbol}: {e}")
    
    async def run_feature_calculation(self):
        """Основной цикл вычисления признаков"""
        while self.is_running:
            try:
                current_time = time.time()
                
                for symbol in TOP_CRYPTO_SYMBOLS:
                    last_time = self.last_calculation_time.get(symbol, 0)
                    
                    if current_time - last_time >= FEATURE_CALCULATION_INTERVAL:
                        await self.process_symbol_features(symbol)
                        self.last_calculation_time[symbol] = current_time
                
                await asyncio.sleep(1)
                
            except Exception as e:
                print(f"Ошибка в цикле вычисления признаков: {e}")
                await asyncio.sleep(5)
    
    async def start(self):
        """Запуск сервиса вычисления признаков"""
        print("🚀 Запуск ОПТИМИЗИРОВАННОГО Feature Calculator...")
        print(f"📊 Вместо 439+ фичей - ~50-60 фичей")
        print(f"⚡ Быстрее обучение, меньше переобучение")
        print(f"🎯 Символы: {TOP_CRYPTO_SYMBOLS}")
        
        self.is_running = True
        await self.run_feature_calculation()
    
    async def stop(self):
        """Остановка сервиса"""
        print("Остановка Optimized Feature Calculator...")
        self.is_running = False


def wait_for_table(engine, table_name, timeout=60):
    """Wait until a table exists in the database, or timeout."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"))
                if result.fetchone():
                    return True
        except Exception:
            pass
        time.sleep(1)
    raise TimeoutError(f"Table {table_name} not found after {timeout} seconds.")


async def main():
    """Главная функция"""
    create_tables()
    engine = create_engine(DATABASE_URL)
    wait_for_table(engine, 'orderbook_entries', timeout=10)
    calculator = OptimizedFeatureCalculator()
    
    try:
        await calculator.start()
    except KeyboardInterrupt:
        print("Получен сигнал остановки...")
    finally:
        await calculator.stop()


if __name__ == "__main__":
    asyncio.run(main()) 