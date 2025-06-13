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


class FeatureCalculator:
    """Сервис для вычисления признаков из orderbook для LSTM+LightGBM"""
    
    def __init__(self):
        self.engine = create_engine(DATABASE_URL)
        self.is_running = False
        self.last_calculation_time = {}
        
    def calculate_market_pressure(self, bids, asks, depth=100):
        """Вычисление market pressure (давления рынка)"""
        if not bids or not asks:
            return {}
        
        # Вычисляем давление покупок и продаж
        buy_pressure = 0
        sell_pressure = 0
        
        # Давление на основе объема и расстояния от mid price
        mid_price = (bids[0]['price'] + asks[0]['price']) / 2
        
        for i, bid in enumerate(bids[:depth]):
            # Вес уменьшается с расстоянием от лучшей цены
            weight = 1.0 / (i + 1)
            # Нормализованное расстояние от mid price
            distance = (mid_price - bid['price']) / mid_price
            buy_pressure += bid['volume'] * weight * (1 - distance)
        
        for i, ask in enumerate(asks[:depth]):
            # Вес уменьшается с расстоянием от лучшей цены
            weight = 1.0 / (i + 1)
            # Нормализованное расстояние от mid price
            distance = (ask['price'] - mid_price) / mid_price
            sell_pressure += ask['volume'] * weight * (1 - distance)
        
        # Общее давление рынка
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
    
    def calculate_features(self, bids, asks, depth=100):
        """Вычисление признаков для LSTM+LightGBM"""
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
        
        # Все 100 уровней для LSTM (последовательные данные)
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
    
    def calculate_time_series_features(self, symbol: str, current_features: dict, window_seconds: int = 30):
        """Вычисление временных рядов признаков для секундных данных"""
        try:
            # Получаем данные за последние N секунд (вместо минут)
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
                        # Вычисляем market pressure для исторических данных
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
                # Сортируем по времени
                historical_data.sort(key=lambda x: x['timestamp'])
                
                # Вычисляем изменения
                mid_prices = [d['mid_price'] for d in historical_data]
                spreads = [d['spread'] for d in historical_data]
                volume_imbalances = [d['volume_imbalance'] for d in historical_data]
                buy_pressures = [d['buy_pressure'] for d in historical_data]
                sell_pressures = [d['sell_pressure'] for d in historical_data]
                net_pressures = [d['net_pressure'] for d in historical_data]
                
                # Изменения цены (для секундных данных)
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
            # Получаем последний orderbook
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
                    
                    # Вычисляем базовые признаки
                    features = self.calculate_features(bids, asks)
                    
                    # Добавляем временные метки
                    features['timestamp'] = row.timestamp
                    features['symbol'] = symbol
                    
                    # Вычисляем временные ряды признаки
                    features = self.calculate_time_series_features(symbol, features)
                    
                    # Сохраняем в CSV
                    await self.save_features_to_csv(features, symbol)
                    
                    print(f"Вычислены признаки для {symbol}: {len(features)} признаков")
                    
        except Exception as e:
            print(f"Ошибка обработки признаков для {symbol}: {e}")
    
    async def save_features_to_csv(self, features: dict, symbol: str):
        """Сохранение признаков в CSV файл"""
        try:
            output_dir = "dataset"
            os.makedirs(output_dir, exist_ok=True)
            filename = f"{output_dir}/dataset_{symbol}_{datetime.utcnow().strftime('%Y%m%d')}.csv"
            print(f"💾 Попытка сохранения фичей для {symbol} в файл: {filename}")
            
            # Создаем DataFrame
            df = pd.DataFrame([features])
            print(f"📊 DataFrame создан: {len(df)} строк, {len(df.columns)} колонок")
            
            # Добавляем к существующему файлу или создаем новый
            try:
                existing_df = pd.read_csv(filename)
                print(f"📁 Найден существующий файл: {filename} с {len(existing_df)} строками")
                updated_df = pd.concat([existing_df, df], ignore_index=True)
                print(f"🔄 Объединен с новыми данными: {len(updated_df)} строк")
            except FileNotFoundError:
                print(f"🆕 Создается новый файл: {filename}")
                updated_df = df
            
            # Сохраняем
            updated_df.to_csv(filename, index=False)
            print(f"✅ Файл сохранен: {filename} ({len(updated_df)} строк)")
            
        except Exception as e:
            print(f"❌ Ошибка сохранения признаков для {symbol}: {e}")
            import traceback
            print(f"🔍 Traceback: {traceback.format_exc()}")
    
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
                
                await asyncio.sleep(1)  # Проверяем каждую секунду (изменено с 10 секунд)
                
            except Exception as e:
                print(f"Ошибка в цикле вычисления признаков: {e}")
                await asyncio.sleep(5)  # Уменьшено с 30 до 5 секунд
    
    async def start(self):
        """Запуск сервиса вычисления признаков"""
        print("Запуск Feature Calculator для LSTM+LightGBM...")
        print(f"Интервал вычисления признаков: {FEATURE_CALCULATION_INTERVAL} секунд")
        print(f"Глубина orderbook: 100 уровней")
        
        self.is_running = True
        await self.run_feature_calculation()
    
    async def stop(self):
        """Остановка сервиса"""
        print("Остановка Feature Calculator...")
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
    # Wait for the orderbook_entries table to exist
    engine = create_engine(DATABASE_URL)
    wait_for_table(engine, 'orderbook_entries', timeout=10)
    calculator = FeatureCalculator()
    
    try:
        await calculator.start()
    except KeyboardInterrupt:
        print("Получен сигнал остановки...")
    finally:
        await calculator.stop()


if __name__ == "__main__":
    asyncio.run(main()) 