import asyncio
import json
import websockets
import aiohttp
import logging
import psutil
import os
from datetime import datetime
from typing import Dict, List, Optional
from database import OrderbookEntry, SessionLocal, create_tables
from config import TOP_CRYPTO_SYMBOLS, BINANCE_WS_URL, BINANCE_REST_URL, ORDERBOOK_DEPTH, SAVE_INTERVAL
import time
import traceback

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

create_tables()

class BinanceOrderbookCollectorV2:
    """Улучшенный коллектор L2 orderbook с Binance"""
    
    def __init__(self):
        logger.info("🔧 Инициализация BinanceOrderbookCollectorV2...")
        self.websocket = None
        self.is_running = False
        self.orderbooks = {}  # Кэш текущих orderbook
        self.last_save_time = {}  # Время последнего сохранения для каждого символа
        self.db = SessionLocal()
        self.last_memory_log = 0
        self.message_counter = 0  # Счетчик WebSocket сообщений
        logger.info(f"📊 Символы для сбора: {TOP_CRYPTO_SYMBOLS}")
        logger.info(f"🗄️ Глубина orderbook: {ORDERBOOK_DEPTH} уровней")
        logger.info(f"⏱️ Интервал сохранения: {SAVE_INTERVAL} секунд")
        
    def log_memory_usage(self):
        """Логирование использования памяти"""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            current_time = time.time()
            
            # Логируем каждые 30 секунд
            if current_time - self.last_memory_log > 30:
                logger.info(f"💾 Использование памяти: {memory_info.rss / 1024 / 1024:.2f} MB")
                logger.info(f"📊 Размер кэша orderbook: {len(self.orderbooks)} символов")
                self.last_memory_log = current_time
                
        except Exception as e:
            logger.warning(f"⚠️ Не удалось получить информацию о памяти: {e}")
        
    async def initialize_orderbooks(self):
        """Инициализация orderbook через REST API"""
        logger.info("🔄 Инициализация orderbook через REST API...")
        
        async with aiohttp.ClientSession() as session:
            for symbol in TOP_CRYPTO_SYMBOLS:
                try:
                    logger.info(f"📡 Получение orderbook для {symbol}...")
                    # Получаем начальный orderbook с 100 уровнями
                    url = f"{BINANCE_REST_URL}/api/v3/depth"
                    params = {
                        'symbol': symbol,
                        'limit': ORDERBOOK_DEPTH
                    }
                    
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            # Обработка данных
                            bids = []
                            for bid in data['bids']:
                                bids.append({
                                    'price': float(bid[0]),
                                    'volume': float(bid[1])
                                })
                            
                            asks = []
                            for ask in data['asks']:
                                asks.append({
                                    'price': float(ask[0]),
                                    'volume': float(ask[1])
                                })
                            
                            # Сохраняем в кэш
                            self.orderbooks[symbol] = {
                                'bids': bids,
                                'asks': asks,
                                'last_update_id': data['lastUpdateId'],
                                'last_save_time': time.time()
                            }
                            
                            # Сразу сохраняем инициализированные данные в БД
                            await self.save_orderbook(symbol, bids, asks, data['lastUpdateId'])
                            
                            logger.info(f"✅ Инициализирован orderbook для {symbol} ({len(bids)} bids, {len(asks)} asks)")
                            
                        else:
                            logger.error(f"❌ Ошибка получения orderbook для {symbol}: {response.status}")
                            
                except Exception as e:
                    logger.error(f"❌ Ошибка инициализации {symbol}: {e}")
                
                await asyncio.sleep(0.1)  # Небольшая пауза между запросами
    
    async def connect_websocket(self):
        """Подключение к WebSocket с 100 уровнями"""
        # Создаем строку подписки для всех символов с 100 уровнями
        # Правильный формат: symbol@depth@100ms
        streams = [f"{symbol.lower()}@depth@100ms" for symbol in TOP_CRYPTO_SYMBOLS]
        stream_url = f"{BINANCE_WS_URL}?streams={'/'.join(streams)}"
        
        logger.info(f"🔌 Подключение к WebSocket: {stream_url}")
        
        try:
            self.websocket = await websockets.connect(stream_url)
            logger.info("✅ WebSocket подключен успешно")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к WebSocket: {e}")
            return False
    
    async def process_orderbook_update(self, data: Dict):
        """Обработка обновления orderbook"""
        try:
            stream_data = data['data']
            symbol = stream_data['s']
            
            # Логируем входящие WebSocket данные
            logger.info(f"📡 WebSocket данные для {symbol}:")
            logger.info(f"   Update ID: {stream_data['u']}")
            logger.info(f"   Bids обновлений: {len(stream_data['b'])}")
            logger.info(f"   Asks обновлений: {len(stream_data['a'])}")
            if stream_data['b']:
                logger.info(f"   Лучший bid: {stream_data['b'][0]}")
            if stream_data['a']:
                logger.info(f"   Лучший ask: {stream_data['a'][0]}")
            
            # Проверяем, что это один из наших символов
            if symbol not in TOP_CRYPTO_SYMBOLS:
                return
            
            # Получаем текущий orderbook
            current_orderbook = self.orderbooks.get(symbol, {
                'bids': [],
                'asks': [],
                'last_update_id': 0,
                'last_save_time': time.time()
            })
            
            # Проверяем последовательность обновлений
            if stream_data['u'] <= current_orderbook['last_update_id']:
                return  # Пропускаем устаревшие обновления
            
            # Обновляем bids
            for bid in stream_data['b']:
                price = float(bid[0])
                volume = float(bid[1])
                
                # Находим и обновляем или добавляем уровень
                updated = False
                for i, existing_bid in enumerate(current_orderbook['bids']):
                    if existing_bid['price'] == price:
                        if volume == 0:
                            current_orderbook['bids'].pop(i)
                        else:
                            existing_bid['volume'] = volume
                        updated = True
                        break
                
                if not updated and volume > 0:
                    current_orderbook['bids'].append({'price': price, 'volume': volume})
            
            # Обновляем asks
            for ask in stream_data['a']:
                price = float(ask[0])
                volume = float(ask[1])
                
                # Находим и обновляем или добавляем уровень
                updated = False
                for i, existing_ask in enumerate(current_orderbook['asks']):
                    if existing_ask['price'] == price:
                        if volume == 0:
                            current_orderbook['asks'].pop(i)
                        else:
                            existing_ask['volume'] = volume
                        updated = True
                        break
                
                if not updated and volume > 0:
                    current_orderbook['asks'].append({'price': price, 'volume': volume})
            
            # Сортируем bids (по убыванию цены) и asks (по возрастанию цены)
            current_orderbook['bids'].sort(key=lambda x: x['price'], reverse=True)
            current_orderbook['asks'].sort(key=lambda x: x['price'])
            
            # Ограничиваем глубину до 100 уровней
            current_orderbook['bids'] = current_orderbook['bids'][:ORDERBOOK_DEPTH]
            current_orderbook['asks'] = current_orderbook['asks'][:ORDERBOOK_DEPTH]
            
            # Обновляем last_update_id
            current_orderbook['last_update_id'] = stream_data['u']
            
            # Сохраняем обновленный orderbook в кэш
            self.orderbooks[symbol] = current_orderbook
            
            # Проверяем, нужно ли сохранить в БД
            current_time = time.time()
            if current_time - current_orderbook.get('last_save_time', 0) >= SAVE_INTERVAL:
                await self.save_orderbook(
                    symbol, 
                    current_orderbook['bids'], 
                    current_orderbook['asks'], 
                    current_orderbook['last_update_id']
                )
                current_orderbook['last_save_time'] = current_time
                self.orderbooks[symbol] = current_orderbook
                
        except Exception as e:
            logger.error(f"❌ Ошибка обработки обновления orderbook: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
    
    async def save_orderbook(self, symbol: str, bids: List[Dict], asks: List[Dict], last_update_id: int):
        """Сохранение orderbook в базу данных"""
        try:
            # Логируем данные перед сохранением
            logger.info(f"💾 Попытка сохранения для {symbol}:")
            logger.info(f"   Bids: {len(bids)} уровней, лучшая цена: {bids[0]['price'] if bids else 'N/A'}")
            logger.info(f"   Asks: {len(asks)} уровней, лучшая цена: {asks[0]['price'] if asks else 'N/A'}")
            logger.info(f"   Last Update ID: {last_update_id}")
            
            entry = OrderbookEntry()
            entry.symbol = symbol
            entry.timestamp = datetime.utcnow()
            entry.set_bids(bids)
            entry.set_asks(asks)
            entry.last_update_id = last_update_id
            
            self.db.add(entry)
            self.db.commit()
            
            logger.info(f"✅ Сохранен orderbook для {symbol} ({len(bids)} bids, {len(asks)} asks)")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения в БД: {e}")
            logger.error(f"   Symbol: {symbol}")
            logger.error(f"   Bids count: {len(bids)}")
            logger.error(f"   Asks count: {len(asks)}")
            self.db.rollback()
    
    async def listen_websocket(self):
        """Прослушивание WebSocket сообщений"""
        logger.info("🎧 Начало прослушивания WebSocket...")
        while self.is_running:
            try:
                if self.websocket is None:
                    logger.info("🔌 Попытка переподключения к WebSocket...")
                    if not await self.connect_websocket():
                        logger.warning("⚠️ Не удалось подключиться к WebSocket, ожидание 5 секунд...")
                        await asyncio.sleep(5)
                        continue
                
                logger.info("📡 Ожидание WebSocket сообщения...")
                
                # Добавляем таймаут для WebSocket
                try:
                    message = await asyncio.wait_for(self.websocket.recv(), timeout=30.0)
                except asyncio.TimeoutError:
                    logger.warning("⏰ Таймаут WebSocket (30 сек), переподключение...")
                    self.websocket = None
                    continue
                
                self.message_counter += 1
                logger.info(f"📡 Получено WebSocket сообщение #{self.message_counter}")
                
                # Логируем каждое 10-е сообщение полностью
                if self.message_counter % 10 == 0:
                    logger.info(f"📡 WebSocket сообщение #{self.message_counter}: {message[:200]}...")
                
                data = json.loads(message)
                
                await self.process_orderbook_update(data)
                
                # Периодический мониторинг памяти
                self.log_memory_usage()
                
            except websockets.exceptions.ConnectionClosed:
                logger.warning("🔌 WebSocket соединение закрыто, переподключение...")
                self.websocket = None
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"❌ Ошибка в WebSocket: {e}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                await asyncio.sleep(1)
    
    async def start(self):
        """Запуск коллектора"""
        logger.info("🚀 Запуск Binance Orderbook Collector V2...")
        logger.info(f"🗄️ Глубина orderbook: {ORDERBOOK_DEPTH} уровней")
        logger.info(f"⏱️ Интервал сохранения: {SAVE_INTERVAL} секунд")
        
        # Создаем таблицы
        logger.info("🗄️ Создание таблиц в базе данных...")
        create_tables()
        logger.info("✅ Таблицы созданы")
        
        # Инициализируем orderbook
        await self.initialize_orderbooks()
        
        # Запускаем WebSocket
        self.is_running = True
        await self.listen_websocket()
    
    async def stop(self):
        """Остановка коллектора"""
        logger.info("🛑 Остановка коллектора...")
        self.is_running = False
        
        if self.websocket:
            await self.websocket.close()
        
        if self.db:
            self.db.close()
        logger.info("✅ Коллектор остановлен")


async def main():
    """Главная функция"""
    collector = BinanceOrderbookCollectorV2()
    
    try:
        await collector.start()
    except KeyboardInterrupt:
        print("Получен сигнал остановки...")
    finally:
        await collector.stop()


if __name__ == "__main__":
    asyncio.run(main()) 