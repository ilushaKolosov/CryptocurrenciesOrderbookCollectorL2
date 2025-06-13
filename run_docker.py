#!/usr/bin/env python3
"""
Скрипт для запуска в Docker с подробными логами
"""

import asyncio
import logging
import sys
import time
import traceback
import psutil
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Добавляем логирование в самом начале
logger.info("🚀 Запуск run_docker.py...")
logger.info(f"Python version: {sys.version}")
logger.info(f"Working directory: {os.getcwd()}")
logger.info(f"Files in directory: {os.listdir('.')}")

def log_system_info():
    """Логирование системной информации"""
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        
        logger.info("🖥️ Системная информация:")
        logger.info(f"   PID: {process.pid}")
        logger.info(f"   Использование памяти: {memory_info.rss / 1024 / 1024:.2f} MB")
        logger.info(f"   Виртуальная память: {memory_info.vms / 1024 / 1024:.2f} MB")
        logger.info(f"   CPU: {psutil.cpu_count()} ядер")
        
        # Информация о контейнере
        if os.path.exists('/proc/1/cgroup'):
            with open('/proc/1/cgroup', 'r') as f:
                cgroup_info = f.read()
                if 'docker' in cgroup_info:
                    logger.info("   Контейнер: Docker")
        
        # Лимиты памяти из cgroup
        try:
            with open('/sys/fs/cgroup/memory/memory.limit_in_bytes', 'r') as f:
                memory_limit = int(f.read().strip())
                logger.info(f"   Лимит памяти контейнера: {memory_limit / 1024 / 1024:.2f} MB")
        except:
            pass
            
    except Exception as e:
        logger.warning(f"⚠️ Не удалось получить системную информацию: {e}")

async def main():
    """Главная функция для Docker"""
    logger.info("🚀 Запуск Binance Orderbook Collector в Docker...")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Working directory: {sys.path[0]}")
    
    # Логируем системную информацию
    log_system_info()
    
    try:
        # Проверяем импорт
        logger.info("📦 Проверка импортов...")
        from binance_orderbook_collector_v2 import BinanceOrderbookCollectorV2
        from feature_calculator import FeatureCalculator
        logger.info("✅ Импорт успешен")
        
        # Создаем коллектор
        logger.info("📊 Инициализация коллектора...")
        collector = BinanceOrderbookCollectorV2()
        logger.info("✅ Коллектор создан")
        
        # Создаем калькулятор фичей
        logger.info("🧮 Инициализация калькулятора фичей...")
        calculator = FeatureCalculator()
        logger.info("✅ Калькулятор фичей создан")
        
        # Запускаем оба процесса параллельно
        logger.info("🔄 Запуск сбора данных и вычисления фичей...")
        
        # Запускаем оба процесса параллельно
        collector_task = asyncio.create_task(collector.start())
        calculator_task = asyncio.create_task(calculator.start())
        
        # Ждем завершения обоих задач
        await asyncio.gather(collector_task, calculator_task)
        
    except KeyboardInterrupt:
        logger.info("🛑 Получен сигнал остановки...")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)
    finally:
        logger.info("✅ Завершение работы")

if __name__ == "__main__":
    try:
        logger.info("🎯 Точка входа в run_docker.py")
        asyncio.run(main())
    except Exception as e:
        logger.error(f"❌ Ошибка запуска: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1) 