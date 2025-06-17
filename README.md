# Cryptocurrencies L2 Orderbook Collector & Feature Engineering

Система для сбора, хранения и генерации оптимизированного датасета L2 orderbook с Binance для гибридных моделей: CNN+LSTM+CatBoost (и Boruta).

## 🎯 Цель проекта

Создать автоматизированную систему, которая:
1. **Собирает** L2 orderbook данные с Binance (100 уровней глубины)
2. **Вычисляет** оптимизированные (~50-60) и derived признаки для ML/DL моделей
3. **Генерирует** датасеты для гибридных пайплайнов (CNN+LSTM+CatBoost)
4. **Работает** в реальном времени с частотой 1 секунда

## 🏗️ Архитектура системы

```
┌───────────────┐    ┌───────────────┐    ┌───────────────┐
│ Binance API   │──▶│ WebSocket     │──▶│ In-Memory     │
│ (REST + WS)   │   │ (100ms)       │   │ Cache         │
└───────────────┘    └───────────────┘    └───────────────┘
                                               │
                                               ▼
┌───────────────┐    ┌───────────────┐    ┌───────────────┐
│ PostgreSQL    │◀──│ Feature       │◀──│ Orderbook     │
│ Database      │   │ Calculator    │   │ Processor     │
└───────────────┘    └───────────────┘    └───────────────┘
      │                      │                      │
      ▼                      ▼                      ▼
┌───────────────┐    ┌───────────────┐    ┌───────────────┐
│ Raw Orderbook │    │ Features      │    │ CSV Datasets  │
│ Data          │    │ (Optimized)   │    │ in dataset/   │
└───────────────┘    └───────────────┘    └───────────────┘
```

## 📊 Отслеживаемые криптовалюты

Система отслеживает топ-10 криптовалют (без стейбкоинов):
- **BTCUSDT** (Bitcoin) - эталонная криптовалюта
- **ETHUSDT** (Ethereum) - смарт-контракты
- **BNBUSDT** (BNB) - Binance Coin
- **ADAUSDT** (Cardano) - научный подход
- **SOLUSDT** (Solana) - высокая производительность
- **XRPUSDT** (Ripple) - банковские решения
- **DOTUSDT** (Polkadot) - интероперабельность
- **DOGEUSDT** (Dogecoin) - мем-коин
- **AVAXUSDT** (Avalanche) - DeFi платформа
- **MATICUSDT** (Polygon) - масштабирование Ethereum

## ⚡ Частота и глубина данных

- **WebSocket обновления**: каждые 100ms (реальное время)
- **Глубина orderbook**: 100 уровней (bids + asks = 200 уровней)
- **Сохранение в БД**: каждую секунду
- **Вычисление признаков**: каждую секунду
- **Общий объем**: ~10,000+ записей в день на символ

## 🔄 Детальный flow данных

### 1. Инициализация (REST API)
```
Binance REST API → Получение начального orderbook (100 уровней) → Кэш в памяти
```

### 2. Реальное время (WebSocket)
```
Binance WebSocket → Обновления orderbook → Обновление кэша → Сохранение в PostgreSQL
```

### 3. Обработка данных
```
PostgreSQL → Feature Calculator → оптимизированные признаки → CSV файлы в dataset/
```

### 4. Экспорт датасетов
```
PostgreSQL → Dataset Exporter → Готовые датасеты для ML/DL
```

## 🧮 ОПТИМИЗИРОВАННЫЕ ФИЧИ (~50-60)

### 📈 Базовые ценовые признаки (3 фичи)
- **mid_price**: средняя цена между лучшим bid и ask
- **spread**: разница между лучшим ask и bid
- **spread_percent**: спред в процентах от mid_price

### 🎯 Market Pressure (5 фичей)
Ключевой индикатор рыночного давления:

**Формула расчета:**
```
weight = 1.0 / (level + 1)  # Вес уменьшается с глубиной
distance = |price - mid_price| / mid_price  # Нормализованное расстояние
pressure = volume × weight × (1 - distance)  # Давление на уровне
```

**Метрики:**
- **buy_pressure**: общее давление покупок
- **sell_pressure**: общее давление продаж
- **net_pressure**: чистое давление (buy - sell)
- **pressure_ratio**: соотношение buy/sell давления
- **total_pressure**: общее рыночное давление

### 📊 LSTM признаки (400 фичей)
Последовательные данные для рекуррентных сетей:

**Нормализованные цены (200 фичей):**
- `bid_price_norm_1` до `bid_price_norm_100`: нормализованные цены bids
- `ask_price_norm_1` до `ask_price_norm_100`: нормализованные цены asks
- Нормализация: `(price - mid_price) / mid_price`

**Объемы (200 фичей):**
- `bid_volume_1` до `bid_volume_100`: объемы на каждом уровне bids
- `ask_volume_1` до `ask_volume_100`: объемы на каждом уровне asks

### 📈 Статистические признаки для LightGBM (20+ фичей)

**Объемные метрики (3 фичи):**
- **total_bid_volume**: сумма всех объемов bids
- **total_ask_volume**: сумма всех объемов asks
- **volume_imbalance**: дисбаланс объемов `(bid_vol - ask_vol) / (bid_vol + ask_vol)`

**Статистики цен (6 фичей):**
- **bid_price_mean/std**: среднее и стандартное отклонение цен bids
- **ask_price_mean/std**: среднее и стандартное отклонение цен asks
- **bid_volume_mean/std**: среднее и стандартное отклонение объемов bids
- **ask_volume_mean/std**: среднее и стандартное отклонение объемов asks

**Концентрация объемов (2 фичи):**
- **bid_volume_concentration**: доля объема в топ-10 уровнях bids
- **ask_volume_concentration**: доля объема в топ-10 уровнях asks

### ⏰ Временные ряды признаки (15+ фичей)
Изменения за последние 30 секунд:

**Изменения цены (4 фичи):**
- **price_change_1sec**: изменение цены за 1 секунду (%)
- **price_change_5sec**: изменение цены за 5 секунд (%)
- **price_change_30sec**: изменение цены за 30 секунд (%)
- **price_volatility_30sec**: волатильность цены за 30 секунд (%)

**Изменения спреда (3 фичи):**
- **spread_change_1sec**: изменение спреда за 1 секунду
- **spread_change_5sec**: изменение спреда за 5 секунд
- **spread_volatility_30sec**: волатильность спреда за 30 секунд

**Изменения дисбаланса (3 фичи):**
- **volume_imbalance_change_1sec**: изменение дисбаланса за 1 секунду
- **volume_imbalance_change_5sec**: изменение дисбаланса за 5 секунд
- **volume_imbalance_volatility_30sec**: волатильность дисбаланса за 30 секунд

**Изменения Market Pressure (6 фичей):**
- **buy_pressure_change_1sec**: изменение давления покупок за 1 секунду
- **sell_pressure_change_1sec**: изменение давления продаж за 1 секунду
- **net_pressure_change_1sec**: изменение чистого давления за 1 секунду
- **buy_pressure_volatility_30sec**: волатильность давления покупок за 30 секунд
- **sell_pressure_volatility_30sec**: волатильность давления продаж за 30 секунд
- **net_pressure_volatility_30sec**: волатильность чистого давления за 30 секунд

## 📁 Структура проекта

```
├── binance_orderbook_collector_v2.py  # Основной сборщик данных
├── feature_calculator.py              # Оптимизированные признаки
├── export_for_lstm_gbm.py             # Экспорт датасетов
├── run_docker.py                      # Запуск в Docker (асинхронно)
├── config.py                          # Конфигурация системы
├── database.py                        # Модели PostgreSQL
├── requirements.txt                   # Python зависимости
├── docker-compose.yml                 # Docker Compose конфигурация
├── Dockerfile                         # Docker образ
├── dataset/                           # Директория с CSV файлами
└── README.md                          # Документация
```

## 🚀 Быстрый старт

### 1. Установка зависимостей
```bash
pip install -r requirements.txt
```

### 2. Настройка базы данных
Проверьте `DATABASE_URL` в `config.py` или установите переменную окружения:
```bash
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orderbook_db"
```

### 3. Запуск системы

**В Docker (рекомендуется):**
```bash
docker-compose up --build
```

**Локально:**
```bash
python run_docker.py
```

## 📊 Выходные файлы

### Реальное время (dataset/)
- `dataset/dataset_{symbol}_{date}.csv` - оптимизированные датасеты
- Обновляется каждую секунду
- Содержит 50-60 признаков

### Экспорт датасетов
- `{symbol}_optimized_lstm_matrix_{date}.npy` - numpy-матрицы для DL
- `{symbol}_optimized_dataset_{date}.csv` - оптимизированный датасет
- `all_symbols_optimized_dataset_{hours}h.csv` - объединенный оптимизированный датасет

## 🔧 Экспорт датасетов

```bash
# Экспорт всех символов за 24 часа
python export_for_lstm_gbm.py --all-symbols --hours 24

# Экспорт одного символа за 48 часов
python export_for_lstm_gbm.py --symbol BTCUSDT --hours 48

# Экспорт в кастомную директорию
python export_for_lstm_gbm.py --all-symbols --hours 24 --output-dir my_datasets
```

## ⚙️ Настройка системы

В `config.py`:
```python
ORDERBOOK_DEPTH = 100                    # Глубина orderbook
SAVE_INTERVAL = 1.0                      # Сохранение в БД (секунды)
FEATURE_CALCULATION_INTERVAL = 1.0       # Признаки (секунды)
TOP_CRYPTO_SYMBOLS = [...]               # Список символов
```

## 📈 Использование для ML

### LSTM модель (последовательные данные)
```python
import numpy as np
import pandas as pd

# Загружаем numpy-матрицу для LSTM
lstm_matrix = np.load('dataset/BTCUSDT_optimized_lstm_matrix_20241201_20241202.npy')
print(f"Форма: {lstm_matrix.shape}")  # (samples, timesteps, features)

# Используем нормализованные цены и объемы
price_features = lstm_matrix[:, :, :200]  # 200 нормализованных цен
volume_features = lstm_matrix[:, :, 200:400]  # 200 объемов
```

### LightGBM модель (статистические метрики)
```python
import pandas as pd
import lightgbm as lgb

# Загружаем датасет
df = pd.read_csv('dataset/BTCUSDT_optimized_dataset_20241201_20241202.csv')

# Выбираем признаки для LightGBM
gbm_features = [
    'mid_price', 'spread', 'spread_percent',
    'buy_pressure', 'sell_pressure', 'net_pressure',
    'volume_imbalance', 'total_bid_volume', 'total_ask_volume',
    'bid_price_mean', 'ask_price_mean', 'bid_volume_mean',
    'price_change_1sec', 'price_volatility_30sec',
    'spread_change_1sec', 'volume_imbalance_change_1sec'
]

X = df[gbm_features]
y = df['price_change_1sec'].shift(-1)  # Предсказываем следующее изменение
```

## 📊 Мониторинг системы

Система выводит подробные логи:
- ✅ Статус подключения к WebSocket
- 📊 Количество сохраненных уровней orderbook
- 🧮 Время вычисления признаков
- 📁 Сохранение датасетов в CSV
- ❌ Ошибки подключения и обработки
- 💾 Использование памяти

## 🎯 Результат

Оптимизированный датасет для обучения ML моделей с:
- **50-60 признаками** для каждого временного среза
- **100 уровнями** orderbook глубины
- **Market Pressure** метриками
- **Временными рядами** изменений
- **Автоматическим** обновлением каждую секунду
- **Оптимизированными** CSV файлами для LSTM и LightGBM

## 🔬 Научная основа

Система основана на исследованиях:
- **Market Microstructure Theory** - структура orderbook
- **High-Frequency Trading** - анализ рыночных данных
- **Time Series Analysis** - временные ряды в финансах
- **Machine Learning** - LSTM и Gradient Boosting для предсказания цен 

## 🤖 Гибридный ML/DL пайплайн

1. **CNN+LSTM** — извлекает скрытые паттерны из временного ряда стакана (сырые уровни)
2. **CatBoost** — работает с derived features и скрытыми признаками из DL
3. **Boruta** — отбирает наиболее важные табличные признаки

**Вход для CNN+LSTM:**
- Последовательность стаканов за N секунд: `[batch, time, features]`, где features — только сырые уровни

**Вход для CatBoost:**
- Derived features + скрытые признаки из CNN+LSTM

## 📚 Зависимости

- websockets, aiohttp, sqlalchemy, psycopg2-binary, pandas, numpy, psutil, python-dotenv

## 📝 Авторы и поддержка

- Проект оптимизирован для гибридных пайплайнов (DL+ML) и реального времени
- Вопросы и предложения — через issues или pull requests 