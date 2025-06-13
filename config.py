import os

# Топ-10 криптовалют (без стейбкоинов)
TOP_CRYPTO_SYMBOLS = [
    "BTCUSDT",   # Bitcoin
    "ETHUSDT",   # Ethereum
    "BNBUSDT",   # BNB
    "ADAUSDT",   # Cardano
    "SOLUSDT",   # Solana
    "XRPUSDT",   # Ripple
    "DOTUSDT",   # Polkadot
    "DOGEUSDT",  # Dogecoin
    "AVAXUSDT",  # Avalanche
    "MATICUSDT"  # Polygon
]

# Настройки WebSocket
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream"
BINANCE_REST_URL = "https://api.binance.com"

# Настройки базы данных (PostgreSQL для Docker)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/orderbook_db")

# Настройки сбора данных
ORDERBOOK_DEPTH = 100  # Количество уровней orderbook (изменено с 20 на 100)
UPDATE_INTERVAL = 1.0  # Интервал обновления в секундах
SAVE_INTERVAL = 1.0  # Интервал сохранения в БД (секунды)
FEATURE_CALCULATION_INTERVAL = 1.0  # Интервал вычисления признаков (секунды) - изменено с 60 на 1 для LSTM+GBM 