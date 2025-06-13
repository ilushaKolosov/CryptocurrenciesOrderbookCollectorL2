from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, Index, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import json
from datetime import datetime
from config import DATABASE_URL

# Создание движка базы данных
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class OrderbookEntry(Base):
    """Модель для записи orderbook"""
    __tablename__ = "orderbook_entries"
    
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    
    # L2 данные
    bids = Column(Text, nullable=False)  # JSON строка с ценами и объемами
    asks = Column(Text, nullable=False)  # JSON строка с ценами и объемами
    
    # Метаданные
    sequence_id = Column(BigInteger, nullable=True)
    last_update_id = Column(BigInteger, nullable=True)
    
    # Индексы для быстрого поиска
    __table_args__ = (
        Index('idx_symbol_timestamp', 'symbol', 'timestamp'),
    )
    
    def get_bids(self):
        """Получить bids как список словарей"""
        return json.loads(self.bids)
    
    def get_asks(self):
        """Получить asks как список словарей"""
        return json.loads(self.asks)
    
    def set_bids(self, bids):
        """Установить bids из списка словарей"""
        self.bids = json.dumps(bids)
    
    def set_asks(self, asks):
        """Установить asks из списка словарей"""
        self.asks = json.dumps(asks)


def get_db():
    """Получить сессию базы данных"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def create_tables():
    """Создать таблицы в базе данных"""
    Base.metadata.create_all(bind=engine) 