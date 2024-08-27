import os
from dotenv import load_dotenv

import asyncio
from sqlalchemy import delete,inspect
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Float, BigInteger, func
from sqlalchemy.sql import text

from datetime import datetime, timedelta
import pandas as pd


load_dotenv()

DATABASE_URL = str(os.getenv('database_url'))


# Создаем базу данных
Base = declarative_base()

class FuturesTrades(Base):
    __tablename__ = 'futures_trades_table'
    id = Column(BigInteger, primary_key=True, index=True)
    timestamp = Column(BigInteger, nullable=False)  # 'T': 1719385801438
    symbol = Column(String, nullable=False)  # 's': 'XRPUSDT'
    side = Column(String, nullable=False)  # 'S': 'Buy'
    volume = Column(Float, nullable=False)  # 'v': '1614'
    price = Column(Float, nullable=False)  # 'p': '0.4729'
    bt = Column(Boolean, nullable=False)  # 'BT': False
    created_at = Column(DateTime(timezone=True), server_default=func.now())


# Создаем асинхронный движок базы данных
engine = create_async_engine(DATABASE_URL, echo=False)


# # Создаем асинхронный сеанс
async_session = sessionmaker(engine, class_=AsyncSession)



# Функция для создания таблицы

async def table_exists(table_name):
    async with engine.connect() as conn:
        result = await conn.scalar(
            text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = :table_name)"),
            {"table_name": table_name}
        )
        return result

async def create_table():
    if not await table_exists(FuturesTrades.__tablename__):
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    else:
        print(f"Table '{FuturesTrades.__tablename__}' already exists, skipping creation.")

# Функция для вставки одной записи
async def insert_trade(timestamp, symbol, side, volume, price, bt):
    async with async_session() as session:
        async with session.begin():
            trade = FuturesTrades(timestamp=timestamp, symbol=symbol, side=side, volume=volume, price=price, bt=bt)
            session.add(trade)
            await session.commit()

# Функция для вставки нескольких записей
async def insert_trades_bulk_ACID(trades):
    async with async_session() as session:
        async with session.begin():
            session.add_all([FuturesTrades(**trade) for trade in trades])
            await session.commit()

# Функция для выборки данных
async def select_trades():
    async with async_session() as session:
        result = await session.execute(select(FuturesTrades))
        trades = result.scalars().all()
        for trade in trades:
            print(trade)

# Функция для обновления записи
async def update_trade(trade_id, new_timestamp, new_symbol, new_side, new_volume, new_price, new_bt):
    async with async_session() as session:
        async with session.begin():
            result = await session.execute(select(FuturesTrades).filter_by(id=trade_id))
            trade = result.scalar_one()
            trade.timestamp = new_timestamp
            trade.symbol = new_symbol
            trade.side = new_side
            trade.volume = new_volume
            trade.price = new_price
            trade.bt = new_bt
            await session.commit()

# Функция для удаления записи
async def delete_trade(trade_id):
    async with async_session() as session:
        async with session.begin():
            result = await session.execute(select(FuturesTrades).filter_by(id=trade_id))
            trade = result.scalar_one()
            await session.delete(trade)
            await session.commit()


async def delete_old_records():
    async with async_session() as session:
        async with session.begin():
            five_minutes_ago = datetime.now() - timedelta(minutes=5)
            stmt = delete(FuturesTrades).where(FuturesTrades.created_at < five_minutes_ago)
            await session.execute(stmt)
            await session.commit()


async def aggregate_ohlc(period_in_seconds):
    async with async_session() as session:
        async with session.begin():
            now = datetime.now()
            period_start_time = now - timedelta(seconds=period_in_seconds)
            period_boundary = int(period_start_time.timestamp() * 1000)  # В миллисекундах

            result = await session.execute(
                select(FuturesTrades)
                .where(FuturesTrades.timestamp < period_boundary)
            )

            trades = result.scalars().all()

            if not trades:
                print("No trades found for the given period.")
                return pd.DataFrame()  # Возвращаем пустой DataFrame, если нет записей

            data = [{
                'timestamp': trade.timestamp,
                'symbol': trade.symbol,
                'side': trade.side,
                'volume': trade.volume,
                'price': trade.price
            } for trade in trades]

            df = pd.DataFrame(data)

            # Преобразуем столбец 'timestamp' в datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

            # Устанавливаем 'timestamp' как индекс
            df.set_index('timestamp', inplace=True)
            # print("Initial DataFrame:\n", df.head())

            # Создание OHLC
            ohlc_dict = {}
            for symbol, group in df.groupby('symbol'):
                # Применяем resample и создаем OHLC
                ohlc = group['price'].resample(f'{period_in_seconds}s').ohlc()
                ohlc['volume'] = group['volume'].resample(f'{period_in_seconds}s').sum()

                # Добавляем столбец 'symbol' к OHLC
                ohlc = ohlc.reset_index()
                # ohlc['symbol'] = symbol
                ohlc.set_index('timestamp', inplace=True)

                ohlc_dict[symbol] = ohlc

            # Конкатенация всех группировок в один DataFrame
            ohlc_df = pd.concat(ohlc_dict.values(), keys=ohlc_dict.keys())
            ohlc_df.reset_index(level=0, inplace=True)
            ohlc_df.rename(columns={'level_0': 'symbol'}, inplace=True)
            # print("DataFrame with OHLC data:\n", ohlc_df.head())

            # Заполнение пропусков
            # Обработка пропущенных значений
            ohlc_df['open'] = ohlc_df['open'].ffill()
            ohlc_df['high'] = ohlc_df['high'].ffill()
            ohlc_df['low'] = ohlc_df['low'].ffill()
            ohlc_df['close'] = ohlc_df['close'].ffill()
            ohlc_df['volume'] = ohlc_df['volume'].fillna(0)

            # Печать структуры данных после заполнения пропусков
            print("DataFrame after filling missing values:\n", ohlc_df.head(20))

            # Удаление записей из базы данных
            stmt = delete(FuturesTrades).where(FuturesTrades.timestamp < period_boundary)
            await session.execute(stmt)
            await session.commit()

            return ohlc_df



async def main():
    await create_table()
    res = await aggregate_ohlc(15)
    #print(res.head())

# Запуск асинхронного кода
if __name__ == "__main__":
    asyncio.run(main())