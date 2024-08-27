import asyncio
import json
from multiprocessing import Process
import os
import pandas as pd
from dotenv import load_dotenv

from db.lin_trades import create_table, delete_old_records, insert_trade, aggregate_ohlc
from api.ws import SocketBybit

load_dotenv()

DATABASE_URL = str(os.getenv('database_url'))

from warnings import filterwarnings
filterwarnings('ignore')

async def custom_on_message(ws, msg):
    try:
        data = json.loads(msg.data)
        if 'data' in data:
            for element in data.get('data'):
                trade = (element['T'], element['s'], element['S'], float(element['v']), float(element['p']))
                try:
                    await insert_trade(*trade)
                except Exception as e:
                    print('failed to insert trade', trade, e)

    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON in custom_on_message: {e}")

async def run_socket(topics, url):
    socket = SocketBybit(url, topics, on_message=custom_on_message)
    await socket.connect()

def run_socket_sync(topics, url):
    asyncio.run(run_socket(topics, url))

async def periodic_delete():
    while True:
        await delete_old_records()
        print('Старые записи удалены')
        await asyncio.sleep(300)  # Подождать 5 минут (300 секунд)

def periodic_delete_sync():
    asyncio.run(periodic_delete())


async def aggregate_ohlc_and_save(period_in_seconds):
    data = pd.DataFrame()  # Локальная переменная для хранения данных

    while True:
        # Получаем OHLC DataFrame
        ohlc_df = await aggregate_ohlc(period_in_seconds)

        if not ohlc_df.empty:
            # Объединяем новые данные с существующими
            data = pd.concat([data, ohlc_df])

            # Оставляем только последние 150 записей по каждому symbol
            data = data.groupby('symbol').tail(150)  #.reset_index()
            print(data.head(50))
            # Сохраняем обновленные данные в CSV файл, не используя индекс
            data.to_csv('data/last_data.csv')
            print(f"DataFrame with {len(data)} rows saved to data/last_data.csv")

        else:
            print("OHLC DataFrame is empty")

        await asyncio.sleep(period_in_seconds)


def run_ohlc_process(period_in_seconds):
    asyncio.run(aggregate_ohlc_and_save(period_in_seconds))

# Основная функция
async def main():
    # Создание таблицы (вызываем один раз)
    await create_table()

    all_topics = [
        #['publicTrade.XRPUSDT'],
        ['publicTrade.ADAUSDT'],
        #['publicTrade.BTCUSDT'],
        #['publicTrade.JUPUSDT'],
    ]

    base_for_topics = 'publicTrade.'
    ws_amount = 1

    url_futures = 'wss://stream.bybit.com/v5/public/linear'

    processes = []
    for topics in all_topics:
        p = Process(target=run_socket_sync, args=(topics, url_futures))
        processes.append(p)
        p.start()

    p = Process(target=periodic_delete_sync)
    processes.append(p)
    p.start()

    # Добавляем процесс для агрегации OHLC данных и сохранения в CSV
    p = Process(target=run_ohlc_process, args=(60,))  # секундный период агрегации
    processes.append(p)
    p.start()

    for p in processes:
        p.join()

if __name__ == '__main__':
    asyncio.run(main())