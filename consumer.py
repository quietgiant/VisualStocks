import json
import logging
import pymongo

from config import MongoConfiguration, KafkaConfiguration
from kafka import KafkaConsumer
from pymongo import MongoClient


def json_deserializer(v):
    if v is None:
        return None
    try:
        return json.loads(v.decode('utf-8'))
    except json.decoder.JSONDecodeError:
        logging.exception('Unable to decode: %s', v)
        return None


def consumer():
    print("Consumer started...")
    consumer = KafkaConsumer(KafkaConfiguration['topic_name'], value_deserializer=json_deserializer)
    cluster = MongoClient(MongoConfiguration['connectionString'])
    dbContext = cluster['stocks']
    for message in consumer:
        # print('Message payload: {}\n\nMessage value: {}\n---\n'.format(message, message.value))
        # compute daily moving average
        symbol = message.value['symbol']
        tenDay = 0.0
        fiftyDay = 0.0
        dates = []
        closePrices = []
        volumes = []
        i = 0
        print(f'Payload: \n\n{message}\n\n')
        print(f'Symbol: \n\n{symbol}\n\n')
        print(f'Value(size = {len(message.value)}): \n\n{message.value}\n\n')
        dailyData = message.value['file-data']
        for daily in dailyData:
            # ty = type(dailyData)
            # print(f'value: {dailyData}\ntype: {ty}\n\n')
            line = daily.split(',')
            if i == 0:
                i += 1
                continue  # skip header line in payload
            date = str(line[0])
            dates.append(transformDate(date))
            close = float(line[4])
            closePrices.append(close)
            volume = int(line[5])
            volumes.append(volume)
            print(f'CLOSE: {close}\n\n')
            if i < 10:
                tenDay += close
            if i < 50:
                fiftyDay += close
            else:
                break
            i += 1

        if i >= 10:
            tenDay = tenDay / 10
        else:
            tenDay = tenDay / (i - 1)

        if i >= 50:
            fiftyDay = fiftyDay / 50
        else:
            fiftyDay = fiftyDay / (i - 1)
        print(f'10 day moving average: {tenDay}')
        print(f'50 day moving average: {fiftyDay}')

        save_daily_data(symbol, dates, closePrices, volumes, dbContext)
        # save_moving_average_data(tenDay, fiftyDay, dbContext)


def save_daily_data(symbol, dates, closePrices, volumes, dbContext):
    doc = {
        'symbol': symbol,
        'dates': dates,
        'closing': closePrices,
        'volume': volumes
    }
    collection = dbContext['daily']
    if recordsExistForSymbol(symbol, collection):
        print(f'Record already exists for symbol: {symbol}\nDid not overwrite existing data record.')
    else:
        collection.insert_one(doc)

def save_moving_average_data(symbol, tenDay, fiftyDay, dbContext):
    doc = {
        'symbol': symbol,
        'ten-day': tenDay, 
        'fifty-day': fiftyDay,
    }
    collection = dbContext['moving_average']
    collection.insert_one(doc)


def recordsExistForSymbol(symbol, collection):
    return collection.count_documents({'symbol': { "$eq": symbol}}) > 0

def transformDate(date):
    # input format: YYYY-MM-DD
    # output format: MM-DD-YYYY
    year = date[:4]
    month = date[5:-3]
    day = date[-2:]
    return str(month + '-' + day + '-' + year)


if __name__ == "__main__":
    while True:
        consumer()
    pass
