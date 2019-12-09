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
            close = float(line[4])
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

        save_daily_data(symbol, dailyData, dbContext)
        # save_moving_average_data(tenDay, fiftyDay, dbContext)


def save_daily_data(symbol, dailyData, dbContext):
    doc = {
        'symbol': symbol,
        'data': dailyData,
    }
    collection = dbContext['daily']
    collection.insert_one(doc)

def save_moving_average_data(symbol, tenDay, fiftyDay, dbContext):
    doc = {
        'symbol': symbol,
        'ten-day': tenDay, 
        'fifty-day': fiftyDay,
    }
    collection = dbContext['moving_average']
    collection.insert_one(doc)


if __name__ == "__main__":
    while True:
        consumer()
    pass
