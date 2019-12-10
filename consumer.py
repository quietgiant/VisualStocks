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
        print('Message received by consumer...')
        print(f'Payload: \n\n{message}\n\n')
        print(f'Value(size = {len(message.value)}): \n\n{message.value}\n\n')
        symbol = message.value['symbol']
        dailyData = message.value['file-data'][1:] # skip the header line in payload

        # get the required data
        dates, closePrices, volumes = parseDailyData(dailyData)

        # data processing
        tenDayMovingAverageDates, tenDayAverages = calculateTenDayMovingAverages(dates, closePrices)
        fiftyDayMovingAverageDates, fiftyDayAverages = calculateFiftyDayMovingAverages(dates, closePrices)
        print(f'Symbol: \n\n{symbol}\n\n')
        print(f'10 day moving average: {tenDayAverages}')
        print(f'50 day moving average: {fiftyDayAverages}')

        # persist data
        save_daily_data(symbol, dates, closePrices, volumes, dbContext)
        save_moving_average_data(symbol, tenDayMovingAverageDates, fiftyDayMovingAverageDates, tenDayAverages, fiftyDayAverages, dbContext)


def parseDailyData(dailyData):
    dates = []
    closePrices = []
    volumes = []
    for daily in dailyData:
        line = daily.split(',')
        date = str(line[0])
        dates.append(date)
        close = float(line[4])
        closePrices.append(close)
        volume = int(line[5])
        volumes.append(volume)
    return dates, closePrices, volumes


def calculateTenDayMovingAverages(dates, closePrices):
    days = 10
    periods = len(dates) - days
    tenDayMovingAverages = []
    movingAverageDates = []
    for i in range(0, periods):
        indexStart = i + 1
        indexEnd = i + days + 1
        foo = closePrices[indexStart:indexEnd]
        result = sum(foo) / days
        print(f'10 day moving average result ({i}): {result}')
        movingAverageDates.append(dates[-indexStart])
        tenDayMovingAverages.append(result)
    return movingAverageDates, tenDayMovingAverages


def calculateFiftyDayMovingAverages(dates, closePrices):
    days = 50
    periods = len(dates) - days
    fiftyDayMovingAverages = []
    movingAverageDates = []
    for i in range(0, periods):
        indexStart = i + 1
        indexEnd = i + days + 1
        foo = closePrices[indexStart:indexEnd]
        result = sum(foo) / days
        print(f'50 day moving average result ({i}): {result}')
        movingAverageDates.append(dates[-indexStart])
        fiftyDayMovingAverages.append(result)
    return movingAverageDates, fiftyDayMovingAverages


def save_daily_data(symbol, dates, closePrices, volumes, dbContext):
    collection = dbContext['daily']
    if recordExistsForSymbol(symbol, collection):
        print(f'Daily data record already exists for symbol: {symbol}\nDid not overwrite existing data record.')
    else:
        doc = {
            'symbol': symbol,
            'dates': dates,
            'closing': closePrices,
            'volume': volumes
        }
        collection.insert_one(doc)
        print(f'Successfully wrote daily data for symbol {symbol}')
    

def save_moving_average_data(symbol, tenDayDates, fiftyDayDates, tenDay, fiftyDay, dbContext):
    collection = dbContext['moving-average']
    if recordExistsForSymbol(symbol, collection):
        print(f'Moving Average data record already exists for symbol: {symbol}\nDid not overwrite existing data record.')
    else:
        tenDayDates.reverse()
        fiftyDayDates.reverse()
        doc = {
            'symbol': symbol,
            'ten-day-dates': tenDayDates,
            'fifty-day-dates': fiftyDayDates,
            'ten-day-closing': tenDay, 
            'fifty-day-closing': fiftyDay,
        }
        collection = dbContext['moving-average']
        collection.insert_one(doc)
        print(f'Successfully wrote moving average data for symbol {symbol}')


def recordExistsForSymbol(symbol, collection):
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
