import datetime
import json
import requests
import time
import os
import fnmatch

from config import kafkaConfiguration
from config import alphaVantageConfiguration
from kafka import KafkaProducer


def producer_stocks(producer, symbol):
    print("hello from producer!")
    stock_data = get_data_by_symbol(symbol, producer)
    producer.send('stock-test', stock_data)
    # producer.send('stock-test', json.dumps(stock_data).encode('utf-8'))
    print("sent data at {}".format(datetime.datetime.now().timestamp()))
    # maybe sleep(5)
    # read share data and DMA from database
    # plot as graph


def get_data_by_symbol(symbol, producer):
    pattern = symbol.lower() + ".us.txt"
    for file in os.listdir("data/Stocks"):
        if fnmatch.fnmatch(file, pattern):
            path = os.path.join("data/Stocks", file)
            data = open(path, 'r')
            # TODO read line by line here? or one payload and let consumer parse it all?
            foo = []
            # for _ in range(0, 3):
                # line = data.readline()
                # foo.append(line)
                # producer.send('stock-test', json.dumps(line).encode('utf-8'))
            return data.readlines()
    return "Not found"


def get_data_alphavantage(symbol):
    interval = '5min'
    uri = construct_request_uri(symbol, interval)
    req = requests.get(uri)

    if req.status_code == 200:
        raw_data = json.loads(req.content)
        try:
            time_series_key = f'Time Series ({interval})'
            price = raw_data[time_series_key]
            meta = raw_data['Meta Data']
        except KeyError:
            print(
                f'Could not find key {time_series_key} in response payload.\n\nResponse payload: {raw_data}\n')
            exit()

        last_price = price[max(price.keys())]
        value = {"symbol": symbol,
                 "time": meta['3. Last Refreshed'],
                 "open": last_price['1. open'],
                 "high": last_price['2. high'],
                 "low": last_price['3. low'],
                 "close": last_price['4. close'],
                 "volume": last_price['5. volume']}

        print('Got {}\ latest min data at {}'.format(
            symbol, datetime.datetime.now()))
    else:
        print('Failed to get data for {} at {} [Response status code: {}]'.format(
            symbol, datetime.datetime.now(), req.status_code))
        value = {"symbol": 'None',
                 "time": 'None',
                 "open": 0.,
                 "high": 0.,
                 "low": 0.,
                 "close": 0.,
                 "volume": 0.}

    return value


def construct_request_uri(symbol, interval):
    window = 'compact'  # compact for last 100 data points, full for all of intraday data points
    return f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&outputsize={window}&interval={interval}&apikey={alphaVantageConfiguration["api_key"]}'
