import datetime
import json
import requests
import time
import os
import fnmatch

from config import KafkaConfiguration
from kafka import KafkaProducer


def emit_stock_data(producer, symbol):
    print("Producer starting...")
    stock_data = get_data_by_symbol(symbol, producer)
    producer.send(KafkaConfiguration['topic_name'], stock_data)
    print("sent data at {}".format(datetime.datetime.now().timestamp()))


def get_data_by_symbol(symbol, producer):
    pattern = symbol.lower() + ".us.txt"
    for file in os.listdir("data/Stocks"):
        if fnmatch.fnmatch(file, pattern):
            path = os.path.join("data/Stocks", file)
            data = open(path, 'r')
            # TODO read line by line here? or one payload and let consumer parse it all?
            # for _ in range(0, 3):
            # line = data.readline()
            # foo.append(line)
            # producer.send('stock-test', json.dumps(line).encode('utf-8'))
            lines = data.readlines()
            data = {
                'symbol': symbol.upper(),
                'file-data': lines
            }
            return data
    return "Not found"