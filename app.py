from config import kafkaConfiguration
from flask import Flask
from kafka import KafkaProducer

import producer_stock_files

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers=kafkaConfiguration['broker'])


@app.route("/")
def hello():
    return "Hello World!"


@app.route("/stock")
def teststock():
    producer_stock_files.producer_stocks(producer)
    return "hit up the producer"


if __name__ == "__main__":
    debug = True
    app.run(debug=debug)
