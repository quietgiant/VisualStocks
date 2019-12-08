import producer_controller
import json
import logging

from config import kafkaConfiguration
from flask import Flask, request
from kafka import KafkaProducer


def json_serializer(v):
    if v is None:
        return None
    try:
        return json.dumps(v).encode('utf-8')
    except ValueError:
        logging.exception('Unable to encode: %s', v)
        return None


app = Flask(__name__)
producer = KafkaProducer(
    bootstrap_servers=kafkaConfiguration['broker'], value_serializer=json_serializer)


@app.route("/")
def hello():
    return "Hello World!"


@app.route("/stock")
def teststock():
    # parse symbol off URI or passed in from text-box
    symbol = request.args.get('symbol')
    producer_controller.producer_stocks(producer, symbol)
    return f"hit up the producer for symbol {symbol}"


if __name__ == "__main__":
    debug = True
    app.run(debug=debug)
