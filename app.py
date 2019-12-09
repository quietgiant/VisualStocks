import producer_controller
import json
import logging
import plotly

from config import kafkaConfiguration
from flask import Flask, request, render_template
from kafka import KafkaProducer
from plotly.graph_objs import Scatter, Layout

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
    bootstrap_servers=kafkaConfiguration['broker'],
    value_serializer=json_serializer,
    api_version=(0, 10, 1))


@app.route("/")
def hello():

    return render_template('index.html');

def ShowData():
    plotly.offline.plot({
        "data": [Scatter(x=["1996-04-12", "1996-05-12", "1996-06-12", "1996-07-12"], y=[4, 3, 2, 1])],
        "layout": Layout(title="Stock Data")
    });
    return "";
@app.route("/stock")
def teststock():
    # parse symbol off URI or passed in from text-box
    symbol = request.args.get('symbol')
    producer_controller.producer_stocks(producer, symbol)
    return f"hit up the producer for symbol {symbol}"


if __name__ == "__main__":
    debug = True
    app.run(debug=debug)
