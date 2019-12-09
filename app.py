import producer
import json
import logging
import plotly
import plotly.plotly as py
import plotly.graph_objs as go
import numpy as np

from config import KafkaConfiguration
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
kafka_producer = KafkaProducer(
    bootstrap_servers=KafkaConfiguration['broker'],
    value_serializer=json_serializer,
    api_version=(0, 10, 1))


@app.route("/")
def hello():
    return render_template('index.html')


@app.route('/chart')
def line():
    count = request.args.get('DateRange')
    if (count is None):
        count = 10
    else:
        count = int(count)
    foo = int(count * 2)
    xScale = np.linspace(0, foo)
    yScale = np.random.randn(count)
    yScale2 = np.random.randn(count)
 
    # Create a trace
    stock_chart = go.Scatter(
        x = ["1-20-1995", "2-20-1995", "3-20-1995"],
        y = [10, 20, 30]
    )

    volume_graph = go.Bar(
        x=[0, 1, 2, 3, 4, 5],
        y=[1, 0.5, 0.7, -1.2, 0.3, 0.4]
    )

    data = [stock_chart]
    graphJSON = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('index.html', graphJSON=graphJSON)


@app.route("/stock")
def teststock():
    # parse symbol off URI or passed in from text-box
    symbol = request.args.get('symbol')
    producer.emit_stock_data(kafka_producer, symbol)
    return f"Requesting producer to emit data for symbol: {symbol}"


if __name__ == "__main__":
    debug = True
    app.run(debug=debug)
