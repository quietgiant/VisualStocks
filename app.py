import json
import logging
import numpy as np
import plotly
import plotly.plotly as py
import plotly.graph_objs as go
import producer
import time

from config import MongoConfiguration, KafkaConfiguration
from flask import Flask, request, render_template
from kafka import KafkaProducer
from plotly.graph_objs import Scatter, Layout, Bar
from pymongo import MongoClient


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
    api_version=(0, 10, 1)
    )
cluster = MongoClient(MongoConfiguration['connectionString'])
dbContext = cluster['stocks']


@app.route("/")
def hello():
    return render_template('index.html')


@app.route('/chart')
def chart():
    dailyData = None
    dailyCollection = dbContext['daily']
    movingAverageData = None
    movingAverageCollection = dbContext['moving-average']
    symbol = getSymbolFromUri(request.args.get('symbol'))
    if (symbol == ""):
        return render_template('index.html')
    rangeInDays = getRangeFromUri(request.args.get('range'))

    symbolExists = recordForSymbolExists(symbol, dailyCollection, movingAverageCollection)
    if symbolExists:
        dailyData = getDailyData(symbol, dailyCollection)
        movingAverageData = getMovingAverageData(symbol, movingAverageCollection)
    else:
        print(f"Requesting producer to emit data for symbol: {symbol}")
        producer.emit_stock_data(kafka_producer, symbol)
    
    while (dailyData is None or movingAverageData is None):
        print('Waiting for consumer to finish write before querying data...')
        time.sleep(1)
        dailyData = getDailyData(symbol, dailyCollection)  
        movingAverageData = getMovingAverageData(symbol, movingAverageCollection)

    chartJSON = generateGraphs(dailyData, movingAverageData, rangeInDays)
    shareRange = calculatePriceRangeFormat(dailyData)
    return render_template('chart.html', chartJSON=chartJSON, symbol=symbol, range=rangeInDays, priceRangeFormat=shareRange)


def generateGraphs(dailyData, movingAverageData, rangeInDays):
    stock_chart = generateLineChart(dailyData, rangeInDays)
    ten_day_moving_average = generateTenDayMovingAverageChart(movingAverageData, rangeInDays)
    fifty_day_moving_average = generateFiftyDayMovingAverageChart(movingAverageData, rangeInDays)
    volume_graph = generateBarChart(dailyData, rangeInDays)
    charts = [stock_chart, ten_day_moving_average, fifty_day_moving_average]#, volume_graph]
    return json.dumps(charts, cls=plotly.utils.PlotlyJSONEncoder)


def generateLineChart(dailyData, rangeInDays):
    xValues = dailyData['dates'][-rangeInDays:]
    yValues = dailyData['closing'][-rangeInDays:]
    print(f'X: {xValues}')
    print(f'Y: {yValues}')
    return go.Scatter(
        x = xValues,
        y = yValues,
        name = dailyData['symbol']
    )


def generateTenDayMovingAverageChart(movingAverageData, rangeInDays):
    xValues = movingAverageData['ten-day-dates'][-rangeInDays:]
    yValues = movingAverageData['ten-day-closing'][-rangeInDays:]
    return go.Scatter(
        x = xValues,
        y = yValues,
        name = "10 Period SMA"
    )


def generateFiftyDayMovingAverageChart(movingAverageData, rangeInDays):
    xValues = movingAverageData['fifty-day-dates'][-rangeInDays:]
    yValues = movingAverageData['fifty-day-closing'][-rangeInDays:]
    return go.Scatter(
        x = xValues,
        y = yValues,
        name = "50 Period SMA"
    )


def generateBarChart(dailyData, rangeInDays):
    xValues = dailyData['dates'][-rangeInDays:]
    yValues = dailyData['closing'][-rangeInDays:]
    return go.Scatter(
        x = xValues,
        y = yValues
    )


def calculatePriceRangeFormat(dailyData):
    minPrice = min(dailyData['closing'])
    maxPrice = max(dailyData['closing'])
    return [(minPrice * 0.90), (maxPrice * 1.1)]


def getDailyData(symbol, dailyCollection):
    return dailyCollection.find_one({'symbol': { "$eq": symbol}})


def getMovingAverageData(symbol, movingAveragesCollection):
    return movingAveragesCollection.find_one({'symbol': { "$eq": symbol}})


def recordForSymbolExists(symbol, dailyCollection, movingAverageCollection):
    return dailyCollection.count_documents({'symbol': { "$eq": symbol}}) > 0


def getRangeFromUri(rangeQueryParameter):
    if (rangeQueryParameter is None):
        return 180
    return int(rangeQueryParameter)


def getSymbolFromUri(symbolQueryParameter):
    if (symbolQueryParameter is None):
        return ''
    return str(symbolQueryParameter).upper()


if __name__ == "__main__":
    debug = True
    app.run(debug=debug)
