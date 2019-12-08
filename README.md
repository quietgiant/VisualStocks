# Real-time stock analytics using Kafka

## Important note

This entire project is built on Python 3.7. You must use Python 3.x to run this code.

## Dataset
Stock Market data as a csv file via Kaggle
https://www.kaggle.com/borismarjanovic/price-volume-data-for-all-us-stocks-etfs/data

## To run
Open a shell, start Zookeeper.
`> ./start-zookeper`

Open another shell, start the Kafka server.
`> ./start-kafka`

Open another shell, start the Flask app. This will also start the Kafka producer.
`> python3 app.py` 

Open another shell, start the Kafka consumer.
`> python3 consumer.py` 

Open the app at
`http://localhost:5000`
