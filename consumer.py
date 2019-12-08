import json
import logging

from kafka import KafkaConsumer


def json_deserializer(v):
    if v is None:
        return None
    try:
        return json.loads(v.decode('utf-8'))
    except json.decoder.JSONDecodeError:
        logging.exception('Unable to decode: %s', v)
        return None


def consumer():
    consumer = KafkaConsumer(
        'stock-test', value_deserializer=json_deserializer)
    for message in consumer:
        print('chugging!')
        # print('Message payload: {}\n\nMessage value: {}\n---\n'.format(message, message.value))
        # compute daily moving average
        tenDay = 0.0
        fiftyDay = 0.0
        i = 0
        print(f'Payload: \n\n{message}\n\n')
        print(f'Value(size = {len(message.value)}): \n\n{message.value}\n\n')
        for dailyData in message.value:
            # ty = type(dailyData)
            # print(f'value: {dailyData}\ntype: {ty}\n\n')
            dailyData = dailyData.split(',')
            if i == 0:
                i += 1
                continue  # skip header line in payload
            close = float(dailyData[4])
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
        # persist share data, DMA


if __name__ == "__main__":
    while True:
        consumer()
    pass
