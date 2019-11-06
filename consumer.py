from kafka import KafkaConsumer


def consumer():
    consumer = KafkaConsumer('stock-test')
    for message in consumer:
        print(
            'Message payload: {}\n\nMessage value: {}\n---\n'.format(message, message.value))


if __name__ == "__main__":
    while True:
        consumer()
    pass
