import socket
import time
import random
import datetime
from kafka import KafkaProducer
from json import dumps

stock = [
    ('IBM', 123.20), ('AAPL', 125.35), ('FB', 264.30), ('AMZN', 3159.50),
    ('GOOG', 2083.80), ('TWTR', 71.90), ('LNKD', 45.00), ('INTC', 63.20),
    ('AMD', 86.90), ('MSFT', 234.50), ('DELL', 81.70), ('ORKL', 64.70)
]

# # encoder of the message that the consumer will decode
serzer = lambda x: dumps(x).encode('utf-8')

# # kafka producer connected to localhost and port 9092
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=serzer)

# # the producer sends encoded messages in json format to topic StockExchange1
while True:
    sl = random.randint(1, len(stock)) - 1
    ticker, price = stock[sl]
    r = random.random() / 10 - 0.5
    price *= 1 + r
    msg = '{{"TICK": "{0}", "PRICE": "{1:.2f}", "TS": "{2}"}}' \
        .format(ticker, price, datetime.datetime.now())
    print(msg)
    producer.send('StockExchange1', value=msg)
    time.sleep(random.randint(2, 5))
