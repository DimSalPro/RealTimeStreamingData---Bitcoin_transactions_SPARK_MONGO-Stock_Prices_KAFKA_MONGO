import socket
import time
import random
import datetime
from kafka import KafkaProducer
from json import dumps

stock = [
    ('HPQ', 28.00), ('CSCO', 45.70), ('ZM', 385.20), ('QCOM', 141.10),
    ('ADBE', 476.60), ('VZ', 57.10), ('TXN', 179.40), ('CRM', 240.50),
    ('AVGO', 480.90), ('NVDA', 580.00), ('VMW', 146.80), ('EBAY', 59.40)
]

# # encoder of the message that the consumer will decode
serzer = lambda x: dumps(x).encode('utf-8')

# # kafka producer connected to localhost and port 9092
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=serzer)

# # the producer sends encoded messages in json format  to topic StockExchange2
while True:
    sl = random.randint(1, len(stock)) - 1
    ticker, price = stock[sl]
    r = random.random() / 10 - 0.5
    price *= 1 + r
    msg = '{{"TICK": "{0}", "PRICE": "{1:.2f}", "TS": "{2}"}}' \
        .format(ticker, price, datetime.datetime.now())
    print(msg)
    producer.send('StockExchange2', value=msg)
    time.sleep(random.randint(2, 5))
