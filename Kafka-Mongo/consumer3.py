from kafka import KafkaConsumer
from json import loads
import time
from collections import defaultdict
import json
import pymongo
import datetime

client = pymongo.MongoClient('localhost', 27017)

db = client.itc6107

db.StockExchangeC.drop()

dezer = lambda x: loads(x.decode('utf-8'))

# # same as the previous two but this consumer reads messages from both StockExchange1 and StockExchange2 topics.
consumer = KafkaConsumer('StockExchange1', 'StockExchange2', bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True, group_id='my-group3', value_deserializer=dezer)


# # here we have the same process but the entry of data in the mongoDb differs. Here we also need to store the min
# # (i get the minimum of the list) the max, and the spread.
while True:
    time.sleep(60)
    msg_pack = consumer.poll(timeout_ms=6000000)
    transactions = defaultdict(list)
    for tp, messages in msg_pack.items():
        for message in messages:
            message_in_dict = dict(json.loads(message.value))
            transactions[message_in_dict['TICK']].append(float(message_in_dict['PRICE']))
            print('Stocks entered: \n', message_in_dict)
    for key, value in dict(transactions).items():
        minimum = min(value)
        maximum = max(value)
        average = sum(value) / len(value)
        spread = ((maximum - minimum) / average) * 100

        db.StockExchangeC.insert_one({'tick': key,
                                      'min': minimum,
                                      'max': maximum,
                                      'avg': average,
                                      'spread': spread,
                                      'ts': datetime.datetime.now().isoformat()})

    print('Unique entries: ', len(dict(transactions)))
