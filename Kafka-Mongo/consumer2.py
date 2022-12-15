from kafka import KafkaConsumer
from json import loads
import time
from collections import defaultdict
import json
import pymongo
import datetime

client = pymongo.MongoClient('localhost', 27017)

db = client.itc6107

db.StockExchange.drop()

dezer = lambda x: loads(x.decode('utf-8'))

# # the process is exactly the same as in consumer1.py with the difference that here messages come from topic
# # StockExchange2
consumer = KafkaConsumer('StockExchange2', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest',
                         enable_auto_commit=True, group_id='my-group2', value_deserializer=dezer)

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
        db.StockExchange.insert_one({'tick': key,
                                     'avg': sum(value)/len(value),
                                     'ts': datetime.datetime.now().isoformat()})
    print('Unique entries: ',len(dict(transactions)))