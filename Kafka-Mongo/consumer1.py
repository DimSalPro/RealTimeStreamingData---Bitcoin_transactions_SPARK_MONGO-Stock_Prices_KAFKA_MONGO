from kafka import KafkaConsumer
from json import loads
import time
from collections import defaultdict
import json
import pymongo
import datetime

# # initiate mongo database connection on localhost and port 27017
client = pymongo.MongoClient('localhost', 27017)

# # use or create database named itc6107
db = client.itc6107

# # drop collection StockExchange if collection already exists and have data
db.StockExchange.drop()

# # decoder of the message received from producer
dezer = lambda x: loads(x.decode('utf-8'))

# # consumes messages from topic StockExchange1, with group id -> 'my-group1' for the consumer group to belong to.
# # auto commit the messages, the decoder of the messages and the auto offset reset is set to earliest( reads
# # again the messages from the previous time that they may be processed so far. Or it may read them twice )
consumer = KafkaConsumer('StockExchange1', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest',
                         enable_auto_commit=True, group_id='my-group1', value_deserializer=dezer)

# # it sleeps for 60s then invoke poll() to get all the messages that the server have already transmit and received
# # in that interval (if there are no messages so far it will stay on the poll phase and wait until the 600000
# # milliseconds finish or until 1 message is received). i initiate a default dictionary which is an dictionary object
# # with values lists. So on this dictionary i can append for the same keys different values that i will use later to
# # make calculations for the MongoDB data entry. The msg_pack contains the topic and a list of the values for the
# # messages received. so i iter through the messages and topic and then again through the list of messages.
# # i use loads because they are in json format and make them into a dictionary in the form {tick: "ticker",price...}
# # Finally i take the dictionary object and turn it into a normal dictionary and for every message(key value pair)
# # i insert to mongo a document having the key(ticker) the avg (sum of list / length of list) and i timestamp of
# # the entry in isoformat. print a the unique ticker entries and the process starts all over again (60 second sleep).
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
                                     'avg': sum(value) / len(value),
                                     'ts': datetime.datetime.now().isoformat()})
    print('Unique entries: ', len(dict(transactions)))
