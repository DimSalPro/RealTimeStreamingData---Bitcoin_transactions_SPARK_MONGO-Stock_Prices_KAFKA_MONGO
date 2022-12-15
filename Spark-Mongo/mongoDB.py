import pymongo
import numpy as np

client = pymongo.MongoClient('localhost', 27017)

db = client.BlockChain

# # q1
sequence_number = int(input('Enter serial number of block that you want to see the nonce: '))
Question1 = db.BlockChain_collection.find({'Block_sequence_number': sequence_number},
                                          {'Block_nonce': 1})

print('\n--------------------------------------------------')
print('Question 1')
print('Block nonce for block sequence number ' + str(sequence_number) + ' is: ')
for item1 in Question1:
    print(item1)
print('--------------------------------------------------')

# # q2
Question2 = db.BlockChain_collection.find().sort('Block_mining_time', pymongo.ASCENDING).limit(1)

print('\n--------------------------------------------------')
print('Question 2')
print('Block with smallest mining time: ')
for item2 in Question2:
    print(item2)
print('--------------------------------------------------')

# # q3
Question3 = db.BlockChain_collection.aggregate([
    {"$group": {
        "_id": {},
        "average": {"$avg": "$Block_mining_time"}
    }}
])

print('\n--------------------------------------------------')
print('Question 3')
print('Average mining time of all blocks so far: ')
for item3 in Question3:
    print(item3)
print('--------------------------------------------------')

# # q4
start_point = int(input('Enter starting point of serial number range(included): '))
end_point = int(input('Enter ending point of serial number range(included): '))
Question4 = db.BlockChain_collection.aggregate([
    {'$match': {'Block_sequence_number': {'$gte': start_point, '$lte': end_point}}},
    {'$group': {'_id': {}, 'accumulator': {'$sum': '$Block_mining_time'}}}
])

print('\n--------------------------------------------------')
print('Question 4')
print('Cumulative mining time of all blocks from sequence number '
      + str(start_point) + ' to sequence number ' + str(end_point))
for item4 in Question4:
    print(item4)
print('--------------------------------------------------')

# # q5
start_range = int(input('Enter starting point of nonce range(included): '))
end_range = int(input('Enter ending point of nonce range(included): '))
Question5 = db.BlockChain_collection.count_documents({'Block_nonce': {'$gte': start_range, '$lte': end_range}})

print('\n--------------------------------------------------')
print('Question 5')
print('Number of blocks that nonce value is from '
      + str(start_range) + ' to ' + str(end_range))
print(Question5)
print('--------------------------------------------------')