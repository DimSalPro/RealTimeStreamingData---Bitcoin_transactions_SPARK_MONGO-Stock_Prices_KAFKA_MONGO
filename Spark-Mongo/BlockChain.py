from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from hashlib import sha256
import time
import pymongo
from pyspark import SparkConf, SparkContext

# # connect to mongo
client = pymongo.MongoClient('localhost', 27017)

# # create/use BlockChain database
db = client.BlockChain

# # create spark context using 5 threads
sc = SparkContext("local[5]", "BlockChain")

# # create streaming context with 120 sec interval
ssc = StreamingContext(sc, 120)

# # connect to socket that the server runs to
lines = ssc.socketTextStream('localhost', 9999)

# # create a list to store the blocks( blockchain) and one for the previous hash
chain = []

# # number of leading zeros
Level_of_Difficulty = 6

# # number used in partitions(split) and also to the ranges function
# # 1 thread is used for running the DStreams functions and the rest for the rest processes
numPartitions = 4


# # input max number of the range you want to split and in how many parts(we will use same as partitions)
# # it outputs a list of ranges
def ranges_creator(until, split_in):
    step = until / split_in
    list_of_ranges = []
    for i in range(split_in):
        list_of_ranges.append(range(round(step * i), round(step * (i + 1))))
    return list_of_ranges


# # given an index(serial number of block) a string of transactions and a range to search for the nonce,
# # starts looking for the nonce and stops in the first he will find(else never stops and returns none)
# # every time computes a hash function that must fulfill the required difficulty level
# # it returns the nonce the hash function that made from this nonce (digest) and the time it took to find it
# # the if else is for the genesis block creation(if yes manually insert index etc, if no take information from
# # the last block of the chain
def hash_solver(index, transactions_str, nonce_range, genesis='no', chain=chain):
    start = time.time()
    if genesis == 'yes':
        previous_hash = '0'
    else:
        previous_hash = chain[-1][3]
    for nonce in nonce_range:
        str_to_hash = str(index) + transactions_str + previous_hash + str(nonce)
        hash_value = sha256(str_to_hash.encode()).hexdigest()
        if hash_value.startswith('0' * Level_of_Difficulty):
            end = time.time()
            print(end - start, nonce_range)
            return nonce, hash_value, end - start


# # gets an index a list of transactions to create the new block of the chain
# # it joins the lists parts to a common string, parallelize a list split in 4 ranges to 4 partitions
# # 1 range for each partition. then we map the previous function to the concatenates transactions, the serial number
# # of the block and the range that the function must search to find a nonce and then we collect
# # the output of the collect is a list of 4 tuples (containing nonce, hash value and time it took to mine) or less
# # depend if no nonce were found in a range(in which case it returns none)
# # we keep only the tuples and exclude the none (if any) from the list and we sort based on the mining time
# # we get the smallest mining time information to create the next block of the chain
# # Has: serial number, list of transaction, the nonce and digest of the block and the mining time
# # we append in the chain and the previous hash and also in mongoDB and print the chain
# # We use tuples to represent a block because it consumes less memory and moreover is immutable, as each block of
# # a block chain should be
# # the if else is for the genesis block creation(if yes manually insert index etc, if no take information from
# # the last block of the chain
def compute_hash(transactions, genesis='no', chain=chain):
    transactions_str = ''.join(transactions)
    if genesis == 'yes':
        index = 0
        rdd = sc.parallelize(ranges_creator(pow(2, 32), numPartitions)) \
                .map(lambda x: hash_solver(index, transactions_str, x, 'yes')) \
                .collect()
    else:
        index = chain[-1][0] + 1
        rdd = sc.parallelize(ranges_creator(pow(2, 32), numPartitions)) \
                .map(lambda x: hash_solver(index, transactions_str, x)) \
                .collect()

    cleaned_rdd = [x for x in rdd if str(x) != 'none']
    best_mine_time = sorted(cleaned_rdd, key=lambda x: x[2])

    nonce, current_hash_value, time_mine = best_mine_time[0]

    new_block = (index, transactions, nonce, current_hash_value, time_mine)

    chain.append(new_block)
    db.BlockChain_collection.insert_one({'Block_sequence_number': chain[-1][0],
                                         'Block_nonce': chain[-1][2],
                                         'Block_mining_time': chain[-1][4]})
    print(chain)


# # mine the genesis block
compute_hash(['Genesis block'], 'yes')

# # use a common key assigned to each line of the stream and then group by that key. It creates tuples with second
# # element an iterable object which we use to make it to a list, the list of transaction. We use that list to invoke
# # the above functions for each rdd it contains (in the function we use rdd internally for parallelization)
parsed = lines.map(lambda x: ('transactions', x)) \
              .groupByKey() \
              .map(lambda x: list(x[1])) \
              .foreachRDD(lambda x: compute_hash(x.collect()[0]))

# # start the computation
ssc.start()
ssc.awaitTermination()
