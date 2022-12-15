# RealTimeStreamingData-Bitcoin_transactions_SPARK_MONGO-Stock_Prices_KAFKA_MONGO

README



DESCRIPTION Spark-Mongo



This code will simulate Bitcoin Exchanges that will be stored in a custom Blockchain using Spark

and will be saved in a MongoDB.



SET UP AND RUN



In order to run the files, all the files have to be in a common folder( 3 .py files and the 1661-0.txt file you already have)


1. In a linux terminal:                                                                sudo systemctl start mongod             -----> starts your mongo database 

2. cd until you reach the common folder ( all the terminals from now on have to be run from that common folder)

3. In a terminal:                                                                      python3 server.py                       -----> Initiates the server 

4. In another terminal:                                                                python3 Project_BlockChain_final.py     -----> Starts the process of mining

5. For the 5th step of quering the database, you will need to wait at least until the first block is created (in order for the database to have data)

Second you have to provide valid queries in order to see results (e.g. sequence number for a block that is already mined)

Open a terminal:                                                                       python3 Project_mongoDB_final.py        -----> you will have to provide the inputs as suggested



DESCRIPTION Kafka-Mongo



This code will simulate multiple Stock Exchanges emitting stock prices to

multiple Kafka topics and the subsequent processing of the collected information.



SET UP AND RUN



1. start kafka :            sudo env PATH=$PATH kafka-server-start.sh $KAFKA_HOME/config/server.properties

dont type any other command in the terminal that you started kafka.



In another terminal:

2. create first topic:      kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic StockExchange1

3. create second topic:     kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic StockExchange2

4. start MongoDB:           sudo systemctl start mongod



For the purpose of this work you will need to open 6 different terminals in the virtual machine to run everything:

Go to the shared folder of windows where all files are contained server1.py, server2.py, consumers1.py, consumers2.py. consumers3.py, app.py


5.  on the first terminal:   python3 server1.py


6.  on the second terminal:  python3 server2.py


7.  on the third terminal:   python3 consumer1.py


8.  on the fourth terminal:  python3 consumer2.py


9.  on the fifth terminal:   python3 consumer3.py


10. on the sixth(or use the one you run mongo command):  python3 app.py

Make sure to wait 1 minute until the first commit to the databse will occur.

You will have to type an interval of dates between 1/1/1970 and 9/4/2021 for the app.py to provide results,

the format must be :     dd/mm/yyyy-dd/mm/yyyy  (start date - end date)

If you want to see the current date you will have to put as start the current date and as end date at least one day after (exp. 8/4/2021-9/4/2021 to see 

all results for the 8th of april 2021)
