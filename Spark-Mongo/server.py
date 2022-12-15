import socket
import time
import random
import datetime
import pandas as pd

PORT = 9999

data = pd.read_csv('1661-0.txt', sep='\n', header=None)
transaction_stream = data.values

ssocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
ssocket.bind(('', PORT))
ssocket.listen()

print("Server ready: listening to port {0} for connections.\n".format(PORT))
(c, addr) = ssocket.accept()

suffix = 0
while True:
    for i in range(len(transaction_stream)):
        lines = transaction_stream[i][0] if suffix == 0 else transaction_stream[i][0] + str(suffix)
        message = '{0}'.format(lines)
        print(message)
        c.send((message + '\n').encode())
        time.sleep(random.randint(2, 5))
    suffix += 1


