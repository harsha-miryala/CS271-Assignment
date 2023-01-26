import socket
from threading import Thread
import pickle
import time 
from util import *
import sys
import heapq
import hashlib

BLOCKCHAIN = []
CHAIN_HEAD = 0
CLOCK = LamportClock(0,0)
CONNECTIONS = {}
PID = 0
REQUEST_QUEUE = []
TRANSACTION_FLAG = False
USER_INPUT = ""
REPLY_COUNT = 0

class Connections(Thread):
    def __init__(self,connection):
        Thread.__init__(self)
        self.connection = connection

    def run(self):
        global REPLY_COUNT
        global TRANSACTION_FLAG
        global CLOCK
        global REQUEST_QUEUE

        while True:
            response = self.connection.recv(BUFFER_SIZE)
            if not response:
                continue
            data = pickle.loads(response)
            CLOCK.updateClock(data.clock)
            print("Current clock of process " + str(PID) + " is " + str(CLOCK))

            if data.reqType == MUTEX:
                # Add the block to blockchain and get the transaction details from data
                REQUEST_QUEUE.append(LamportClock(data.reqClock.clock, data.reqClock.pid))
                heapq.heapify(REQUEST_QUEUE)
                print("REQUEST recieved from " + str(data.fromPid) + " at " + str(CLOCK))
                CLOCK.incrementClock()
                sleep()
                print("REPLY sent to " + str(data.fromPid) + " at " + str(CLOCK))
                reply = RequestMessage(PID, CLOCK, REPLY)
                self.connection.send(pickle.dumps(reply))

            if data.reqType == REPLY:
                print("REPLY recieved from " + str(data.fromPid) + " at " + str(CLOCK))
                REPLY_COUNT += 1
                # you have all replies, how do u know when to enter into lock
                if REPLY_COUNT == CLIENT_COUNT-1 and REQUEST_QUEUE[0].pid == PID:
                    print("Local Queue:")
                    for i in REQUEST_QUEUE:
                        print(str(i))
                    self.handle_transaction(data)
                    heapq.heappop(REQUEST_QUEUE)
                    heapq.heapify(REQUEST_QUEUE)
                    REPLY_COUNT = 0
                    # send release with status of transaction
                    broadcast(RELEASE)
                    TRANSACTION_FLAG = True

            if data.reqType == RELEASE:
                print("Inside release")
                print("Local Queue:")
                # check the status from data and update the blockchain
                # data.fromPid and go back and check
                # time complexity = O(no.of clients)
                for i in REQUEST_QUEUE:
                    print(str(i))
                print("RELEASE recieved from " + str(data.fromPid) + " at " + str(CLOCK))
                heapq.heappop(REQUEST_QUEUE)
                heapq.heapify(REQUEST_QUEUE)
                if len(REQUEST_QUEUE) > 0 and REQUEST_QUEUE[0].pid == PID and REPLY_COUNT == 2:
                    print("Execute Transaction")
                    self.handle_transaction(data)
                    heapq.heappop(REQUEST_QUEUE)
                    heapq.heapify(REQUEST_QUEUE)
                    REPLY_COUNT = 0
                    # send release with status of transaction
                    broadcast("RELEASE")
                    TRANSACTION_FLAG = True

    def handle_transaction(self, data):
        global CLOCK
        global PID
        global CONNECTIONS
        global USER_INPUT
        reciever, amount = [int(x) for x in USER_INPUT.split()]
        transaction = Transaction(PID, reciever, amount)
        CLOCK.incrementClock()
        sleep()
        print("Balance request sent to server" + " at " + str(CLOCK))
        request = RequestMessage(PID, CLOCK, BALANCE)
        CONNECTIONS[0].sendall(pickle.dumps(request))
        balance = pickle.loads(CONNECTIONS[0].recv(BUFFER_SIZE))
        sleep()
        print("======================================")
        print("Balance before transaction - {} $".format(balance))
        if balance >= amount:
            CLOCK.incrementClock()
            # Instead should add the last block
            block = Block(hashlib.sha256(b"").digest(), transaction)
            # request = RequestMessage(pid, data.clock, "ADD_BLOCK", None, block)
            request = RequestMessage(PID, data.clock, TRANSACT, None, block)
            CONNECTIONS[0].send(pickle.dumps(request))
            message = pickle.loads(CONNECTIONS[0].recv(BUFFER_SIZE))
            CLOCK.incrementClock()
            print("Transaction was " + str(message))
            print("Balance after transaction is {} $".format(balance-amount))
            print("======================================")
        else:
            print("Insufficient Balance")
            print("======================================")

def sleep():
    time.sleep(SLEEP_TIME)

def sendRequest(client, reqType, reqClock):
    global CLOCK
    global PID
    global CONNECTIONS
    CLOCK.incrementClock()
    print("Current clock of process {} is {}".format(PID, CLOCK))
    sleep()
    if reqType == "MUTEX":
        print("REQUEST sent to " + str(client) + " at " + str(CLOCK))
    elif reqType == "RELEASE":
        print("RELEASE sent to " + str(client) + " at " + str(CLOCK))

    msg = RequestMessage(PID, CLOCK, reqType, reqClock)
    data_string = pickle.dumps(msg)
    CONNECTIONS[client].sendall(data_string)

def broadcast(reqType, reqClock = None):
    global PID
    for dest in range(1, CLIENT_COUNT+1):
        if PID != dest:
            sendRequest(dest, reqType, reqClock)

def close_sockets():
    global CONNECTIONS
    for connection in CONNECTIONS.values():
        connection.close()

def get_connection(source, dest):
    global CONNECTIONS
    client2client = socket.socket()
    client2client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    client2client.bind((HOST, CLIENT_TO_CLIENT_PORTS[source][dest]))
    if dest > source:
        # Accept the connection from the dest server
        client2client.listen(10)
        conn, client_address = client2client.accept()
        CONNECTIONS[dest] = conn
        new_client = Connections(conn)
        new_client.start()
        print("Connected to Client {} at port {} from {}".format(dest, client_address[1], CLIENT_TO_CLIENT_PORTS[source][dest]))
    else:
        # Connection to the port already up
        try:
            client2client.connect((HOST, CLIENT_TO_CLIENT_PORTS[dest][source]))
            CONNECTIONS[dest] = client2client
            new_connection = Connections(client2client)
            new_connection.start()
            print("Connected to Client {} at port {} from {}".format(dest, CLIENT_TO_CLIENT_PORTS[dest][source], CLIENT_TO_CLIENT_PORTS[source][dest]))
        except socket.error as e:
            print(str(e))

def main():
    global CLOCK
    global PID
    global CONNECTIONS
    global REQUEST_QUEUE
    global REPLY_COUNT
    global TRANSACTION_FLAG
    global USER_INPUT
    if int(sys.argv[1])>CLIENT_COUNT or int(sys.argv[1])<0:
        print("PID not in the set of allowed pids".format(sys.argv[1]))
        exit()
    PID = int(sys.argv[1])
    current_port = CLIENT_TO_SERVER_PORTS[PID]
    
    # Connection to the server
    print("Initiating connection to the server")
    # Bind to the current port
    client_socket = socket.socket()
    try:
        client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        client_socket.bind((HOST, current_port))
        print("Hosted the client for server at {}".format(current_port))
    except socket.error as e:
        print(str(e))
        exit()
    CLOCK = LamportClock(0, PID)
    # Connect to the server
    try:
        client_socket.connect((HOST, SERVER_PORT))
        print("Connected to server at port {} from {}".format(SERVER_PORT, current_port))
    except socket.error as e:
        print(str(e))
        exit()
    
    CONNECTIONS[0] = client_socket
    
    # Connection to the clients
    for dest in range(1, CLIENT_COUNT+1):
        if PID != dest:
            get_connection(PID, dest)
    
    print("=======================================================")
    print("Current Balance is $10")
    print("==============================================================")
    print("| For Balance type : 'BAL'                                   |")
    print("| For transferring money type : 'RECV_ID AMOUNT' Eg.(2 5)    |")
    print("| To quit type 'Q'                                           |") 
    print("==============================================================")
    while True:
        TRANSACTION_FLAG = False
        print("===== Enter a command to compute =====")
        USER_INPUT = input()
        if USER_INPUT != QUIT and USER_INPUT != BALANCE and len(USER_INPUT.split()) != 2:
            print("Please enter valid input")
            continue

        if USER_INPUT == QUIT:
            break
        
        if USER_INPUT == BALANCE:
            request = RequestMessage(PID, CLOCK, BALANCE)
            CONNECTIONS[0].sendall(pickle.dumps(request))
            balance = pickle.loads(CONNECTIONS[0].recv(BUFFER_SIZE))
            print("======================================")
            print("The balance for Client {} is {} $".format(PID, balance))
            print("======================================")
        
        else:
            CLOCK.incrementClock()
            print("Current clock of process " + str(PID) + " : " + str(CLOCK))
            REQUEST_QUEUE.append(requestClock)
            heapq.heapify(REQUEST_QUEUE)
            REPLY_COUNT = 0
            broadcast(MUTEX, requestClock)
            while TRANSACTION_FLAG == False:
                time.sleep(1)

    close_sockets()

if __name__ == "__main__":
    main()