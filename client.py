import socket
from threading import Thread
import pickle
import time
from util import *
import sys

BLOCKCHAIN = Blockchain()
CHAIN_HEAD = 0
CLOCK = LamportClock(0,0)
CONNECTIONS = {}
PID = 0
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
        global BLOCKCHAIN

        while True:
            response = self.connection.recv(BUFFER_SIZE)
            if not response:
                continue
            data = pickle.loads(response)

            if data.reqType == MUTEX:
                # Add the block to blockchain and get the transaction details from data
                # use insert 
                CLOCK.updateClock(data.clock)
                BLOCKCHAIN.insert(data.transaction, data.clock.copy())
                print("REQUEST recieved from " + str(data.fromPid) + " at " + str(data.clock))
                sleep()
                print("REPLY sent to " + str(data.fromPid) + " at " + str(CLOCK))
                reply = RequestMessage(PID, CLOCK, REPLY)
                self.connection.send(pickle.dumps(reply))

            if data.reqType == REPLY:
                print("REPLY recieved from " + str(data.fromPid) + " at " + str(data.clock))
                REPLY_COUNT += 1
                # you have all replies, how do u know when to enter into lock
                if REPLY_COUNT == CLIENT_COUNT-1 and BLOCKCHAIN.header().transaction.sender == PID:
                    print("Local Queue: ")
                    for idx in range(BLOCKCHAIN.head, BLOCKCHAIN.length):
                        print("{}, {}".format(BLOCKCHAIN.data[idx].clock, BLOCKCHAIN.data[idx].transaction))
                    print("End of Local Queue")
                    print("Executing Transaction")
                    self.handle_transaction(data)
                    REPLY_COUNT = 0
                    TRANSACTION_FLAG = True

            if data.reqType == RELEASE:
                print("Inside release")
                print("Local Queue:")
                # check the status from data and update the blockchain
                # data.fromPid and go back and check
                # time complexity = O(no.of clients)
                for idx in range(BLOCKCHAIN.head, BLOCKCHAIN.length):
                    print("{}, {}".format(BLOCKCHAIN.data[idx].clock, BLOCKCHAIN.data[idx].transaction))
                print("End of Local Queue")
                print("RELEASE recieved from " + str(data.fromPid) + " at " + str(CLOCK))
                BLOCKCHAIN.header().update_status(data.status)
                BLOCKCHAIN.move()
                if BLOCKCHAIN.head-1 != BLOCKCHAIN.length and BLOCKCHAIN.header().transaction.sender == PID and REPLY_COUNT == CLIENT_COUNT-1:
                    print("Executing Transaction")
                    self.handle_transaction(data)
                    REPLY_COUNT = 0
                    TRANSACTION_FLAG = True

    def handle_transaction(self, data):
        global CLOCK
        global PID
        global CONNECTIONS
        global USER_INPUT
        transaction = BLOCKCHAIN.header().transaction
        sleep()
        print("Balance request sent to server" + " at " + str(CLOCK))
        request = RequestMessage(PID, CLOCK, BALANCE)
        CONNECTIONS[0].sendall(pickle.dumps(request))
        balance = pickle.loads(CONNECTIONS[0].recv(BUFFER_SIZE))
        sleep()
        print("======================================")
        print("Balance before transaction - {} $".format(balance))
        status = None
        if balance >= transaction.amount:
            request = RequestMessage(PID, data.clock, TRANSACT, None, transaction)
            CONNECTIONS[0].send(pickle.dumps(request))
            message = pickle.loads(CONNECTIONS[0].recv(BUFFER_SIZE))
            print("Transaction was " + str(message))
            print("Balance after transaction is {} $".format(balance-transaction.amount))
            print("======================================")
            status = SUCCESS
        else:
            print("Insufficient Balance")
            print("======================================")
            status = ABORT
        BLOCKCHAIN.header().update_status(status)
        BLOCKCHAIN.move()
        # send release with status of transaction
        broadcast(RELEASE, clock=CLOCK.copy(), status=status)

def sleep():
    time.sleep(SLEEP_TIME)

def sendRequest(client, clock, reqType, status, transaction):
    global PID
    global CONNECTIONS
    sleep()
    if reqType == "MUTEX":
        print("REQUEST sent to " + str(client) + " at " + str(clock))
    elif reqType == "RELEASE":
        print("RELEASE sent to " + str(client) + " at " + str(clock))
    msg = RequestMessage(PID, clock, reqType, status, transaction)
    data_string = pickle.dumps(msg)
    CONNECTIONS[client].sendall(data_string)

def broadcast(reqType, clock=CLOCK, status=None, transaction=None):
    global PID
    for dest in range(1, CLIENT_COUNT+1):
        if PID != dest:
            sendRequest(dest, clock, reqType, status, transaction)

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
    global BLOCKCHAIN
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
    print("| For Blockchain type : 'BCHAIN'                             |")
    print("| For Head of BCHAIN type : 'HEAD'                           |")
    print("| For transferring money type : 'RECV_ID AMOUNT' Eg.(2 5)    |")
    print("| To quit type 'Q'                                           |") 
    print("==============================================================")
    while True:
        TRANSACTION_FLAG = False
        print("===== Enter a command to compute =====")
        USER_INPUT = input()
        if USER_INPUT not in [QUIT, BALANCE, BCHAIN, HEAD] and len(USER_INPUT.split()) != 2:
            print("Please enter valid input")
            continue

        if USER_INPUT == BCHAIN:
            BLOCKCHAIN.print()
            continue

        if USER_INPUT == HEAD:
            print(str(BLOCKCHAIN.header()))
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
            continue
        
        else:
            CLOCK.incrementClock()
            print("Current clock of process " + str(PID) + " : " + str(CLOCK))
            # Add the transaction
            reciever, amount = [int(x) for x in USER_INPUT.split()]
            transaction = Transaction(PID, reciever, amount)
            BLOCKCHAIN.insert(transaction, CLOCK.copy())
            broadcast(MUTEX, clock=CLOCK.copy(), transaction=transaction)
            REPLY_COUNT = 0
            while TRANSACTION_FLAG == False:
                time.sleep(3)

    close_sockets()

if __name__ == "__main__":
    main()