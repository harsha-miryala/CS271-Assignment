import socket
from threading import Thread, Lock
import pickle
import time
from util import *
import sys

BLOCKCHAIN = Blockchain()
CHAIN_HEAD = 0
CLOCK = LamportClock(0,0)
REQ_CLOCK = LamportClock(0,0)
CONNECTIONS = {}
PID = 0
TRANSACTION_FLAG = False
USER_INPUT = ""
REPLY_COUNT = None
B_LOCK = Lock()

class Connections(Thread):
    def __init__(self, connection, to_client):
        Thread.__init__(self)
        self.connection = connection
        self.to_client = to_client

    def run(self):
        global REPLY_COUNT
        global TRANSACTION_FLAG
        global CLOCK
        global REQ_CLOCK
        global BLOCKCHAIN
        global B_LOCK

        while True:
            try:
                response = self.connection.recv(BUFFER_SIZE)
            except:
                self.connection.close()
                print("Closing the connection to {}".format(self.to_client))
                break
            if not response:
                continue
            data = pickle.loads(response)

            if data.reqType == MUTEX:
                # Add the block to blockchain and get the transaction details from data and reply to the client
                with B_LOCK:
                    # storing the clock of the most recent request from other client
                    # this can be used when assigning for a new process from client
                    REQ_CLOCK = data.clock.copy()
                    BLOCKCHAIN.insert(data.transaction, REQ_CLOCK)
                    print("MUTEX received from Client_{} at {}".format(data.fromPid, CLOCK))
                    # Adding sleep to mimic realtime - optional
                    sleep()
                    print("REPLY sent to Client_{} at {}".format(data.fromPid, CLOCK))
                    reply = RequestMessage(PID, CLOCK, REPLY)
                    self.connection.send(pickle.dumps(reply))

            if data.reqType == REPLY:
                # Checking to see if client can enter critical section
                # by confirming if its head of queue and have received all replies
                print("REPLY received from Client_{} at {}".format(data.fromPid, CLOCK))
                REPLY_COUNT.add(data.fromPid)
                with B_LOCK:
                    while BLOCKCHAIN.head != -1 and BLOCKCHAIN.header().transaction.sender == PID and REPLY_COUNT.count()>=1:
                        print("Entering critical section through reply from Client_{}".format(data.fromPid))
                        print("Printing local queue:")
                        for idx in range(BLOCKCHAIN.head, BLOCKCHAIN.length):
                            print("{}, {}".format(BLOCKCHAIN.data[idx].clock, BLOCKCHAIN.data[idx].transaction))
                        print("End of local queue")
                        print("Executing transaction for block with clock : {}".format(BLOCKCHAIN.header().clock))
                        self.handle_transaction()
                        REPLY_COUNT.decrement()
                        data.fromPid = PID

            if data.reqType == RELEASE:
                # Checking to see if client can enter critical section
                # by confirming if its head of queue post receiving the release response
                print("RELEASE received from Client_{} at {}".format(data.fromPid, CLOCK))
                BLOCKCHAIN.header().update_status(data.status)
                BLOCKCHAIN.move()
                with B_LOCK:
                    while BLOCKCHAIN.head != -1 and BLOCKCHAIN.header().transaction.sender == PID and REPLY_COUNT.count()>=1:
                        print("Entering critical section through release from Client_{}".format(data.fromPid))
                        print("Printing local queue:")
                        for idx in range(BLOCKCHAIN.head, BLOCKCHAIN.length):
                            print("{}, {}".format(BLOCKCHAIN.data[idx].clock, BLOCKCHAIN.data[idx].transaction))
                        print("End of local queue")
                        print("Executing transaction for block with clock : {}".format(BLOCKCHAIN.header().clock))
                        self.handle_transaction()
                        REPLY_COUNT.decrement()
                        data.fromPid = PID

    def handle_transaction(self):
        global CLOCK
        global PID
        global CONNECTIONS
        global USER_INPUT
        # getting the transaction to be processed
        transaction = BLOCKCHAIN.header().transaction
        # obtaining balance from server to make sure we have enough
        print("Balance request sent to server at {}".format(CLOCK))
        request = RequestMessage(PID, CLOCK, BALANCE)
        CONNECTIONS[0].sendall(pickle.dumps(request))
        balance = pickle.loads(CONNECTIONS[0].recv(BUFFER_SIZE))
        # Adding sleep to mimic realtime - optional
        sleep()
        print("======================================")
        print("Balance before transaction : ${}".format(balance))
        status = None
        if balance >= transaction.amount:
            # intiating the transfer
            request = RequestMessage(PID, CLOCK, TRANSACT, None, transaction)
            CONNECTIONS[0].send(pickle.dumps(request))
            message = pickle.loads(CONNECTIONS[0].recv(BUFFER_SIZE))
            # Adding sleep to mimic realtime - optional
            sleep()
            print("Transaction was {}".format(message))
            print("Balance after transaction : ${}".format(balance-transaction.amount))
            print("======================================")
            status = message
        else:
            print("Insufficient Balance")
            print("======================================")
            status = ABORT
        BLOCKCHAIN.header().update_status(status)
        BLOCKCHAIN.move()
        # send release to other clients with status of transaction
        broadcast(RELEASE, clock=CLOCK.copy(), status=status)

def sleep(sleep_time=SLEEP_TIME):
    time.sleep(sleep_time)

def sendRequest(client, clock, reqType, status, transaction):
    global PID
    global CONNECTIONS
    # Adding sleep to mimic realtime - optional
    sleep()
    if reqType == "MUTEX":
        print("MUTEX sent to Client_{} at {}".format(client, clock))
    elif reqType == "RELEASE":
        print("RELEASE sent to Client_{} at {} with status {}".format(client, clock, status))
    msg = RequestMessage(PID, clock, reqType, status, transaction)
    data_string = pickle.dumps(msg)
    CONNECTIONS[client].sendall(data_string)

def broadcast(reqType, clock=CLOCK, status=None, transaction=None):
    # broadcasting the req to all other clients
    global PID
    for dest in range(1, CLIENT_COUNT+1):
        if PID != dest:
            sendRequest(dest, clock, reqType, status, transaction)

def close_sockets():
    # function to close connections
    global CONNECTIONS
    for connection in CONNECTIONS.values():
        connection.close()

def get_connection(source, dest):
    # generic utility to connect from client source to dest
    global CONNECTIONS
    client2client = socket.socket()
    client2client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    client2client.bind((HOST, CLIENT_TO_CLIENT_PORTS[source][dest]))
    if dest > source:
        # Accept the connection from the dest server
        client2client.listen(10)
        conn, client_address = client2client.accept()
        CONNECTIONS[dest] = conn
        new_client = Connections(conn, dest)
        new_client.start()
        print("Connected to Client_{} at port {} from {}".format(dest,
              client_address[1], CLIENT_TO_CLIENT_PORTS[source][dest]))
    else:
        # Connection to the port already up
        try:
            client2client.connect((HOST, CLIENT_TO_CLIENT_PORTS[dest][source]))
            CONNECTIONS[dest] = client2client
            new_connection = Connections(client2client, dest)
            new_connection.start()
            print("Connected to Client_{} at port {} from {}".format(dest,
                  CLIENT_TO_CLIENT_PORTS[dest][source], CLIENT_TO_CLIENT_PORTS[source][dest]))
        except socket.error as e:
            print(str(e))

def main():
    global CLOCK
    global REQ_CLOCK
    global PID
    global CONNECTIONS
    global BLOCKCHAIN
    global REPLY_COUNT
    global TRANSACTION_FLAG
    global USER_INPUT
    global B_LOCK

    if int(sys.argv[1])>CLIENT_COUNT or int(sys.argv[1])<0:
        print("PID not in the set of allowed pids".format(sys.argv[1]))
        exit()
    PID = int(sys.argv[1])
    REPLY_COUNT = Reply(PID)
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
        # TRANSACTION_FLAG = False
        print("===== Enter a command to compute =====")
        USER_INPUT = input()
        if USER_INPUT not in [QUIT, BALANCE, BCHAIN, HEAD, CLK] and len(USER_INPUT.split()) != 2:
            print("Please enter valid input")
            continue

        if USER_INPUT == BCHAIN:
            BLOCKCHAIN.print()
            continue

        if USER_INPUT == CLK:
            print("Clock of my client is {}".format(CLOCK.copy().updateClock(REQ_CLOCK, inplace=False)))
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
            # Adding sleep to mimic realtime - optional
            sleep()
            print("======================================")
            print("The balance for Client_{} is ${}".format(PID, balance))
            print("======================================")
        
        else:
            try:
                reciever, amount = [int(x) for x in USER_INPUT.split()]
            except:
                print("Please enter valid input")
                continue
            if reciever == PID or reciever > CLIENT_COUNT:
                print("Can't send money to yourself or non-existing member")
                continue
            with B_LOCK:
                CLOCK.updateClock(REQ_CLOCK)
                print("Current clock of Client_{} : {}".format(PID, CLOCK))
                # Add the transaction
                transaction = Transaction(PID, reciever, amount)
                BLOCKCHAIN.insert(transaction, CLOCK.copy())
                broadcast(MUTEX, clock=CLOCK.copy(), transaction=transaction)

    close_sockets()

if __name__ == "__main__":
    main()