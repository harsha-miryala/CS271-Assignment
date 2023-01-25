import socket
from threading import Thread
import pickle
from util import *

BALANCE_SHEET = {}
CLIENT_MAP = {}

class Server_Thread(Thread):
    def __init__(self, connection, host, port, pid):
        Thread.__init__(self)
        self.connection = connection
        self.host = host
        self.port = port
        self.pid = pid

    def run(self):
        self.handle_messages()
        self.connection.close()

    def handle_messages(self):
        while True:
            try:
                request = self.connection.recv(BUFFER_SIZE)
            except:
                print("Closing the connection for Client {}".format(self.pid))
                break
            if not request:
                continue
            data = pickle.loads(request)
            if data.reqType == BALANCE:
                self.get_balance(data)
            elif data.reqType == TRANSACT:
                self.add_transaction(data)

    def get_balance(self, data):
        global BALANCE_SHEET
        print("Sending the balance to Client {} as {}".format(data.fromPid, BALANCE_SHEET[data.fromPid]))
        CLIENT_MAP[data.fromPid].connection.sendall(pickle.dumps(BALANCE_SHEET[data.fromPid]))

    def add_transaction(self, data):
        global BALANCE_SHEET
        global CLIENT_MAP
        print("Adding a transaction of {} $ from Client {} to Client {}".format(data.block.transaction.amount,
                                    data.block.transaction.sender, data.block.transaction.reciever))
        try:
            BALANCE_SHEET[data.block.transaction.sender] -= data.block.transaction.amount
            BALANCE_SHEET[data.block.transaction.reciever] += data.block.transaction.amount
            CLIENT_MAP[data.fromPid].connection.sendall(pickle.dumps(SUCCESS))
        except:
            CLIENT_MAP[data.fromPid].connection.sendall(pickle.dumps(ABORT))

def printBalance():
    global BALANCE_SHEET
    print("=========================================")
    for pid in BALANCE_SHEET:
        print("Client {} : {}".format(pid, BALANCE_SHEET[pid]))
    print("=========================================")

def main():
    global BALANCE_SHEET
    server_socket = socket.socket()
    try:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((HOST, SERVER_PORT))
        print("Hosted the server at {}".format(SERVER_PORT))
    except socket.error as e:
        print(str(e))

    print("Waiting for the connection to clients")
    server_socket.listen(CLIENT_COUNT)

    BALANCE_SHEET = {x+1: 10 for x in range(CLIENT_COUNT)}

    client_count = 0
    while client_count < CLIENT_COUNT:
        connection, client_address = server_socket.accept()
        print("Connected to Client {} at port : {}".format(client_address[1]%10, client_address[1]))
        pid = client_address[1]%10
        client = Server_Thread(connection, client_address[0], client_address[1], pid)
        client.start()
        CLIENT_MAP[pid] = client
        client_count+=1
    
    print("==============================================================")
    print("| For Balance type : '{}'                                   |".format(BALANCE))
    print("| To quit type : '{}'                                         |".format(QUIT)) 
    print("==============================================================")
    while True:
        print("===== Enter a command to compute =====")
        user_input = input()
        if user_input == BALANCE:
            printBalance()
        elif user_input == QUIT:
            server_socket.close()
            for connection in CLIENT_MAP.values():
                connection.connection.close()
            break


if __name__ == "__main__":
    main()