HOST = "127.0.0.1"
CLIENT_TO_SERVER_PORTS = {1: 65001, 2: 65002, 3: 65003}
CLIENT_COUNT = len(CLIENT_TO_SERVER_PORTS)
CLIENT_TO_CLIENT_PORTS = {
    1: {
        2: 65010,
        3: 65010
    },
    2: {
        1: 65020,
        3: 65021
    },
    3: {
        1: 65030,
        2: 65031
    }
}
SERVER_PORT =  65500
BUFFER_SIZE = 1024
SLEEP_TIME = 3
BALANCE = "BAL"
QUIT = "Q"
TRANSACT = "TRANSACT"
SUCCESS = "SUCCESS"
ABORT = "ABORT"
MUTEX = "MUTEX"
RELEASE = "RELEASE"
REPLY = "REPLY"

class RequestMessage:
    def __init__(self, fromPid, clock, reqType, reqClock = None, block = None):
        self.fromPid = fromPid
        self.clock = clock
        self.reqType = reqType
        self.reqClock = reqClock
        self.block = block

class LamportClock:
    def __init__(self, clock, pid):
        self.clock = clock
        self.pid = pid

    def incrementClock(self):
        self.clock += 1

    def __lt__(self, other):
        if self.clock < other.clock:
            return True
        elif self.clock == other.clock:
            return self.pid < other.pid
        return False

    def updateClock(self, other):
        self.clock = max(self.clock, other.clock) + 1

    def __str__(self):
        return str(self.clock) + "." + str(self.pid)

class Transaction:
    def __init__(self, sender, reciever, amount):
        self.sender = sender
        self.reciever = reciever
        self.amount = amount

    def __str__(self):
        return str(self.sender) + "|" + str(self.reciever) + "|" + str(self.amount)

class Block:
    def __init__(self, headerHash, transaction):
        self.headerHash = headerHash
        self.transaction = transaction

    def __str__(self):
        return str(self.headerHash) + "|" + str(self.transaction)