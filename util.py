import hashlib

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
BCHAIN = "BCHAIN"
TRANSACT = "TRANSACT"
SUCCESS = "SUCCESSFUL"
ABORT = "ABORTED"
MUTEX = "MUTEX"
RELEASE = "RELEASE"
REPLY = "REPLY"
IN_PROGRESS = "IN_PROGRESS"
HEAD = "HEAD"

class RequestMessage:
    def __init__(self, fromPid, clock, reqType, status = None, transaction = None):
        self.fromPid = fromPid
        self.clock = clock
        self.reqType = reqType
        self.status = status
        self.transaction = transaction

class LamportClock:
    def __init__(self, clock, pid):
        self.clock = clock
        self.pid = pid
    
    def copy(self):
        return LamportClock(self.clock, self.pid)

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
        return "Client_{} pays Client_{} ${}".format(self.sender, self.reciever, self.amount)

class Block:
    def __init__(self, headerHash, transaction, clock):
        self.headerHash = headerHash
        self.transaction = transaction
        self.clock = clock
        self.status = IN_PROGRESS

    def __str__(self):
        return str(self.headerHash) + "|\n" + str(self.transaction) + \
               " | " + self.status + " | " + str(self.clock)
    
    def update_status(self, status):
        self.status = status

class Blockchain:
    def __init__(self):
        self.data = []
        self.head = -1
        self.length = 0
    
    def header(self):
        return self.data[self.head] if self.length!=0 else "Empty Blockchain"
    
    def move(self):
        self.head += 1
        if self.head >= self.length:
            self.head = -1
    
    def append(self, transaction, clock):
        prev_hash = "" if self.length==0 else str(self.data[self.length-1])
        headerHash = hashlib.sha256(prev_hash.encode()).hexdigest()
        block = Block(headerHash, transaction, clock)
        self.data.append(block)
        self.length += 1
        self.head = self.length - 1
        print("{} added at {} with clock - {}".format(transaction, self.head, clock))
    
    def insert(self, transaction, clock):
        if self.head == -1:
            self.append(transaction, clock)
            return
        # check if it can be added back
        reqd_pos = self.head
        for idx in range(self.head, -1, -1):
            if self.data[idx].status != IN_PROGRESS:
                break
            if clock < self.data[idx].clock:
                reqd_pos -= 1
                self.head = reqd_pos + 1
            else:
                break
        for idx in range(self.head+1, self.length):
            if self.data[idx].clock < clock:
                reqd_pos += 1
            else:
                break
        prev_hash = ""
        headerHash = hashlib.sha256(prev_hash.encode()).hexdigest()
        block = Block(headerHash, transaction, clock)
        self.data.insert(reqd_pos+1, block)
        self.length += 1
        print("{} added at {} with clock - {}".format(transaction, reqd_pos+1, clock))
        self.update_chain(reqd_pos+1)
    
    def update_chain(self, pos):
        for idx in range(pos, self.length):
            prev_hash = "" if idx==0 else str(self.data[idx-1])
            headerHash = hashlib.sha256(prev_hash.encode()).hexdigest()
            self.update_hash(idx, headerHash)

    def update_hash(self, pos, hash):
        self.data[pos].headerHash = hash

    def print(self):
        print("======================================")
        print("Total of {} nodes in blockchain".format(self.length))
        for block in self.data:
            print(str(block))
        print("======================================")
