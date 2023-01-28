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
CLK = "CLK"

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
        # generating copy so that pass by ref issue doesnt arise
        return LamportClock(self.clock, self.pid)

    def incrementClock(self):
        self.clock += 1

    def __lt__(self, other):
        # using total lamport ordering to break ties
        if self.clock < other.clock:
            return True
        elif self.clock == other.clock:
            return self.pid < other.pid
        return False

    def updateClock(self, other, inplace=True):
        self.clock = max(self.clock, other.clock) + 1
        if not inplace:
            return self.copy()

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
        return str(self.headerHash) + " |\n" + str(self.transaction) + \
               " | " + self.status + " | " + str(self.clock)
    
    def update_status(self, status):
        self.status = status

class Blockchain:
    def __init__(self):
        self.data = []
        self.head = -1
        self.length = 0
    
    def header(self):
        # points to the header .. index could be -1 i.e. picks already processed transaction
        return self.data[self.head] if self.length!=0 else None
    
    def move(self):
        # move the header to the next one if something is waiting for processing
        self.head += 1
        if self.head >= self.length:
            self.head = -1
    
    def append(self, transaction, clock):
        # add the block at the end of the queue
        prev_hash = "" if self.length==0 else str(self.data[self.length-1])
        headerHash = hashlib.sha256(prev_hash.encode()).hexdigest()
        block = Block(headerHash, transaction, clock)
        self.data.append(block)
        self.length += 1
        self.head = self.length - 1
        print("{} added at {} in blockchain with clock : {}".format(transaction, self.head, clock))
    
    def insert(self, transaction, clock):
        if self.head == -1:
            self.append(transaction, clock)
            return
        # check if it can be added back
        reqd_pos = self.head
        # checking if it has to be inserted before head
        for idx in range(self.head, -1, -1):
            if self.data[idx].status != IN_PROGRESS:
                break
            if clock < self.data[idx].clock:
                reqd_pos -= 1
                self.head = reqd_pos + 1
            else:
                break
        # checking if it has to be inserted after head
        # if it enters above loop, for sure it wont enter here
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
        print("{} added at {} in blockchain with clock : {}".format(transaction, reqd_pos+1, clock))
        self.update_chain(reqd_pos+1)
    
    def update_chain(self, pos):
        # recompute the hash from the given position
        for idx in range(pos, self.length):
            prev_hash = "" if idx==0 else str(self.data[idx-1])
            headerHash = hashlib.sha256(prev_hash.encode()).hexdigest()
            self.update_hash(idx, headerHash)

    def update_hash(self, pos, hash):
        # update the hash to the given hash
        self.data[pos].headerHash = hash

    def print(self):
        print("======================================")
        print("Total of {} nodes in blockchain".format(self.length))
        for block in self.data:
            print(str(block))
        print("======================================")

class Reply:
    """
    Created to track the replies obtained from other clients
    Tracking the individual count of responses from each client
    """
    def __init__(self, pid):
        self.data = {}
        for id in range(1, CLIENT_COUNT+1):
            if id!=pid:
                self.data[id] = 0

    def add(self, pid):
        # add count for the given client
        self.data[pid] += 1
    
    def decrement(self):
        # decrement count for each client
        for ele in self.data:
            self.data[ele] -= 1
    
    def count(self):
        # compute the min count of reponses per client
        ans = 1e9
        for val in self.data.values():
            ans = min(ans, val)
        return ans
