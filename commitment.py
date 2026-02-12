from utils import *
import threading
import json
import copy
import os

class Committer:
    def __init__(self, shardID: int, myPort : int, peerPorts : list[int], dataCenter : int, recovery = False):
        self.path = os.path.join(".", "raft", f"shard{dataCenter}{shardID}.txt")

        self.shardID = shardID
        self.peerProxies = [
            xmlrpc.client.ServerProxy(f"http://localhost:{port}/", allow_none=True)
            for port in peerPorts
        ]

        if self.shardID == 0:
            self.peerIDs = [1, 2]
        elif self.shardID == 1:
            self.peerIDs = [0, 2]
        else:
            self.peerIDs = [0, 1]

        if not recovery:
            self.balances : dict[str, int] = balanceDict(self.shardID)
            self.save(self.path)
        else:
            self.balances = dict()
            self.load(self.path)

        self.locks : dict[str, threading.Lock] = dict()
        for key in self.balances.keys():
            self.locks[key] = threading.Lock()

        self.lockLock = threading.Lock()

        self.server = SimpleThreadedXMLRPCServer((f"localhost", myPort), logRequests=False, allow_none=True)
        self.registerFunctions()



    def registerFunctions(self):
        f = self.server.register_function
        f(self.tryCommit, "tryCommit")
        f(self.confirm, "confirm")
        f(self.abort, "abort")
        f(self.getShard, "getShard")


    def tryCommit(self, keyFrom : str, keyTo : str, amount : int) -> bool:
        userCallback(f"Shard {self.shardID} trying to commit: {keyFrom}, {keyTo}, {amount}", logging.DEBUG)
        # The coordinator is the 2PC leader.
        # First: acquire locks.
        with self.lockLock:
            shouldAbort = self.isLocked(keyFrom) or self.isLocked(keyTo)
            if shouldAbort:
                userCallback(f"Shard {self.shardID} isLocked: [{keyFrom}]{self.isLocked(keyFrom)}, [{keyTo}]{self.isLocked(keyTo)}", logging.DEBUG)
            shouldAbort = shouldAbort or (int(amount) > self.balances.get(keyFrom, 999999999))
            if shouldAbort:
                userCallback(f"Shard {self.shardID} fail: {int(amount)} < {self.balances.get(keyFrom, -1)}", logging.DEBUG)
                return(False)
            else:
                userCallback(f"Shard {self.shardID} locking: {keyFrom}, {keyTo}, {amount}", logging.DEBUG)
                self.lock(keyFrom)
                self.lock(keyTo)
        userCallback(f"Shard {self.shardID} success: {keyFrom}, {keyTo}, {amount}", logging.DEBUG)
        # Then: respond YES.
        return(True)
    
    def isLocked(self, key : str):
        if key in self.locks.keys():
            return(self.locks[key].locked())
        else:
            return(False)
        
    def lock(self, key : str):
        if key in self.locks.keys():
            self.locks[key].acquire()

    def unlock(self, key : str):
        if key in self.locks.keys():
            self.locks[key].release()
    
    # Precondition: Must already have locks 
    def confirm(self, keyFrom : str, keyTo : str, amount : int):
        if keyFrom in self.balances.keys():
            self.balances[keyFrom] -= int(amount)
        if keyTo in self.balances.keys():
            self.balances[keyTo] += int(amount)

        with self.lockLock:
            self.unlock(keyTo)
            self.unlock(keyFrom)
            self.save(self.path)

        return 
    
    def abort(self):
        with self.lockLock:
            for l in self.locks.values():
                if l.locked:
                    l.release()
        
    def getShard(self):
        return(self.balances)
    

    def save(self, path: str):
        data = self.balances

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def load(self, path: str):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.balances = copy.deepcopy(data)