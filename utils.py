import xmlrpc.server, xmlrpc.client
import threading
from socketserver import ThreadingMixIn
from dataclasses import dataclass
import random
import time

class SimpleThreadedXMLRPCServer(ThreadingMixIn, xmlrpc.server.SimpleXMLRPCServer):
    pass

def balanceDict(shardID : int):
    assert (shardID >= 0 and shardID <= 2), "Invalid Shard ID"
    result = dict()
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    for c in chars:
        result[c + str(shardID)] = 1000
    return(result)
        
    
def userCallback(userPort : int, msg : str):
    proxy = xmlrpc.client.ServerProxy(f"http://localhost:{userPort}/", allow_none=True)
    try:
        proxy.callback(msg)
    except Exception as e:
        print(f"Failed to deliver user callback: {e}")

@dataclass
class Transaction:
    keyFrom : str
    keyTo : str
    amount : int
    term : int

class Raft:
    def __init__(self, id : int, myPort : int, peerPorts : list[int], shardPorts : list[int]):
        self.id = id

        # Networking
        self.server = SimpleThreadedXMLRPCServer((f"localhost", myPort), logRequests=False, allow_none=True)
        self.peerProxies = [
            xmlrpc.client.ServerProxy(f"http://localhost:{port}/", allow_none=True)
            for port in peerPorts
        ]
        self.shardProxies = [
            xmlrpc.client.ServerProxy(f"http://localhost:{port}/", allow_none=True)
            for port in shardPorts
        ]

        # Persistent state
        self.role = "Follower"
        self.currentTerm = 0
        self.votedFor = None
        self.log : list[Transaction] = []

        # Volatile state
        self.commitIndex = 0
        self.lastApplied = 0
        self.clock = time.time()
        self.electionTimeout = 0
        self.timeIn()

        # Leader stuff
        nextIndex : list[int] = None
        matchIndex : list[int] = None

    def AppendEntries(self, term : int, leaderId : int, prevLogIndex : int, 
                      prevLogTerm : int, entries : list[Transaction], 
                      leaderCommit : int) -> tuple[bool, int]:
        rejection = (False, self.currentTerm)
        
        # Outdated append RPC is rejected.
        if term < self.currentTerm:
            return(rejection)
        
        # If this Raft's log is outdated, we can't append anything.
        if len(self.log) <= prevLogIndex:
            return(rejection)
        if self.log[prevLogIndex].term != prevLogTerm:
            return(rejection)
        
        # Update state
        self.timeIn()
        if term > self.currentTerm:
            self.currentTerm = term
        if self.role == "candidate" or self.role == "leader":
            self.role = "follower"
        
        # Loop through the log and delete as necessary
        matches = 0
        series = enumerate(zip(self.log[prevLogIndex + 1:], entries))
        for (i, (myTransaction, givenTransaction)) in series:
            indexOfMine = i + prevLogIndex
            if myTransaction != givenTransaction:
                self.log = self.log[:indexOfMine]
                break
            else:
                matches += 1
        
        # Append
        self.log = self.log + entries[matches:]

        # Commit
        if leaderCommit > self.commitIndex:
            self.commitIndex = leaderCommit
            self.doCommit()

        return(True, self.currentTerm)
        
    def RequestVote(self, term : int, candidateId : int, 
                    lastLogIndex : int, lastLogTerm : int) -> tuple[bool, int]:
        rejection = (False, self.currentTerm)
        # Basics
        if term < self.currentTerm or \
                  self.votedFor != None:
            return(rejection)
        # Outdated log
        elif self.log[-1].term > lastLogTerm:
            return(rejection)
        elif (self.log[-1].term == lastLogTerm) and (len(self.log) > lastLogIndex + 1):
            return(rejection)
        
        # Success
        if term > self.currentTerm:
            self.currentTerm = term
        self.votedFor = candidateId
        return(True, self.currentTerm)
    
    def timeIn(self):
        self.clock = time.time()
        self.electionTimeout = random.randint(5, 10)

    def checkTimeout(self):
        return(time.time() - self.clock() > self.electionTimeout)

    def getIndex(self):
        return(len(self.log))

    def doCommit(self):
        pass
        
             
        

        

