import xmlrpc.server, xmlrpc.client
import threading
from socketserver import ThreadingMixIn
from dataclasses import dataclass
import random
import time
import subprocess
from utils import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
import logging


class Role(Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2

@dataclass
class Transaction:
    keyFrom : str
    keyTo : str
    amount : int
    term : int

class Raft:
    def __init__(self, id : int, myPort : int, peerPorts : list[int], shardPorts : list[int]):
        self.id = id
        self.userPort = 8000

        # Networking
        userCallback(f"Establishing a coordinator with id {id} and port {myPort}", logging.DEBUG)
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
        self.role : Role = Role.FOLLOWER
        self.currentTerm = 0
        self.votedFor = None
        self.log : list[Transaction] = []

        # Volatile state
        self.commitIndex = 0
        self.lastApplied = 0
        self.clock = time.time()
        self.electionTimeout = 0
        self.timeIn()
        self.knownLeader = None

        # Leader stuff
        self.nextIndex : list[int] = [0 for _ in peerPorts]
        self.matchIndex : list[int] = [0 for _ in peerPorts]

        self.registerFunctions()

    def registerFunctions(self):
        self.server.register_function(self.AppendEntries, "AppendEntries")
        self.server.register_function(self.Heartbeat, "Heartbeat")
        self.server.register_function(self.RequestVote, "RequestVote")
        self.server.register_function(self.getIndex, "getIndex")
        self.server.register_function(self.isLeader, "isLeader")
        self.server.register_function(self.transfer, "transfer")

    def lastLogTerm(self):
        term = 0
        if len(self.log) > 0:
            term = self.log[-1].term
        return(term)

    # Begin an election with this machine as the candidate.
    def beginElection(self):
        self.votedFor = self.id
        self.currentTerm += 1
        userCallback(f"Coordinator {self.id} is starting an election~")

        # Send vote RPCs
        becameLeader = self.campaign()

        # If we won, time to step up!
        if becameLeader:
            userCallback(f"Coordinator {self.id} has become the leader! Heartbeating to {len(self.peerProxies)} proxies.")
            self.votedFor = None
            for proxy in self.peerProxies:
                try:
                    result = proxy.Heartbeat(self.currentTerm, self.id, len(self.log) - 1, self.lastLogTerm(), self.commitIndex)
                    userCallback(f"Coordinator {self.id} got the heartbeat result: {result}")
                except Exception as e:
                    userCallback(e)

        self.timeIn()

    def campaign(self):
        def sendVoteRequest(proxy : xmlrpc.client.ServerProxy):
            success, term = proxy.RequestVote(self.currentTerm, self.id, len(self.log) - 1, self.lastLogTerm())
            if term > self.currentTerm:
                self.currentTerm = term
            return(success)

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(sendVoteRequest, proxy) for proxy in self.peerProxies]

            results = []
            for f in as_completed(futures):
                try: 
                    res = f.result()
                    # userCallback(f"Coordinator {self.id} got the vote req result: {res}")
                    results.append(res)
                except Exception as e:
                    userCallback(f"Coordinator {self.id} tried to submit a vote request but got the error: {e}")
                    results.append(False)
                    
            # assert len(results) > 1, "No results were received; consensus cannot be reached if both other datacenters have failed!"
            userCallback(f"Coordinator {self.id} results from campaign: {results}")
            # The following code only works with 3 coordinators but that's okay.
            success = (True in results)
            return(success)
        
    # Wrapper for append entries
    def Heartbeat(self, term : int, leaderId : int, prevLogIndex : int, prevLogTerm : int, leaderCommit : int) -> tuple[bool, int]:
        userCallback(f"Coordinator {self.id} got a Heartbeat from leader {leaderId}")
        return(self.AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, [], leaderCommit))

    def AppendEntries(self, term : int, leaderId : int, prevLogIndex : int, 
                      prevLogTerm : int, entries : list[Transaction], 
                      leaderCommit : int) -> tuple[bool, int]:
        rejection = (False, self.currentTerm)
        
        # Outdated append RPC is rejected.
        if term < self.currentTerm:
            userCallback(f"Coordinator {self.id} is rejecting an outdated Append from {leaderId} b/c of terms: {term} < {self.currentTerm} ")
            return(rejection)
        
        # Otherwise, we at least acknowledge that the leader who sent this is recent.
        self.knownLeader = leaderId
        self.votedFor = None
        self.timeIn()
        if term > self.currentTerm:
            self.currentTerm = term
        if self.role == Role.CANDIDATE or self.role == Role.LEADER:
            self.role = Role.FOLLOWER
        
        # If this Raft's log is outdated, we can't append anything.
        if len(self.log) <= prevLogIndex:
            return(rejection)
        if (not self.empty()) and (self.log[prevLogIndex].term != prevLogTerm):
            return(rejection)
                
        
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
        # userCallback(f"Coordinator {self.id} got a RequestVote from {candidateId}, term {term}.")
        rejection = (False, self.currentTerm)
        # Basics
        if term < self.currentTerm or \
                  (self.votedFor != None and self.votedFor != candidateId):
            return(rejection)
        # Outdated log
        elif (not self.empty) and (self.log[-1].term > lastLogTerm):
            return(rejection)
        elif (not self.empty) and ((self.log[-1].term == lastLogTerm) and (len(self.log) > lastLogIndex + 1)):
            return(rejection)
        
        # Success
        if term > self.currentTerm:
            self.currentTerm = term
        self.votedFor = candidateId
        userCallback(f"Coordinator {self.id} is granting {candidateId}'s vote.")
        return(True, self.currentTerm)
    
    def transfer(self):
        pass
    
    def timeIn(self):
        self.clock = time.time()
        self.electionTimeout = (random.random() * 3) + 2

    def timedOut(self) -> bool:
        return((time.time() - self.clock) > self.electionTimeout)

    def getIndex(self):
        return(len(self.log))
    
    def isLeader(self):
        userCallback(f"Coordinator {self.id} got a request to see if it's the leader.", logging.DEBUG)
        return(self.role == Role.LEADER)

    def doCommit(self):
        for t in self.log[:self.commitIndex + 1]:
            userCallback(f"Coordinator {self.id}, committed transaction: {t.keyFrom} -> [{t.amount}] -> {t.keyTo}")

    def empty(self):
        return(len(self.log) == 0)
        
             
        

        

