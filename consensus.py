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
import os
import json
from enum import Enum
from dataclasses import dataclass, asdict
from typing import Optional
from colorama import Fore, Style, init


class Raft:
    def __init__(self, id : int, myPort : int, peerPorts : list[int], shardPorts : list[int], recovery = False):
        self.path = os.path.join(".", "raft", f"{id}.txt")
        self.id = id
        self.userPort = 8000
        # jank ass id calculation fix later idk
        if self.id == 0:
            self.peerIDs = [1, 2]
        elif self.id == 1:
            self.peerIDs = [0, 2]
        else:
            self.peerIDs = [0, 1]

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
        self.commitIndex = -1

        if recovery:
            self.load(self.path)

        # Volatile state
        # Represents the last index that WAS committed
        self.lastApplied = 0
        self.clock = time.time()
        self.electionTimeout = 0
        self.timeIn()
        self.knownLeader = None

        # Leader stuff
        # Next index to send to the peer
        self.nextIndex = [len(self.log) for _ in self.peerProxies]
        # Last index known to match for the peer
        self.matchIndex = [-1 for _ in self.peerProxies]
        # Heartbeat clock
        self.timeSinceHeartbeat = time.time()

        # Mutex
        self.mut = threading.Lock()

        self.registerFunctions()

        # Coordinator with id 2 steps up at the beginning of the program
        if (self.currentTerm == 0 and self.id == 0):
            time.sleep(0.5)
            self.stepUp()

    def save(self, path: str):
        data = {
            "role": self.role.value,
            "currentTerm": self.currentTerm,
            "votedFor": self.votedFor,
            "log": [tx.to_dict() for tx in self.log],
            "commitIndex" : self.commitIndex,
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def load(self, path: str):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.role = Role(data["role"])
        self.currentTerm = data["currentTerm"]
        self.votedFor = data["votedFor"]
        self.commitIndex = data["commitIndex"]
        self.log = [Transaction.from_dict(tx) for tx in data["log"]]

    def registerFunctions(self):
        f = self.server.register_function
        f(self.AppendEntries, "AppendEntries")
        f(self.Heartbeat, "Heartbeat")
        f(self.RequestVote, "RequestVote")
        f(self.getIndex, "getIndex")
        f(self.isLeader, "isLeader")
        f(self.transfer, "transfer")
        f(self.printLog, "printLog")

    def lastLogTerm(self):
        term = 0
        if len(self.log) > 0:
            term = self.log[-1].term
        return(term)

    # Begin an election with this machine as the candidate.
    def beginElection(self):
        self.role = Role.CANDIDATE
        self.votedFor = self.id
        self.currentTerm += 1
        userCallback(f"Coordinator {self.id} is starting an election~", logging.DEBUG)
        self.save(self.path)

        # Send vote RPCs
        becameLeader = self.campaign()

        # If we won, time to step up!
        if becameLeader:
            self.stepUp()

        self.timeIn()

    def stepUp(self):
        userCallback(f"Coordinator {self.id} has become the leader! Heartbeating to {len(self.peerProxies)} proxies.", logging.DEBUG)
        self.votedFor = None
        self.matchIndex = [-1 for _ in self.peerProxies]
        self.nextIndex = [len(self.log) for _ in self.peerProxies]
        self.role = Role.LEADER
        self.save(self.path)
        self.catchUp()


    def catchUp(self):
        self.lastHeartbeat = time.time()
        for (i, proxy) in enumerate(self.peerProxies):
            peerID = self.peerIDs[i]
            result = False
            while result == False:
                idx = self.nextIndex[i]
                try:
                    userCallback(f"Coordinator {self.id} sending AppendEntries RPC to {peerID} with idx {idx}", logging.DEBUG)
                    try:
                        sendTerm = self.log[idx - 1].term
                    except:
                        sendTerm = 0
                    result, term = proxy.AppendEntries(self.currentTerm, self.id, idx - 1, sendTerm,
                                    self.log[idx:], self.commitIndex)
                except Exception as e:
                    userCallback(f"Coordinator {self.id} could not reach peer {peerID}: {e}", logging.DEBUG)
                    break
                self.currentTerm = max(term, self.currentTerm)
                if result:
                    self.matchIndex[i] = idx
                    userCallback(f"Coordinator {self.id} has matchIndex[{peerID}] = {idx}", logging.DEBUG)
                else:
                    self.nextIndex[i] -= 1
        self.save(self.path)

    def campaign(self):
        with self.mut:
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
                        userCallback(f"Coordinator {self.id} tried to submit a vote request but got the error: {e}", logging.DEBUG)
                        results.append(False)
                        
                # assert len(results) > 1, "No results were received; consensus cannot be reached if both other datacenters have failed!"
                userCallback(f"Coordinator {self.id} results from campaign: {results}", logging.DEBUG)
                # The following code only works with 3 coordinators but that's okay.
                success = (True in results)
                return(success)
            
    # Wrapper for append entries
    def Heartbeat(self, term : int, leaderId : int, prevLogIndex : int, prevLogTerm : int, leaderCommit : int) -> tuple[bool, int]:
        userCallback(f"Coordinator {self.id} got a Heartbeat from leader {leaderId}", logging.DEBUG)
        return(self.AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, [], leaderCommit))

    def AppendEntries(self, term : int, leaderId : int, prevLogIndex : int, 
                      prevLogTerm : int, entries : list[Transaction], 
                      leaderCommit : int) -> tuple[bool, int]:
        with self.mut:
            rejection = (False, self.currentTerm)

            # Convert entries to real Transactionsgot
            for (i, d) in enumerate(entries):
                if type(d) != Transaction:
                    entries[i] = Transaction(d["keyFrom"], d["keyTo"], d["amount"], d["term"], d["ID"])
            
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
                userCallback(f"{self.id} rejecting Append from {leaderId} because my log length {len(self.log)} <= {prevLogIndex} prevLogIndex")
                return(rejection)
            if (not self.empty()) and (prevLogIndex >= 0) and (self.log[prevLogIndex].term != prevLogTerm):
                userCallback(f"{self.id} rejecting Append from {leaderId} self.log[{prevLogIndex}] is {self.log[prevLogIndex]} and prevLogTerm is {prevLogTerm}")
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
            self.doCommit(leaderCommit)

            self.save(self.path)
            return(True, self.currentTerm)
        
    def RequestVote(self, term : int, candidateId : int, 
                    lastLogIndex : int, lastLogTerm : int) -> tuple[bool, int]:
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
        userCallback(f"Coordinator {self.id} is granting {candidateId}'s vote.", logging.DEBUG)
        self.save(self.path)
        return(True, self.currentTerm)
    
    # This will only execute if this machine is the leader.
    def transfer(self, fromKey : str, toKey : str, amount : int, ID : int) -> bool:
        if self.role != Role.LEADER:
            return(False)
        self.timeIn()
        term = self.currentTerm
        t = Transaction(fromKey, toKey, amount, term, ID)

        # Append the transaction to the log.
        self.log.append(t)
        
        # Get consensus.
        # We repeatedly try to query each friend until we get success.
        self.catchUp()
        self.save(self.path)
        userCallback(f"Your transaction with ID {t.ID} has been logged: {t}", colorID = 3)

        self.doCommit(self.canCommit())
        return(True)
    
    def timeIn(self):
        self.clock = time.time()
        self.electionTimeout = (random.random() * 3) + 2

    def timedOut(self) -> bool:
        return((time.time() - self.clock) > self.electionTimeout)

    def getIndex(self):
        return(len(self.log))
    
    def isLeader(self):
        userCallback(f"Coordinator {self.id} got a request to see if it's the leader. Its role is {self.role}", logging.DEBUG)
        return(self.role == Role.LEADER)
    
    # This function is only for leaders.
    # We see who matches among the peers to see how high we can increment commitIndex.
    def canCommit(self):
        # If the leader has no entries in the log from its latest term,
        # we can't commit anything.
        if self.log[-1].term != self.currentTerm:
            return(self.commitIndex)
        
        # Otherwise, see matches.
        # There are only 3 coordinators, so the commitIndex is just the greatest among
        # the match indices (this indicates majority.)
        newIdx = max(self.commitIndex, max(self.matchIndex))
        userCallback(f"Coordinator {self.id} can commit: {newIdx}", logging.DEBUG)
        return(newIdx)

    def doCommit(self, newCommitIndex : int):
        if newCommitIndex <= self.commitIndex:
            return
        
        userCallback(f"Coordinator {self.id} committing from {self.commitIndex} to {newCommitIndex}", logging.DEBUG)
        for t in self.log[self.commitIndex + 1:newCommitIndex + 1]:
            # Phase 1: Get agreement
            success2p = True
            for (i, proxy) in enumerate(self.shardProxies):
                try:
                    success2p = success2p and proxy.tryCommit(t.keyFrom, t.keyTo, t.amount)
                except ConnectionRefusedError as e:
                    userCallback(f"Coordinator {self.id} could not connect to shard {i}", colorID=self.id)
                    success2p = False
                except Exception as e:
                    userCallback(f"During 2pc, coordinator {self.id} got an unknown error from shard {i}: {e}")
            if not success2p:
                break

            # Phase 2: Confirm commitment
            for proxy in self.shardProxies:
                proxy.confirm(t.keyFrom, t.keyTo, t.amount)

            userCallback(f"Coordinator {self.id} committed transaction with ID {t.ID}: {t}", colorID=self.id)

            self.commitIndex += 1
            self.save(self.path)
            # userCallback(f"Coordinator {self.id} now has commit index: {self.commitIndex}")


    def empty(self):
        return(len(self.log) == 0)
    
    # Prints the log along with other stats about the state of the Raft.
    def printLog(self):
        msg = ""
        msg += f"-------LOG OF DATA CENTER {self.id}-------\n"
        for t in self.log:
            msg += f"{t}\n"
        msg += f"----COMMITTED: {self.commitIndex} | ROLE: {self.role} | TERM: {self.currentTerm}----"
        userCallback(msg, colorID=self.id)
        
             
        

        

