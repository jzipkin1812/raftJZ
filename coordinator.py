#! /usr/bin/python3
import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import xmlrpc.server
import threading
from socketserver import ThreadingMixIn
from utils import *
from consensus import Raft, Role, Transaction


# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--datacenter', type=int, default=0, help='Data center ID')
parser.add_argument('--port', type=int, default=0, help='Listener Port') 
parser.add_argument('--user', type=int, default=8000, help='User Port') 
parser.add_argument('--shards', nargs='*', help='A list of ports that represent the coordinator\'s shards')
parser.add_argument('--peers', nargs='*', help='A list of ports that represent the other coordinators') 
parser.add_argument('--recovery', action='store_true', default=False, help='Include if this coordinator should recover by reading from a file') 

args = parser.parse_args()

myDataCenter : int = args.datacenter
myPort : int = args.port
userPort : int = args.user
peers = [int(port) for port in args.peers] if args.peers is not None else []
shards = [int(port) for port in args.shards] if args.shards is not None else []
recovery = args.recovery

# State information for raft
raft = Raft(myDataCenter, myPort, peers, shards, recovery)

# Begin the server
serverExecutor = ThreadPoolExecutor(max_workers=1)
serverExecutor.submit(lambda : raft.server.serve_forever())

# Timeout-based loop
while True:
    time.sleep(0.1)
    # userCallback(f"Loopin' with raft {raft.id}")
    # All roles: Begin an election
    if raft.timedOut():
        raft.beginElection()

    # if raft.role == Role.FOLLOWER:

