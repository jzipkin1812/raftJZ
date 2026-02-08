#! /usr/bin/python3
import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import xmlrpc.server
import threading
from socketserver import ThreadingMixIn
from utils import SimpleThreadedXMLRPCServer, userCallback, Raft


# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--datacenter', type=int, default=0, help='Data center ID')
parser.add_argument('--port', type=int, default=0, help='Listener Port') 
parser.add_argument('--user', type=int, default=8000, help='User Port') 
parser.add_argument('--shards', nargs='*', help='A list of ports that represent the coordinator\'s shards')
parser.add_argument('--peers', nargs='*', help='A list of ports that represent the other coordinators') 
args = parser.parse_args()

myDataCenter : int = args.datacenter
myPort : int = args.port
userPort : int = args.user
peers = [int(port) for port in args.peers] if args.peers is not None else None
shards = [int(port) for port in args.shards] if args.shards is not None else None

# State information for raft
raft = Raft(myDataCenter, myPort, peers, shards)


server = SimpleThreadedXMLRPCServer((f"localhost", myPort), logRequests=False, allow_none=True)
done = False
while not done:
    pass
server.register_function(raft.AppendEntries, "AppendEntries")
server.register_function(raft.RequestVote, "RequestVote")
server.register_function(raft.getIndex, "getIndex")

# server.register_function(printBalance, "printBalance")
# server.register_function(moneyTransfer, "moneyTransfer")
# server.register_function(prepare, "prepare")
# server.register_function(accept, "accept")
# server.register_function(decide, "decide")

server.serve_forever()    
