#! /usr/bin/python3
import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import xmlrpc.server
import threading
from socketserver import ThreadingMixIn
from utils import SimpleThreadedXMLRPCServer, userCallback
import utils
from commitment import Committer


# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--datacenter', type=int, default=0, help='Data center ID')
parser.add_argument('--shard', type=int, default=0, help='Shard ID')
parser.add_argument('--port', type=int, default=0, help='Listener Port') 
parser.add_argument('--friends', nargs='*', help='A list of ports that represent the nearby shards')
parser.add_argument('--user', type=int, default=8000, help='User Port') 
parser.add_argument('--coordinator', type=int, default=8100, help='Coordinator Port') 
parser.add_argument('--recovery', action='store_true', default=False, help='Include if this coordinator should recover by reading from a file') 

args = parser.parse_args()

# Identification / port variables
myShard : int = args.shard
myDataCenter : int = args.datacenter
myPort : int = args.port
userPort : int = args.user
coordinatorPort : int = args.coordinator
friends = [int(port) for port in args.friends] if args.friends is not None else []
userProxy = xmlrpc.client.ServerProxy(f"http://localhost:{userPort}/", allow_none=True)
coordinatorProxy = xmlrpc.client.ServerProxy(f"http://localhost:{coordinatorPort}/", allow_none=True)

# Data store (the transaction log is only held by the coordinator)
balances : dict = utils.balanceDict(myShard)

# Network Delay
delay = 1
# userCallback(f"About to run shard {myShard}{myDataCenter} with myPort {myPort}")
my2pc = Committer(myShard, myPort, friends, myDataCenter, args.recovery)
my2pc.server.serve_forever()