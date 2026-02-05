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


# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--datacenter', type=int, default=0, help='Data center ID')
parser.add_argument('--shard', type=int, default=0, help='Shard ID')
parser.add_argument('--port', type=int, default=0, help='Listener Port') 
parser.add_argument('--friends', nargs='*', help='A list of ports that represent the nearby shards')
parser.add_argument('--user', type=int, default=8000, help='User Port') 
parser.add_argument('--coordinator', type=int, default=8100, help='Coordinator Port') 
args = parser.parse_args()

# Identification / port variables
myShard : int = args.shard
myDataCenter : int = args.datacenter
myPort : int = args.port
userPort : int = args.user
coordinatorPort : int = args.coordinator
friends = [int(port) for port in args.friends] if args.friends is not None else None
userProxy = xmlrpc.client.ServerProxy(f"http://localhost:{userPort}/", allow_none=True)
coordinatorProxy = xmlrpc.client.ServerProxy(f"http://localhost:{coordinatorPort}/", allow_none=True)

# Data store (the transaction log is only held by the coordinator)
balances : dict = utils.balanceDict(myShard)

# Network Delay
delay = 1


# RPC helper for majority
# Func: The asynchronous RPC function to call
# resultFilter: A boolean function that takes the RPC response as input.
# This function will return if A) all reponses are received OR B) A majority of responses
# are received and evaluate to True when input to the filter.
def getMajority(func, resultFilter) -> list:
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(func, port) for port in friends]

        results = []
        for f in as_completed(futures):
            try:
                res = f.result()
                results.append(res)
            except Exception as e:
                userCallback(f"Error from node: {e}")

            # Break if a majority approve
            affirmativeResponses = 0
            for res in results:
                if resultFilter(res):
                    affirmativeResponses += 1
            # Or break if everyone responded
            if affirmativeResponses > 2 or len(results) == 4:
                break

        assert len(results) > 1, "No results were received"
        return(results)

# User API
def moneyTransfer(recipient : int, amount : int):
    # Validate parameters
    if recipient == myShard:
        userCallback("A shard cannot send money to itself.")
        return("")

    # Mine the next block in the blockchain.
    return("")
    
def printBalance():
    # for i in range(0, 5):
    #     userCallback(f"Balance of process {i} is {myBlocks.getBalance(i)}")
    return("")

def printBlockchain():
    # userCallback(f"------Blockchain {myId}------\n" + myBlocks.getBlockchain())
    return("")

# Peer-to-peer API
# These all represent *receiving* messages.
# def prepare(ballot : Ballot):
#     time.sleep(delay)


# def accept(ballot : Ballot, value : Block):
#     time.sleep(delay)

# def decide(block : Block):
#     time.sleep(delay)

# Register and start RPC server
server = SimpleThreadedXMLRPCServer((f"localhost", myPort), logRequests=False, allow_none=True)
# server.register_function(printBlockchain, "printBlockchain")
# server.register_function(printBalance, "printBalance")
# server.register_function(moneyTransfer, "moneyTransfer")
# server.register_function(prepare, "prepare")
# server.register_function(accept, "accept")
# server.register_function(decide, "decide")

server.serve_forever()    
