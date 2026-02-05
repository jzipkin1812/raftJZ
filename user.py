import argparse
import time
import subprocess
import xmlrpc.client
import xmlrpc.server
from concurrent.futures import ThreadPoolExecutor
import threading

def asyncRpc(method, *args):
    try:
        method(*args)
        return "SUCCESS"
    except ConnectionRefusedError as e:
        return f"That node is offline."
    except Exception as e:
        return f"Unknown RPC Error: {e}"

def submitAsync(method, *args):
    def task():
        result = asyncRpc(method, *args)
        if result != "SUCCESS": 
            print(result)
    executor.submit(task)

def callback(result : str):
    print(result)
    return("Thanks! --the user")

def tryRPC(port : int, function):
    pass


parser = argparse.ArgumentParser()
parser.add_argument('--callback', type=int, default=8000, help='A callback RPC port')
parser.add_argument('--coordinator', type=int, default=8001, help='Starting port for coordinators')
parser.add_argument('--shard', type=int, default=8004, help='Starting port for shard')

args = parser.parse_args()

myCallbackPort = args.callback
coordinatorPorts = [int(port) for port in range(args.coordinator, args.coordinator + 3)]
shardPorts = [int(port) for port in range(args.shard, args.shard + 9)]
coordinatorProcesses = []
shardProcesses = []
nodeID = -1

print("Starting", len(shardPorts), "coordinators...")

# Launch coordinators
for i, port in enumerate(coordinatorPorts):
    cArgs = ["python3", "-u", "./coordinator.py", "--datacenter", str(i),
                  "--user", str(myCallbackPort), "--port", str(port), "--peers"]
    # Coordinator peers
    for otherPort in coordinatorPorts:
        if otherPort != port:
            cArgs.append(str(otherPort))
    # Shard children
    cArgs.append("--shards")
    for shardPort in shardPorts[(i * 3):(i * 3 + 3)]:
        cArgs.append(str(shardPort))

    process = subprocess.Popen(cArgs,
                           stderr=subprocess.PIPE,
                           stdout=subprocess.PIPE,
                           text=True)
    coordinatorProcesses.append(process)

# Launch shards
print("Starting", len(shardPorts), "shards...")

# Launch shards
for i, port in enumerate(shardPorts):
    dataCenterID = i // 3
    shardID = i % 3
    friendID1 = ((shardID + 1) % 3) + (dataCenterID * 3)
    friendID2 = ((shardID + 2) % 3) + (dataCenterID * 3)
    sArgs = ["python3", "-u", "./shard.py", "--datacenter", str(dataCenterID), "--shard", str(shardID),
                  "--user", str(myCallbackPort), "--coordinator", str(coordinatorPorts[dataCenterID]), 
                  "--friends", str(shardPorts[friendID1]), str(shardPorts[friendID2])]
    process = subprocess.Popen(sArgs,
                           stderr=subprocess.PIPE,
                           stdout=subprocess.PIPE,
                           text=True)
    shardProcesses.append(process)

print("Coordinators and shards are running.")
print("-----------------------------------\n")

# Asynchronous RPC primitives
# Callback request executor
serverExecutor = ThreadPoolExecutor(max_workers=1)
# Asynchronous client requests executor
executor = ThreadPoolExecutor(max_workers=len(coordinatorProcesses))
# Start the callback xmlrpc server
server = xmlrpc.server.SimpleXMLRPCServer(("127.0.0.1", myCallbackPort), logRequests=False, allow_none=True)
server.register_function(callback, "callback")
serverExecutor.submit(lambda: server.serve_forever())
time.sleep(0.5)

# Main input loop
# while True:
#     try:
#         line = input()
#         words = [w for w in line.split(" ") if len(w) > 0]
#         if len(words) < 1:
#             continue
#         func = words[0]

#         if func == "wait":
#             time.sleep(float(words[1]))

#         elif func in ["moneyTransfer", "transfer", "transaction", "trans", "t"]:
#             _, sender, receiver, amount = words
#             nodePort = ports[int(sender)]
#             proxy = xmlrpc.client.ServerProxy(f"http://localhost:{nodePort}/")
#             submitAsync(proxy.moneyTransfer, int(receiver), int(amount))

#         elif func in ["failProcess", "fail", "kill"]:
#             _, node = words
#             p : subprocess.Popen = processes[int(node)]
#             p.terminate()
#             running[int(node)] = False
#             print("Node", node, "killed.")

#         elif func in ["printBlockchain", "blockchain", "chain", "blocks"]:
#             _, node = words
#             nodePort = ports[int(node)]
#             proxy = xmlrpc.client.ServerProxy(f"http://localhost:{nodePort}/")
#             submitAsync(proxy.printBlockchain)
        
#         elif func in ["printBalance", "balance", "balances"]:
#             for i, nodePort in enumerate(ports):
#                 if running[i]:
#                     proxy = xmlrpc.client.ServerProxy(f"http://localhost:{nodePort}/")
#                     submitAsync(proxy.printBalance)
#                     break

#         elif func in ["all", "blockchains", "allBlockchains"]:
#             for node in range(5):
#                 nodePort = ports[int(node)]
#                 proxy = xmlrpc.client.ServerProxy(f"http://localhost:{nodePort}/")
#                 try:
#                     proxy.printBlockchain()
#                 except ConnectionRefusedError:
#                     print(f"Node {int(node)} is offline.\n")
#         else:
#             print("Error: Unrecognized function.")
#     except EOFError:
#         break


# Cleanup
# Terminate RPCs
executor.shutdown(wait=True)
server.shutdown()
serverExecutor.shutdown()
# Terminate processes
print("-------TERMINATING ALL SHARDS-------")
for port, process in zip(shardPorts, shardProcesses):
    # print("\n--------PROCESS---------", port)
    # out, err = process.communicate(timeout=2)
    # print(out, end="")
    # if err:
    #     print(err, end="")
    # print("-------------------------")

    process.terminate()
    print("Killed process", process.pid, "at port", port)

print("-------TERMINATING ALL COORDINATORS-------")
for port, process in zip(coordinatorPorts, coordinatorProcesses):
    # print("\n--------PROCESS---------", port)
    # out, err = process.communicate(timeout=2)
    # print(out, end="")
    # if err:
    #     print(err, end="")
    # print("-------------------------")

    process.terminate()
    print("Killed process", process.pid, "at port", port)
