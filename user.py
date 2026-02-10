import argparse
import time
import subprocess
import xmlrpc.client
import xmlrpc.server
from concurrent.futures import ThreadPoolExecutor
import threading
import logging
from utils import examine, parseAndStart, getLeader, startCoordinator, startShard
import random
import os
from colorama import Fore, Style, init
init(autoreset=True)

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger()

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

def callback(result : str, level : int = logging.INFO, colorID = None):
    colors = [Fore.CYAN, Fore.LIGHTGREEN_EX, Fore.YELLOW, Fore.WHITE]
    if colorID is not None:
        output = colors[colorID] + result
    else:
        output = result

    log.log(level, msg=output)  
    return("Thanks! --the user")


def tryRPC(port : int, function):
    pass

myCallbackPort, coordinatorPorts, shardPorts, coordinatorProcesses, shardProcesses = parseAndStart()

# Asynchronous RPC primitives
# Callback request executor
serverExecutor = ThreadPoolExecutor(max_workers=1)
# Asynchronous client requests executor
executor = ThreadPoolExecutor(max_workers=len(coordinatorProcesses))
# Start the callback xmlrpc server
server = xmlrpc.server.SimpleXMLRPCServer(("127.0.0.1", myCallbackPort), logRequests=False, allow_none=True)
server.register_function(callback, "callback")
serverExecutor.submit(lambda: server.serve_forever())
time.sleep(0.6)

while True:
    try:
        line = input()
        words = [w for w in line.split(" ") if len(w) > 0]
        if len(words) < 1:
            continue
        func = words[0]

        if func in ["wait", "sleep"]:
            time.sleep(float(words[1]))

        elif func in ["moneyTransfer", "transfer", "transaction", "trans", "t"]:
            _, fromKey, toKey, amount = words
            nodePort = getLeader(coordinatorPorts)
            proxy = xmlrpc.client.ServerProxy(f"http://localhost:{nodePort}/")
            submitAsync(proxy.transfer, fromKey, toKey, amount, random.randint(1, 1000000))

        elif func in ["log", "printLog", "printTransactions"]:
            _, ID = words
            nodePort = coordinatorPorts[int(ID)]
            proxy = xmlrpc.client.ServerProxy(f"http://localhost:{nodePort}/")
            submitAsync(proxy.printLog)

        elif func in ["all", "allLogs", "printAllLogs", "logs"]:
            for nodePort in coordinatorPorts:
                proxy = xmlrpc.client.ServerProxy(f"http://localhost:{nodePort}/")
                submitAsync(proxy.printLog)

        elif func in ["failProcess", "fail", "kill"]:
            _, node = words
            if len(node) == 1:
                i = int(node)
                p : subprocess.Popen = coordinatorProcesses[i]
                p.terminate()
                print("Data center", node, "killed.")
                for shard in shardProcesses[i*3:i*3+3]:
                    shard.terminate()
                print("All shards from data center", node, "killed.")
            else:
                p : subprocess.Popen = shardProcesses[int(node[0]) * 3 + int(node[1])]
                p.terminate()
                print("Shard", node, "killed.")

        elif func in ["recover", "restart", "start", "revive"]:
            _, node = words
            if len(node) == 1:
                i = int(node)
                process = startCoordinator(i, myCallbackPort, coordinatorPorts, shardPorts, True)
                coordinatorProcesses[i] = process
                print("Data center", node, "has started back up.")
                for j in range(i*3, i*3+3):
                    process = startShard(i, j, myCallbackPort, coordinatorPorts, shardPorts, True)
                print("All shards from data center", node, "have started back up.")
            else:
                i = int(node[0])
                j = int(node[1])
                process = startShard(i, j, myCallbackPort, coordinatorPorts, shardPorts, True)
                shardProcesses[i * 3 + j] = process
                print("Shard", node, "has started back up.")
        else:
            print("Error: Unrecognized function.")
    except EOFError:
        break


# Cleanup
# Terminate RPCs
executor.shutdown(wait=True)
server.shutdown()
serverExecutor.shutdown()
# Terminate processes
print(Fore.RED + "-------TERMINATING ALL SHARDS-------")
for port, process in zip(shardPorts, shardProcesses):
    result = process.poll()
    if not (result is None):
        log.debug(f"Shard {process.pid} at port {port} has already exited with code {result}")
    else:
        process.terminate()
        log.debug(f"Killed shard {process.pid} at port {port}")

print(Fore.RED + "-------TERMINATING ALL COORDINATORS-------")
for port, process in zip(coordinatorPorts, coordinatorProcesses):
    # examine(process)
    result = process.poll()
    if not (result is None):
        log.debug(f"Coordinator {process.pid} at port {port} has already exited with code {result}")
    else:
        process.terminate()
        log.debug(f"Killed coordinator {process.pid} at port {port}")

print(Fore.RED + "-------CLEARINIG FILE SYSTEM-------")
for id in [0, 1, 2]:
    path = os.path.join(".", "raft", f"{id}.txt")
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
