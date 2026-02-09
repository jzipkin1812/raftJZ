import argparse
import time
import subprocess
import xmlrpc.client
import xmlrpc.server
from concurrent.futures import ThreadPoolExecutor
import threading
import logging
from utils import examine, parseAndStart, getLeader

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
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

def callback(result : str, level : int = logging.INFO):
    log.log(level, msg=result)
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
time.sleep(0.5)

while True:
    try:
        line = input()
        words = [w for w in line.split(" ") if len(w) > 0]
        if len(words) < 1:
            continue
        func = words[0]

        if func == "wait":
            time.sleep(float(words[1]))

        elif func in ["moneyTransfer", "transfer", "transaction", "trans", "t"]:
            _, sender, receiver, amount = words
            nodePort = getLeader(coordinatorPorts)
            print(nodePort)
            # proxy = xmlrpc.client.ServerProxy(f"http://localhost:{nodePort}/")
            # submitAsync(proxy.moneyTransfer, int(receiver), int(amount))

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
print("-------TERMINATING ALL SHARDS-------")
for port, process in zip(shardPorts, shardProcesses):
    result = process.poll()
    if not (result is None):
        print(f"Shard {process.pid} at port {port} has already exited with code {result}")
    else:
        process.terminate()
        print(f"Killed shard {process.pid} at port {port}")

print("-------TERMINATING ALL COORDINATORS-------")
for port, process in zip(coordinatorPorts, coordinatorProcesses):
    examine(process)
    result = process.poll()
    if not (result is None):
        print(f"Coordinator {process.pid} at port {port} has already exited with code {result}")
    else:
        process.terminate()
        print(f"Killed coordinator {process.pid} at port {port}")
