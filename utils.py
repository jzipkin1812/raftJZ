import xmlrpc.server, xmlrpc.client
import threading
from socketserver import ThreadingMixIn
from dataclasses import dataclass
import random
import time
import subprocess
import argparse
import logging

def parseAndStart():
    parser = argparse.ArgumentParser()
    parser.add_argument('--callback', type=int, default=8000, help='A callback RPC port')
    parser.add_argument('--coordinator', type=int, default=8001, help='Starting port for coordinators')
    parser.add_argument('--shard', type=int, default=8004, help='Starting port for shard')

    args = parser.parse_args()

    myCallbackPort = args.callback
    coordinatorPorts = [int(port) for port in range(args.coordinator, args.coordinator + 3)]
    shardPorts = [int(port) for port in range(args.shard, args.shard + 9)]
    coordinatorProcesses : list[subprocess.Popen] = []
    shardProcesses : list[subprocess.Popen] = []

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

    return(myCallbackPort, coordinatorPorts, shardPorts, coordinatorProcesses, shardProcesses)


class SimpleThreadedXMLRPCServer(ThreadingMixIn, xmlrpc.server.SimpleXMLRPCServer):
    pass

def balanceDict(shardID : int):
    assert (shardID >= 0 and shardID <= 2), "Invalid Shard ID"
    result = dict()
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    for c in chars:
        result[c + str(shardID)] = 1000
    return(result)
        
    
def userCallback(msg : str, level : int = logging.INFO, userPort : int = 8000):
    proxy = xmlrpc.client.ServerProxy(f"http://localhost:{userPort}/", allow_none=True)
    try:
        proxy.callback(msg, level)
    except Exception as e:
        print(f"Failed to deliver user callback: {e}")

def examine(process : subprocess.Popen):
    print(f"--------PROCESS {process.pid}---------")
    if process.poll() != None:
        out, err = process.communicate(timeout=2)
        print(out, end="")
        if err:
            print(err, end="")
        print(f"------------EXIT CODE: {process.returncode}-------------")
    else:
        print(f"STILL RUNNING.")

def getLeader(ports : list[int]) -> int:
    for port in ports:
        try:
            proxy = xmlrpc.client.ServerProxy(f"http://localhost:{port}/")
            if proxy.isLeader():
                return(port)
        except:
            pass
    return(-1)