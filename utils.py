import xmlrpc.server
import threading
from socketserver import ThreadingMixIn

class SimpleThreadedXMLRPCServer(ThreadingMixIn, xmlrpc.server.SimpleXMLRPCServer):
    pass

def balanceDict(shardID : int):
    assert (shardID >= 0 and shardID <= 2), "Invalid Shard ID"
    result = dict()
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    for c in chars:
        result[c + str(shardID)] = 1000
    return(result)
        
    
def userCallback(userPort : int, msg : str):
    proxy = xmlrpc.client.ServerProxy(f"http://localhost:{userPort}/", allow_none=True)
    try:
        proxy.callback(msg)
    except Exception as e:
        print(f"Failed to deliver user callback: {e}")

