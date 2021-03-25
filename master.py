#!/urs/bin/env python3.9
from xmlrpc.server import SimpleXMLRPCServer
import logging
from multiprocessing import Process
import redis

WORKERS = {} #lista de workers
WORKER_ID = 0 #indice del worker
r = None
cola = "colaTareas"

logging.basicConfig(level=logging.INFO)

server = SimpleXMLRPCServer(
    ('localhost',8000),
    logRequests=True,
    allow_none=True
)


def start_worker(name):
    value = r.lpop(cola)
    print(value)
    while(True){
        
    }


def create_worker():
    s = 'Creando worker...'
    global WORKERS
    global WORKER_ID

    proc = Process(target=start_worker, args=(WORKER_ID,))
    proc.start()
    WORKERS[WORKER_ID] = proc
    WORKER_ID += 1

    return s

def delete_worker(index):
    s = 'Borrando worker... {}'.format(index)
    '''work = WORKERS[index]
    work.terminate()'''
    global WORKERS

    WORKERS[index].terminate()
    return s

def list_worker():
    s = str(WORKERS)
    return s

def job(mensaje):
    global r
    r.rpush(cola,mensaje)

server.register_function(create_worker)
server.register_function(delete_worker)
server.register_function(list_worker)
server.register_function(job)

try:
    print('Use Control-C to exit')
    server.serve_forever()
    try:
        r = redis.Redis(
            host="localhost",
            port=8000
            )
    except Exception as e:
        print("Error: {}".format(e))
except KeyboardInterrupt:
    print('Exiting')
