from xmlrpc.server import SimpleXMLRPCServer
import logging
from multiprocessing import Process

WORKERS = {} #lista de workers
WORKER_ID = 0 #indice del worker

logging.basicConfig(level=logging.INFO)

server = SimpleXMLRPCServer(
    ('localhost',8000),
    logRequests=True,
    allow_none=True
)


def start_worker(name):
    print("Hola worker No:",name)

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

server.register_function(create_worker)
server.register_function(delete_worker)
server.register_function(list_worker)

try:
    print('User Control-C to exit')
    server.serve_forever()
except KeyboardInterrupt:
    print('Exiting')
