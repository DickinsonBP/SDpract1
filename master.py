#!/urs/bin/env python3.9
from xmlrpc.server import SimpleXMLRPCServer
import logging
from multiprocessing import Process
import redis

WORKERS = {} #lista de workers
WORKER_ID = 0 #indice del worker
JOBID = 0 #idetificador del job
r = redis.Redis(host='localhost',port=6379,db=0)
cola = "colaTareas"

logging.basicConfig(level=logging.INFO)

server = SimpleXMLRPCServer(
    ('localhost',8000),
    logRequests=True,
    allow_none=True
)


def start_worker(name):
    global r
    global cola
    while(True):
        value = r.lpop(cola)
        if(value!= None):
            print(value)
        

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
    global JOBID
    mensaje.split(' ')
    t = (JOBID)
    i=0
    while(i < len(mensaje)-1):
        t.append(mensaje[i])
        i += 1

    r.rpush(cola,mensaje)
    print('t = {}'.format(t))
    JOBID += 1

server.register_function(create_worker)
server.register_function(delete_worker)
server.register_function(list_worker)
server.register_function(job)

try:
    print('Use Control-C to exit')
    server.serve_forever()
except KeyboardInterrupt:
    print('Exiting')
