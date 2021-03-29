#!/usr/bin/python3
from xmlrpc.server import SimpleXMLRPCServer
import logging
from multiprocessing import Process
import redis
import requests
import pickle
import pycurl
from io import BytesIO

WORKERS = {} #lista de workers
WORKER_ID = 0 #indice del worker
JOBID = 0 #idetificador del job
r = redis.Redis(host='localhost',port=6379,db=0)
r.flushdb() #limpiar base de datos
cola = "colaTareas"

logging.basicConfig(level=logging.INFO)

server = SimpleXMLRPCServer(
    ('localhost',7000),
    logRequests=True,
    allow_none=True
)


def start_worker(name):
    global r
    global cola
    lista = list()
    while(True):
        value = pickle.loads(r.rpop(cola))
        if(value is not None):
            print("Worker {} Value {}".format(name,value))
            if(value[1] in ("run-countwords")): countWords(value[2])
            elif (value[1] in ("run-wordcout")): wordCount(value[2])
       

def countWords(url):
    curl = pycurl.Curl()
    buffer = BytesIO()
    url = url[1:-1]
    print(url)
    curl.setopt(curl.URL,url)
    curl.setopt(curl.WRITEDATA,buffer)
    curl.perform()
    curl.close()
    body = buffer.getvalue()
    words = body.split()
    print("Longitud de {} es : {}".format(url,len(words)))

def wordCount(url):
    return 0

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
    lista = [JOBID]
    for i in mensaje.split(' '):
        lista.append(i)

    message=tuple(lista)        
    #r.rpush(cola,*message)
    r.rpush(cola,pickle.dumps(message))
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
