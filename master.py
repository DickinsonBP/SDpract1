#!/usr/bin/python3
from xmlrpc.server import SimpleXMLRPCServer
import logging
from multiprocessing import Process, Pipe
import redis
import requests
import pickle
import pycurl
from io import BytesIO
from time import sleep
import warnings


WORKERS = {} #lista de workers
WORKER_ID = 0 #indice del worker
JOBID = 0 #idetificador del job
r = redis.Redis(host='localhost',port=6379,db=0)
r.flushdb() #limpiar base de datos
cola = "colaTareas"
#server_pipe = Pipe()

warnings.filterwarnings("ignore",category=DeprecationWarning)

logging.basicConfig(level=logging.INFO)

server = SimpleXMLRPCServer(
    ('localhost',7000),
    logRequests=True,
    allow_none=True
)


def start_worker(name,pipe):
    global r
    global cola
    print("Name: {} Pipe: {}".format(name,pipe))
    while(True):
        sleep(10)
        value = pickle.loads(r.rpop(cola))
        if(value is not None):
            #print("Worker {} Value {}".format(name,value))
            #obtener lista de archivos
            archivos = value[2]
            archivos = archivos[1:-1]
            archivos = archivos.split(",")
                
            if(value[1] in ("run-countwords")): 
                countWords(name,archivos,pipe)
            elif (value[1] in ("run-wordcout")): 
                wordCount(name,archivos,pipe)

def countWords(worker,archivos,pipe):
    result = 0
    for url in archivos:
        curl = pycurl.Curl()
        buffer = BytesIO()
        curl.setopt(curl.URL,url)
        curl.setopt(curl.WRITEDATA,buffer)
        curl.perform()
        curl.close()
        body = buffer.getvalue()
        words = body.split()
        result += len(words)
        s = "Worker: {} Longitud de {} es : {}".format(worker,url,result)
        #print(s)
        words.clear()
    r = (s)
    pipe.send(r)

def wordCount(url):
    return 0

def create_worker():
    s = 'Creando worker...'
    global WORKERS
    global WORKER_ID
    #global server_pipe

    server_pipe, worker_pipe = Pipe()
    argumentos = (WORKER_ID, worker_pipe)
    proc = Process(target=start_worker, args=([WORKER_ID,worker_pipe]))
    proc.start()
    WORKERS[WORKER_ID] = proc
    WORKER_ID += 1
    result = server_pipe.recv()
    print("Create Worker: "+str(result))
    return result

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
    for i in mensaje.split():
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
