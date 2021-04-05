#!/usr/bin/python3
from xmlrpc.server import SimpleXMLRPCServer
import logging
from multiprocessing import Process, Queue
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
q = Queue()

warnings.filterwarnings("ignore",category=DeprecationWarning)

logging.basicConfig(level=logging.INFO)

server = SimpleXMLRPCServer(
    ('localhost',7000),
    logRequests=True,
    allow_none=True
)


def start_worker(name,q):
    global r
    global cola
    while(True):
        value = pickle.loads(r.rpop(cola))
        if(value is not None):
            #obtener lista de archivos
            archivos = value[2]
            archivos = archivos[1:-1]
            archivos = archivos.split(",")
                
            if(value[1] in ("run-countwords")): 
                countWords(name,archivos,q)
            elif (value[1] in ("run-wordcout")): 
                wordCount(name,archivos,q)
        sleep(5)

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
    q.put(s)

def wordCount(url):
    return 0

def create_worker():
    global WORKERS
    global WORKER_ID
    global q
    #result = ''
    #i=0
    #while(i < numWorkers):
    proc = Process(target=start_worker, args=([WORKER_ID,q]))
    print(proc)
    proc.start()
    WORKERS[WORKER_ID] = proc
    WORKER_ID += 1 
     #   i+=1

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

def results():
    global q
    print(q)
    result = []
    if(not q.empty()):
        result.append(q.get())
    else:
        result.append("No hay resultados")

    return result

server.register_function(create_worker)
server.register_function(delete_worker)
server.register_function(list_worker)
server.register_function(job)
server.register_function(results)

try:
    print('Use Control-C to exit')
    server.serve_forever()
except KeyboardInterrupt:
    print('Exiting')
