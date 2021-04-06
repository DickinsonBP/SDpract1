#!/usr/bin/python3.8
from xmlrpc.server import SimpleXMLRPCServer
import logging
from multiprocessing import Process, Queue, Manager
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
#numTareas = {} #diccionario para controlar las tareas
manager = Manager()
numTareas = manager.dict()


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
    global numTareas
    value = tuple()
    while(True):
        value = r.rpop(cola)
        if(value is not None):
            value = pickle.loads(value)
            print("Value: {}".format(value))
            if(value[1] in ("run-countwords")):
                print("Tareas antes: {}".format(numTareas))
                result = countWords(value[2])
                print("Worker: {} Count words: {}".format(name,result))
                actualizarTareas(value[0])
                #actualizarCountwords(value[0],result)
                #print("Tareas: {} Resultados: {}".format(numTareas,resultadoTareas))
                print("Tareas despues: {}".format(numTareas))
            elif (value[1] in ("run-wordcount")):
                print("Tareas antes: {}".format(numTareas))
                result = wordCount(value[2])
                print("Worker: {} Word Count: {}".format(name,result))
                actualizarTareas(value[0])
                print("Tareas despues: {}".format(numTareas))
               # actualizarWordcount(value[0],result)
                #print("Tareas: {} Resultados: {}".format(numTareas,resultadoTareas))    

def actualizarTareas(id):
    global numTareas
    #si no esta vacio el diccionario
    if(numTareas):
        veces = numTareas.get(id)
        if(veces is not None) and (veces != 0):
            veces = veces - 1
            if(veces == 0):
                del numTareas[id]
            else:
                numTareas.update({id:veces})
            

def countWords(url):
    result = 0
    curl = pycurl.Curl()
    buffer = BytesIO()
    curl.setopt(curl.URL,url)
    curl.setopt(curl.WRITEDATA,buffer)
    curl.perform()
    curl.close()
    body = buffer.getvalue()
    words = body.split()
    result = len(words)
    #s = "Worker: {} Longitud de {} es : {}".format(worker,url,result)
     #print(s)
    words.clear()
    return result

def wordCount(url):
    dict1 = {}
    curl = pycurl.Curl()
    buffer = BytesIO()
    curl.setopt(curl.URL,url)
    curl.setopt(curl.WRITEDATA,buffer)
    curl.perform()
    curl.close()
    body = buffer.getvalue()
    words = body.split()
    for word in words:
        i=0
        if word not in dict1:
            i += 1
            dict1.setdefault(word, i)
        else:
            veces = dict1.get(word)
            veces += 1
            dict1.update({word:veces})
    return dict1

def create_worker():
    global WORKERS
    global WORKER_ID
    global q
    proc = Process(target=start_worker, args=([WORKER_ID,q]))
    print(proc)
    proc.start()
    WORKERS[WORKER_ID] = proc
    WORKER_ID += 1 

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
    #mensaje viene con el formato: funcion url,url2,..
    global r
    global JOBID
    global numTareas
    mensaje = mensaje.split()
    tarea = mensaje[0]
    archivos = mensaje[1]
    archivos = archivos[1:-1]
    archivos = archivos.split(",")
    for arch in archivos:
        lista = [JOBID]
        lista.append(tarea)
        lista.append(arch)
        message=tuple(lista)        
        r.rpush(cola,pickle.dumps(message))
    numTareas.setdefault(JOBID,len(archivos))
    print("Tareas: {}".format(numTareas))
    JOBID += 1
    #sleep(1.5)

def results():
    global q
    result = []
    while(not q.empty()):
        result.append(q.get())
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
