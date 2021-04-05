#!/usr/bin/python3
import xmlrpc.client
import sys

#client.py worker create = 3
#client.py worker delete  x = 4

proxy = xmlrpc.client.ServerProxy('http://localhost:7000')

if(len(sys.argv) == 3):
    #solo puede ser worker create o worker list
    '''if(sys.argv[2] == "create"):
        print(proxy.create_worker())'''
    if(sys.argv[2] == "list"):
        print(proxy.list_worker())
    else:
        print("Error de sintaxis")
elif(len(sys.argv) >= 4):
    #solo puede ser los jobs o worker delete
    if(sys.argv[2] == "create"):
        numWorkers = int(sys.argv[3])
        print(proxy.create_worker(numWorkers))

    if(sys.argv[1] == "worker") and (sys.argv[2] == "delete"):
        index = int(sys.argv[3])
        proxy.delete_worker(index)
    
    if(sys.argv[1] == "job"):
        i=2
        mensaje = ''
        while(i < len(sys.argv)):
            mensaje+=sys.argv[i]+' '
            i+=1
        proxy.job(mensaje)

    