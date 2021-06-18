#!/usr/bin/python3
import xmlrpc.client

#client.py worker create = 3
#client.py worker delete  x = 4

proxy = xmlrpc.client.ServerProxy('http://localhost:7000')

while True:
    resultados = proxy.results()
    if(resultados):
        print("Resultados: {}".format(resultados))
        
    entrada = input(">")
    if(entrada == "exit"):
        break
    elif("worker create" in entrada) and (len(entrada) == 13):
        proxy.create_worker(1)
    elif("worker create" in entrada) and (len(entrada) > 13):
        num_workers = list(entrada.split())[2]
        num_workers = int(num_workers)
        proxy.create_worker(num_workers)
    elif(entrada == "worker list"):
        print(proxy.list_worker())
    elif(len(entrada.split()) == 3) and (entrada.split()[1] == "delete"):
        index = int(entrada.split()[2])
        print(proxy.delete_worker(index))
    elif(len(entrada.split()) == 3) and (entrada.split()[0] == "job"):
        i=1
        mensaje = ''
        while(i < len(entrada.split())):
            mensaje+=entrada.split()[i]+' '
            i+=1
        #print(mensaje)
        proxy.job(mensaje)

    resultados = proxy.results()
    if(resultados):
        print("Resultados: {}".format(resultados))


        



    