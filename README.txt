Práctica 1 de Sistemas Distribuidos
Se debe ejecutar en Linux

Se deben tener las siguintes librerias:
    xmlrpc.sever
    xmlrpc.client
    request
    loggin
    multiprocessing
    redis
    pickle
    pycurl
    BytesIO
    sleep


Debe estar inicializado el servidor HTTP desde la carpeta files (python3 -m http.server)
Primero hay que ejecutar el fichero master.py y a continuación el de cliente.py.
Las instrucciones que se escriben en cliente son:
	crear worker --> worker create [numWorkers] (numWorkers por defecto es 1)
	listar workers --> worker list
	eliminar worker --> delete worker
	count words --> job run-countwords [http://localhost:8000/f1,http://localhost:8000/f2] (sin espacios después de la coma que separa los archivos)
	word count --> job run-wordcount [http://localhost:8000/f1,http://localhost:8000/f2] (sin espacios después de la coma que separa los archivos)
	salir --> exit

El cliente y el master mantienen una comunicación directa (peer-to-peer), los workers y el máster se comunican mediante comunicación indirecta a través de una cola de redis.
Los resultados generados por los workers se envian al cliente de forma indirecta ya que este no hace falta que esté bloqueado para recibir estos resultados y puede ir realizando otras operaciones.
A medida que se van ejecutando varias operaciones se van mostrando en la consola del cliente y, si solo ejecuta una hará falta pulsar enter otra vez para poder ver estos resultados.








Anna Gracia Colmenarejo y Dickinson Bedoya Pérez. 