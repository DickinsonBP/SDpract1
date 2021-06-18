import random
palabras = open('listado-general.txt','r')
palabras = list(palabras.readlines())

num_files = 0
while (num_files < 10):
    nombre = 'files/f'+str(num_files+1)
    f = open(nombre,'w')
    for i in range(200):
        word = random.choice(palabras)
        f.write(word)
    f.close()
    print('Se ha escrito archivo '+str(num_files+1))
    num_files += 1

