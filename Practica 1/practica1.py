#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 11 01:03:07 2023

@author: nickmachin
"""

from multiprocessing import Process
from multiprocessing import BoundedSemaphore, Semaphore, Lock
from multiprocessing import current_process
from multiprocessing import Value, Array
from time import sleep
from random import random, randint


# K = capacidad del buffer
N = 10      # nº de elems del productor (cantidad de numeros aleatorios)
NPROD = 3   # nº de productores 


def delay(factor = 3):
    sleep(random()/factor)


# funcion que devuelve el min elem y su indice de cada lista de productores
def cmin(prods):
    
    n = len(prods)
    C = []
    maximo = max(prods)
    
    for i in range(n):
        if prods[i] >= 0 and prods[i] < maximo:
            
            v = prods[i]
            C.append(v)
    
    minimo = min(C) 
    index = C.index(minimo)
        
    return minimo, index 
    

# funcion que añade cada num a un buffer
def add_data(storage, mutex, num):
    
    mutex.acquire()
    capacidad = len(storage)
    i = 0 
    
    try:
        while i < capacidad:
            if (storage[i] == -2 and storage[i] != -1): # mientras el proceso sea vacio y no finalizado
                storage[i] = num
            delay(6)
            i = i + 1
            
    finally:
        mutex.release()


# funcion que coge el valor del buffer
def get_data(storage, mutex):
    
    mutex.acquire()
    capacidad = len(storage)
    
    try:
        data = storage[0]
        delay()
        for i in range(capacidad):
            if storage[i] != -2: 
                storage[i] = storage[i + 1]
        storage[capacidad-1] = -2 # se queda vacio
        
    finally:
        mutex.release()
        
    return data


def producer(storage, index, empty, non_empty, mutex):
    
    num = 0 
    
    for i in range(N):
        delay(6)
        empty.acquire()
            
        print (f"producer {current_process().name} produciendo")
        # producir
        num = num + random.randint(0,N) # generamos numeros aleatorios de forma creciente
        add_data(storage, mutex, num)
        
        non_empty.release()
        print (f"producer {current_process().name} almacenado {num}")
            
    # cuando terminamos de producir, añadimos un -1 para indicar que el proceso esta finalizado
    empty.acquire()
    add_data(storage, mutex, -1)
    print (f"producer {current_process().name} terminado")
    non_empty.release()


def consumer(storage, lista, index, empty, non_empty, mutex):
   
    for i in range(NPROD):
        
        non_empty.acquire()
    
    while storage[0] != -1 : # mientras el proceso no haya terminado 
        print (f"consumer {current_process().name} desalmacenando")
            
        # consumir (elegimos el minimo y lo ponemos en la lista)
        minimo, indice = cmin(storage)
        lista.append(minimo)
        
        dato = get_data(storage[indice], index, mutex[indice])
        
        empty[indice].release()
        non_empty[indice].acquire()
        print (f"consumer {current_process().name} consumiendo {dato}")
        delay()
        

def main():
    
    storage = Array('i', N) # cada productor tiene su capacidad N
    lista = [Array('i',N) for i in range(NPROD)] 
    
    # indice 
    index = Value('i', 0)
    
    
    for i in range(N):
        storage[i] = -2 # inicializacion = vacio
    print ("Almacen Inicial", storage[:], "Indice", index.value)

    # lista de semaforos
    non_empty = [Semaphore(0) for i in range(NPROD)]
    empty = [BoundedSemaphore(N) for i in range(NPROD)]
    mutex = [Lock() for i in range(NPROD)]

    prodlst = [ Process(target=producer, name=f'prod_{i}', args=(storage, index, empty, non_empty, mutex)) for i in range(NPROD) ]
    conslst = [ Process(target=consumer, name=f"cons_{i}", args=(storage, lista, index, empty, non_empty, mutex)) ]

    for p in prodlst + conslst:
        p.start()

    for p in prodlst + conslst:
        p.join()


if __name__ == '__main__':
    main()
