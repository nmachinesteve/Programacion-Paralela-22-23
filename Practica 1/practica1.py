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
N = 10  # nº de elems del productor (cantidad de números aleatorios)
NPROD = 3  # nº de productores  

def delay(factor=3):
    sleep(random()/factor)

# función que devuelve el mínimo elemento y su índice de cada lista de productores
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

# función que añade cada número a un buffer
def add_data(storage, mutex, num):
    mutex.acquire()
    capacidad = len(storage)
    i = 0 
    
    try:
        while i < capacidad:
            if (storage[i] == -2 and storage[i] != -1):  # mientras el proceso sea vacío y no finalizado
                storage[i] = num
                break
            delay(6)
            i = i + 1
            
    finally:
        mutex.release()

# función que obtiene el valor del buffer
def get_data(storage, index, mutex):
    mutex.acquire()
    capacidad = len(storage)
    
    try:
        data = storage[0]
        delay()
        for i in range(capacidad - 1):
            storage[i] = storage[i + 1]
        storage[capacidad - 1] = -2  # se queda vacío
        
    finally:
        mutex.release()
        
    return data


def producer(storage, index, empty, non_empty, mutex):
    num = 0 
    
    for i in range(N):
        delay(6)
        empty.acquire()
            
        print(f"producer {current_process().name} produciendo")
        # producir
        num = num + randint(0, N)  # generamos números aleatorios de forma creciente
        add_data(storage, mutex, num)
        
        empty.release()
        print(f"producer {current_process().name} almacenado {num}")
            
    # cuando terminamos de producir, añadimos un -1 para indicar que el proceso ha finalizado
    non_empty.acquire()
    add_data(storage, mutex, -1)
    print(f"producer {current_process().name} terminado")
    non_empty.release()


def consumer(storage, lista, index, empty, non_empty, mutex):
    for i in range(NPROD):
        non_empty.acquire()
    
    while True:  # mientras haya elementos por consumir
        print(f"consumer {current_process().name} desalmacenando")
            
        minimo, indice = cmin(storage)
        
        if minimo == float('inf'):  # Si no hay más elementos a consumir, finaliza el proceso
            break
        
        mutex[indice].acquire()
        dato = get_data(storage, index, mutex[indice])
        
        empty[indice].release()
        non_empty[indice].acquire()
        print(f"consumer {current_process().name} consumiendo {dato}")
        delay()


def main():
    storage = Array('i', N)  # cada productor tiene su capacidad N
    lista = [Array('i', N) for _ in range(NPROD)] 
    
    # indice 
    index = Value('i', 0)
    
    for i in range(N):
        storage[i] = -2  # inicialización = vacío
    print("Almacen Inicial", storage[:], "Indice", index.value)

    # lista de semáforos
    non_empty = [Semaphore(0) for _ in range(NPROD)]
    empty = [BoundedSemaphore(N) for _ in range(NPROD)]
    mutex = [Lock() for _ in range(NPROD)]

    prodlst = [Process(target=producer, name=f'prod_{i}', args=(storage, index, empty[i], non_empty[i], mutex[i])) for i in range(NPROD)]
    conslst = [Process(target=consumer, name=f"cons_{i}", args=(storage, lista[i], index, empty[i], non_empty[i], mutex[i])) for i in range(NPROD)]

    for p in prodlst + conslst:
        p.start()

    for p in prodlst + conslst:
        p.join()

    print("final del programa")


if __name__ == '__main__':
    main()
