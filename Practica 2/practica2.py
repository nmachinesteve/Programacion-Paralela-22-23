"""
Solution to the one-way tunnel
"""
import time
import random
from multiprocessing import Lock, Condition, Process
from multiprocessing import Value

SOUTH = 1
NORTH = 0

NCARS = 100
NPED = 10
TIME_CARS_NORTH = 0.5  # a new car enters each 0.5s
TIME_CARS_SOUTH = 0.5  # a new car enters each 0.5s
TIME_PED = 5 # a new pedestrian enters each 5s
TIME_IN_BRIDGE_CARS = (1, 0.5) # normal 1s, 0.5s
TIME_IN_BRIDGE_PEDESTRIAN = (30, 10) # normal 1s, 0.5s


class Monitor():
    def __init__(self):
        self.mutex = Lock()
        self.patata = Value('i', 0)
        
        self.cargonorte = Value('i', 0) # coches que quieren ir al norte
        self.cargosur = Value('i', 0) # coches que quieren ir al sur
        self.peaton = Value('i', 0) # peatones que quieren cruzar
        
        self.cruzandonorte = Value('i', 0) # coches cruzando direccion norte
        self.cruzandosur = Value('i', 0) # coches cruzando direccion sur
        self.cruzandopeaton = Value('i', 0) # peatones cruzando
        
        self.waitnorte = Value('i', 0) # coches esperando ir al norte
        self.waitsur = Value('i', 0) # coches esperando ir al sur
        self.waitpeaton = Value('i', 0) # peatones esperando cruzar
        
        # coches y peatones que ya han cruzado
        self.yanorte = Value('i', 0) 
        self.yasur = Value('i', 0)
        self.yacruzado= Value('i', 0)
        
        self.norte = Condition(self.mutex)
        self.sur = Condition(self.mutex)
        self.ped = Condition(self.mutex)
        
        '''
        elegimos que pasen coches de 3 en 3 de cada sentido, cruzando 2 peatones 
        entre cada turno de coches 
        self.prioridadnorte = Value('i', 0)
        self.prioridadsur = Value('i', 0)
        self.prioridadpeaton = Value('i', 0)
        '''
    
    def ningun_car_norte(self):
        return self.cargonorte.value == 0 
    
    def ningun_car_sur(self):
        return self.cargosur.value == 0 
    
    def ningun_peaton(self):
        return self.peaton.value == 0 
    
    
    # establecemos que los coches pueden cruzar de 3 en 3, variando entre norte y sur 
    # y cruzando 2 peatones entre cada turno de coches
    def cruzar_norte(self):
        return (self.ningun_car_sur() and self.ningun_peaton())  \
                or (self.yanorte.value <= 3 and self.yacruzado.value == 2 and self.yasur.value == 3)
        
    def cruzar_sur(self):
        return (self.ningun_car_norte() and self.ningun_peaton()) \
                or (self.yasur.value <= 3 and self.yacruzado.value == 2 and self.yanorte.value == 3)
        
    def cruzar_peaton(self):
        return (self.ningun_car_norte() and self.ningun_car_sur()) \
                or (self.yanorte.value == 3 and self.yasur.value == 0) \
                or (self.yasur.value == 3 and self.yanorte.value == 0) \
                or (self.yasur.value == 3 and self.yanorte.value == 3) 
                
        
    
    def wants_enter_car(self, direction: int) -> None:
        self.mutex.acquire()
        self.patata.value += 1
       
        # direccion norte
        if direction == 0:
            self.cruzandonorte.value = 1
            self.waitnorte.value += 1
            self.norte.wait_for(self.cruzar_norte)
            self.waitnorte.value -= 1
            
            self.cruzandonorte.value = 0
            
            '''
            if self.waitnorte.value == 0:
                self.cargonorte.value = 0 #duda
            
           self.cargonorte += 1 
           '''
           
        # direccion sur (direction == 1)
        else: 
            self.cruzandosur.value = 1
            self.waitsur.value += 1
            self.sur.wait_for(self.cruzar_sur)
            self.waitsur.value -= 1
            
            self.cruzandosur.value = 0
            
            
        self.mutex.release()
        

    def leaves_car(self, direction: int) -> None:
        self.mutex.acquire() 
        self.patata.value += 1
        
        # direccion norte
        if direction == 0:
            
            self.yanorte.value += 1 
            self.cargonorte.value -= 1
        
            if self.cargonorte.value == 0:
                self.norte.notify_all()
                self.ped.notify_all()
        
        # direccion sur (direction == 1)
        else:
            self.yasur.value += 1 
            self.cargosur.value -= 1
        
            if self.cargosur.value == 0:
                self.sur.notify_all()
                self.ped.notify_all()
        
            
        self.mutex.release()
        

    def wants_enter_pedestrian(self) -> None:
        self.mutex.acquire()
        self.patata.value += 1
        
        self.cruzandopeaton.value = 1
        self.waitpeaton.value += 1 
        self.ped.wait_for(self.cruzar_peaton())
        self.waitpeaton.value -= 1 
        
        '''
        if self.waitpeaton.value == 0 
        '''
        
        self.mutex.release()
        

    def leaves_pedestrian(self) -> None:
        self.mutex.acquire()
        self.patata.value += 1
        
        self.yacruzado.value += 1 
        self.peaton.value -= 1 
        
        if self.peaton.value == 0:
            self.norte.notify_all()
            self.sur.notify_all()
        
        
        self.mutex.release()


    def __repr__(self) -> str:
        return f'Monitor: {self.patata.value}'


def delay_car_north(factor = 3) -> None:
    time.sleep(random.random()/factor)

def delay_car_south() -> None:
    time.sleep(random.random()/factor)

def delay_pedestrian() -> None:
    time.sleep(random.random()/factor)


def car(cid: int, direction: int, monitor: Monitor)  -> None:
    print(f"car {cid} heading {direction} wants to enter. {monitor}")
    monitor.wants_enter_car(direction)
    print(f"car {cid} heading {direction} enters the bridge. {monitor}")
    if direction==NORTH :
        delay_car_north()
    else:
        delay_car_south()
    print(f"car {cid} heading {direction} leaving the bridge. {monitor}")
    monitor.leaves_car(direction)
    print(f"car {cid} heading {direction} out of the bridge. {monitor}")


def pedestrian(pid: int, monitor: Monitor) -> None:
    print(f"pedestrian {pid} wants to enter. {monitor}")
    monitor.wants_enter_pedestrian()
    print(f"pedestrian {pid} enters the bridge. {monitor}")
    delay_pedestrian()
    print(f"pedestrian {pid} leaving the bridge. {monitor}")
    monitor.leaves_pedestrian()
    print(f"pedestrian {pid} out of the bridge. {monitor}")


def gen_pedestrian(monitor: Monitor) -> None:
    pid = 0
    plst = []
    for _ in range(NPED):
        pid += 1
        p = Process(target=pedestrian, args=(pid, monitor))
        p.start()
        plst.append(p)
        time.sleep(random.expovariate(1/TIME_PED))

    for p in plst:
        p.join()


def gen_cars(direction: int, time_cars, monitor: Monitor) -> None:
    cid = 0
    plst = []
    for _ in range(NCARS):
        cid += 1
        p = Process(target=car, args=(cid, direction, monitor))
        p.start()
        plst.append(p)
        time.sleep(random.expovariate(1/time_cars))

    for p in plst:
        p.join()


def main():
    monitor = Monitor()
    gcars_north = Process(target=gen_cars, args=(NORTH, TIME_CARS_NORTH, monitor))
    gcars_south = Process(target=gen_cars, args=(SOUTH, TIME_CARS_SOUTH, monitor))
    gped = Process(target=gen_pedestrian, args=(monitor,))
    gcars_north.start()
    gcars_south.start()
    gped.start()
    gcars_north.join()
    gcars_south.join()
    gped.join()


if __name__ == '__main__':
    main()
