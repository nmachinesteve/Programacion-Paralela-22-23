# Ejercicio 2: Numeros

from paho.mqtt.client import Client
from multiprocessing import Process, Manager 
from time import sleep
import random
import sys

NUMBERS = 'numbers'
CLIENTS = 'clients'
STOP = f'{CLIENTS}/stop' 

def is_prime(n): 
    i= 2
    while (i*i < n) and (n % i != 0): 
        i = i + 1
    return i*i > n

def timer(time, data): 
    mqttc = Client()
    mqttc.connect(data['broker'])
    msg = f'timer working. timeout: {time}'
    print(msg) 
    mqttc.publish(STOP, msg) 
    sleep(time)
    msg = f'timer working. timeout: {time}' 
    mqttc.publish(STOP, msg)
    print('timer ended') 
    mqttc.disconnect()

def on_message(mqttc, data, msg):
    
    try:
        if is_prime(int(msg.payload)):
            print (f"The number {int(msg.payload)} is prime")
        else:
            print (f"The number {int(msg.payload)} is not prime")

    except ValueError as e:
        print(e)
        pass

def on_log(mqttc, userdata, level, string):
    print("LOG", userdata, level, string)

def main(broker):
    data = {'client': None,'broker': broker}
    mqttc = Client(userdata=data) 
    data['client'] = mqttc
    mqttc.enable_logger()
    mqttc.on_message = on_message
    mqttc.on_log = on_log
    mqttc.connect(broker)
    mqttc.subscribe(NUMBERS)
    mqttc.loop_forever()


if __name__ == '__main__':
    hostname = 'simba.fdi.ucm.es'
    if len(sys.argv)>1:
        hostname = sys.argv[1]
    main(hostname)
