from paho.mqtt.client import Client
from multiprocessing import Process, Manager 
from time import sleep
import paho.mqtt.publish as publish
import time
import sys

def on_message(mqttc, data, msg):
    print(f"MESSAGE:data:{data}, msg.topic:{msg.topic}, payload:{msg.payload}")

def on_log(mqttc, userdata, level, string): 
    print("LOG", userdata, level, string)

def main(broker):
    data = {'status':0}
    mqttc = Client(userdata=data) 
    mqttc.enable_logger() 
    mqttc.on_message = on_message 
    mqttc.on_log = on_log 
    mqttc.connect(broker)

    res_topics = ['clients/uno', 'clients/dos']
    for t in res_topics:
        mqttc.subscribe(t)
    mqttc.loop_start()
    tests = [
        (res_topics[0], 4, 'one'),
        (res_topics[1], 1, 'two'),
        (res_topics[0], 2, 'three'),
        (res_topics[1], 5, 'four')
    ]
    topic = 'clients/timeout' 
    for test in tests:
        mqttc.publish(topic, f'{test[0]},{test[1]},{test[2]}')
    time.sleep(10)

if __name__ == '__main__':
    hostname = 'simba.fdi.ucm.es'
    if len(sys.argv)>1:
        hostname = sys.argv[1]
    main(hostname)
