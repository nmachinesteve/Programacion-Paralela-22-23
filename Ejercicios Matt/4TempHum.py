from paho.mqtt.client import Client 
import sys

TEMP = 'temperature'
HUMIDITY = 'humidity'

def on_message(mqttc, data, msg):
    print (f'message:{msg.topic}:{msg.payload}:{data}') 
    if data['status'] == 0:
        temp = int(msg.payload) # we are only susbribed in temperature 
        if temp>data['temp_threshold']:
            print(f'Temperature {temp} exceedeed, trying humidity') 
            mqttc.subscribe(HUMIDITY)
            data['status'] = 1

    elif data['status'] == 1:
        if msg.topic==HUMIDITY:
            humidity = int(msg.payload)
            if humidity>data['humidity_threshold']:
                print(f'Humidity {humidity} exceeded') 
                mqttc.unsubscribe(HUMIDITY) # Esto debe ser lo Ãoltimo 
                data['status'] = 0
        elif TEMP in msg.topic:
            temp = int(msg.payload)
            if temp<=data['temp_threshold']:
                print(f'Temperature {temp} under threshold')
                data['status']=0
                mqttc.unsubscribe(HUMIDITY)
                
def on_log(mqttc, data, level, buf): 
    print(f'LOG: {data}:{msg}')

def main(broker):
    data = {'temp_threshold':20,'humidity_threshold':80,
        'status': 0}
    mqttc = Client(userdata=data)
    mqttc.on_message = on_message
    mqttc.enable_logger()
    
    mqttc.connect(broker) 
    mqttc.subscribe(f'{TEMP}/t1')
    mqttc.loop_forever()
    
if __name__ == '__main__':
    hostname = 'simba.fdi.ucm.es'
    if len(sys.argv)>1:
        hostname = sys.argv[1]
    main(hostname)
