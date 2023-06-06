from paho.mqtt.client import Client 
import sys

TEMP = 'temperature'
HUMIDITY = 'humidity'








if __name__ == '__main__':
    hostname = 'simba.fdi.ucm.es'
    if len(sys.argv)>1:
        hostname = sys.argv[1]
    main(hostname)
