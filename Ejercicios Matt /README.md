# Protocolo Mqtt

El protocolo MQTT es un machine-to-machine (M2M)/Internet of Things protocolo de conectividad. Está diseñado como un esquema ligero y eciente de intercambio de mensajes en modo publish/subscribe, con bajo consumo de recursos y ancho de banda.


## Ejercicio 1: Broker 

Un componente esencial del sistema es un broker que se encarga de gestionar las publicaciones y subscripciones de los distintos elementos que se conectan.Para los ejercicios posteriores utilizaremos el broker en simba.fdi.ucm.es.
Los usuarios que se conectan, pueden enviar y recibir mensajes en el topic clients. También podréis crear vuestros propios canales de forma jerárquica a partir de esta raíz. Es decir, podéis publicar y leer en topics del estilo clients/mi_tema/mi_subtema.
Comprueba, en primer lugar, que puedes conectarte al broker y enviar y recibir mensajes.


## Ejercicio 2: Numeros

En el topic numbers se están publicando constantemente númeroslo, s hay enteros y los hay reales. Escribe el código de un cliente mqtt que lea este topic y que realice tareas con los números leídos,por ejemplo, separar los enteros y reales,calcular la frecuencia de cada uno de ellos, estudiar propiedades (como ser o no primo) en los enteros, etc.


## Ejercicio 3: Temperaturas 

En el topic temperature puede haber varios sensores emitiendo valores. Escribe el código de un cliente mqtt que lea los subtopics y que jado un intervalo de tiempo (mejor pequeño, entre 4 y 8 segundos) calcule la temperatura máxima, mínima y media para cada sensor y de todos los sensores.


## Ejercicio 4: Temperatura y Humedad 

Elige un termómetro concreto al que escuchar,es decir, uno de los sensores que publican en temperature. Escribe ahora el código para un cliente mqtt cuya misión es escuchar un termómetro y, si su valor supera una determinada temperatura,K_0, entonces pase a escuchar también en el topic humidity. Si la temperatura baja de K_0 o el valor de humidity sube de K_1 entonces el cliente dejará de escuchar en el topic humidity.


## Ejercicio 5: Temporizador

Escribe el código de un cliente mqtt que podamos utilizar como temporizador. El cliente leerá mensajes (elige tú mismo el topic) en los que se indicarán: tiempo de espera, topic y mensaje a publicar una vez pasado el tiempo de espera. El cliente tendrá que encargarse de esperar el tiempo adecuado y luego publicar el mensaje en el topic correspondiente.
