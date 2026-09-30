Redactar un breve informe en el archivo `INFORME.md` explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes, grándes volúmens de datos y la cantidad de controles.


En particular, primero que nada los clientes se identifican mediante un uuid que se pasa entre las distintas estructuras para reconocer a los mismos.
Esto es posible porque existe un messagehandler por cliente en el gateway.
Cada sum y cada aggregator tienen un diccionario con el id de cliente como clave y como valor tienen los datos relevantes a cada fruta.

Los sums compiten por los mensajes que llegan del gateway, lo que supone un problema de concurrencia al llegar el EOF, si solo un sum escucha el EOF los demas no se enteran.
Este problema lo resolvi difundiendo el mensaje de EOF por medio del SUM_CONTROL_EXCHANGE.
Todos los nodos sum (incluido el mismo que envia el eof) estan bindeados con la misma routing key y cierran a ese cliente por el mismo camino.

Comentario de implementacion, tuve que iterar sobre el diseño inicial ya que escuchaba a una cola de datos en un thread y el exchange de control en otro, esto producia una condicion de carrera que no era controlable del todo sin un contador de id de mensaje, lo que termine descartando.
En la solucion final se consumen los 2 en un mismo canal y en un solo hilo ya que se procesan los mensajes de un canal en orden.

Cada fruta de un cliente va a un solo aggregator, utilizamos como "hash" el valor de cada caracter de 2 datos, tanto el id de cliente como el nombre de la fruta y haciendo el modulo entre la cantidad de aggregators. esto lo hacemos suponiendo que se va a distribuir de manera mas equitativa ya que el id de por si tiene la suficiente entropia y no va a quedar un aggregator con todas las frutas o con un subset especifico de clientes.

El aggregator necesita un "quorum" entre los sums, todos deben haber recibido el EOF y deben haber enviado su suma de frutas, una vez alcanzado el quorum los aggregator envian su top parcial al join, el cual espera que le llegue un mensaje de cada aggregator (otro quorum) y recien ahi genera el top total y lo devuelve al gateway
Es importante destacar que rabbit preserva el orden de los datos, entonces llegan primero todos los mensajes con fruit y amount antes del EOF desde el nodo sum y como no se alcanza quorum hasta que todos los nodos envien su EOF, no se pierden datos.


podemos decir que el sistema es escalable porque podemos crecer de manera horizontal, tan solo sumando mas aggregators o mas sums podemos crear un sistema que distribuya de manera mas "fair" la cantidad de mensajes, los sums compitiendo entre ellos y distribuyendo de manera equitativa entre los aggregators la suma parcial.
De la misma forma podemos soportar alto volumen de datos debido a que el aggregation filtra datos que no entran en el top "local" de cada aggregation. El join como mucho recibe un tamaño fijo de frutas por cada aggregation.
Los datos de los clientes solo se mantienen en cada nodo por el tiempo que estan siendo utilizados, al salir de un nodo, se eliminan los datos del cliente del mismo.
