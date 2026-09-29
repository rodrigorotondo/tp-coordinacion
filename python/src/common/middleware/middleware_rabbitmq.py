import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareCloseError,MessageMiddlewareDeleteError,MessageMiddlewareDisconnectedError,MessageMiddlewareMessageError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    
    def __init__(self, host, queue_name, channel=None):
        self.queue_name = queue_name
        try:
            if channel is None:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
                self.channel = self.connection.channel()
            else:
                self.channel = channel
                self.connection = channel.connection
            self.channel.queue_declare(queue=queue_name)
        except(pika.exceptions.AMQPConnectionError,pika.exceptions.ChannelClosedByBroker,pika.exceptions.ChannelWrongStateError) as error:
            raise MessageMiddlewareDisconnectedError(f"{error}")
        except Exception as error:
            raise MessageMiddlewareMessageError(f"{error}")
        

    def close(self):
        try:
            self.connection.close()
        except Exception as error:
            raise MessageMiddlewareCloseError(f"{error}")

    def send(self,message):
        try:
            self.channel.basic_publish(exchange='',routing_key=self.queue_name,body=message)
        except(pika.exceptions.AMQPConnectionError,pika.exceptions.ChannelClosedByBroker,pika.exceptions.ChannelWrongStateError) as error:
            raise MessageMiddlewareDisconnectedError(f"{error}")
        except Exception as error:
            raise MessageMiddlewareMessageError(f"{error}")

    #Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
    #cada mensaje de datos o de control con el cuerpo del mensaje.
    # on_message_callback tiene como parámetros:
    # message - El valor tal y como lo recibe el método send de esta clase.
    # ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
    # nack - Función que al invocarse realiza nack al mensaje que se está consumiendo. 
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def register_consumer(self, on_message_callback):
        def callback(ch, method, properties, body):
            delivery_tag = method.delivery_tag
            def ack():
                ch.basic_ack(delivery_tag)
            def nack():
                ch.basic_nack(delivery_tag, requeue=True)

            on_message_callback(body,ack,nack)
        try:
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback,auto_ack=False)
        except(pika.exceptions.AMQPConnectionError,pika.exceptions.ChannelClosedByBroker,pika.exceptions.ChannelWrongStateError) as error:
            raise MessageMiddlewareDisconnectedError(f"{error}")
        except Exception as error:
            raise MessageMiddlewareMessageError(f"{error}")

    def start_consuming(self, on_message_callback):
        self.register_consumer(on_message_callback)
        try:
            self.channel.start_consuming()
        except(pika.exceptions.AMQPConnectionError,pika.exceptions.ChannelClosedByBroker,pika.exceptions.ChannelWrongStateError) as error:
            raise MessageMiddlewareDisconnectedError(f"{error}")
        except(MessageMiddlewareMessageError,MessageMiddlewareDisconnectedError):
            raise
        except Exception as error:
            raise MessageMiddlewareMessageError(f"{error}")
        
    #Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
    #no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self):
        if self.channel is None or self.channel.is_closed:
            return
        try:
            self.channel.stop_consuming()
        except(pika.exceptions.AMQPConnectionError,pika.exceptions.ChannelClosedByBroker,pika.exceptions.ChannelWrongStateError) as error:
                    raise MessageMiddlewareDisconnectedError(f"{error}")

        

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys, channel=None):
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        try:
            if channel is None:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
                self.channel = self.connection.channel()
            else:
                self.channel = channel
                self.connection = channel.connection

            self.channel.exchange_declare(exchange=exchange_name, exchange_type="direct", durable=True)
            result = self.channel.queue_declare(queue='', exclusive=True)
            self.queue_name = result.method.queue
            for routing_key in routing_keys:
                self.channel.queue_bind(exchange=exchange_name,queue=self.queue_name,routing_key=routing_key)
        except(pika.exceptions.AMQPConnectionError,pika.exceptions.ChannelClosedByBroker,pika.exceptions.ChannelWrongStateError) as error:
            raise MessageMiddlewareDisconnectedError(f"{error}")
        except Exception as error:
            raise MessageMiddlewareMessageError(f"{error}")
        
        
    def register_consumer(self, on_message_callback):
        def callback(ch, method, properties, body):
            delivery_tag = method.delivery_tag
            def ack():
                ch.basic_ack(delivery_tag)
            def nack():
                ch.basic_nack(delivery_tag, requeue=True)

            on_message_callback(body,ack,nack)
        try:
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback,auto_ack=False)
        except(pika.exceptions.AMQPConnectionError,pika.exceptions.ChannelClosedByBroker,pika.exceptions.ChannelWrongStateError) as error:
            raise MessageMiddlewareDisconnectedError(f"{error}")
        except Exception as error:
            raise MessageMiddlewareMessageError(f"{error}")

    def start_consuming(self, on_message_callback):
        self.register_consumer(on_message_callback)
        try:
            self.channel.start_consuming()
        except(pika.exceptions.AMQPConnectionError,pika.exceptions.ChannelClosedByBroker,pika.exceptions.ChannelWrongStateError) as error:
            raise MessageMiddlewareDisconnectedError(f"{error}")
        except(MessageMiddlewareMessageError,MessageMiddlewareDisconnectedError):
            raise
        except Exception as error:
            raise MessageMiddlewareMessageError(f"{error}")

    def stop_consuming(self):
        if self.channel is None or self.channel.is_closed:
            return
        try:
            self.channel.stop_consuming()
        except(pika.exceptions.AMQPConnectionError,pika.exceptions.ChannelClosedByBroker,pika.exceptions.ChannelWrongStateError) as error:
                    raise MessageMiddlewareDisconnectedError(f"{error}")
            
    def send(self,message):
        for topic in self.routing_keys:
            try:
                self.channel.basic_publish(exchange=self.exchange_name,routing_key=topic,body=message)
            except(pika.exceptions.AMQPConnectionError,pika.exceptions.ChannelClosedByBroker,pika.exceptions.ChannelWrongStateError) as error:
                raise MessageMiddlewareDisconnectedError(f"{error}")
            except Exception as error:
                raise MessageMiddlewareMessageError(f"{error}")
    def close(self):
            try:
                self.connection.close()
            except Exception as error:
                raise MessageMiddlewareCloseError(f"{error}")