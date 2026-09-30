import os
import logging
import signal

from common import middleware, message_protocol, fruit_item
from common.message_protocol.internal_message_enums import MsgField,MsgType

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


def text_to_number(text):
    total = 0
    for char in text:
        total += ord(char)
    return total


def choose_aggregator(client_id, fruit):
    number = text_to_number(client_id) + text_to_number(fruit)
    return number % AGGREGATION_AMOUNT


class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        self.control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST,SUM_CONTROL_EXCHANGE,[f"{SUM_PREFIX}_CONTROL"],channel=self.input_queue.channel)
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_fruit_by_user = {}
        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.input_queue.stop_consuming()

    def _process_data(self, fields):
        logging.info(f"Process data")
        client_id = fields[MsgField.CLIENT_ID]
        fruit, amount = fields[MsgField.DATA]

        amount_by_fruit = self.amount_by_fruit_by_user.setdefault(client_id, {})
        amount_by_fruit[fruit] = amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

  
    def _process_eof(self,fields):
        logging.info(f"Broadcasting EOF notice to sum instances")
        self.control_exchange.send(message_protocol.internal.serialize({MsgField.TYPE:MsgType.EOF,MsgField.CLIENT_ID:fields[MsgField.CLIENT_ID]}))

    def _process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if fields[MsgField.TYPE] == MsgType.EOF:
            client_id = fields[MsgField.CLIENT_ID]
            amount_by_fruit = self.amount_by_fruit_by_user.pop(client_id, {})

            logging.info(f"Sending data messages")
            for final_fruit_item in amount_by_fruit.values():
                aggregator = choose_aggregator(client_id, final_fruit_item.fruit)
                self.data_output_exchanges[aggregator].send(
                    message_protocol.internal.serialize(
                        {MsgField.TYPE:MsgType.DATA, MsgField.CLIENT_ID:client_id,MsgField.DATA:(final_fruit_item.fruit,final_fruit_item.amount)}
                    )
                )

            logging.info(f"Broadcasting EOF message")
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(message_protocol.internal.serialize({MsgField.TYPE:MsgType.EOF,MsgField.CLIENT_ID:client_id}))
        ack()

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if fields[MsgField.TYPE] == MsgType.DATA:
            self._process_data(fields)
        else:
            self._process_eof(fields)
        ack()

    def start(self):
        self.control_exchange.register_consumer(self._process_control_message)
        self.input_queue.start_consuming(self.process_data_messsage)
        self.input_queue.close()
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.close()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
