import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item
from common.message_protocol.internal_message_enums import MsgField,MsgType

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_user = {}
        self.eof_count_by_user = {}

    def _process_data(self, fields):
        logging.info("Processing data message")
        client_id = fields[MsgField.CLIENT_ID]
        fruit, amount = fields[MsgField.DATA]

        fruit_top = self.fruit_top_by_user.setdefault(client_id, [])
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                updated = fruit_top.pop(i) + fruit_item.FruitItem(fruit, amount)
                bisect.insort(fruit_top, updated)
                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, fields):
        logging.info("Received EOF")
        client_id = fields[MsgField.CLIENT_ID]
        self.eof_count_by_user[client_id] = self.eof_count_by_user.get(client_id, 0) + 1
        if self.eof_count_by_user[client_id] < SUM_AMOUNT:
            return

        self.eof_count_by_user.pop(client_id)
        fruit_top = self.fruit_top_by_user.pop(client_id, [])
        fruit_chunk = list(fruit_top[-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(message_protocol.internal.serialize({MsgField.TYPE: MsgType.DATA, MsgField.CLIENT_ID: client_id, MsgField.DATA: fruit_top}))

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if fields[MsgField.TYPE] == MsgType.DATA:
            self._process_data(fields)
        else:
            self._process_eof(fields)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
