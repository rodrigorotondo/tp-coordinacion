from common import message_protocol
from common.message_protocol.internal_message_enums import MsgType,MsgField
from enum import IntEnum, StrEnum
import uuid


class FruitTop(list):
    def __bool__(self):
        return True


class MessageHandler:

    def __init__(self):
        self.id = str(uuid.uuid4())
        self.message_id = 0
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        parsed_message = {MsgField.TYPE:MsgType.DATA, MsgField.CLIENT_ID:self.id,MsgField.MESSAGE_ID:self.next_message_id(),MsgField.DATA:(fruit,amount)}
        return message_protocol.internal.serialize(parsed_message)

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize({MsgField.TYPE:MsgType.EOF,MsgField.CLIENT_ID:self.id,MsgField.MESSAGE_ID:self.next_message_id()})

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if fields[MsgField.CLIENT_ID] != self.id:
            return None
        return FruitTop(fields[MsgField.DATA])

    def next_message_id(self):
        message_id = self.message_id
        self.message_id = self.message_id + 1
        return message_id
