from enum import IntEnum,StrEnum
class MsgType(IntEnum):
    DATA = 0
    EOF = 1

class MsgField(StrEnum):
    TYPE = "type"
    CLIENT_ID = "client_id"
    MESSAGE_ID = "message_id"
    DATA = "data"