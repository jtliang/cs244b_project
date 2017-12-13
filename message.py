import struct
from binascii import hexlify

class Message:
  # Message type enums
  GET = 0
  PUT = 1
  DELETE = 2
  REPLICATE = 3
  ACK = 4
  SUCCESS = 5
  FAILURE = 6

  MAX_SIZE = 2048
  # size of key in bytes
  SIZEOF_KEY = 16

  def __init__(self, length = None, type=None, ip=None, _id=None, key_length=None, key=None, value=None):
    self.length = None
    self.type = type
    self.ip = ip
    self.id = _id
    self.key_length = key_length
    self.key_string = key
    self.value = value

  @staticmethod
  def from_data(data, user_readable_key=False):
    message = Message()
    data_as_bytes = list(data)
    message.length = struct.unpack_from("=I", data, 0)[0]
    message.type, message.ip, message.id, message.key_length = struct.unpack_from("=iiii", data, 4)
    start_of_key = struct.calcsize("=Iiiii")
    message.key_string = struct.unpack_from("%ds" % message.key_length, data, start_of_key)[0]
    if not user_readable_key:
      message.key_num = int(message.key_string, 16)
    value_length = message.length - start_of_key - (message.key_length)
    if value_length > 0:
      message.value = struct.unpack_from(("%ds" % value_length), data, start_of_key + (message.key_length))[0]
    return message

  def hash_key(self, hash_ring):
    if self.key_string:
      self.key_string = hexlify(bytearray(hash_ring.gen_digest(self.key_string)))
      self.key_length = len(self.key_string)
      self.key_num = int(self.key_string, 16)

  def to_buff(self):
    start_of_key = struct.calcsize("=Iiiii")
    total_length = start_of_key + self.key_length
    self.length = total_length
    if self.value is not None and len(self.value) > 0:
      self.length += len(self.value)
      return struct.pack("=Iiiii%ds%ds" % (self.key_length, len(self.value)), self.length, self.type, self.ip, self.id, self.key_length, self.key_string, self.value)
    else:
      return struct.pack("=Iiiii%ds" % self.key_length, self.length, self.type, self.ip, self.id, self.key_length, self.key_string)
