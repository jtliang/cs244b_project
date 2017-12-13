from message import Message
import struct
from binascii import hexlify, unhexlify

class FAWNDatastore:
  LENGTH_NUM_BYTES = 4

  def __init__(self, filename, mode):
    self.filename = filename
    self.mode = mode
    self.fd = open(filename, mode)

  def seek(self, offset):
    self.fd.seek(offset)

  def read_key(self):
    return int(self.read_key_as_string(), 16)

  def read_key_as_string(self):
    return self.fd.read(2 * Message.SIZEOF_KEY)

  def read_length(self):
    length = struct.unpack("i", self.fd.read(FAWNDatastore.LENGTH_NUM_BYTES))[0]
    return length

  def read_value(self, length):
    return str(self.fd.read(length))

  def append(self, key, value):
    assert(self.mode == 'a' or self.mode == 'a+')
    length = len(value)
    buff_to_write = struct.pack("%dsI%ds" % (2 * Message.SIZEOF_KEY, length), key, length, value)
    self.fd.write(buff_to_write)
    self.fd.flush()

  # Given previous EOF, read till we found the newly written key that matches
  def find_key_offset(self, start_offset, target_key_string):
    curr_offset = start_offset
    self.seek(curr_offset)
    while True:
      curr_key = self.read_key_as_string()
      # Key's not found. (Shouldn't happen since we call this right after append)
      if not curr_key:
        assert(False)
      if curr_key != target_key_string:
        curr_length = self.read_length()
        curr_offset += 2 * Message.SIZEOF_KEY + FAWNDatastore.LENGTH_NUM_BYTES + curr_length
        self.seek(curr_offset)
      else:
        return curr_offset
    assert(False)

  def close(self):
    self.fd.close()
