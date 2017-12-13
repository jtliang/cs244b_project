import threading
import binascii
import struct
from message import Message

index_bits = 16

INDEX_FLAG = (2**index_bits - 1)
KEY_FRAG_FLAG = 0xefff

def get_index_bits(key):
  return key & INDEX_FLAG

def get_key_frag(key):
  return (key >> index_bits) & KEY_FRAG_FLAG

def is_valid(key_frag):
  return key_frag & 0x1

def mark_valid(key_frag):
  return (key_frag << 1) | 0x1

class FAWNTableEntry:
  def __init__(self, key_frag=None, offset=None):
    self.key_frag = key_frag
    self.offset = offset

class FAWNTable:
  def __init__(self):
    self.table = [[] for i in range(2**index_bits)]
    self.lock = threading.Lock()

  def insert(self, ro_datastore, key, new_offset=None):
    self.lock.acquire()
    index = get_index_bits(key)
    key_frag = mark_valid(get_key_frag(key))
    chain = self.table[index]
    found = None
    first_invalid = None
    for entry in chain:
      if is_valid(entry.key_frag):
        if entry.key_frag == key_frag:
          # Actual entry with correct key frag and valid bit
          entry_offset = entry.offset
          ro_datastore.seek(entry_offset)
          on_disk_key = ro_datastore.read_key()
          if on_disk_key == key:
            # Read from file and serve bytes after releasing lock on table
            if new_offset is None:
              self.lock.release()
              on_disk_length = ro_datastore.read_length()
              return ro_datastore.read_value(on_disk_length)
            else:
              found = entry
              break
      else:
        # Found the next available invalid entry so just use this
        first_invalid = entry
    # Couldn't find entry with keyfrag, so just hijack first invalid entry
    if found is None:
      found = first_invalid
    if new_offset is not None:
      if found is None:
        found = FAWNTableEntry(key_frag, new_offset)
        chain.append(found)
      else:
        found.key_frag = key_frag
        found.offset = new_offset
    else:
      # Is a read call. But if got to here, means no entry was found
      found = None
    self.lock.release()
    return found

  def find(self, log_fd, key):
    return self.insert(log_fd, key)