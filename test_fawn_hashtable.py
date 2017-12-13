from fawn_hashtable import FAWNTable, FAWNTableEntry
from binascii import hexlify
import struct

index_bits = 16
INDEX_FLAG = (2**index_bits - 1)
KEY_FRAG_FLAG = 0xefff

def get_index_bits(key):
  return key & INDEX_FLAG
def get_key_frag(key):
  return (key >> index_bits) & KEY_FRAG_FLAG
def mark_valid(key_frag):
  return (key_frag << 1) | 0x1

def print_as_hex(bytestring):
  print binascii.hexlify(bytearray(bytestring))

new_fd = open("file0", "w+")
append_fd = open("file0", "a+")
read_fd = open("file0", "r")

fawnTable = FAWNTable()
key = 0x99887766554433221199887766554433
key_index = get_index_bits(key)
key_frag = mark_valid(get_key_frag(key))

append_fd.write(format(key, '032X'))
append_fd.write(format(len("Jintian"), '04X'))
append_fd.write("Jintian")
append_fd.flush()
inserted = fawnTable.insert(append_fd, key, "Jintian", 0)
assert(inserted.key_frag == key_frag)
assert(inserted.offset == 0)

found = fawnTable.find(read_fd, key)
assert(found == "Jintian")

bogus_key = 0x99887766554433221199887766554432
bogus_found = fawnTable.find(read_fd, bogus_key)
assert(bogus_found == None)

bogus_key = 0x89887766554433221199887766554433
bogus_found = fawnTable.find(read_fd, bogus_key)
assert(bogus_found == None)

print("SUCCESS")