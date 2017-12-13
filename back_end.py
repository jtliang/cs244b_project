import hash_ring
import SocketServer
from message import Message
import socket
import threading
from fawn_hashtable import FAWNTable, FAWNTableEntry
import io
import os
import sys
from fawn_datastore import FAWNDatastore
from subprocess import check_output

ip = check_output(['hostname', '--all-ip-addresses'])

def get_ip_port(ip_string):
  return ip_string.split(",")

class BackEndServer:
  def __init__(self):
    self.fawn_table = FAWNTable()
    self.front_server = ('10.138.0.2', 9999)
    with open('next_server.txt', 'r') as next_server_file:
      self.next_server = next_server_file.readline().strip().split(':')
      self.next_server = (self.next_server[0], int(self.next_server[1]))
    self.ip = ip
    self.version = 0
    # Create file
    open("file" + str(self.version), "w+").close()
    self.write_datastore = FAWNDatastore("file" + str(self.version), 'a')

def send_message_to_front_server(message, state):
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.connect(state.front_server)
  s.send(message.to_buff())
  s.close()
  
def send_message_to_next_server(message, state):
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.connect(state.next_server)
  s.send(message.to_buff())
  s.close()
  
class BackEndTCPHandler(SocketServer.BaseRequestHandler):
  """
  """

  def handle(self):
    request_data = self.request.recv(Message.MAX_SIZE)
    message = Message.from_data(request_data)

    recent_filename = "file" + str(back_state.version)
    read_datastore = FAWNDatastore(recent_filename, 'r')

    if message.type == Message.PUT or message.type == Message.REPLICATE:
      end_of_file_pos = os.path.getsize(recent_filename)
      back_state.write_datastore.append(message.key_string, message.value)
      new_offset = read_datastore.find_key_offset(end_of_file_pos, message.key_string)
      back_state.fawn_table.insert(read_datastore, message.key_num, new_offset)
      if message.type == Message.PUT:
        # Send message to next server to replicate. We don't do anything else here
        replicate_message = Message(None, message.REPLICATE, message.ip, message.id, 2 * Message.SIZEOF_KEY, message.key_string, message.value)
        send_message_to_next_server(replicate_message, back_state)
      else:
        # Send reply to front-end server so that they can forward to client
        reply_message = Message(None, message.SUCCESS, message.ip, message.id, 2 * Message.SIZEOF_KEY, message.key_string)
        send_message_to_front_server(reply_message, back_state)

    elif message.type == Message.GET:
      data = back_state.fawn_table.find(read_datastore, message.key_num)
      reply_message = Message(None, message.SUCCESS, message.ip, \
                              message.id, 2 * Message.SIZEOF_KEY, message.key_string, data)
      send_message_to_front_server(reply_message, back_state)

    read_datastore.close()

if __name__ == "__main__":
  back_state = BackEndServer()
  HOST, PORT = ip, 9999

  # Create the server, binding to localhost on port 9999
  server = SocketServer.ThreadingTCPServer((HOST, PORT), BackEndTCPHandler)

  # Activate the server; this will keep running until you
  # interrupt the program with Ctrl-C
  server.serve_forever()