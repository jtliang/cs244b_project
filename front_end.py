from message import Message
import hash_ring
import SocketServer
import socket
from binascii import hexlify
import threading
import math
import time

from subprocess import check_output

ip = check_output(['hostname', '--all-ip-addresses'])

# Hardcoded list of storge servers
storage_servers = ['10.138.0.3:9999', '10.138.0.4:9999', '10.138.0.5:9999',
                   '10.138.0.6:9999', '10.138.0.7:9999', '10.138.0.8:9999',
                   '10.138.0.9:9999']

intervals = (2**32 / len(storage_servers))

def get_ip_port(ip_string):
  return ip_string.split(",")

def get_node(hash_ring, key_string, offset=0):
  key = hash_ring.gen_key(key_string)
  index = math.ceil(int(key / intervals))
  if index > 7:
    index = 0
  return storage_servers[(int(index) + offset) % len(storage_servers)]

class FrontEndServer:
  def __init__(self):
    self.storage_ring = hash_ring.HashRing(storage_servers)
    self.max_sessions = 300
    self.sessions = [[None, threading.Condition()] for i in range(self.max_sessions)]
    self.id = 0
    self.sessions_lock = threading.Lock()

class FrontEndTCPHandler(SocketServer.BaseRequestHandler):
  """
  """

  def handle(self):
    # self.request is the TCP socket connected to the client
    self.data = self.request.recv(Message.MAX_SIZE)
    message = Message.from_data(self.data, True)
    dest_ip, dest_port = None, None

    # From client
    if message.type == Message.PUT or message.type == Message.GET or message.type == Message.DELETE:
      server_state.sessions_lock.acquire()
      session_id = server_state.id
      server_state.id += 1
      condition = server_state.sessions[session_id % server_state.max_sessions][1]
      server_state.sessions_lock.release()

      original_key_string = message.key_string
      message.hash_key(server_state.storage_ring)
      message.id = session_id

      if message.type == Message.PUT or message.type == Message.DELETE:
        dest_ip, dest_port = get_node(server_state.storage_ring, original_key_string).split(":")
      elif message.type == Message.GET:
        dest_ip, dest_port = get_node(server_state.storage_ring, original_key_string, 1).split(":")
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      s.connect((dest_ip, int(dest_port)))
      s.send(message.to_buff())
      s.close()

      #Response came back so send reply to client
      condition.acquire()
      condition.wait()
      reply_message = server_state.sessions[session_id % server_state.max_sessions][0]
      condition.release()
      try:
        self.request.sendall(reply_message)
      finally:
        self.request.close()
    # From other servers, so forward back to client
    else:
      server_state.sessions_lock.acquire()
      condition = server_state.sessions[message.id % server_state.max_sessions][1]
      server_state.sessions_lock.release()
      condition.acquire()
      server_state.sessions[message.id % server_state.max_sessions][0] = message.to_buff()
      condition.notify()
      condition.release()
    
if __name__ == "__main__":
  server_state = FrontEndServer()

  HOST, PORT = ip, 9999

  # Create the server, binding to localhost on port 9999
  server = SocketServer.ThreadingTCPServer((HOST, PORT), FrontEndTCPHandler)

  # Activate the server; this will keep running until you
  # interrupt the program with Ctrl-C
  server.serve_forever()