import socket

server = 'localhost'
port = 9999

try:
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.connect((server, port))
  s.send("Hello")
  s.close()
  print("SUCCESS")
except:
  print("FAILURE: Are you sure the front end server is running on (%s, %d)?" % (server, port))