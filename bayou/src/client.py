# Server class file

import sys, os
import subprocess
import time
from threading import Thread, Lock
from socket import SOCK_STREAM, socket, AF_INET, SOL_SOCKET, SO_REUSEADDR
from select import select

address = 'localhost'
baseport = 20000
server_baseport = 25000

class Client(Thread):
	def __init__(self, index, master_port):
		global baseport

		Thread.__init__(self)
		self.index   = index
		self.my_port = baseport + self.index

		self.my_sock = socket(AF_INET, SOCK_STREAM)
		self.my_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

		self.master = socket(AF_INET, SOCK_STREAM)
		self.master.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

		# Listen for connections
		self.my_sock.bind((address, self.my_port))
		self.my_sock.listen(10000)

		# Listen for master connection
		self.master.bind((address, master_port))
		self.master.listen(5)
		(self.master, _) = self.master.accept()

	def run(self):
		global sever_baseport, address

		self.comm_channels = [self.my_sock, self.master]

		while(1):
			(active, _, _) = select(self.comm_channels, [], [])

			for sock in active:
				
				if (sock == self.my_sock):
					(newsock, _) = self.my_sock.accept()
					self.comm_channels.append(newsock)
				else:
					# Are we communicating with master, clients, or other servers?
					try:
						line = sock.recv(1024)
					except:
						continue
					self.send(self.master, "Got here! " + str(self.index))
					if line == '':
						self.comm_channels.remove(sock)

					for data in line.split('\n'):
						if data == '':
							continue

						received = data.strip().split(' ')
						if (received[0] == "add"):
							# Just pass this request on to the server.
							connect_sock = socket(AF_INET, SOCK_STREAM)
							connect_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

							connect_sock.connect((address, server_baseport+int(received[3])))

							self.send(connect_sock, ' '.join(received[:2]))

							connect_sock.close()

						elif ((received[0] == "delete") or (received[0] == "get")):
							# Just pass this request on to the server.
							connect_sock = socket(AF_INET, SOCK_STREAM)
							connect_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

							connect_sock.connect((address, server_baseport+int(received[2])))

							self.send(connect_sock, ' '.join(received[:1]))

							connect_sock.close()

						else:
							self.send(self.master, "Invalid command " + str(self.index))

	def send(self, sock, s):
		sock.send(str(s) + '\n')
		
def main():
	global address

	# Read in command line arguments and start the different server parts.
	index = int(sys.argv[1])
	port = int(sys.argv[2])

	client = Client(index, port)

	# Start the acceptor, then leader, then replica.
	client.start()

	sys.exit(0)
	

if __name__ == '__main__':
	main()