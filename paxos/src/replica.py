# File containing the replica class

import sys, os
import subprocess
import time
from threading import Thread, Lock
from socket import SOCK_STREAM, socket, AF_INET, SOL_SOCKET, SO_REUSEADDR
from select import select

address = 'localhost'
baseport = 20000
n = 0

class Replica(Thread):
	def __init__(self, index, address, port):
		global n, baseport

		Thread.__init__(self)
		self.index = index
		self.master_port = port
		self.my_port = baseport + self.index*3
		
		self.master = socket(AF_INET, SOCK_STREAM)
		self.master.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

		self.leader = socket(AF_INET, SOCK_STREAM)
		self.leader.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

		self.my_sock = socket(AF_INET, SOCK_STREAM)
		self.my_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

	def run(self):
		global n, address

		# Connect to the leader socket.
		self.leader.connect((address, baseport+3*self.index+2))

		# Listen for master connection
		self.my_sock.bind((address, self.my_port))
		self.my_sock.listen(n)

		print("listening for master")

		# Listen for master connection
		self.master.bind((address, self.master_port))
		self.master.listen(5)
		(self.master, _) = self.master.accept()

		self.comm_channels = [self.master, self.leader, self.my_sock]

		self.send(self.master, "Accepted master connection " + str(self.index))

		while(1):
			(active, _, _) = select(self.comm_channels, [], [])

			for sock in active:
				
				if (sock == self.my_sock):
					(newsock, _) = self.my_sock.accept()
					self.comm_channels.append(newsock)
				else:
					# Are we communicating with master, coord, or other servers?
					line = sock.recv(1024)
					#self.send(self.master, "Got here! " + str(self.index))
					if not line:
						self.comm_channels.remove(sock)

					for data in line.split('\n'):
						if data == '':
							continue
						
						#self.send(self.master, str(self.index) + ' received from master')
						#self.handle_master_comm(sock, data)

	def propose(self):
		# Propose a clients message to the next available slot.
		pass

	def perform(self):
		# Basically just send a repsonse back to client
		pass

	def send(self, sock, s):
		sock.send(str(s) + '\n')

