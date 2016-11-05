# File containing the acceptor class

import sys, os
import subprocess
import time
from threading import Thread, Lock
from socket import SOCK_STREAM, socket, AF_INET, SOL_SOCKET, SO_REUSEADDR
from select import select

address = 'localhost'
baseport = 20000
n = 0

class Acceptor(Thread):
	def __init__(self, index, address):
		global n, baseport

		Thread.__init__(self)
		self.index = index
		self.my_port = baseport + self.index*3 + 2

		self.my_sock = socket(AF_INET, SOCK_STREAM)
		self.my_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

	def run(self):
		global n, address

		# Listen for connections from other servers.
		self.my_sock.bind((address, self.my_port))
		self.my_sock.listen(n)

		self.comm_channels = [self.my_sock]

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

	def p1a(self):
		# Send vote for ballot number proposed by leader, if it is highest ballot # received.
		pass

	def p2a(self):
		# Decide on this ballot number for the slot, send back an ack to leader.
		pass

	def send(self, sock, s):
		sock.send(str(s) + '\n')