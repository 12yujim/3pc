# Server class file

import sys, os
import subprocess
import time
from threading import Thread, Lock
from socket import SOCK_STREAM, socket, AF_INET, SOL_SOCKET, SO_REUSEADDR
from select import select

address = 'localhost'
baseport = 25000

class Server(Thread):
	def __init__(self, index, master_port):
		global baseport

		Thread.__init__(self)
		self.index   = index
		self.my_port = baseport + self.index

		self.server_socks = []

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
		global baseport, address

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
					
					if line == '':
						self.comm_channels.remove(sock)

					for data in line.split('\n'):
						if data == '':
							continue

						received = data.strip().split(' ')
						if (received[0] == "add"):
							self.send(self.master, "Got add command " + str(self.index))
								
						elif (received[0] == "delete"):
							self.send(self.master, "Got delete command " + str(self.index))

						elif (received[0] == "get"):
							self.send(self.master, "Got get command " + str(self.index))

						elif (received[0] == "createConn"):
							# Connect to all servers listed
							for i in received[1:]:
								connect_sock = socket(AF_INET, SOCK_STREAM)
								connect_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
								
								connect_sock.connect((address, baseport+int(i)))
								self.server_socks.append((int(i), connect_sock))

								# Add a created entry in our log.

						elif (received[0] == "breakConn"):
							# Close all connections listed
							for i in received[1:]:
								for (ID, sock) in self.server_socks:
									if ID == int(i):
										self.server_socks.remove((ID, sock))
										sock.close()


						elif (received[0] == "retire"):
							pass

						elif (received[0] == "printLog"):
							pass


						else:
							self.send(self.master, "Invalid command " + str(self.index))

	def send(self, sock, s):
		sock.send(str(s) + '\n')

	# anti-entropy protocol for S to R
	# after sending initiate message, compare logs and send updates
	# should be run similar to a heartbeat function
	# TODO: interruptions during anti-entropy? can create anti-entropy receive function that ignores all commands outside anti-entropy
	def anti_entropyS(self, sock, data=None):
		if not data:
			# initiate anti-entropy
			self.send(sock, 'anti-entropy')
		else:
			# have received response from R
			rV, rCSN = data  # TODO: figure out how data is transferred, assume works for now
			if self.OSN > rCSN:
				# rollback DB to self.O
				self.rollback()
				self.send(sock, ' '.join([self.db, self.o, self.OSN]) + '\n') # TODO: data transfer protocol (what should R expect to receive)
			if rCSN < self.CSN:
				unknownCommits = rCSN + 1 # we assume CSN points to most recent (see TODO below)
				while unknownCommits < self.CSN:
					w = self.writelog[unknownCommits]
					# TODO: should self.CSN point to most recent, or next spot (and therefore not indexed in writelog)
					# TODO 2: depending on how writes are ordered our message to R can simply be w
					wCSN = w[0]
					wAcceptT = w[1]
					wRepID = w[2]
					if int(wAcceptT) <= rV[wRepID]:
						# do we need to include R in the commit? 
						self.send(sock, 'COMMIT ' + ' '.join([wCSN, wAcceptT, wRepID]) + '\n')
					else:
						self.send(sock, w + '\n')
					unknownCommits += 1
				tentative = unknownCommits
				while tentative < len(self.writelog):
					w = self.writelog[tentative]
					wAcceptT = w[1]
					wRepID = w[2]
					if rV[wRepID] < wAcceptT:
						self.send(sock, w + '\n')
					tentative += 1

def main():
	global address

	# Read in command line arguments and start the different server parts.
	index = int(sys.argv[1])
	port = int(sys.argv[2])

	server = Server(index, port)

	# Start the acceptor, then leader, then replica.
	server.start()

	sys.exit(0)
	

if __name__ == '__main__':
	main()