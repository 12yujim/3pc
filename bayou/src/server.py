# Server class file

import sys, os
import subprocess
import time
from threading import Thread, Lock
from socket import SOCK_STREAM, socket, AF_INET, SOL_SOCKET, SO_REUSEADDR
from select import select

address = 'localhost'
baseport = 25000

# Next LC value is max(LC+1, client's version). Also update the client's VC!

class Server(Thread):
	def __init__(self, index, master_port):
		global baseport

		Thread.__init__(self)
		self.index   = index
		self.my_port = baseport + self.index

		self.server_socks   = []	# Current list of sockets we are connected to.
		self.VC = {}				# Vector clock for every server with the most recent accept-order, format (server_name, accept_num)
		self.tentative_log  = [] 	# Format is (accept_timestamp, write_info)
		self.commited_log   = []	# Format is (CSN, accept_timestamp, write_info)
		self.database = {}			# Holds (songName, (version_num, URL)) pairs determined by applying writes.
		self.version_commit = {}	# Holds the latest version commited for every key in storage.

		self.LC  = 0				# Keeps track of our most recent accept-order.
		self.CSN = 0				# Keeps track of the current commit sequence number.
		self.primary = False
		self.name = ''

		if (self.index == 0):
			self.primary = True
			self.name = ("BD")


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

					# Send a create message if we don't have a name.
					if (self.name == ''):
						self.server_socks.append(newsock)
						self.send(newsock, "create " + str(self.index))

				else:
					# Are we communicating with master, clients, or other servers?
					try:
						line = sock.recv(1024)
					except:
						continue
					
					if line == '':
						self.send(self.master, "Socket closed " + str(self.index))
						self.comm_channels.remove(sock)

					for data in line.split('\n'):
						if data == '':
							continue

						received = data.strip().split(' ')
						if (received[0] == "add"):
							self.send(self.master, "Got add command " + ' '.join(received))
							songName = received[1]
							URL = received[2]
							VN  = int(received[3])

							# Apply the add/modify to our database and write it to the log tentatively.
							self.LC = max(self.LC + 1, VN)

							self.database[songName] = (self.LC, URL)
							self.tentative_log.append((self.LC, ' '.join(received[:3]) + ' ' + self.name))

							# Send the updated VN if we used our LC
							if self.LC != VN:
								self.send(sock, 'VNupdate ' + songName + ' ' + str(self.LC))
								self.comm_channels.remove(sock)
								sock.close()

							# Try to commit if we are the primary.
							if (self.primary):
								self.commit_writes()


						elif (received[0] == "delete"):
							self.send(self.master, "Got delete command " + str(self.index))
							songName = received[1]
							VN = int(received[2])

							# Apply the add/modify to our database and write it to the log tentatively.
							self.LC = max(self.LC + 1, VN)

							self.database[songName] = (self.LC, URL)
							self.tentative_log.append((self.LC, ' '.join(received[:2]) + ' ' + self.name))

							# Send the updated VN if we used our LC
							if self.LC != VN:
								self.send(sock, 'VNupdate ' + songName + ' ' + str(self.LC))
								self.comm_channels.remove(sock)
								sock.close()

							# Try to commit if we are the primary.
							if (self.primary):
								self.commit_writes()


						elif (received[0] == "get"):
							self.send(self.master, "Got get command " + str(self.index))
							songName = received[1]
							VN = int(received[2])
							response = 'getResp '

							# If we don't have the key logged, return ERR_KEY
							if not songName in self.database:
								response += '<' + songName + ':ERR_KEY>'
							else:
								entry = self.database[songName]
								# If we don't have the most recent version of an entry, return ERR_DEP.
								if VN != entry[0]:
									response += '<' + songName + ':ERR_DEP>'
								else:
									response += '<' + songName + ':' + entry[1] + '>'

							# Send the response back to the client.
							self.send(sock, response)
							self.comm_channels.remove(sock)
							sock.close()


						elif (received[0] == "createConn"):
							# Connect to all servers listed
							for i in received[1:]:
								connect_sock = socket(AF_INET, SOCK_STREAM)
								connect_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

								self.server_socks.append((int(i), connect_sock))
								self.comm_channels.append(connect_sock)
								
								connect_sock.connect((address, baseport+int(i)))

								# Send a create message to the server if we don't have a name.
								if (self.name == None):
									self.send(connect_sock, "create " + str(self.index))
								

						elif (received[0] == "breakConn"):
							# Close all connections listed
							for i in received[1:]:
								for (ID, sock1) in self.server_socks:
									if ID == int(i):
										self.server_socks.remove((ID, sock1))
										self.comm_channels.remove(sock1)
										sock1.close()


						elif (received[0] == "create"):
							if (self.name == ''):
								# Record our new name and set our LC, also create an entry in our VC table for ID.
								# TODO: The responding server should actually bring us up to date with its logs.
								self.name = received[1]
								self.LC   = int(received[1][1:received[1].index(',')]) + 1
								self.VC[received[2]] = self.LC - 1
								self.send(self.master, "Adding new name " + str(self.index))

								# For now just add the write to our tentative log (server should update us)
								self.tentative_log.append((self.LC - 1, ' '.join(received[:2])))
							else:
								# Add this entry to our log and respond with the new server's name.
								new_name = "<" + str(self.LC) + "," + self.name + ">"
								self.tentative_log.append((self.LC, "create " + new_name))

								# Initialize a VC entry for this server.
								self.VC[new_name] = self.LC
								self.LC += 1

								self.send(self.master, "Creating new name " + str(self.index))

								self.send(sock, "create " + new_name + ' ' + self.name)

								# Try to commit if we are the primary.
								if (self.primary):
									self.commit_writes()


						elif (received[0] == "retire"):
							pass

						elif (received[0] == "printLog"):
							out = 'log '

							# Record those stable writes in the commit log.
							for entry in self.commited_log:
								# Parse the write_info
								info = self.parse_info(entry[2])
								out += '<' + info[0] + ':(' + info[1] + '):TRUE>'
							# Record those tentative writes in the log.
							for entry in self.tentative_log:
								# Parse the write info
								info = self.parse_info(entry[1])
								out += '<' + info[0] + ':(' + info[1] + '):FALSE>'

							self.send(sock, out)

						else:
							self.send(self.master, "Invalid command " + str(self.index))

	def send(self, sock, s):
		sock.send(str(s) + '\n')


	# Commit writes, deletes, creates, and retirements. This function is run after each of these operations and after
	# anti-entropy. Writes may or may not be commited based on if we have the writes casually preceding them. (Primary only)
	def commit_writes(self):
		self.send(self.master, "Commiting writes... " + str(self.index))
		# Iterate through the tentative write log and commit anything without dependent writes elsewhere.
		for entry in self.tentative_log:
			# Commit every entry with a accept time lower than the lowest in VC.
			if (entry[0] <= min(self.VC.values())):
				self.tentative_log.remove(entry)
				self.commited_log.append((self.CSN, entry[0], entry[1]))
				self.CSN += 1
				self.send(self.master, "Committed write " + str(self.CSN) + ' ' + str(entry[0]) + ' ' + entry[1])


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

	# Apply all writes in the log to our database/VC logs.
	def process_writes(self):
		pass

	def parse_info(self, s):
		m = s.split(' ')
		if (m[0] == "add"):
			return ["PUT", m[1] + ',' + m[2]]
		elif (m[0] == "delete"):
			return ["DELETE", m[1]]
		elif (m[0] == "create"):
			return ["CREATE", m[1]]
		elif (m[0] == "retire"):
			return ["RETIRE", m[1]]

		

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