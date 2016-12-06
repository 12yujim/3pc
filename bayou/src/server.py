# Server class file

import sys, os, random
import subprocess
import time
import random
from threading import Thread, Lock
from socket import SOCK_STREAM, socket, AF_INET, SOL_SOCKET, SO_REUSEADDR
from select import select

address = 'localhost'
baseport = 23000
heartbeat_baseport = 26000

# Next LC value is max(LC+1, client's version). Also update the client's VC!

class Server(Thread):
	def __init__(self, index, master_port):
		global baseport, heartbeat_baseport

		Thread.__init__(self)
		self.index   = index
		self.my_port = baseport + self.index
		self.heartbeat_port = heartbeat_baseport + self.index

		self.server_socks   = []	# Current list of sockets we are connected to.
		self.known_servers  = []	# Current list of known server ids.
		self.VC = {}				# Vector clock for every server with the most recent accept-order, format (server_name, accept_num)
		self.tentative_log  = [] 	# Format is (accept_timestamp, name, write_info)
		self.commited_log   = []	# Format is (CSN, accept_timestamp, name, write_info)
		self.database = {}			# Holds (songName, URL) pairs determined by applying writes.
		self.VN = {}				# Holds (songName, version_num) pairs determined by applying writes.
		self.version_commit = {}	# Holds the latest version commited for every key in storage.

		self.LC  = 0				# Keeps track of our most recent accept-order.
		self.CSN = 0				# Keeps track of the current commit sequence number.
		self.primary = False
		self.retire = False
		self.name = ''

		if (self.index == 0):
			self.primary = True
			self.name = ("BD")
			self.VC[self.name] = self.LC
		else:
			self.VC["BD"] = 0



		self.my_sock = socket(AF_INET, SOCK_STREAM)
		self.my_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

		self.master = socket(AF_INET, SOCK_STREAM)
		self.master.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

		self.heartbeat = socket(AF_INET, SOCK_STREAM)
		self.heartbeat.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

		# Listen for connections
		self.my_sock.bind((address, self.my_port))
		self.my_sock.listen(10000)

		# Listen for heartbeat connection and spawn thread.
		timeout_thread = Thread(target = anti_entropy_heartbeat, args = [self.heartbeat_port])
		timeout_thread.daemon = True
		timeout_thread.start()

		self.heartbeat.bind((address, self.heartbeat_port))
		self.heartbeat.listen(5)
		(self.heartbeat, _) = self.heartbeat.accept()

		# Listen for master connection
		self.master.bind((address, master_port))
		self.master.listen(5)
		(self.master, _) = self.master.accept()


	def run(self):
		global baseport, address

		self.comm_channels = [self.my_sock, self.master, self.heartbeat]

		while(1):
			# timeout every 3 seconds for heartbeat
			(active, _, _) = select(self.comm_channels, [], [], 3)

			for sock in active:
				if (sock == self.my_sock):
					(newsock, port) = self.my_sock.accept()
					self.comm_channels.append(newsock)

					# Send a create message if we don't have a name.
					if (self.name == ''):
						print "Sending create " + str(self.index)

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

						# Remove this socket if its in our server list.
						for (i, s) in self.server_socks:
							if s is sock:
								print "removing sock id " + str(i) + ' ' + str(self.index)
								self.server_socks.remove((i,s))

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
							self.VC[self.name] = self.LC

							self.database[songName] = URL
							self.VN[songName] = self.LC
							self.tentative_log.append((self.LC, self.name, ' '.join(received[:3])))

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
							self.VC[self.name] = self.LC

							del self.database[songName]
							self.VN[songName] = self.LC
							self.tentative_log.append((self.LC, self.name, ' '.join(received[:2])))

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
							if songName in self.VN and VN != self.VN[songName]:
								response += '<' + songName + ':ERR_DEP>'
							else:
								if not songName in self.database:
									response += '<' + songName + ':ERR_KEY>'
								else:
									entry = self.database[songName]
									# If we don't have the most recent version of an entry, return ERR_DEP.
									
									response += '<' + songName + ':' + entry + '>'

							# Send the response back to the client.
							self.send(sock, response)
							self.comm_channels.remove(sock)
							sock.close()


						elif (received[0] == "createConn"):
							# Connect to all servers listed
							for i in received[1:]:
								connect_sock = socket(AF_INET, SOCK_STREAM)
								connect_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

								self.comm_channels.append(connect_sock)
								
								connect_sock.connect((address, baseport+int(i)))

								# Send a create message to the server if we don't have a name.
								if (int(i) in self.known_servers):
									self.server_socks.append((int(i), connect_sock))

									self.send(connect_sock, "create " + str(self.index))
								

						elif (received[0] == "breakConn"):
							# Close all connections listed
							for i in received[1:]:
								for (ID, sock1) in self.server_socks:
									if ID == int(i):
										print "Removing sock id " + str(ID) + ' ' + str(self.index)
										self.server_socks.remove((ID, sock1))
										self.comm_channels.remove(sock1)
										sock1.close()


						elif (received[0] == "create"):
							if (self.name == ''):
								# Record our new name and set our LC, also create an entry in our VC table for ID.
								# TODO: The responding server should actually bring us up to date with its logs.
								self.name = received[1]
								print received
								self.LC   = int(received[1][1:received[1].index(',')]) + 1
								self.VC[received[2]] = 0
								self.VC[self.name]   = self.LC

								self.server_socks.append((int(received[3]), sock))
								self.known_servers.append(int(received[3]))
								
								self.send(self.master, "Adding new name " + str(self.index) + ' ' + received[3])

							else:
								if (int(received[1]) in self.known_servers):
									print "Just appending create " + str(self.index)
									self.server_socks.append((int(received[1]), sock))

								else:
									# Add this entry to our log and respond with the new server's name.
									new_name = "<" + str(self.LC) + "," + self.name + ">"
									self.tentative_log.append((self.LC, self.name, "create " + new_name))

									# Initialize a VC entry for this server.
									self.VC[new_name] = self.LC
									self.LC += 1
									self.VC[self.name] = self.LC

									self.send(self.master, "Creating new name " + str(self.index))

									self.server_socks.append((int(received[1]), connect_sock))
									self.known_servers.append(int(received[1]))
									
									self.send(sock, "create " + new_name + ' ' + self.name + ' ' + str(self.index))

									# Try to commit if we are the primary.
									if (self.primary):
										self.commit_writes()


						elif (received[0] == "retire"):
							# set retirement to True to exit after next anti-entropy
							self.tentative_log.append((self.LC, self.name, 'retire ' + str(self.index)))
							self.retire = True

						elif (received[0] == "anti-entropy"):
							#Send to a random server in our list.
							try:
								pair = random.choice(self.server_socks)
								#print "Starting entropy from " + str(pair[0]) + ' to ' + self.name

								# send CSN and VC (flipped order for simplicity)
								startmsg = 'BEGIN ' + str(self.CSN) + ' ' + str(self.VC)

								self.send(pair[1], startmsg)
							except:
								pass

						elif (received[0] == 'BEGIN'):
							#self.send(self.master, "entering anti-entropy")
							self.anti_entropyS(sock, received[1:])

						elif (received[0] == "COMMIT"):
							# Could be data commit or already in our logs. Remove from tent and add to commit.
							w = (received[1], received[2], received[3])
							i = 0
							while i < len(self.commited_log):
								currW = self.commited_log[i]
								if currW[0] > w[0]:
									break
								i += 1
							self.commited_log.insert(i, w)
							# TODO: should this be incremented?
							self.CSN += 1

						elif (received[0] == "TENTATIVE"):
							w = eval(' '.join(received[1:]))
							print "received tentative " + repr(w) + ' ' + str(self.index)
							self.insert_tentative(w)

							# Reapply all writes in the new order.
							self.process_writes()

							# TODO: not sure about this
							# Update LC, VC and VN! for the specified server name
							self.VC[w[1]] = int(w[0])
							self.LC = max(int(w[0]), self.LC + 1)
							self.VC[self.name] = self.LC
							print str(self.VC) + ' ' + str(self.index)
							print str(self.tentative_log) + ' ' + str(self.index)
							# self.VN[songName] = VN[songName]


						elif (received[0] == "printLog"):
							out = 'log '

							# Record those stable writes in the commit log.
							for entry in self.commited_log:
								# Parse the write_info
								info = self.parse_info(entry[3])
								out += '<' + info[0] + ':(' + info[1] + '):TRUE>'
							# Record those tentative writes in the log.
							for entry in self.tentative_log:
								# Parse the write info
								info = self.parse_info(entry[2])
								out += '<' + info[0] + ':(' + info[1] + '):FALSE>'

							self.send(sock, out)

						else:
							self.send(self.master, "Invalid command " + str(self.index))

	def send(self, sock, s):
		sock.send(str(s) + '\n')


	def insert_tentative(self, new_entry):
		# Insert if empty
		if len(self.tentative_log) == 0:
			self.tentative_log.append(new_entry)
		else:	
			# Sort first by accept time then by server name.
			for entry, i in zip(self.tentative_log, range(len(self.tentative_log))):
				if (entry[0] > new_entry[0]):
					self.tentative_log.insert(i, new_entry)
					return

				elif (entry[0] == new_entry[0]):
					# Sort by server name
					if (self.server_name_comp(entry[1], new_entry[1]) < 0):
						self.tentative_log.insert(i, new_entry)
					elif (self.server_name_comp(entry[1], new_entry[1]) > 0):
						self.tentative_log.insert(i+1, new_entry)
					return

			# Insert at the end if we get here.
			self.tentative_log.append(new_entry)


	# Returns -1 if name1 < name 2, 1 if name1 > name2, 0 if equal
	def server_name_comp(self, name1, name2):
		comp1 = name1.strip('<').strip('>')
		comp2 = name2.strip('<').strip('>')

		# Split based on ',<'
		comp1 = comp1.split(',<')
		comp2 = comp2.split(',<')

		# Split the last element based on ,
		comp1 = comp1[:len(comp1)-1] + comp1[len(comp1)-1].split(',') 
		comp2 = comp2[:len(comp1)-1] + comp2[len(comp1)-1].split(',') 

		# Compare each element
		for elm1, elm2 in zip(comp1, comp2):
			if elm1 < elm2:
				return -1
			elif elm1 > elm2:
				return 1

		return 0


	# Commit writes, deletes, creates, and retirements. This function is run after each of these operations and after
	# anti-entropy. Writes may or may not be commited based on if we have the writes casually preceding them. (Primary only)
	def commit_writes(self):
		self.send(self.master, "Commiting writes... " + str(self.index))
		# Iterate through the tentative write log and commit anything without dependent writes elsewhere.
		for entry in self.tentative_log:
			# Commit every entry with a accept time lower than the lowest in VC.
			if self.VC.values() and (entry[0] <= min(self.VC.values())):
				self.tentative_log.remove(entry)
				self.commited_log.append((self.CSN, entry[0], entry[1], entry[2]))
				self.CSN += 1
				self.send(self.master, "Committed write " + str(self.CSN) + ' ' + str(entry[0]) + ' ' + entry[1] + ' ' + entry[2])



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
			rCSN = int(data[0])
			rV = eval(' '.join(data[1:]))
			# if self.OSN > rCSN:
			# 	# rollback DB to self.O
			# 	self.rollback()
			# 	self.send(sock, ' '.join([self.db, self.o, self.OSN])) # TODO: data transfer protocol (what should R expect to receive)
			# if rCSN < self.CSN:
			# 	unknownCommits = rCSN # we assume CSN points to most recent (see TODO below)
			# 	while unknownCommits < self.CSN:
			# 		w = self.commited_log[unknownCommits]
			# 		# TODO: should self.CSN point to most recent, or next spot (and therefore not indexed in writelog)
			# 		# TODO 2: depending on how writes are ordered our message to R can simply be w
			# 		wCSN = int(w[0])
			# 		wAcceptT = int(w[1])
			# 		wRepID = w[2]
			# 		if wRepID in rV and int(wAcceptT) <= rV[wRepID]:
			# 			# do we need to include R in the commit? 
			# 			self.send(sock, 'COMMIT ' + ' '.join([wCSN, wAcceptT, wRepID]))
			# 		else:
			# 			self.send(sock, w)
			# Send all tentative writes.
			for w in self.tentative_log:
				wAcceptT = int(w[0])
				wRepID = w[1]
				#print wRepID + ' ' + self.name + ' ' + str(rV)
				if rV[wRepID] < wAcceptT:
					print "Seding tentative " + repr(w) + ' ' + self.name + ' ' + str(self.index)
					self.send(sock, 'TENTATIVE ' + repr(w))
		if self.retire:
			sys.exit(0)


	# Apply all writes in the log to our database/VC logs.
	def process_writes(self):
		for _, _, write_info in self.tentative_log:
			m = write_info.split(' ')
			if (m[0] == "add"):
				self.database[m[1]] = m[2]
			elif (m[0] == "delete"):
				try:
					del self.database[m1]
				except:
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


def anti_entropy_heartbeat(port):
	# Create a connection between this thread and the server.
	time.sleep(.1)
	connection = socket(AF_INET, SOCK_STREAM)
	connection.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

	connection.connect((address, port))

	while(True):
		# Sleep for .05 second intervals and send a initiate message to all servers.
		time.sleep(1)
		connection.send("anti-entropy\n")


def main():
	global address

	# Read in command line arguments and start the different server parts.
	index = int(sys.argv[1])
	port = int(sys.argv[2])

	server = Server(index, port)

	# Start the server.
	server.start()

	sys.exit(0)
	

if __name__ == '__main__':
	main()