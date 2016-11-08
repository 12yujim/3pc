# File containing the leader class

import sys, os
import subprocess
import time
from threading import Thread, Lock
from socket import SOCK_STREAM, socket, AF_INET, SOL_SOCKET, SO_REUSEADDR
from select import select
from operator import itemgetter
from ast import literal_eval
from replica import Replica
from acceptor import Acceptor

address = 'localhost'
baseport = 20000
n = 3

class Leader(Thread):
	def __init__(self, index, address):
		global n, baseport

		Thread.__init__(self)
		self.index   = index
		self.my_port = baseport + self.index*3 + 1

		self.my_sock = socket(AF_INET, SOCK_STREAM)
		self.my_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

		self.proposals 	= {}
		self.active 	= False
		self.ballot_num = (0, self.index)

	def run(self):
		global n, address

		# Listen for connections
		self.my_sock.bind((address, self.my_port))
		self.my_sock.listen(3*n)

		self.comm_channels = [self.my_sock]

		# Spawn a scout
		scout = Scout(self.index, n, self.ballot_num)
		scout.start()
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

						received = data.strip().split(' ')
						if (received[0] == "propose"):
							print(received)
							print(int(received[1]))
							print(not (int(received[1]) in self.proposals.keys()))
							# If we already have a mapping for this slot, ignore.
							if not (int(received[1]) in self.proposals.keys()):
								self.proposals[int(received[1])] = (int(received[1]), self.tup(received[2:]))
								print(self.proposals)

								# If a majority have adopted this ballot number, send out decision messages.
								if self.active:
									# Spawn Commander.
									print(str((self.ballot_num, int(received[1]), self.tup(received[2:]))))
									commander = Commander(self.index, n, (self.ballot_num, int(received[1]), self.tup(received[2:])))
									commander.start()
								
						elif (received[0] == "adopted"):
							print(received)
							# Find the max ballot pval for each slot
							updated_proposals = {}
							print received[3:]
							recv_pvals = self.format_pvals(received[3:])
							print(recv_pvals)
							for pval in recv_pvals:
								updated_proposals[pval[1]] = 0
							for slot in updated_proposals.keys():
								slot_maps = [pval for pval in recv_pvals if pval[1] == slot]
								max_pval  = max(slot_maps, key=itemgetter(0))

								updated_proposals[max_pval[1]] = (max_pval[1], max_pval[2])
								print(updated_proposals)

							# Update entries in our proposal table.
							for slot in updated_proposals.keys():
								self.proposals[slot] = updated_proposals[slot]
								print(self.proposals)

							# Spawn commanders for all proposals.
							for slot in self.proposals:
								# Spawn commander
								commander = Commander(self.index, n, (self.ballot_num, slot, self.proposals[slot][1]))
								commander.start()

							self.active = True


						elif (received[0] == "preempted"):
							print received
							new_ballot_num = self.tup(received[1:3])
							# Update our ballot number if a larger one is found.
							if (self.comp_ballots(new_ballot_num, self.ballot_num) == 1):
								self.active = False
								self.ballot_num = (new_ballot_num[0] + 1, self.index)

								# Spawn a scout.
								scout = Scout(self.index, n, self.ballot_num)
								scout.start()

						else:
							print line
						
						#self.send(self.master, str(self.index) + ' received from master')
						#self.handle_master_comm(sock, data)

	def comp_ballots(self, b1, b2):
		if b1[0] < b2[0]:
			return -1
		elif b1[0] > b2[0]:
			return 1
		else:
			if b1[1] < b2[1]:
				return -1
			elif b1[1] > b2[1]:
				return 1
			else:
				return 0


	def send(self, sock, s):
		sock.send(str(s) + '\n')

	def tup(self, sl):
		return literal_eval(' '.join(sl))

	def format_pvals(self, sl):
		ret = []
		for i in range(len(sl)/3):
			ret.append(self.tup(sl[i:i+3]))

		return ret




class Scout(Thread):
	def __init__(self, lead_id, n, ballot):
		global address, baseport

		Thread.__init__(self)

		self.leader_id = lead_id
		self.num_acc   = n
		self.b 		   = ballot
		self.timeout   = .05

		self.wait_for = [i for i in range(n)]
		self.pvalues  = []

		self.acc_sockets = []

		self.lead_sock = socket(AF_INET, SOCK_STREAM)
		self.lead_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

		self.lead_sock.connect((address, baseport + self.leader_id*3 + 1))

	def run(self):
		# Broadast p1a messages.
		self.send_p1a()

		while(1):
			(active, _, _) = select(self.acc_sockets, [], [], self.timeout)

			# Timeout, resend p1a messages
			if (len(active) == 0):
				self.wait_for 	 = [i for i in range(n)]
				self.pvalues 	 = []
				self.acc_sockets = []

				self.send_p1a()


			for sock in active:
				# Loop waiting for responses
				line = sock.recv(1024)

				if not line:
					self.acc_sockets.remove(sock)

				response = line.strip().split(' ')
				if (response[0] == "p1b"):
					print(response)
					if (self.tup(response[2:4]) == self.b):
						# Add the response to the list of our pvalues.
						pvals = self.format_pvals(response[4:])
						for pval in pvals:
							if not pval in self.pvalues:
								self.pvalues.append(pval)
								print(self.pvalues)

						# Update wait_for and terminate if we have received a majority
						print(int(response[1]))
						self.wait_for.remove(int(response[1]))
						if (len(self.wait_for) < self.num_acc/2.0):
							print("IN ADOPTED")
							message = "adopted " + str(self.b) + " " + ' '.join([str(pval) for pval in self.pvalues])

							# Send message to leader.
							self.lead_sock.send(message + "\n")
							print(message)

							sys.exit()
					# We received a ballot greater than our leaders.
					else:
						print response
						message = "preempted " + str(self.tup(response[2:4]))
						print(message)

						# Send to leader.
						self.lead_sock.send(message + "\n")

						sys.exit()

				else:
					print line

	def tup(self, sl):
		print sl
		return literal_eval(','.join(sl))

	def format_pvals(self, sl):
		ret = []
		for i in range(len(sl)/3):
			ret.append(self.tup(sl[i:i+3]))

		return ret

	def send_p1a(self):
		# Connect to all available acceptors.
		for i in range(self.num_acc):
			sock = socket(AF_INET, SOCK_STREAM)
			sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

			try:
				print("Connecting: " + str(i))
				sock.connect((address, baseport + i*3 + 2))
				print("Connected to " + str(i))
				self.acc_sockets.append(sock)
			except:
				pass

		# Send a p1a message to all acceptors.
		for i, sock in zip(range(self.num_acc),self.acc_sockets):
			message = "p1a " + str(self.leader_id) + " " + str(self.b)
			print(message)
			# Send message to all acceptors
			sock.send(message + "\n")



class Commander(Thread):
	def __init__(self, lead_id, n, pval):
		global address, baseport

		Thread.__init__(self)

		self.leader_id = lead_id
		self.num_acc   = n

		self.wait_for = [i for i in range(n)]
		self.pval = pval

		self.acc_sockets = []
		self.rep_sockets = []

		# Establish connections with all acceptors.
		for i in range(self.num_acc):
			sock = socket(AF_INET, SOCK_STREAM)
			sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

			try:
				sock.connect((address, baseport + i*3 + 2))

				self.acc_sockets.append(sock)
			except:
				pass


		# Establish connections with all replicas.
		for i in range(self.num_acc):
			sock = socket(AF_INET, SOCK_STREAM)
			sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

			try:
				sock.connect((address, baseport + i*3 + 0))

				self.rep_sockets.append(sock)
			except:
				pass

		# Connect to out leader
		self.lead_sock = socket(AF_INET, SOCK_STREAM)
		self.lead_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

		self.lead_sock.connect((address, baseport + self.leader_id*3 + 1))

	def run(self):
		# Send a p2a message to all acceptors.
		for i, sock in zip(range(self.num_acc),self.acc_sockets):
			message = "p2a " + str(self.leader_id) + " " + str(self.pval)
			print(i, message)
			# Send message to all acceptors
			sock.send(message + "\n")

		while(1):
			(active, _, _) = select(self.acc_sockets, [], [])

			for sock in active:
				# Loop waiting for responses
				line = sock.recv(1024)

				if not line:
					self.acc_sockets.remove(sock)

				response = line.strip().split(' ')
				if (response[0] == "p2b"):
					print(response)
					if (self.tup(response[2:4]) == self.pval[0]):
						# Update wait_for and terminate if we have received a majority
						self.wait_for.remove(int(response[1]))
						if (len(self.wait_for) < self.num_acc/2.0):
							print(self.pval)
							message = "decision " + str(self.pval[1]) + " " + str(self.pval[2])
							print(message)

							# Send message to all replicas.
							for rep_sock in self.rep_sockets:
								print(":D")
								rep_sock.send(message + "\n")

							sys.exit()
					# We received a ballot greater than our leaders.
					else:
						message = "preempted " + str(self.tup(response[2:4]))
						print(message)

						# Send to leader.
						self.lead_sock.send(message + "\n")

						sys.exit()

				else:
					print line

	def tup(self, sl):
		return literal_eval(' '.join(sl))

	def format_pvals(self, sl):
		ret = []
		for i in range(len(sl)/3):
			ret.append(self.tup(sl[i:i+3]))

		return ret


# Testing
def main():
	# acc_sock = socket(AF_INET, SOCK_STREAM)
	# acc_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

	# rep_sock = socket(AF_INET, SOCK_STREAM)
	# rep_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

	# # Listen for connections, acc
	# acc_sock.bind((address, baseport + 0 + 2))
	# acc_sock.listen(4*n)

	# # Listen for connections, replica
	# rep_sock.bind((address, baseport + 0 + 0))
	# rep_sock.listen(4*n)
	master_sock = socket(AF_INET, SOCK_STREAM)
	master_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

	master_sock2 = socket(AF_INET, SOCK_STREAM)
	master_sock2.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

	master_sock3 = socket(AF_INET, SOCK_STREAM)
	master_sock3.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

	replica   = Replica(0, 'localhost', 10000)
	leader    = Leader(0, 'localhost')
	acceptor  = Acceptor(0, 'localhost')

	# Start the acceptor, then leader, then replica.
	acceptor.start()
	time.sleep(.1)
	leader.start()
	time.sleep(.1)
	replica.start()
	time.sleep(.1)

	print("FIRST SERVER STARTED")

	master_sock.connect((address, 10000))

	replica   = Replica(1, 'localhost', 10001)
	leader    = Leader(1, 'localhost')
	acceptor  = Acceptor(1, 'localhost')

	# Start the acceptor, then leader, then replica.
	acceptor.start()
	time.sleep(.1)
	leader.start()
	time.sleep(.1)
	replica.start()
	time.sleep(.1)

	print("SECOND SERVER STARTED")

	master_sock2.connect((address, 10001))

	replica   = Replica(2, 'localhost', 10002)
	leader    = Leader(2, 'localhost')
	acceptor  = Acceptor(2, 'localhost')

	# Start the acceptor, then leader, then replica.
	acceptor.start()
	time.sleep(.1)
	leader.start()
	time.sleep(.1)
	replica.start()
	time.sleep(.1)

	print("ALL SERVERS STARTED")

	time.sleep(.1)

	master_sock3.connect((address, 10002))
	master_sock.send('msg 0 hellothere' + '\n')
	time.sleep(.1)
	master_sock.send('msg 3 goodbye' + '\n')
	# sock.send('propose 0 Goodbye!' + '\n')

	while(1):
		pass
	# connections = [acc_sock, rep_sock]
	# while(1):
	# 	(active, _, _) = select(connections, [], [])

	# 	for sock in active:

	# 		if (sock == acc_sock):
	# 			(newsock, _) = acc_sock.accept()
	# 			connections.append(newsock)
	# 			continue

	# 		if (sock == rep_sock):
	# 			(newsock, _) = rep_sock.accept()
	# 			connections.append(newsock)
	# 			continue

	# 		line = sock.recv(1024)

	# 		if (line.strip() == ''):
	# 			connections.remove(sock)
	# 		else:
	# 			receive = line.strip().split()
	# 			if (receive[0] == 'p1a'):
	# 				message = "p1b 0 " + str((0,0)) + " " + str((0,0,"Hellothere"))
	# 				print(message)
	# 				sock.send(message + "\n")
	# 			elif (receive[0] == 'p2a'):
	# 				message = "p2b 0 " + str((0,0))
	# 				print(message)
	# 				sock.send(message + "\n")
	# 			elif (receive[0] == 'decision'):
	# 				#message = "p1b 0 " + str((1,0)) + " " + str((1,0,"I'mamessage!"))
	# 				print(receive)
	# 				#sock.send(message + "\n")
	# 			else:
	# 				print "Bad Message"


if __name__ == '__main__':
	main()


