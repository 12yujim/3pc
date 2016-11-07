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

        self.ballot_num = None
        self.accepted = set()

        self.crashAfterP1b = False
        self.crashAfterP2b = False

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
                        # if receive "phase 1a" with ballot num b, go to p1a
                        msg = data.split(' ')
                        if msg[0] == 'p1a':
                            self.p1a(sock, msg[1])
                        elif msg[0] == 'p2a':
                            self.p2a(sock, msg[1:])
                        elif msg[0] == 'crashAfterP1b':
                            self.crashAfterP1b = True
                        elif msg[0] == 'crashAfterP2b':
                            self.crashAfterP2b = True
                        #self.send(self.master, str(self.index) + ' received from master')
                        #self.handle_master_comm(sock, data)

    def p1a(self, lead, b):
        # Send vote for ballot number proposed by leader, if it is highest ballot # received.
        if (self.ballot_num == None) or (b > self.ballot_num):
            self.ballot_num = b
        # send 'phase 1b' + ballot_num + accepted
        resp = 'p1b {}'.format(self.ballot_num)
        for acc in self.accepted:
            resp += ' ' + acc
        self.send(lead, resp)
        self.crash()

    def p2a(self, lead, pval):
        b = pval[0]
        # Decide on this ballot number for the slot, send back an ack to leader.
        if (self.ballot_num == None) or (b >= self.ballot_num):
            self.ballot_num = b
            self.accepted = self.accepted.add(' '.join(pval))
        resp = 'p2b {}'.format(self.ballot_num)
        self.send(lead, resp)
        self.crash()

    def send(self, sock, s):
        sock.send(str(s) + '\n')


    def crash(self):
        # crashes the associated acceptor, replica, and leader
        crashCmd = "ps aux | grep \"src/server.py {}\" | awk '{print $2}' | xargs kill".format(self.index)
        subprocess.call(crashCmd)

