# File containing the acceptor class

import sys, os
import subprocess
import time
from threading import Thread, Lock
from socket import SOCK_STREAM, socket, AF_INET, SOL_SOCKET, SO_REUSEADDR
from select import select
from ast import literal_eval

address = 'localhost'
baseport = 20000
n = 4

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
        self.my_sock.listen(100*n)

        self.comm_channels = [self.my_sock]

        while(1):
            (active, _, _) = select(self.comm_channels, [], [])

            for sock in active:
                
                if (sock == self.my_sock):
                    (newsock, _) = self.my_sock.accept()
                    self.comm_channels.append(newsock)
                else:
                    # Are we communicating with master, coord, or other servers?
                    try:
                        line = sock.recv(1024)
                    except:
                        continue
                    #self.send(self.master, "Got here! " + str(self.index))
                    if not line:
                        self.comm_channels.remove(sock)

                    for data in line.split('\n'):
                        if data == '':
                            continue
                        # if receive "phase 1a" with ballot num b, go to p1a
                        msg = data.split(' ')
                        if msg[0] == 'p1a':
                            self.p1a(sock, self.tup(msg[2:]))

                        elif msg[0] == 'p2a':
                            self.p2a(sock, self.tup(msg[2:]))
                        elif msg[0] == 'crashAfterP1b':
                            self.crashAfterP1b = True
                        elif msg[0] == 'crashAfterP2b':
                            self.crashAfterP2b = True

    def p1a(self, lead, b):
        # Send vote for ballot number proposed by leader, if it is highest ballot # received.
        if (self.comp_ballots(b, self.ballot_num) > 0):
            self.ballot_num = b
        # send 'phase 1b' + ballot_num + accepted
        resp = 'p1b ' + str(self.index) + ' ' + str(self.ballot_num)
        for acc in self.accepted:
            resp += ' ' + str(acc)
        self.send(lead, resp)

        if (self.crashAfterP1b):
            self.crash()

    def p2a(self, lead, pval):
        b = pval[0]
        # Decide on this ballot number for the slot, send back an ack to leader.
        if (self.comp_ballots(b, self.ballot_num) > -1):
            self.ballot_num = b

            self.accepted.add(pval)
        resp = 'p2b {} {}'.format(self.index, self.ballot_num)
        self.send(lead, resp)

        if self.crashAfterP2b:
            self.crash()

    def send(self, sock, s):
        sock.send(str(s) + '\n')


    def crash(self):
        # crashes the associated acceptor, replica, and leader
        crashCmd = "ps aux | grep \"src/server.py {}\" | awk '{{print $2}}' | xargs kill".format(self.index)
        subprocess.call(crashCmd)

    def tup(self, sl):
        return literal_eval(' '.join(sl))

    def comp_ballots(self, b1, b2):
        if b1 == None:
            return -1
        if b2 == None:
            return 1

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

