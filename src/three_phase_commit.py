#!/usr/bin/env python
"""
The master program for CS5414 three phase commit project.
"""

import sys, os
import subprocess
import time
from threading import Thread, Lock
from socket import SOCK_STREAM, socket, AF_INET

leader = 0 # leader starts with pid 0
address = 'localhost'
n = 0 # number of process
PORT_BASE = 20000 # port_base

class Client(object):
    def __init__(self, index, address, port):
        self.index = index
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.valid = True
        (clientsocket, address) = self.initialize_socket(port)
        self.vote = 'yes'
        self.crashAfterVote = False
        self.crashAfterAck = False

    def initialize_socket(self, port):
        self.sock.bind((address, port))
        self.sock.listen(5)
        self.sock.accept()

    def run(self):
        global leader, n, PORT_BASE
        while self.valid:
            try:
                # listen for input
                data = self.sock.recv(1024)
                line = data.split('\n')
                for l in line:
                    s = l.split()
                    if len(s) < 2:
                        continue
                    if s[0] == 'add':
                        if leader == self.index:
                            # begin vote process
                            pass
                        else:
                            continue
                    elif s[0] == 'delete':
                        if leader == self.index:
                            # begin vote process
                            pass
                        else:
                            continue
                    elif s[0] == 'get':
                        # return the song (lookup in DT)
                        pass
                    elif s[0] == 'crash':
                        #invoke crash
                        pass
                    elif s[0] == 'vote':
                        if s[1] == 'NO':
                            self.vote = 'no'
                        else:
                            continue
                    elif s[0] == 'crashAfterVote':
                        self.crashAfterVote = True
                    elif s[0] == 'crashAfterAck':
                        self.crashAfterAck = True
                    elif s[0] == 'crashPretialPreCommit':
                        pass
                    elif s[0] == 'crashPretialCommit':
                        pass
            except:
                self.sock.close()
                break

    def send(self, s):
        if self.valid:
            self.sock.send(str(s) + '\n')

    def close(self):
        try:
            self.valid = False
            self.sock.close()
        except:
            pass

def main():
    global n, address
    index = int(sys.argv[1])
    n = int(sys.argv[2])
    my_port = PORT_BASE + index
    # assert(int(sys.argv[3]) == my_port)
    client = Client(index, address, my_port)
    while True:
        try:
            pass
        except:
            serversocket.close()

if __name__ == '__main__':
    main()
