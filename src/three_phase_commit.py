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
        global leader
        self.index = index
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.valid = True
        (clientsocket, address) = self.initialize_socket(port)
        leader = self.determineLeader() # initialize leader, start check at 0
        if leader == self.index:
            # notify master you are coordinator
            self.send('coordinator ' + str(leader) + '\n')
        self.vote = True
        self.crashAfterVote = False
        self.crashAfterAck = False

    def initialize_socket(self, port):
        self.sock.bind((address, port))
        self.sock.listen(5)
        return self.sock.accept()

    def determineLeader(self):
        global leader, address, PORT_BASE
        for i in xrange(n):
            try:
                if i == self.index:
                    continue
                connectSock = socket(AF_INET, SOCK_STREAM)
                connectSock.connect((address, PORT_BASE + i))
                if self.valid:
                    connectSock.send('info ' + str(self.index) + '\n')
                ans = connectSock.recv(1024).split('\n')
                leader = int(ans)
                connectSock.close()
            except:
                continue
        # did not connect to any process
        return self.index

    def run(self):
        global leader, n, address, PORT_BASE
        while self.valid:
            try:
                # listen for input
                data = self.sock.recv(1024)
                line = data.split('\n')
                for l in line:
                    s = l.split()
                    if len(s) < 2:
                        continue
                    if s[0] == 'info':
                        # new process is asking for information, send leaderpid
                        asker = s[1]
                        sendSock = socket(AF_INET, SOCK_STREAM)
                        sendSock.connect((address, PORT_BASE + asker))
                        if self.valid:
                            sendSock.send(str(leader) + '\n')
                        sendSock.close()
                    if s[0] == 'add':
                        if leader == self.index:
                            # begin vote process
                            if self.voteReq():
                                self.send('resp commit\n')
                                # write to file
                            else:
                                self.send('resp abort\n')
                        else:
                            continue
                    elif s[0] == 'delete':
                        if leader == self.index:
                            # begin vote process
                            if self.voteReq():
                                self.send('resp commit\n')
                                # delete from file
                            else:
                                self.send('resp abort\n')
                        else:
                            continue
                    elif s[0] == 'get':
                        # return the song (lookup in playlist)
                        pass
                    elif s[0] == 'crash':
                        #invoke crash
                        self.close()
                    elif s[0] == 'vote':
                        if s[1] == 'NO':
                            self.vote = False
                        else:
                            continue
                    elif s[0] == 'crashAfterVote':
                        self.crashAfterVote = True
                    elif s[0] == 'crashAfterAck':
                        self.crashAfterAck = True
                    elif s[0] == 'crashVoteREQ':
                        pass
                    elif s[0] == 'crashPretialPreCommit':
                        pass
                    elif s[0] == 'crashPretialCommit':
                        pass
            except:
                self.sock.close()
                break

    def voteReq(self):
        # return True if all processes vote commit, else return False
        if not self.vote:
            # short circuit because coordinator votes no
            return False
        return True

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
    my_port = int(sys.argv[3])
    client = Client(index, address, my_port)
    client.run()

if __name__ == '__main__':
    main()
