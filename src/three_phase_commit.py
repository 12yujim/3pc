#!/usr/bin/env python
"""
The master program for CS5414 three phase commit project.
"""

import sys, os
import subprocess
import time
from threading import Thread, Lock
from socket import SOCK_STREAM, socket, AF_INET
from select import select

address = "localhost"
n = 0 # number of process
PORT_BASE = 20000 # port_base

class Client(object):
    def __init__(self, index, address, port):
        self.leader = 0;
        self.index = index
        self.valid = True

        self.master = socket(AF_INET, SOCK_STREAM)
        self.my_sock = socket(AF_INET, SOCK_STREAM)
        #self.my_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        self.leader = self.determineLeader() # initialize leader, start check at 0

        # Connect with master after determining coordinator.
        (self.master, _) = self.initialize_socket(self.master, port)
        if self.leader == self.index:
            # notify master you are coordinator
            self.send(self.master, "coordinator " + str(self.leader))

        # Listen for other processes on my designated port.
        self.send(self.master, "Hello there")
        self.send(self.master, address + " there")
        self.my_sock.bind((address, 20000 + self.index))
        self.send(self.master, "Making there")
        self.my_sock.listen(n)
        self.comm_channels = [self.my_sock, self.master]
        

        self.vote = True
        self.crashAfterVote = False
        self.crashAfterAck = False

    def initialize_socket(self, sock, port):
        sock.bind((address, port))
        sock.listen(5)
        return sock.accept()

    # Determine the current leader on startup. May need to retrieve state if recovering.
    # Also may be recovering after total failure, so handle that here also *todo*
    def determineLeader(self):
        global address, PORT_BASE
        for i in xrange(n):
            try:
                if i == self.index:
                    continue
                connectSocket = socket(AF_INET, SOCK_STREAM)
                connectSocket.connect((address, PORT_BASE + i))
                if self.valid:
                    connectSocket.send("info " + str(self.index) + "\n")
                ans = connectSocket.recv(1024).split('\n')[0]

                return int(ans)
            except:
                continue

        return self.index

    def run(self):
        global n, address, PORT_BASE
        while self.valid:
            try:
                # listen for input from all channels
                (active, _, _) = select(self.comm_channels, [], [])

                for sock in active:
                    # We are receving a new connection, so accept it.
                    if (sock == self.my_sock):
                        (newsock, _) = self.my_sock.accept()
                        self.comm_channels.append(newsock)
                    else:
                        # Are we communicating with master, coord, or other servers?
                        data = sock.recv(1024)
                        if (data == ''):
                            sock.close()
                            self.comm_channels.remove(sock)
                        if (sock == self.master):
                            self.handle_master_comm(sock, data)
                        elif (sock.getpeername()[1] == self.leader):
                            self.handle_coord_comm(sock, data)
                        else:
                            self.handle_server_comm(sock, data)
            except:
                #self.sock.close()
                break

    # Handles communication between normal servers.
    def handle_server_comm(self, sock, data):
        line = data.split('\n')
        for l in line:
            s = l.split()
            if len(s) < 2:
                continue
            if s[0] == 'info':
                # new process is asking for information, send leaderpid
                if self.valid:
                    self.send(sock, str(self.leader))

    # Handles communication between normal servers and the coordinator.
    def handle_coord_comm(self, sock,  data):
        pass

    # Handles communication between servers (coord or normal) and master
    def handle_master_comm(self, sock, data):
        line = data.split('\n')
        for l in line:
            s = l.split()
            if len(s) < 2:
                continue
            if s[0] == 'add':
                if self.leader == self.index:
                    # begin vote process
                    if self.voteReq():
                        self.send(sock, 'resp commit\n')
                        # write to file
                    else:
                        self.send('resp abort\n')
                else:
                    continue
            elif s[0] == 'delete':
                if self.leader == self.index:
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


    def voteReq(self):
        # return True if all processes vote commit, else return False
        if not self.vote:
            # short circuit because coordinator votes no
            return False
        return True

    def send(self, sock, s):
        if self.valid:
            sock.send(str(s) + '\n')

    def close(self, sock):
        try:
            self.valid = False
            sock.close()
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
