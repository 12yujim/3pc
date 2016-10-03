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

class Client(object):
    # state constants
    IDLE = 0
    FIRSTVOTE = 1
    PRECOMMIT = 2
    ACKNOWLEDGE = 3

    PORT_BASE = 20000 # port_base

    def __init__(self, index, address, port):
        self.index = index
        self.library = {}
        self.valid = True
        self.state = self.IDLE
        self.currCmd = None
        self.currData = None

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
        self.my_sock.bind((address, self.PORT_BASE + self.index))
        self.my_sock.listen(n)
        self.comm_channels = [self.my_sock, self.master]

        self.vote = True
        self.crashAfterVote = False
        self.crashAfterAck = False

        #self.dtLog = open('dt' + self.index, 'a+')

    def initialize_socket(self, sock, port):
        global address
        sock.bind((address, port))
        sock.listen(5)
        return sock.accept()

    # Determine the current leader on startup. May need to retrieve state if recovering.
    # Also may be recovering after total failure, so handle that here also *todo*
    def determineLeader(self):
        global n, address
        for i in xrange(n):
            try:
                if i == self.index:
                    continue
                connectSocket = socket(AF_INET, SOCK_STREAM)
                connectSocket.connect((address, self.PORT_BASE + i))
                if self.valid:
                    connectSocket.send("info " + str(self.index) + "\n")
                ans = connectSocket.recv(1024).split('\n')[0]

                return int(ans)
            except:
                continue

        return self.index

    def run(self):
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
                        data = sock.recv(1024).strip()
                        self.send(self.master, str(self.index) + ' ' + data)
                        self.send(self.master, str(sock.getsockname()) + ' peername')
                        if (data == ''):
                            self.comm_channels.remove(sock)
                        if (sock == self.master):
                            self.send(self.master, str(self.index) + ' received from master')
                            self.handle_master_comm(sock, data)
                        elif (sock.getsockname()[1] == self.leader):
                            self.send(self.master, str(self.index) + ' received from coordinator')
                            self.handle_coord_comm(sock, data)
                        else:
                            self.send(self.master, str(self.index) + ' received from other')
                            self.handle_server_comm(sock, data)
            except:
                #self.send(self.master, 'exception???')
                self.close()
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
                self.send(sock, self.leader)

    # Handles communication between normal servers and the coordinator.
    def handle_coord_comm(self, sock, data):
        line = data.split('\n')
        try:
            for l in line:
                s = l.split()
                if s[0] == 'voteREQ':
                    self.state = self.FIRSTVOTE
                    self.currCmd = s[1]
                    self.currData = ','.join(s[2:])
                    self.send(self.master, 'we made it')
                    # write to DT log
                #elif s[0] == 'participants':
                 #   others = list(s[1])
                    # write participants to DT log
                    # write vote to DT log
                    # send vote
                    self.send(sock, self.vote)
                    self.state = self.PRECOMMIT
                elif s[0] == 'precommit':
                    # write to log
                    self.send(sock, 'ack')
                    self.state = self.ACKNOWLEDGE
                elif s[0] == 'commit':
                    # write to log
                    songName = self.data.split(',')[0]
                    if self.cmd == 'delete':
                        if songName in self.library:
                            del self.library[songName]
                    if self.cmd == 'add':
                        url = self.data.split(',')[1]
                        self.library[songName] = url
                else:
                    self.abort()
        except:
            self.send(self.master, 'failure complete')

    # Handles communication between servers (coord or normal) and master
    def handle_master_comm(self, sock, data):
        line = data.split('\n')
        for l in line:
            s = l.split()
            if len(s) < 2:
                continue
            if s[0] == 'status':
                self.send(self.master, str(self.index) + ' alive')
            if s[0] == 'add':
                if self.leader == self.index:
                    # begin vote process
                    if self.voteReq(s[0], ','.join(s[1:])):
                       self.send(sock, 'resp commit')
                       # write to library
                       self.library[s[1]] = s[2]
                    else:
                       self.send(self.master, 'resp abort')
                else:
                    continue
            elif s[0] == 'delete':
                if self.leader == self.index:
                    # begin vote process
                    if self.voteReq(s[0], s[1]):
                        self.send(self.master, 'resp commit')
                        # delete from library
                        if s[1] in self.library:
                            del self.library[s[1]]
                        else:
                            pass
                    else:
                        self.send(self.master, 'resp abort')
                else:
                    continue
            elif s[0] == 'get':
                # return the song (lookup in playlist)
                if s[1] in self.library:
                    self.send(self.master, 'resp ' + self.library[s[1]])
                else:
                    self.send(self.master, 'resp NONE')
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


    def voteReq(self, cmd, data):
        global n, address
        # return True if all processes vote commit, else return False
        success = True
        if not self.vote:
            # short circuit because coordinator votes no
            return False
        # send voteREQ to all participants and wait for response
        request = 'voteREQ ' + cmd + ' ' + data
        participants = []
        for i in xrange(n):
            try:
                if i == self.index:
                    continue
                connectSocket = socket(AF_INET, SOCK_STREAM)
                connectSocket.connect((address, self.PORT_BASE + i))
                if self.valid:
                    self.send(connectSocket, request)
                participants.append(connectSocket)
            except:
                continue
        # sent out all requests, inform participants of all other participants
        # for i in participants:
        #     try:
        #         self.send(i, 'participants ' + ''.join(particpiants))
        #     except:
        #         continue
        acks = 0
        while (acks < len(participants)):
            # add timeout
            votes = self.my_sock.recv(1024).split('\n')
            for vote in votes:
                self.send(self.master, 'waiting for acks')
                if (vote == 'False'):
                    success = False
                acks += 1
            if not success:
                break
        # if not success:
        #     for i in participants:
        #         try:
        #             self.send(i, 'abort')
        #             self.abort()
        #         except:
        #             continue
        #     return success
        # for i in participants:
        #     try:
        #         self.send(i, 'precommit')
        #     except:
        #         continue
        # acks = 0
        # while (acks != len(participants)):
        #     # add timeout
        #     vote = self.my_sock.recv(1024)
        #     if vote == 'ack':
        #         acks += 1
        # if (acks != len(participants)):
        #     success = False
        #     for i in participants:
        #         try:
        #             self.send(i, 'abort')
        #         except:
        #             continue
        #     self.abort()
        # for i in participants:
        #     try:
        #         self.send(i, 'commit')
        #     except:
        #         continue
        return success

    def abort(self):
        self.state = self.IDLE
        self.currCmd = None
        self.currData = None

    def send(self, sock, s):
        if self.valid:
            sock.send(str(s) + '\n')

    def close(self):
        try:
            self.valid = False
            for s in sock.comm_channels:
                 s.close()
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
