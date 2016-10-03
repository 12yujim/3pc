#!/usr/bin/env python
"""
The master program for CS5414 three phase commit project.
"""

import sys, os
import subprocess
import time
from threading import Thread, Lock
from socket import SOCK_STREAM, socket, AF_INET, SOL_SOCKET, SO_REUSEADDR
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
        self.others = None

        self.master = socket(AF_INET, SOCK_STREAM)
        self.my_sock = socket(AF_INET, SOCK_STREAM)
        self.my_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.master.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

        # Read from the data log and see if this is the first time.
        self.log = open("log" + str(self.index) + ".txt", 'w+')
        store = self.log.readline().split()
        for [key,value] in [pair.split(',') for pair in store]:
            self.library['key'] = value

        # If there was no store we are starting the servers.
        # Otherwise we're recovering from a crash and need to request state.
        recover = False
        if self.library:
            recover = True
        
        self.leader = self.determineLeader(recover) # initialize leader, start check at 0

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
        self.crashVoteREQ = (False, [])
        self.crashPartialPreCommit = (False, [])
        self.crashPartialCommit = (False, [])

    def initialize_socket(self, sock, port):
        global address
        sock.bind((address, port))
        sock.listen(5)
        return sock.accept()

    # Determine the current leader on startup. May need to retrieve state if recovering.
    # Also may be recovering after total failure, so handle that here also *todo*
    def determineLeader(self, recover):
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

                lead = ans.split()[0]
                if recover:
                    lib  = ans.split()[1:]
                    for key, value in [pair.split(',') for pair in lib]:
                        self.library[key] = value

                return int(lead)
            except:
                continue

        # We've experienced a total failure.
        if recover:
            pass

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
                        line = sock.recv(1024).split('\n')
                        for data in line:
                            if data == '':
                                continue
                            if (data == ''):
                                self.comm_channels.remove(sock)
                            if (sock == self.master):
                                self.send(self.master, str(self.index) + ' received from master')
                                self.handle_master_comm(sock, data)
                            else:
                                self.send(self.master, str(self.index) + ' received from server')
                                self.handle_server_comm(sock, data)
            except:
                #self.send(self.master, 'exception???')
                self.close()
                break

    # Handles communication between normal servers.
    def handle_server_comm(self, sock, data):
        line = data.split('\n')
        try:
            for l in line:
                s = l.split()
                if s[0] == 'info':
                    # new process is asking for information, send leaderpid

                    self.send(sock, str(self.leader) + ' ' + ' '.join([key + "," + value for key,value in self.library.items()]))
                if s[0] == 'voteREQ':
                    #self.send(self.master, 'data ' + s[2])
                    self.state = self.FIRSTVOTE
                    self.currCmd = s[1]
                    self.currData = s[2]
                    # write to DT log
                    self.others = list(s[3])
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
                    songName = self.currData.split(',')[0]
                    if self.currCmd == 'delete':
                        if songName in self.library:
                            del self.library[songName]
                    if self.currCmd == 'add':
                        url = self.currData.split(',')[1]
                        self.library[songName] = url
                else:
                    self.abort()
        except:
            #self.send(self.master, 'failure complete')
            pass

    # Handles communication between servers (coord or normal) and master
    def handle_master_comm(self, sock, data):
        line = data.split('\n')
        self.send(self.master, "Received master comm " + data)
        for l in line:
            s = l.split()
            if s[0] == 'status':
                # delete this command
                self.send(self.master, str(self.index) + ' alive')
            elif s[0] == 'add':
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
                self.send(self.master, "Received crashVoteREQ")
                self.crashVoteREQ = (True, [int(i) for i in s[1:]])
                self.send(self.master, "Received crashVoteREQ " + str(self.crashVoteREQ[1][0]))
            elif s[0] == 'crashPartialPreCommit':
                self.crashPartialPreCommit = (True, [int(i) for i in s[1:]])
            elif s[0] == 'crashPartialCommit':
                self.crashPartialCommit = (True, [int(i) for i in s[1:]])


    def voteReq(self, cmd, data):
        global n, address
        # return True if all processes vote commit, else return False
        success = True
        if not self.vote:
            # short circuit because coordinator votes no
            return False
        # send voteREQ to all participants and wait for response
        request = 'voteREQ ' + cmd + ' ' + data + ' '
        p_sock = []
        participants = []
        for i in xrange(n):
            try:
                if i == self.index:
                    continue
                connectSocket = socket(AF_INET, SOCK_STREAM)
                connectSocket.connect((address, self.PORT_BASE + i))
                p_sock.append(connectSocket)
                participants.append(str(i))
            except:
                continue
        request += ''.join(participants)
        # sent out all requests, inform participants of all other participants
        self.send(self.master, "Sending requests")
        for i,s in zip(participants, p_sock):
            try:
                self.send(self.master, "Trying to send request to " + i + str(self.crashVoteREQ[1]))
                if not self.crashVoteREQ[0]:
                    self.send(s, request)
                    continue

                if self.crashVoteREQ[0] and (int(i) in self.crashVoteREQ[1]):
                    self.send(self.master, "Sent request to " + i)
                    self.send(s, request)
            except:
                continue
        if self.crashVoteREQ[0]:
            self.send(self.master, "Coordinator " + str(self.index) + " crashing!")
            sys.exit(0)
        acks = 0
        while (acks != len(participants)):
            # add timeout
            (active, _, _) = select(p_sock, [], [])
            for sock in active:
                data = sock.recv(1024).split('\n')
                for votes in data:
                    if votes == '':
                        continue
                    if (votes == 'False'):
                        success = False
                    acks += 1
            if not success:
                break
        if not success:
            for i in p_sock:
                try:
                    self.send(i, 'abort')
                    self.abort()
                except:
                    continue
            return success
        for i in p_sock:
            try:
                self.send(i, 'precommit')
            except:
                continue
        acks = 0
        while (acks != len(participants)):
            # add timeout
            (active, _, _) = select(p_sock, [], [])
            for sock in active:
                data = sock.recv(1024).split('\n')
                for votes in data:
                    if votes == '':
                        continue
                    if (votes == 'ack'):
                        acks += 1
        if (acks != len(participants)):
            success = False
            for i in p_sock:
                try:
                    self.send(i, 'abort')
                except:
                    continue
            self.abort()
        for i in p_sock:
            try:
                self.send(i, 'commit')
            except:
                continue
        return success

    def abort(self):
        self.state = self.IDLE
        self.currCmd = None
        self.currData = None
        self.others = None

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
