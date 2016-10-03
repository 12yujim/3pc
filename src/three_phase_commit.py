#!/usr/bin/env python
"""
The master program for CS5414 three phase commit project.
"""

import sys, os
import subprocess
import time
import socket
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
        self.log = "log" + str(self.index) + ".txt"
        store = ''
        try:
            with open(self.log, 'r') as logfile:
                store = logfile.readline().split()
        except:
            with open(self.log, 'w') as logfile:
                pass
        for [key,value] in [pair.split(',') for pair in store]:
            self.library[key] = value

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
                    self.send(connectSocket, "info " + str(self.index))
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
        global address
        while self.valid:
            try:
                # listen for input from all channels
                if self.state == self.IDLE:
                    (active, _, _) = select(self.comm_channels, [], [])
                else:
                    (active, _, _) = select(self.comm_channels, [], [], 500)

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
                                #self.send(self.master, str(self.index) + ' received from master')
                                self.handle_master_comm(sock, data)
                            else:
                                #self.send(self.master, str(self.index) + ' received from server')
                                self.handle_server_comm(sock, data)
            except timeout:
                self.leader = self.leader + 1 % n
                if self.index == self.leader:
                    # you are new coordinator
                    self.send(self.master, 'coordinator ' + str(self.index))
                # abort current protocol
                self.abort()
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
                    self.state = self.FIRSTVOTE
                    self.currCmd = s[1]
                    self.currData = s[2]
                    self.others = list(s[3])
                    # write to DT log
                    with open(self.log, 'w') as logfile:
                        for songName in self.library:
                            logfile.write('%s,%s ' % (songName, self.library[songName]))
                        logfile.write('\n')
                        logfile.write('START_3PC %s,%s' % (self.currCmd, self.currData))
                        logfile.write('\n')
                        logfile.write(','.join(self.others) + '\n')
                        vote = 'yes' if self.vote else 'no'
                        logfile.write(vote + '\n')
                    # send vote
                    self.send(sock, self.vote)
                    self.vote = True
                    self.state = self.PRECOMMIT
                elif s[0] == 'precommit':
                    # write to log
                    with open(self.log, 'a') as logfile:
                        logfile.write('precommit\n')
                        logfile.write('ack\n')
                    self.send(sock, 'ack')
                    self.state = self.ACKNOWLEDGE
                elif s[0] == 'commit':
                    # write to log
                    with open(self.log, 'a') as logfile:
                        logfile.write('commit\n')
                    songName = self.currData.split(',')[0]
                    if self.currCmd == 'delete':
                        if songName in self.library:
                            del self.library[songName]
                    if self.currCmd == 'add':
                        url = self.currData.split(',')[1]
                        self.library[songName] = url
                elif s[0] == 'abort':
                    with open(self.log, 'a') as logfile:
                        logfile.write('abort\n')
                    self.abort()
        except:
            #self.send(self.master, 'failure complete')
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
            elif s[0] == 'status':
                # delete this command
                self.send(self.master, str(self.index) + ' alive')


    def voteReq(self, cmd, data):
        global n, address
        self.currCmd = cmd
        self.currData = data
        # return True if all processes vote commit, else return False
        success = True
        with open(self.log, 'w') as logfile:
            for songName in self.library:
                logfile.write('%s,%s ' % (songName, self.library[songName]))
            logfile.write('\n')
            logfile.write('START_3PC %s,%s' % (self.currCmd, self.currData))
            logfile.write('\n')
        if not self.vote:
            # short circuit because coordinator votes no
            with open(self.log, 'a') as logfile:
                logfile.write(str(self.index) + '\n')
                logfile.write('abort\n')
            return False

        #########
        # START #
        #########
        # send voteREQ to all participants and wait for response
        request = 'voteREQ ' + cmd + ' ' + data + ' '
        p_sock = []
        participants = [str(self.index)]
        for i in xrange(n):
            try:
                if i == self.index:
                    continue
                # connect to and keep track of participants
                connectSocket = socket(AF_INET, SOCK_STREAM)
                connectSocket.connect((address, self.PORT_BASE + i))
                p_sock.append(connectSocket)
                participants.append(str(i))
            except:
                continue
        # inform participants of all participants
        request += ''.join(participants)
        with open(self.log, 'a') as logfile:
            logfile.write(','.join(participants) + '\n')
            vote = 'yes' if self.vote else 'no'
            logfile.write(vote + '\n')
        # sent out all requests, inform participants of all other participants
        for i in p_sock:
            try:
                self.send(i, request)
            except:
                continue

        ##########
        # VOTING #
        ##########
        acks = 0
        while (acks != len(p_sock)):
            # add timeout
            # wait for participants to send votes
            (active, _, _) = select(p_sock, [], [])
            for sock in active:
                data = sock.recv(1024).split('\n')
                for votes in data:
                    if votes == '':
                        continue
                    if (votes == 'False'):
                        # participant voted false
                        success = False
                    # received a response
                    acks += 1
            if not success:
                # decide abort
                with open(self.log, 'a') as logfile:
                    logfile.write('abort\n')
                break
        if not success:
            # send abort to all participants
            for i in p_sock:
                try:
                    self.send(i, 'abort')
                except:
                    # participant failure
                    continue
            # abort self and let master know
            self.abort()
            return success

        ###################
        # ENTER PRECOMMIT #
        ###################
        # all processes voted yes
        with open(self.log, 'a') as logfile:
            logfile.write('precommit\n')
        for i in p_sock:
            try:
                # move to precommit stage
                self.send(i, 'precommit')
            except:
                # ???
                continue

        #############
        # PRECOMMIT #
        #############
        with open(self.log, 'a') as logfile:
            logfile.write('ack\n')
        acks = 0
        while (acks != len(p_sock)):
            # add timeout
            # wait for acks in precommit stage
            (active, _, _) = select(p_sock, [], [])
            for sock in active:
                data = sock.recv(1024).split('\n')
                for votes in data:
                    if votes == '':
                        continue
                    if (votes == 'ack'):
                        # received ack
                        acks += 1
        if (acks != len(p_sock)):
            with open(self.log, 'a') as logfile:
                logfile.write('abort\n')
            # did not receive ack from everyone
            success = False
            for i in p_sock:
                try:
                    self.send(i, 'abort')
                except:
                    continue
            self.abort()

        ##########
        # COMMIT #
        ##########
        with open(self.log, 'a') as logfile:
            logfile.write('commit\n')
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
        self.log.close()
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
