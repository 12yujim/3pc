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

    TIMEOUT = 2.0
    COORDINATOR_TIMEOUT = 1.5

    PORT_BASE = 20000 # port_base

    def __init__(self, index, address, port):
        self.index = index
        self.library = {}
        self.send_info = []
        self.valid = True
        self.state = self.IDLE
        self.currCmd = None
        self.currData = None
        self.others = []
        self.upset  = [str(self.index)]

        self.master = socket(AF_INET, SOCK_STREAM)
        self.my_sock = socket(AF_INET, SOCK_STREAM)
        self.my_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.master.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

        # Read from the data log and see if this is the first time.
        recover = True
        self.log = "log" + str(self.index) + ".txt"
        store = ''
        try:
            with open(self.log, 'r') as logfile:
                store = logfile.readline().split()
        except:
            with open(self.log, 'w') as logfile:
                recover = False
        for [key,value] in [pair.split(',') for pair in store]:
            self.library[key] = value 
            
        # Connect with master before determining coordinator.
        (self.master, _) = self.initialize_socket(self.master, port)
        
        self.leader = self.determineLeader(recover) # initialize leader, start check at 0
        #self.leader = 0

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

        valid_contacts = []
        for i in xrange(n):
            try:
                if i == self.index:
                    continue
                connectSocket = socket(AF_INET, SOCK_STREAM)
                connectSocket.connect((address, self.PORT_BASE + i))

                valid_contacts.append((i, connectSocket))
            except:
                continue

        lead = -1
        running = []
        intersection = []
        for i,sock in valid_contacts:
            try:
                self.send(sock, 'info ' + str(self.index) + ' ' + ','.join(self.upset))

                # The contact may be in a transaction, or possibly died after a transaction.
                # Either way we move on until all have timed out. This is then a total failure.
                self.sock.settimeout(self.TIMEOUT)
                data = sock.recv(1024)
                if not data:
                    continue

                ans = data.split('\n')[0].split()
                lead = int(ans[0])


                if not recover:
                    self.upset = ans[1].split(',')
                    self.upset.append(str(self.index))

                    return lead

                # If we receive -1 for leader, then we are in a total failure state
                if lead == -1:
                    running = ans[1].split(',')
                    intersection = ans[2].split(',')

                    # If intersection of UPs is subset of Running then run termination protocol.
                    if all([p in running for p in intersection]):
                        # Run termination, elect new leader.
                        try:
                            with open(self.log, 'r') as logfile:
                                self.others = logfile.read().split('\n')[2].split(',')
                        except:
                            with open('leaderDT.txt', 'r') as logfile:
                                self.others = logfile.read().split('\n')[2].split(',')

                        self.determine_state()

                        self.termination()
                    else:
                        # Wait for latest process to wake up.
                        break

                lib  = ans[2:]
                for key, value in [pair.split(',') for pair in lib]:
                    self.library[key] = value

                return lead
            except:
                continue


        if not recover:
            return self.index

        # We've experienced a total failure. See if can decide what to do, if not wait for last process.
        tf_listen = socket(AF_INET, SOCK_STREAM)
        tf_listen.bind((address, self.PORT_BASE + self.index))
        tf_listen.listen(n)
        channels = [tf_listen]
        while True:
            try:
                (active, _, _) = select(channels, [], [])

                for sock in active:

                    if (sock == tf_listen):
                        (newsock, _) = tf_listen.accept()
                        channels.append(newsock)
                        continue
                    else:
                        ans = sock.recv(1024).split('\n')[0].split()
                        if not ans:
                            channels.remove(sock)
                            continue

                        proc_id = ans[1]
                        proc_part = ans[2].split(',')

                        # Once a process wakes up, check to see if it's in our participant group.
                        # Send it the running group and current intersection if it is.
                        # Once the last process wakes up it will run the termination protocol.
                        # See if the responder is in our participant list.
                        with open(self.log, 'r') as logfile:
                            self.others = logfile.read().split('\n')[2].split(',')

                        # Send them the current recovered processes and the instersection of UP
                        if proc_id in self.others:
                            running.append(proc_id)
                            intersection = ['0'] #[p for p in intersection if p in proc_part]

                            self.send(sock, str(lead) + ' ' + ','.join(running) + ' ' + ','.join(intersection))
                        else:
                            self.send_info.append(proc_id)
            except:
                break

    def determine_state(self):
        try:
            with open(self.log, 'r') as logfile:
                line = logfile.readline()
                line = logfile.readline()
                while (line != ''):
                    if (line.split()[0] == 'START_3PC'):
                        self.state = FIRSTVOTE
                        line = logfile.readline()
                    if (line == 'yes'):
                        self.state = PRECOMMIT
                        line = logfile.readline()
                    if (line == 'ack'):
                        self.state = ACKNOWLEDGE
                    line = logfile.readline()
        except:
            with open('leaderDT.txt', 'r') as logfile:
                line = logfile.readline()
                line = logfile.readline()
                while (line != ''):
                    if (line.split()[0] == 'START_3PC'):
                        self.state = FIRSTVOTE
                        line = logfile.readline()
                    if (line == 'yes'):
                        self.state = PRECOMMIT
                        line = logfile.readline()
                    if (line == 'ack'):
                        self.state = ACKNOWLEDGE
                    line = logfile.readline()

    # elect new coordinator        
    def termination(self, sock=None):
        global n
        self.leader = (self.leader + 1) % n
        aborted = False
        if self.leader == self.index:
            # you are now the leader
            if self.IDLE < self.state < self.ACKNOWLEDGE:
                # in middle of protocol and have not yet acknowledged, abort
                for i in self.others:
                    try:
                        connectSocket = socket(AF_INET, SOCK_STREAM)
                        connectSocket.connect((address, self.PORT_BASE + i))
                        self.send(connectSocket, 'abort')
                    except:
                        continue
                aborted = True
            elif self.state == self.ACKNOWLEDGE:
                # have acknowledged, check if commit
                with open('leaderDT.txt', 'r') as logfile:
                    lastAct = logfile.readlines()[-1]
                    if lastAct.strip() == 'commit':
                        # have committed
                        for i in self.others:
                            try:
                                connectSocket = socket(AF_INET, SOCK_STREAM)
                                connectSocket.connect((address, self.PORT_BASE + i))
                                self.send(connectSocket, 'commit')
                            except:
                                continue
                    else:
                        for i in self.others:
                            try:
                                connectSocket = socket(AF_INET, SOCK_STREAM)
                                connectSocket.connect((address, self.PORT_BASE + i))
                                self.send(connectSocket, 'abort')
                            except:
                                continue
                        aborted = True
            if self.state != self.IDLE:
                if aborted:
                    self.abort()
                    self.send(self.master, 'resp abort')
                else:
                    self.send(self.master, 'resp commit')
            if sock:
                self.comm_channels.remove(sock)
            self.send(self.master, str(self.index) + ' ' + str(self.leader))
            self.send(self.master, 'coordinator ' + str(self.leader))


    def run(self):
        global address
        while self.valid:
            try:
                # listen for input from all channels
                if self.state == self.IDLE:
                    # Send info after transaction if we missed someone.
                    for sock in self.send_info:
                        self.send(sock, str(self.leader) + ' ' + ','.join(self.upset) + ' ' + ' '.join([key + "," + value for key,value in self.library.items()]))

                if self.index == self.leader:
                    # give coordinator time to send heartbeat
                    (active, _, _) = select(self.comm_channels, [], [], self.COORDINATOR_TIMEOUT)
                else:
                    (active, _, _) = select(self.comm_channels, [], [], self.TIMEOUT)

                for sock in active:
                    # We are receving a new connection, so accept it.
                    if (sock == self.my_sock):
                        (newsock, _) = self.my_sock.accept()
                        self.comm_channels.append(newsock)
                    else:
                        # Are we communicating with master, coord, or other servers?
                        line = sock.recv(1024)
                        if not line:
                            self.comm_channels.remove(sock)
                        for data in line.split('\n'):
                            #self.send(self.master, str(self.index) + ' ' + data)
                            if data == '':
                                continue
                            if (sock == self.master):
                                #self.send(self.master, str(self.index) + ' received from master')
                                self.handle_master_comm(sock, data)
                            else:
                                #self.send(self.master, str(self.index) + ' received from server')
                                self.handle_server_comm(sock, data)
                self.heartbeat(active)
            except Exception, e:
                self.send(self.master, str(e))
                self.close()
                break

    # send heartbeat to ensure coordinator is alive
    # if not coordinator, 
    def heartbeat(self, active):
        global n, address
        if (self.index == self.leader):
            # you are coordinator, send everyone a heartbeat
            for i in xrange(n):
                try:
                    if i == self.index:
                        continue
                    connectSocket = socket(AF_INET, SOCK_STREAM)
                    connectSocket.connect((address, self.PORT_BASE + i))
                    self.send(connectSocket, 'heartbeat')
                except:
                    continue
        elif active:
            # do nothing
            return
        else:
            # timed out waiting for heartbeat
            self.termination()

    # Handles communication between normal servers.
    def handle_server_comm(self, sock, data):
        line = data.split('\n')
        for l in line:
            s = l.split()
            if s[0] == 'info':
                # new process is asking for information, send leaderpid
                try:
                    self.send(sock, str(self.leader) + ' ' + ','.join(self.upset) + ' ' + ' '.join([key + "," + value for key,value in self.library.items()]))
                except:
                    continue
            if s[0] == 'voteREQ':
                self.participantVoteREQ(sock, s)
            if s[0] == 'heartbeat':
                # everything is fine, continue
                continue

    # Handles communication between servers (coord or normal) and master
    def handle_master_comm(self, sock, data):
        line = data.split('\n')
        #self.send(self.master, "Received master comm " + data)
        for l in line:
            s = l.split()
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
                sys.exit(0)
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
                self.crashVoteREQ = (True, [int(i) for i in s[1:]])
            elif s[0] == 'crashPartialPreCommit':
                self.crashPartialPreCommit = (True, [int(i) for i in s[1:]])
            elif s[0] == 'crashPartialCommit':
                self.crashPartialCommit = (True, [int(i) for i in s[1:]])
            elif s[0] == 'status':
                # delete this command
                self.send(self.master, str(self.index) + ' ' + str(self.crashAfterVote))


    def voteReq(self, cmd, data):
        global n, address
        self.currCmd = cmd
        self.currData = data
        # return True if all processes vote commit, else return False
        success = True
        leaderLog = 'leaderDT.txt'
        writedata = ''
        for songName in self.library:
            writedata += '%s,%s ' % (songName, self.library[songName])
        writedata += '\nSTART_3PC %s,%s\n' % (self.currCmd, self.currData)
        self.logwrite(self.log, writedata)
        self.logwrite(leaderLog, writedata)
            
        if not self.vote:
            # short circuit because coordinator votes no
            self.logwrite(self.log, writedata)
            self.logwrite(leaderLog, writedata)
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
        participants.remove(str(self.index))
        self.logwrite(self.log, ','.join(participants) + '\n')
        self.logwrite(leaderLog, ','.join(participants) + '\n')
        # sent out all requests, inform participants of all other participants
        #participants.remove(str(self.index))
        for i,s in zip(participants, p_sock):
            try:
                if not self.crashVoteREQ[0]:
                    self.send(s, request)
                    continue

                if self.crashVoteREQ[0] and (int(i) in self.crashVoteREQ[1]):
                    self.send(s, request)
            except:
                continue
        vote = 'yes' if self.vote else 'no'
        self.logwrite(self.log, vote + '\n')
        self.logwrite(leaderLog, vote + '\n')
        if self.crashVoteREQ[0]:
            sys.exit(0)

        ##########
        # VOTING #
        ##########
        if self.crashAfterVote:
            sys.exit(0)
        acks = 0
        while (acks != len(p_sock)):
            try:
                # wait for participants to send votes
                (active, _, _) = select(p_sock, [], [], self.TIMEOUT)
                for sock in active:
                    data = sock.recv(1024)
                    if not data:
                        # closed socket, participant failure
                        sock.close()
                        raise
                    for votes in data.split('\n'):
                        if votes == '':
                            continue
                        if (votes == 'False'):
                            # participant voted false
                            success = False
                        # received a response
                        acks += 1
                if not success:
                    # decide abort
                    self.logwrite(self.log, 'abort\n')
                    self.logwrite(leaderLog, 'abort\n')
                    break
            except:
                # participant failure
                success = False
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
        self.logwrite(self.log, 'precommit\n')
        self.logwrite(leaderLog, 'precommit\n')
        for i,s in zip(participants, p_sock):
            try:
                if not self.crashPartialPreCommit[0]:
                    self.send(s, 'precommit')
                    continue

                if self.crashPartialPreCommit[0] and (int(i) in self.crashPartialPreCommit[1]):
                    self.send(s, 'precommit')
            except:
                continue
        if self.crashPartialPreCommit[0]:
            sys.exit(0)

        #############
        # PRECOMMIT #
        #############
        self.logwrite(self.log, 'ack\n')
        self.logwrite(leaderLog, 'ack\n')
        if self.crashAfterAck:
            sys.exit(0)
            return
        acks = 0
        while (acks != len(p_sock)):
            try:
                # wait for acks in precommit stage
                (active, _, _) = select(p_sock, [], [], self.TIMEOUT)
                for sock in active:
                    data = sock.recv(1024)
                    if not data:
                        # socket closed, participant failure
                        sock.close()
                        raise
                    for votes in data.split('\n'):
                        if votes == '':
                            continue
                        if (votes == 'ack'):
                            # received ack
                            acks += 1
            except:
                # participant failure
                break
        if (acks != len(p_sock)):
            self.logwrite(self.log, 'abort\n')
            self.logwrite(leaderLog, 'abort\n')
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
        self.logwrite(self.log, 'commit\n')
        self.logwrite(leaderLog, 'commit\n')
        for i,s in zip(participants, p_sock):
            try:
                if not self.crashPartialCommit[0]:
                    self.send(s, 'commit')
                    continue

                if self.crashPartialCommit[0] and (int(i) in self.crashPartialCommit[1]):
                    self.send(s, request)
            except:
                continue
        if self.crashPartialCommit[0]:
            sys.exit(0)
        return success

    def participantVoteREQ(self, sock, s):
        self.state = self.FIRSTVOTE
        self.currCmd = s[1]
        self.currData = s[2]
        self.others = list(s[3])
        self.others.remove(str(self.index))
        # write to DT log
        writedata = ''
        for songName in self.library:
            writedata += '%s,%s ' % (songName, self.library[songName])
        writedata += '\n'
        writedata += 'START_3PC %s,%s\n' % (self.currCmd, self.currData)
        writedata +=  ','.join(self.others) + '\n'
        vote = 'yes' if self.vote else 'no'
        writedata += vote + '\n'
        self.logwrite(self.log, writedata)
        # send vote
        coordinatorFailure = False
        self.state = self.PRECOMMIT
        try:
            self.send(sock, self.vote)
        except:
            coordinatorFailure = True
        if self.crashAfterVote:
            sys.exit(0)
        if coordinatorFailure:
            self.termination(sock)
            return
        self.vote = True

        sock.settimeout(self.TIMEOUT)

        #############
        # PRECOMMIT #
        #############
        try:
            data = sock.recv(1024)
            if not data:
                # closed socket, coordinator failure
                sock.close()
                raise
            for line in data.split('\n'):
                if line == '':
                    continue
                elif (line == 'abort'):
                    # protocol aborted
                    self.logwrite(self.log, 'abort\n')
                    self.abort()
                    return
                elif (line == 'precommit'):
                    break
        except:
            self.termination(sock)
            return
        # write to log
        self.logwrite(self.log, 'precommit\nack\n')
        coordinatorFailure = False
        try:
            self.send(sock, 'ack')
        except:
            coordinatorFailure = True
        if self.crashAfterAck:
            sys.exit(0)
        if coordinatorFailure:
            self.termination(sock)
            return
        

        ##########
        # COMMIT #
        ##########
        try:
            data = sock.recv(1024)
            if not data:
                # closed socket, coordinator failure
                sock.close()
                raise
            for line in data.split('\n'):
                if line == '':
                    continue
                if (line == 'abort'):
                    # protocol aborted
                    self.logwrite(self.log, 'abort\n')
                    self.abort()
                    return
                elif (line == 'commit'):
                    break
        except:
            self.termination(sock)
            return
        # write to log
        self.logwrite(self.log, 'commit\n')
        songName = self.currData.split(',')[0]
        if self.currCmd == 'delete':
            if songName in self.library:
                del self.library[songName]
        if self.currCmd == 'add':
            url = self.currData.split(',')[1]
            self.library[songName] = url
        # protocol complete, use abort to clear
        self.abort()

    def logwrite(self, logfile, data):
        with open('logfile', 'a') as logfile:
            logfile.write(data)

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
