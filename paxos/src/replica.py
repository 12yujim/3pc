# File containing the replica class

import sys, os
import subprocess
import time
from threading import Thread, Lock
from socket import SOCK_STREAM, socket, AF_INET, SOL_SOCKET, SO_REUSEADDR
from select import select

address = 'localhost'
baseport = 20000
n = 0

class Replica(Thread):

    def __init__(self, index, address, port):
        global n, baseport

        Thread.__init__(self)
        self.index = index
        self.master_port = port
        self.my_port = baseport + self.index*3
        
        self.master = socket(AF_INET, SOCK_STREAM)
        self.master.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

        self.leader = socket(AF_INET, SOCK_STREAM)
        self.leader.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

        self.my_sock = socket(AF_INET, SOCK_STREAM)
        self.my_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

        self.slot_num = 1
        self.proposals = {}
        self.decisions = {}

        self.chatLogFile = 'rep{}.txt'.format(index)
        self.chatLog = []
        try:
            with open(self.chatLogFile, 'r') as logfile:
                self.chatLog = logfile.read().split(',')
        except:
            pass

    def run(self):
        global n, address

        # Connect to the leader socket.
        self.leader.connect((address, baseport+3*self.index+2))

        # Listen for master connection
        self.my_sock.bind((address, self.my_port))
        self.my_sock.listen(n)

        print("listening for master")

        # Listen for master connection
        self.master.bind((address, self.master_port))
        self.master.listen(5)
        (self.master, _) = self.master.accept()

        self.comm_channels = [self.master, self.leader, self.my_sock]

        self.send(self.master, "Accepted master connection " + str(self.index))

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
                        elif data[0] == 'msg':
                            self.propose(data[1:])
                        elif data[0] == 'decision':
                            s = data[1]
                            p = data[2:]
                            self.decisions[s] = p
                            while (self.slot_num in self.decisions and self.decisions[self.slot_num] != p):
                                p2 = self.decisions[self.slot_num]
                                if (self.slot_num in self.proposals and self.proposals[self.slot_num] != p2):
                                    self.propose(self.proposals[self.slot_num])
                                self.perform(p2)
                        elif data[0] == 'get' and data[1] == 'chatLog':
                            self.getChat()
                        else:
                            self.handleCrash(data)
                        #self.send(self.master, str(self.index) + ' received from master')
                        #self.handle_master_comm(sock, data)

    def handleCrash(self, cmd):
        if cmd == 'crash':
            # figure out how to crash
            sys.exit(0)
        elif cmd == 'crashAfterP1b':
            pass
        elif cmd == 'crashAfterP2b':
            pass
        else:
            multiCmd = cmd.split()
            if multiCmd[0] == 'crashAfterP1a':
                pass
            elif multiCmd[0] == 'crashAfterP2a':
                pass
            elif multiCmd[0] == 'crashDecision':
                pass

    def getChat(self):
        self.send(self.master, ','.join(self.chatLog))

    def ack(self, msgID, seqID):
        ackMsg = 'ack {} {}'.format(msgID, seqID)

    def propose(self, p):
        p.prepend(self.master)
        # Propose a clients message to the next available slot.
        if p not in self.decisions.keys():
            try:
                decisionMax = max(sd for sd in self.decisions)
            except:
                decisionMax = 0
            try:
                proposalMax = max(sp for sp in self.proposals)
            except:
                decisionMax = 0
            s = max(decisionMax, proposalMax) + 1
            self.proposals[s] = p
            propose = 'propose,{},'.format(s) + ';'.join(p)
            self.send(self.leader, propose)

    def perform(self, p):
        # Basically just send a repsonse back to client
        exists = False
        for s in self.decisions:
            if (s < self.slot_num) and (self.decisions[s] == p):
                self.slot_num += 1
                exists = True
                break
        if not exists:
            ret = p[0]
            cid = p[1]
            msg = p[2]
            self.chatLog.append(msg)
            self.log(msg)
            self.slot_num += 1
            self.ack(cid, len(self.chatLog))

    def log(self, chat):
        with open(self.chatLogFile, 'a') as logfile:
            logfile.write(chat)

    def send(self, sock, s):
        sock.send(str(s) + '\n')

