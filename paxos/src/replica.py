# File containing the replica class

import sys, os
import subprocess
import time
from threading import Thread, Lock
from socket import SOCK_STREAM, socket, AF_INET, SOL_SOCKET, SO_REUSEADDR
from select import select
from ast import literal_eval

address = 'localhost'
baseport = 20000
n = 3

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

        self.msgList = []
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
        self.leader.connect((address, baseport+3*self.index+1))

        # Listen for master connection
        self.my_sock.bind((address, self.my_port))
        self.my_sock.listen(100*n)

        print("listening for master")

        # Listen for master connection
        self.master.bind((address, self.master_port))
        self.master.listen(5)
        (self.master, _) = self.master.accept()

        self.comm_channels = [self.master, self.leader, self.my_sock]

        print("Accepted")

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

                    for unparsed in line.split('\n'):
                        data = unparsed.split(' ')
                        self.send(self.master, data)
                        if data == '':
                            continue
                        elif data[0] == 'msg':
                            print(data)
                            self.msgList.append(int(data[1]))
                            self.propose((int(data[1]), data[2]))
                        elif data[0] == 'decision':
                            s = int(data[1])
                            p = self.tup(data[2:])
                            print(s, p)
                            self.decisions[s] = p
                            print(self.slot_num)
                            while (self.slot_num in self.decisions.keys()):
                                p2 = self.decisions[self.slot_num]
                                all_props = [pval for (slot, pval) in self.proposals.items() if (slot == self.slot_num)]
                                for prop in all_props:
                                    if (prop != p2):
                                        self.propose(prop)
                                self.perform(p2)
                        elif data[0] == 'get' and data[1] == 'chatLog':
                            self.getChat()
                        elif 'crash' in unparsed:
                            self.handleCrash(data[0])
                        #self.send(self.master, str(self.index) + ' received from master')
                        #self.handle_master_comm(sock, data)

    def handleCrash(self, cmd):
        if cmd == 'crash':
            self.crash()
        else:
            print("Sending CRASH to leader " + str(self.index))
            self.send(self.leader, cmd)

    def getChat(self):
        self.send(self.master, 'chatLog ' + ','.join(self.chatLog))

    def ack(self, msgID, seqID):
        ackMsg = 'ack {} {}'.format(msgID, seqID)
        self.send(self.master, ackMsg)
        print(ackMsg)

    def propose(self, p):
        # Propose a clients message to the next available slot.
        if p not in self.decisions.values():
            combined = self.decisions.keys() + self.proposals.keys()

            # First find max slot, then iterate to find next available.
            try:
                smax = max(combined)
            except:
                smax = 1

            s = 1
            for i in range(1,smax+2):
                if not i in combined:
                    print(i)
                    s = i
                    break

            self.proposals[s] = p
            propose = 'propose ' + str(s) + ' ' + str(p)
            self.send(self.master, propose)
            self.send(self.leader, propose)

    def perform(self, p):
        # Basically just send a repsonse back to client
        exists = False
        print(p)
        for s in self.decisions:
            if (s < self.slot_num) and (self.decisions[s] == p):
                self.slot_num += 1
                exists = True
                break
        if not exists:
            cid = p[0]
            msg = p[1]
            self.chatLog.append(msg)
            self.log(msg)
            self.slot_num += 1
            print(cid)
            print(self.msgList)
            if cid in self.msgList:
                self.msgList.remove(cid)
                # only receiving process sends ack
                self.ack(cid, len(self.chatLog))

    def log(self, chat):
        with open(self.chatLogFile, 'a') as logfile:
            logfile.write(chat)

    def send(self, sock, s):
        sock.send(str(s) + '\n')

    def crash(self):
        # crashes the associated acceptor, replica, and leader
        crashCmd = "ps aux | grep \"src/server.py {}\" | awk '{{print $2}}' | xargs kill".format(self.index)
        subprocess.call(crashCmd)

    def tup(self, sl):
        return literal_eval(' '.join(sl))

