#!/usr/bin/env python
"""
The master program for CS5414 Bayou project.
"""

import sys, os
import subprocess
import time
from threading import Thread, Lock
from socket import SOCK_STREAM, socket, AF_INET

address = 'localhost'
threads = {}
wait_resp = False

debug = False

class ClientHandler(Thread):
    def __init__(self, index, address, port):
        Thread.__init__(self)
        self.daemon = True
        self.index = index
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.connect((address, port))
        self.buffer = ""
        self.valid = True

    def run(self):
        global threads, wait_resp
        while self.valid:
            if "\n" in self.buffer:
                (l, rest) = self.buffer.split("\n", 1)
                self.buffer = rest
                wait_resp = False
                print l
            else:
                try:
                    data = self.sock.recv(1024)
                    #sys.stderr.write(data)
                    self.buffer += data
                except:
                    #print sys.exc_info()
                    self.valid = False
                    del threads[self.index]
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

def send(index, data, set_wait=False):
    global threads, wait_resp
    while wait_resp:
        time.sleep(0.01)
    pid = int(index)
    if set_wait:
        wait_resp = True
    threads[pid].send(data)

def exit(is_exit=False):
    global threads, wait_resp

    wait = wait_resp
    wait = wait and (not is_exit)
    while wait:
        time.sleep(0.01)
        wait = wait_resp

    time.sleep(2)
    for k in threads:
        threads[k].close()
    subprocess.Popen(['./stopall'], stdout=open('/dev/null'), stderr=open('/dev/null'))
    sys.stdout.flush()
    time.sleep(1)
    if is_exit:
        os._exit(0)
    else:
        sys.exit(0)

def timeout():
    time.sleep(120)
    exit(True)

def main():
    global threads, wait_resp, debug
    timeout_thread = Thread(target = timeout, args = ())
    timeout_thread.daemon = True
    timeout_thread.start()

    while True:
        line = ''
        try:
            line = sys.stdin.readline()
        except: # keyboard exception, such as Ctrl+C/D
            os._exit(0)
        if line == '': # end of a file
            exit()
        line = line.strip() # remove trailing '\n'
        if line == 'exit': # exit when reading 'exit' command
            exit()
        sp1 = line.split(None, 1)
        sp2 = line.split()
        if len(sp1) != 2: # validate input
            continue
        pid = int(sp2[0]) # first field is pid
        cmd = sp2[1] # second field is command
        if cmd[:5] == 'start':
            port = int(sp2[2])
            # start the process
            command = './' + cmd[5:].lower()
            if debug:
                subprocess.Popen([command, str(pid), sp2[2]])
            else:
                subprocess.Popen([command, str(pid), sp2[2]], stdout=open('/dev/null'), stderr=open('/dev/null'))
            # sleep for a while to allow the process be ready
            time.sleep(1)
            # connect to the port of the pid
            handler = ClientHandler(pid, address, port)
            threads[pid] = handler
            handler.start()
        else:
            if cmd == 'get' or cmd == 'printLog': # get chatLog
                send(pid, sp1[1], set_wait=True)
            else: # other commands
                print cmd
                send(pid, sp1[1], set_wait=False)
        time.sleep(1)

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'debug':
        debug = True
    main()
