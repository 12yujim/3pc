# Main process file for PAXOS

import sys, os
import subprocess
import time
from threading import Thread, Lock
from socket import SOCK_STREAM, socket, AF_INET, SOL_SOCKET, SO_REUSEADDR
from select import select
from replica import Replica
from leader  import Leader
from acceptor import Acceptor

address = 'localhost'
n = 0


def main():
    global n, address

    # Read in command line arguments and start the different server parts.
    index = int(sys.argv[1])
    n = int(sys.argv[2])
    port = int(sys.argv[3])

    replica   = Replica(index, address, port)
    leader    = Leader(n, index, address, Lock())
    acceptor  = Acceptor(index, address)

    # Start the acceptor, then leader, then replica.
    acceptor.start()
    time.sleep(.1)
    leader.start()
    time.sleep(.1)
    replica.start()
    time.sleep(.1)
    sys.exit(0)
    

if __name__ == '__main__':
    main()