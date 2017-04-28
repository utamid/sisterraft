#SERVER SCRIPT
"""
A server runs on a machine that contains server, daemon, and worker script.
Server indicates that it is alive by sending its workload.
Timeout defined as 5 seconds.
"""

import socket
import sys
import threading

#Define server's life
reset = False
alive = True

#Define server IP and Port
server_ip = '127.0.0.1'
server_port = 10050

#Class for life thread
class Life(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.event = threading.Event()
        self.lifetime = 5

    def run(self):
        while(alive):
            while self.lifetime > 0:
                if(reset):
				    self.lifetime = 5
                else:
				    self.lifetime -= 0.1
                if (self.lifetime) <= 0 :
                    alive = False
                self.event.wait(0.1)
    def stop(self):
        self.event.set()

#Create socket
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
except socket.error, msg:
    print 'Failed to create socket'
    sys.exit()
print 'Socket created'

#Bind socket
try:
    s.bind((server_ip, server_port))
except socket.error:
    print 'Error binding'
    sys.exit()
print 'Bind socket completed'

#Listen
s.listen(10)
print 'Listening...'

#Start Thread
life = Life()
life.start()

#Main
while(alive):
    #Receive connection. Everytime a request received, timeout is reset
    conn, addr = s.accept()
    while 1:
        print 'Connected with ' + addr[0] + ':' + str(addr[1])
        data = conn.recv(1024)
        if data:
            print data
            reset = True
conn.close()
s.close()
life.stop()
