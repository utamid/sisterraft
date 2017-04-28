import socket
import sys
import os
import psutil
import time
import json

#DAEMON SCRIPT

#Define daemon ip and port
daemon_ip = 127.0.0.1
daemon_port = 10040

#Define nodes ports, nodes ip 127.0.0.1
nodes_ip = 127.0.0.1
nodes_ports = []


#Read nodes ports
num_nodes = len(sys.argv) - 1;
for i in range(0, num_nodes)
    nodes_ports[i] = sys.argv[i + 1]

#MAIN
while 1:
    count = 0
    while (count < 3):
		#Create Socket
        try:
	        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	        print 'Socket created'
        except socket.error, msg:
	        print 'Failed to create socket'
	        sys.exit();
	    
	    #Create connection
	    try:
		    s.connect((nodes_ip, nodes_ports[count]))
        except socket.error, msg:
            print 'Failed to open connection'
        print 'Connected on 127.0.0.1:' + nodes_ports[count]
        
        """
        Data transmission
        """
        
        #Define data
        free_mem = int(100-psutil.virtual_memory().percent)
        data = {}
        data['ip'] = daemon_ip
        data['port'] = daemon_port
        data['val'] = free_mem
        data['type'] = ''
        message = json.dumps(data)

        #Send data
        try :
            s.sendall(message)
        except socket.error:
            print 'Send failed'
        print 'Message send successfully'
        
        #Done, close connection
        s.close()
        
        #Update counter
        count = count + 1
    
    time.sleep(5)
