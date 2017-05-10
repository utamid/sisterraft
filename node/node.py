import json, queue
import sys, random
import urllib.request
import signal, os, time
import threading, socket

from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
from socketserver import ThreadingMixIn 

#Read list of nodes from file
nodes_file = open("nodeslist.txt", "r")
nodeslist = nodes_file.read().split('\n')
n_nodes = nodeslist.size()

#Read list of servers from file
server_file = open("serverlist.txt", "r")
serverlist = server_file.read().split('\n')
n_server = serverlist.size()

def getIP(address):
	return address.rsplit(':')[0]
	
def getPort(address):
	return address.rsplit(':')[1]


class WorkerHandler(BaseHTTPRequestHandler):
    def get_lowest_load_address(self):
        highest_free_mem = 0
        addr = None
        for key, value in node.servers_load.items():
        if (value > highest_free_mem):
            highest_free_mem = value
            addr = key
        return addr

    def request_data(self, addr, n):
        return urllib.request.urlopen("http://" + addr + "/" + n).read()

    def do_GET(self):
        try:
            args = self.path.split('/')
            if len(args) != 2:
                raise Exception()
            n = args[1]
            data = None
            while(1):
                lowest_load_addr = self.get_lowest_load_address()
                if (lowest_load_addr == None):
                    raise Exception()
                try:
                    data = self.request_data(lowest_load_addr, n)
                    if (data):
                        break
                except:
                    node.append_log(LogEntry(node.term, lowest_load_addr, 0))
                    node.update_log(node.log_idx)
                    time.sleep(0.2)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(data)
        except Exception as ex:
            self.send_response(500)
            self.end_headers()
            print(ex)

class Log:
	"""
	Log consist of entries that specify term, command, server load, and commit val
	"""
    def __init__(term, cmd, load):
	    self.term = term
	    self.command = cmd
	    self.args = load
	    self.commit_val = 0
        
	#jsonify message
	def getLog:
        msg = {}
        msg['term'] = self.term
        msg['cmd'] = self.command
        msg['args'] = self.args
        return json.dumps(msg)

class Node:
    """
	Init
	"""    
	def __init__(self, self_ip, self_port):
        #Rank
        """
        Rank is divided to 3 categories: leader(2), follower(0), and candidate(1)
        """
        self.rank = 0
    
        #Vote
        self.vote = 0
    
        #Own Address
        self.ip = self_ip
        self.port = self_port
    
        #Term
        self.term = 0
        
        #Log
        self.log = []
        
        #Leader ID, initiated with node 0
        self.leaderID = 0
        
    """
    GetState
    """
    def getState(self):
		return (self.state)
		
    """
    Send Receive Message
    """
    
    #Send message in synchronous connection
    def send_msg(self, dest_ip, dest_port, msg):
		try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((dest_ip, dest_port))
            s.send(bytes(msg, 'UTF-8'))
            s.close()
		except:
            s.close()
        return
   
   #Asynchronous connection, use thread
   def send_msg_t(self, dest_ip, dest_port, msg):
       t = threading.Thread(target=self.send_msg, args = (dest_ip, dest_port, msg))
       t.daemon = True
       t.start()
       return
       
    def recv_msg(self, msg):
        """
        Flag description for reference
        dm = Daemon message
        vr = Vote request
        vo = Receive vote
        al = Append log
        """
	    data = json.loads(msg.decode('UTF-8'))
	    dest_ip = data['ip']
	    dest_port = data['port']
        dest_address = dest_ip + ":" + dest_port
        
        #Daemon message, if rank is leader
        if(data['flag'] == 'dm' and self.rank == 2):
            self.append_entries(Log(self.term, dest_address, data['load']))
            self.log[self.log_idx].commit_val += 1
            if(self.log[self.log_idx].commit_val >= n_nodes//2 + 1):
                self.commit_log(self.log_idx)
            #reset time
        
        elif(data['flag'] == 'vr'):
		    data_args = json.loads(data['args'])
		    reply = {}
		    #If term < sender term, match term, change state to follower
		    if(self.term < data_args['term']):
			    self.term = data_args['term']
			    self.rank = 0
	  	    #If term matches and state is follower, give vote
		    if(self.term == data_args['term'] and self.rank == 0):
			    reply['voteGranted'] = 1
		    #Else (state != follower or term is bigger), don't grant vote
		    else:
			    reply['voteGranted'] = 0
		    #Send reply to sender 
		    reply['term'] = self.term
		    reply['flag'] = 'vo'
		    self.send_msg_t(dest_ip, dest_port, json.dumps(reply))
	   
        elif(data['flag'] == 'vo'):
		    data_args = json.loads(data['args'])
		    
		    #Vote granted, increment vote
		    if (data_args['voteGranted']):
			    self.reset_timeout()
			    self.increment_vote()
		    #Vote declined, match term
            elif (self.term < data_val['term']):
			    self.rank = 0
			    self.term = data_val['term']
			    # Reset timeout
			    self.reset_timeout()

        elif(data['flag'] == 'al'):
            args = {}
            args['term'] = self.term
            data_val = json.loads(data['val'])
            # Self term is above leader's term
            if (data_val['term'] < self.term):
                args['success'] = 0
            else:
                self.term = data_val['term']
                self.state = 0
                self.reset_timeout()
            # If self have more log than leader undo log
            args['success'] = -1
            if (self.log_idx > data_val['prev_log_idx']):
                # Remove elements that are located after previous log index
                for i in range(self.log_idx - data_val['prev_log_idx']):
                    self.log.pop(-1)
                    self.log_idx -= 1
            if (self.log_idx < data_val['prev_log_idx']):
                args['success'] = -1
            # Previous data is equal
            elif (data_val['prev_log_term'] == self.log[data_val['prev_log_idx']][0].term):
                # Append all new entries
                for entry in data_val['entries']:
                    entry_data = json.loads(entry)
            self.append_log(LogEntry(entry_data['term'], entry_data['command'], entry_data['value']))
            args['success'] = self.log_idx + 1
            # Commit all uncommitted data
            self.commit_log(data_val['committed_idx'])
            self.send_message_async(jsonify_msg(APPEND_LOG_RESPONSE_IDENTIFIER, json.dumps(args)), sender)

            # Got append log RPC response      
        elif (data['flag'] == ai):
            data_val = json.loads(data['val'])
            if (data_val['success'] >= 0):
                self.next_idx[sender.__str__()] = data_val['success']
                self.update_log(data_val['success']-1)
            else:
                self.next_idx[sender.__str__()] -= 1
        return
        
    """
    Log Entry Handler
    """
    #State = Leader
    def append_entries(self, entry):
	    #Append to self
	    self.log_idx += 1
	    self.log.append(entry)
	    
	    #Broadcast to other nodes
	    logmsg = {}
	    logmsg['term'] = self.term
	    #Define previous entry to match
	    previdx = self.log_idx - 1
	    logmsg['prev_idx'] = previdx
	    logmsg['prev_term'] = self.log[previdx].term
	    
	    logmsg['entry'] = self.log[self.log_idx].getLog
	    logmsg['commit_idx'] = self.commit_idx
	    logmsg['flag'] = "al"
	    for nodes in nodeslist:
		    self.send_msg_t(json.dumps(logmsg), getIP(nodes), getPort(nodes))
		return
	     	    
	def commit_log(self, idx):
		file = open("log.txt", 'a')
		for i in range(self.commit_idx+1, idx+1):
		    entry = self.log[i]
            self.commit_idx += 1
            file.write(entry.getLog + "\n")
        file.close()
        return
        
    """
    Vote as Candidate, from Follower
    """
    
    def req_vote(self):
        self.reset_timeout()
		self.term += 1
		args = {}
		args['log_idx'] = self.log_idx
		args['term'] = self.term
		args['flag'] = 'vr'
		self.broadcast_message(json.dumps(args), self.nodes)
		return
		
    def recv_vote(self):
        self.vote += 1
	    if (self.vote >= nodes/2 + 1):
            self.rank = 2
            self.vote = 0
        return
	
	def announce_candidate(self):
	    self.rank = 1
	    self.req_vote()
	    self.recv_vote()
	    return
   
    """
    Vote other Leader
    """
    def giveVote():
	
#Main
if (len(sys.argv) == 3):
    self_ip = sys.argv[1]
    self_port = sys.argv[2]
else:
    print 'Usage: [IP_ADDRESS] [PORT]'
    sys.exit()

node = Node(self_ip, self_port)
