import socket
import sys
import json
import time
import random
import traceback
import logging
from threading import Thread
#from collections import deque
# are v assumig tht there will be a single poposer in a quorum ?? No... 
#if you are a proposer you shud have a dictionary for ur own work 
#When I am acting as a proposer.. canI concurrentln act as acceptor or shud Isend my "prepar" or "accept" atomically and then act as an acceptor
# I have then wait for proposer or acceptor action to finish to swithc to another

class ClientThread(Thread):

    def __init__(self,ip,port,conn,kiosk_obj,sys_config): 
        Thread.__init__(self) 
        self.ip = ip 
        self.port = port
        self.conn = conn
        self.kiosk = kiosk_obj
        self.config = sys_config
        #self.config.logger.info("[+] New socket thread started for " + ip + ":" + str(port))
        #logger.info('New Client\'s thread started')

    def run(self): 
            self.receiveData();
            

    def receiveData(self):
        while True:
            try:
                received_data = self.conn.recv(2048)
                if received_data != '':
                    received_data_json=json.loads(received_data)
                    request_type = received_data_json['type']
                    sender_id = received_data_json['senderID']
                    print 'Received some data..'+received_data
                    if request_type == 'prep':
                        if self.kiosk.HIGHEST_PREPARE_ID is None or \
                        received_data_json['proposalNum'] > self.kiosk.HIGHEST_PREPARE_ID[0] or \
                        ( received_data_json['proposalNum'] == self.kiosk.HIGHEST_PREPARE_ID[0] \
                            and int(sender_id) > self.kiosk.HIGHEST_PREPARE_ID[1]):
                            self.kiosk.HIGHEST_PREPARE_ID = (received_data_json['proposalNum'],int(sender_id))
                            #Populate the ack dictionary
                            print 'Populating ack dictionary now..'
                            self.kiosk.send_ack_prepare_dict[sender_id] = self.config.client_id
                    elif request_type == 'ack_prep':
                        self.kiosk.ack_counter += 1
                        req_id = None
                        if received_data_json['last_accept_id'] is not None:
                            req_id = received_data_json['last_accept_id'] #This will now be a tuple
                        self.kiosk.ack_arr.append({'id':req_id,'val':received_data_json['last_accept_val']})
                        print 'Ack requests received till now'
                        print self.kiosk.ack_arr
                        if self.kiosk.ack_counter == self.kiosk.majority_count:
                            print 'Got majority. I can now set accept val'
                            max_item = max(self.kiosk.ack_arr, key=lambda x:x['id'])
                            if max_item['val'] is None:
                                self.kiosk.FINAL_ACCEPT_VALUE_SENT = self.kiosk.CURRENT_MESSAGE
                            else:
                                self.kiosk.FINAL_ACCEPT_VALUE_SENT = max_item['val']

                            print 'Accept val to be sent '+str(self.kiosk.FINAL_ACCEPT_VALUE_SENT)
                            #Ready to send accept requests now
                            for key in self.config.REM_CLIENTS:
                                self.kiosk.send_accept_dict[key]=self.config.client_id

                            print 'Send accept dict is '
                            print self.kiosk.send_accept_dict
                            #The sender should also send ack accept request to all other users here
                            #Because it will accept its own request always. and so let other processes know about this ack_accept
                            #The other processes should get this ack_accept request.?
                            #Or may be I don't have to send it because of the way counters are handled. I initialize it to 1.
                            #1 means it has already has ack accept from itself.

                            
                    elif request_type == 'accept':
                        if received_data_json['proposalNum'] >= self.kiosk.HIGHEST_PREPARE_ID[0]:
                            self.kiosk.ACCEPTED_BALLT_ID = (received_data_json['proposalNum'],int(sender_id))
                            self.kiosk.ACCEPT_BALLT_VAL = received_data_json['accept_val']
                            #Populate the ack dictionary
                            #Broadcast to all processes now instead of just the sender (Phase 2)
                            print 'Populating ack accept dictionary now..'
                            self.kiosk.send_ack_accept_dict[sender_id] = self.config.client_id
                    elif request_type == 'ack_accept':
                        self.kiosk.accept_counter += 1
                        #The modified if condition makes sure a delayed ack accept from a process
                        #after the majority does not trigger further message sending
                        #Even if this is not sender, it will get ack accepts from ther processes and calculate majority (Phase 2)
                        if self.kiosk.accept_counter == self.kiosk.majority_count:
                            #Update the self.kiosk.ACCEPT_BALLT_VAL for the sender
                            #Confirm this with Ishani.
                            accept_id_tuple = (received_data_json['accept_id'][0],received_data_json['accept_id'][1])
                            #If this ballot number does not exist in log only then go ahead.
                            if not any(d['id'] == accept_id_tuple for d in self.kiosk.log):
                                self.kiosk.ACCEPT_BALLT_VAL = self.kiosk.FINAL_ACCEPT_VALUE_SENT
                                self.kiosk.ACCEPTED_BALLT_ID = (self.kiosk.CURRENT_PREPARE_ID,int(self.config.client_id))
                                print 'Received accept from majority'
                                print 'Broadcast commit message now'
                                #Ready to send commit requests now
                                for key in self.config.REM_CLIENTS:
                                    self.kiosk.send_commit_dict[key]=self.config.client_id

                                print 'Send commit dict is '
                                print self.kiosk.send_commit_dict
                                print 'Also add '+str(self.kiosk.ACCEPT_BALLT_VAL)+' to my log right now'
                                self.kiosk.log.append({'id':self.kiosk.ACCEPTED_BALLT_ID,'val':self.kiosk.ACCEPT_BALLT_VAL})
                                #Reset counters
                                self.kiosk.ack_counter = 1
                                self.kiosk.accept_counter = 1
                                #Reset acknowledgement array
                                self.kiosk.ack_arr = []
                                print 'Contents of the log========='
                                print self.kiosk.log
                                #Current process is the leader!
                                self.kiosk.CURRENT_LEADER = self.config.client_id
                                #Inform the other processes that you are the current leader through some broadcast.
                    elif request_type=='commit':
                        print 'tym to add in my log'
                        accept_id_tuple = (received_data_json['accept_id'][0],received_data_json['accept_id'][1])
                        self.kiosk.log.append({'id':accept_id_tuple,'val':received_data_json['msg']})
                        #Reset counters
                        self.kiosk.ack_counter = 1
                        self.kiosk.accept_counter = 1
                        #Reset acknowledgement array
                        self.kiosk.ack_arr = []
                        print 'Contents of the log========='
                        print self.kiosk.log
                        #Reset kiosk variables as log entry is committed
                        print 'Resetting the variables'
                        self.kiosk.ACCEPTED_BALLT_ID= None #This will be a tuple now
                        self.kiosk.CURRENT_PREPARE_ID=None #This is just an integer. Represents the proposal number for the sender
                        self.kiosk.CURRENT_MESSAGE=None
                        self.kiosk.FINAL_ACCEPT_VALUE_SENT=None
                        self.kiosk.ACCEPT_BALLT_VAL=None

                    elif request_type== 'leaderAuth':
                        print 'I got leader auth '

                    else:
                        print 'some error'

                    
                    

            except Exception as e:
                #print e
                #self.config.logger.info("Some Exception")
                print "Some Exception"
                traceback.print_exc()
                break
                self.conn.close()
                serverSocket.close()


#This class will have all the System specific configuration
class SystemConfig():
    def __init__(self):

        #Initialize client_id
        client_id = ' '.join(sys.argv[1:])
        self.client_id = client_id


        #Read data from the file
        with open('config.json') as data_file:    
            data = json.load(data_file)
            self.TCP_IP = data['tcp_ip']
            self.TCP_PORT = data[self.client_id]['tcp_port']
            self.REPLY_DELAY = data['reply_delay']
            self.REQUEST_DELAY = data['request_delay']
            self.TOTAL_CLIENTS = data['total_clients']
            self.PROC_ID_MAPPING = data['reverse_dict']
            self.per_threshold = data['per_threshold']

        #Initialize logger
        self.logger= logging.getLogger(__name__)

        #Initialze channel states, incoming and outgoing channels
        self.TOTAL_CLIENTS.remove(self.TCP_PORT)
        self.channel_states = {}
        self.send_channels = {}
        self.receive_channels = {}
        self.queue_send_markers = []
        self.REM_CLIENTS = []
        print 'Total clients is '
        print self.TOTAL_CLIENTS
        for client in self.TOTAL_CLIENTS:
            key = self.PROC_ID_MAPPING[str(client)]['proc_id']
            self.REM_CLIENTS.append(key)
            self.channel_states[key] = {}
            self.channel_states[key]['flag'] = False
            self.channel_states[key]['state'] = []

#This class will have all the Kiosk specific configuration
class Kiosk():
    def __init__(self):
        self.TICKETS=100
        self.CURRENT_LEADER=None
        self.ACCEPTED_BALLT_ID= None #This will be a tuple now
        self.HIGHEST_PREPARE_ID=None #This will be a tuple now
        self.CURRENT_PREPARE_ID=None #This is just an integer. Represents the proposal number for the sender
        self.CURRENT_MESSAGE=None
        self.FINAL_ACCEPT_VALUE_SENT=None
        self.ACCEPT_BALLT_VAL=None
        self.ack_arr = []
        self.majority_count = 2
        self.ack_counter = 1
        self.accept_counter = 1
        ##Log list
        self.log = []
        ##Introducing 5 Dicts
        self.send_prepare_dict={}; ##Dict for sending prepare
        self.send_ack_prepare_dict={}; ##Dict for sending ack to prepare
        self.send_accept_dict={}; ##Dict for sending accept Phase 2
        self.send_ack_accept_dict={}; ##Dict for sending ack to accept
        self.send_commit_dict={};##Dict for sending commit as distinguished learner

class Server():

    #Constructor for Server Class
    def __init__(self,kiosk_obj,sys_config):
        self.kiosk = kiosk_obj
        self.config = sys_config
        self.threads = []
        self.clients = []
        self.serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.serverSocket.bind((self.config.TCP_IP, self.config.TCP_PORT))
        self.serverSocket.listen(10)


        self.setup()

    def setup(self):
        #Initialize snapshot dictionary
        self.startLoggin()
        #create_snapshot_dict()
        
        print 'You are in ! Send some Message!'
        sys.stdout.write('[Me] ')
        sys.stdout.flush()
        masterThread = Thread(target=self.listenNewClients) #creates new thread for every client that connects to me !
        masterThread.start()
        connectThread = Thread(target=self.connectAsClient) #thread to connect as clients to others
        connectThread.setDaemon(True)
        connectThread.start()
        sendThread = Thread(target=self.sendData) #thread to send data !!
        sendThread.setDaemon(True)
        sendThread.start()
        self.checkForClientRqst()

    #Method to initialize Logging
    def startLoggin(self):
        #I can access logger through self.config.logger
        print 'I have started logging ! Check the logs !'
        log_file_name= self.config.client_id+ "_client.log"

        self.config.logger.setLevel(logging.DEBUG)

        #creating logger file
        handler=logging.FileHandler(log_file_name)
        handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.config.logger.addHandler(handler)

    ########################
    #  Connection helpers  #
    ########################
    def listenNewClients(self):
        while True:
            try:
                #self.config.logger.info('Waiting for connections...')
                #print 'Waiting for connections...'
                (conn, (ip,port)) = self.serverSocket.accept()
                #Pass the kiosk and system config instances to ClientThread class as well
                newthread = ClientThread(ip,port,conn,self.kiosk,self.config)
                self.threads.append(newthread)
                self.clients.append(conn)
                newthread.start()
               
                #conn.settimeout(60)
            except:
                #self.config.logger.info('listenNewClients Exception')
                #print 'Some problem !'
                conn.close()
                self.serverSocket.close()
                break

    def connectAsClient(self):
        #self.config.logger.info('Gonna try and connect to following ports now..')
        print 'Gonna try and connect to following ports now..'
        #print 'Gonna try and connect to following ports now..'
        #self.config.logger.info(self.config.TOTAL_CLIENTS)
        print self.config.TOTAL_CLIENTS
        #print self.config.TOTAL_CLIENTS
        while True:
            time.sleep(5)
            try:
                if len(self.config.TOTAL_CLIENTS) > 0:
                    tcpClient = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                    #self.config.logger.info( 'Trying to connect to '+str(self.config.TOTAL_CLIENTS[0]))
                    print 'Trying to connect to '+str(self.config.TOTAL_CLIENTS[0])                 
                    tcpClient.connect((self.config.TCP_IP, self.config.TOTAL_CLIENTS[0]))
                    key = self.config.PROC_ID_MAPPING[str(self.config.TOTAL_CLIENTS[0])]['proc_id']
                    #self.config.logger.info('Key is '+key)
                    self.config.send_channels[key] = tcpClient
                    self.config.logger.debug( self.config.send_channels) 
                    #self.config.logger.info('Length of send channels')
                    print 'Length of send channels'
                    print len(self.config.send_channels)          
                    self.config.logger.debug( len(self.config.send_channels))
                    del self.config.TOTAL_CLIENTS[0]
                    #self.config.logger.info('Remaining clients are ')    
                    print 'Remaining clients are '
                    print self.config.TOTAL_CLIENTS             
                    self.config.logger.debug(self.config.TOTAL_CLIENTS)
                    
                else:
                    #self.config.logger.info('Have successfully connected to all clients!')
                    print 'Have successfully connected to all clients!'
                    break
            except Exception as e:
                #self.config.logger.info('Unable to connect')
                print 'Some error occured: ' + str(e)
                print 'Unable to connect'


    ########################
    #  Communication func  #
    ########################

    def sendData(self):
        #you can send proposal.
        #you can send acknolwdgemtnet.
        #you can send acceptance.
        #you can send acceptance ackn to distinguished learner.
        #you can send commit as distinguished learner.
        while True:
            try:
                #Sending messages and handling dictionaries goes here
                #print "Vacating all the dictionaries !!"
                # dictionaries format== {to:from, to:from, to:from} #can "to" be non unique??
                send_prep_list = list( self.kiosk.send_prepare_dict.keys() )
                for chan in send_prep_list:
                    print "@@@to_send_prepare" + chan
                    to_val=chan
                    from_val=self.kiosk.send_prepare_dict[chan]
                    print 'Sending prepare request to: '+ to_val
                    to_send = json.dumps({'senderID': from_val, 'proposalNum':self.kiosk.CURRENT_PREPARE_ID, 'type': 'prep','msg': 'makeMeLeader'})
                    self.config.send_channels[to_val].send(to_send)
                    del(self.kiosk.send_prepare_dict[chan])


                send_ack_prepare_list=list(self.kiosk.send_ack_prepare_dict.keys())
                for chan in send_ack_prepare_list:
                    to_val=chan
                    from_val=self.kiosk.send_ack_prepare_dict[chan]
                    print 'Sending acknowled. for prepare to: '+ to_val
                    to_send = json.dumps({'senderID': from_val, 'type': 'ack_prep','last_accept_id': self.kiosk.ACCEPTED_BALLT_ID , 'last_accept_val' : self.kiosk.ACCEPT_BALLT_VAL})
                    self.config.send_channels[to_val].send(to_send)
                    del(self.kiosk.send_ack_prepare_dict[chan])

                send_accept_list= list(self.kiosk.send_accept_dict.keys())
                for chan in send_accept_list:
                    to_val=chan
                    from_val=self.kiosk.send_accept_dict[chan]
                    print 'Sending accept request to: '+to_val
                    to_send = json.dumps({'senderID': from_val, 'proposalNum':self.kiosk.CURRENT_PREPARE_ID, 'type': 'accept','accept_val': self.kiosk.FINAL_ACCEPT_VALUE_SENT })
                    self.config.send_channels[to_val].send(to_send)
                    del(self.kiosk.send_accept_dict[chan])
                    
                

                send_ack_accept_list=list(self.kiosk.send_ack_accept_dict.keys())
                for chan in send_ack_accept_list:
                    to_val=chan
                    from_val=self.kiosk.send_ack_accept_dict[chan]
                    print 'Sending acknowled. for accept to: '+to_val
                    to_send = json.dumps({'senderID': from_val, 'type': 'ack_accept', 'accept_id': self.kiosk.ACCEPTED_BALLT_ID, 'msg': self.kiosk.ACCEPT_BALLT_VAL})
                    self.config.send_channels[to_val].send(to_send)
                    del(self.kiosk.send_ack_accept_dict[chan])
 
                send_commit_list= list(self.kiosk.send_commit_dict.keys())
                len_commit_list = len(send_commit_list)
                for chan in send_commit_list:
                    to_val=chan
                    from_val=self.kiosk.send_commit_dict[chan]
                    print 'Sending commit request to: '+to_val
                    to_send = json.dumps({'senderID': from_val, 'type': 'commit', 'accept_id': self.kiosk.ACCEPTED_BALLT_ID, 'msg': self.kiosk.ACCEPT_BALLT_VAL })
                    self.config.send_channels[to_val].send(to_send)
                    del(self.kiosk.send_commit_dict[chan])
                if len_commit_list > 0:
                    #Commit message has been sent to all followers
                    #reset variables now
                    print 'Resetting the variables'
                    self.kiosk.ACCEPTED_BALLT_ID= None #This will be a tuple now
                    self.kiosk.CURRENT_PREPARE_ID=None #This is just an integer. Represents the proposal number for the sender
                    self.kiosk.CURRENT_MESSAGE=None
                    self.kiosk.FINAL_ACCEPT_VALUE_SENT=None
                    self.kiosk.ACCEPT_BALLT_VAL=None
                time.sleep(5)
                    
            except Exception as e:
                print 'Error while sending message (vacating dictionaries)' + str(e)
                #self.config.logger.info('Some error occured: ' + str(e))
                #self.config.logger.info('It got out of the while loop? some prob??')

    ########################
    #  Entry point for leader election  #
    ########################     
    def checkForClientRqst(self):

        while 1==1:
            #time.sleep(1)

            msg = sys.stdin.readline()
            msg = msg[:-1]  # omitting the newline
            if msg == 'exit':
                break
            if msg != '':
                #self.config.logger.info('Value proposed by client...'+msg)
                tokens = msg.split(' ')
                if tokens[0] == 'buy':
                    #Command would be of the form buy 5 which means buy 5 tickets
                    print 'Initiating election'
                   
                    #proposer_id =  #any ballot number greater than it has knowldege of !

                    #This is just a test message send to show how kiosk and config can be used
                    #self.kiosk.CURRENT_LEADER = msg
                    #Populate the send_prepare_dict

                    for key in self.config.REM_CLIENTS:
                        self.kiosk.send_prepare_dict[key]=self.config.client_id

                    print 'Proposal dict is '
                    print self.kiosk.send_prepare_dict
                    self.kiosk.CURRENT_MESSAGE = tokens[1]
                    #Set the proposal ID too
                    if self.kiosk.HIGHEST_PREPARE_ID is None:
                        #This is a tuple now
                        self.kiosk.HIGHEST_PREPARE_ID = (1,int(self.config.client_id))
                        self.kiosk.CURRENT_PREPARE_ID = 1
                    else:
                        #This is a tuple now. So I fetch the first element and increment it to set current prepare id
                        self.kiosk.CURRENT_PREPARE_ID = self.kiosk.HIGHEST_PREPARE_ID[0] + 1
                        #Update highest ballot number as current request proposal is largest
                        self.kiosk.HIGHEST_PREPARE_ID = (self.kiosk.CURRENT_PREPARE_ID,int(self.config.client_id))
                    print 'highest proposal num'+str(self.kiosk.HIGHEST_PREPARE_ID)
                    print 'current proposal num'+str(self.kiosk.CURRENT_PREPARE_ID)
                    #send proposal to multiple acceptors ...we will send to ppl in qurom and expect to get reply from all to reach majority !
                    #think upon how this qurom is going to be formed ...config file !
                    #HIGHEST_PREPARE_ID=proposer_id #it sassums itself in quorum too
                    #TO_SEND_DICT_PROPOSER ={#all the qurom ppl }
                    #SEND_STATE='Proposer' #acting as a proposer
            sys.stdout.write('[Me] ')
            sys.stdout.flush()

if __name__ == "__main__":
    sys_config = SystemConfig()
    kiosk_obj = Kiosk()

    #Start server and pass in configuration information
    server = Server(kiosk_obj,sys_config)