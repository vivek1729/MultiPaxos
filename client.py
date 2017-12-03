import socket
import sys
import os
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
                    #print 'Received data #########'+received_data
                    received_data_json=json.loads(received_data)
                    request_type = received_data_json['type']
                    sender_id = received_data_json['senderID']
                    #print 'Received some data..'+received_data
                    if request_type == 'config_meta':
                        #data = json.dumps({'senderID': self.config.client_id, 'type': 'config_meta','msg':filtered_rem_clients })
                        #Reconcile with rem_clients, add new server if not already present. Populate total_clients accordingly
                        aux_list = received_data_json['msg']
                        print 'Aux list is'
                        print aux_list
                        diff_list = [item for item in aux_list if item not in self.config.REM_CLIENTS]
                        print 'Extra elements if any'
                        print diff_list
                        if len(diff_list) > 0:
                            print 'Since it does not exist in Rem list. It does not exist in total list. So add to total list.'
                            self.config.REM_CLIENTS.extend(diff_list)
                            self.config.TOTAL_CLIENTS.extend(diff_list)
                    elif request_type == 'forward':
                        #You are the leader and you received some forwarded request from other server.
                        print 'Starting a new request for the forwarded request'
                        self.kiosk.CURRENT_MESSAGE = received_data_json['msg']
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
                        
                        self.kiosk.FINAL_ACCEPT_VALUE_SENT = self.kiosk.CURRENT_MESSAGE
                        self.kiosk.ACCEPTED_BALLT_ID = (self.kiosk.CURRENT_PREPARE_ID,int(self.config.client_id))
                        self.kiosk.ACCEPT_BALLT_VAL = self.kiosk.CURRENT_MESSAGE
                        current_ballot_id = (self.kiosk.CURRENT_PREPARE_ID,int(self.config.client_id))
                        for key in self.config.REM_CLIENTS:
                            #Current kiosk is the leader. Send a normal accept** request
                            print 'Directly send accept request.'
                            self.kiosk.send_accept_list.append({'to':key,'from':self.config.client_id,'prop':self.kiosk.CURRENT_PREPARE_ID,'accept_val':self.kiosk.FINAL_ACCEPT_VALUE_SENT})
                            self.kiosk.send_ack_accept_list.append({'to':key,'from':self.config.client_id,'accept_id':current_ballot_id,'accept_val':self.kiosk.FINAL_ACCEPT_VALUE_SENT})

                    elif request_type == 'prep':
                        if self.kiosk.HIGHEST_PREPARE_ID is None or \
                        received_data_json['proposalNum'] > self.kiosk.HIGHEST_PREPARE_ID[0] or \
                        ( received_data_json['proposalNum'] == self.kiosk.HIGHEST_PREPARE_ID[0] \
                            and int(sender_id) > self.kiosk.HIGHEST_PREPARE_ID[1]):
                            self.kiosk.HIGHEST_PREPARE_ID = (received_data_json['proposalNum'],int(sender_id))
                            #Populate the ack dictionary
                            print 'Populating ack dictionary now..'
                            #self.kiosk.ACCEPTED_BALLT_ID , 'last_accept_val' : self.kiosk.ACCEPT_BALLT_VAL})
                            self.kiosk.send_ack_prepare_list.append({'to':sender_id,'from':self.config.client_id,'last_accept_id':self.kiosk.ACCEPTED_BALLT_ID,'last_accept_val':self.kiosk.ACCEPT_BALLT_VAL})
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
                                #self.kiosk.send_accept_dict[key]=self.config.client_id
                                self.kiosk.send_accept_list.append({'to':key,'from':self.config.client_id,'prop':self.kiosk.CURRENT_PREPARE_ID,'accept_val':self.kiosk.FINAL_ACCEPT_VALUE_SENT})

                            print 'Send accept dict is '
                            print self.kiosk.send_accept_dict
                            #The sender should also send ack accept request to all other users here
                            #Because it will accept its own request always. and so let other processes know about this ack_accept
                            #The other processes should get this ack_accept request.?
                            #Or may be I don't have to send it because of the way counters are handled. I initialize it to 1.
                            #1 means it has already has ack accept from itself.
                            current_ballot_id = (self.kiosk.CURRENT_PREPARE_ID,int(self.config.client_id))
                            for key in self.config.REM_CLIENTS:
                                self.kiosk.send_ack_accept_list.append({'to':key,'from':self.config.client_id,'accept_id':current_ballot_id,'accept_val':self.kiosk.FINAL_ACCEPT_VALUE_SENT})

                            
                    elif request_type == 'accept':
                        if self.kiosk.HIGHEST_PREPARE_ID is None or \
                        received_data_json['proposalNum'] > self.kiosk.HIGHEST_PREPARE_ID[0] or \
                        ( received_data_json['proposalNum'] == self.kiosk.HIGHEST_PREPARE_ID[0] \
                            and int(sender_id) >= self.kiosk.HIGHEST_PREPARE_ID[1]):
                            if self.kiosk.HIGHEST_PREPARE_ID is None:
                                self.kiosk.HIGHEST_PREPARE_ID = (received_data_json['proposalNum'],int(sender_id))
                            self.kiosk.ACCEPTED_BALLT_ID = (received_data_json['proposalNum'],int(sender_id))
                            self.kiosk.ACCEPT_BALLT_VAL = received_data_json['accept_val']
                            #Populate the ack dictionary
                            #Broadcast to all processes now instead of just the sender (Phase 2)
                            print 'Populating ack accept list now. Need to broadcast to all clients'
                            for key in self.config.REM_CLIENTS:
                                self.kiosk.send_ack_accept_list.append({'to':key,'from':self.config.client_id,'accept_id':self.kiosk.ACCEPTED_BALLT_ID,'accept_val':self.kiosk.ACCEPT_BALLT_VAL})
                    elif request_type == 'ack_accept':
                        if received_data_json['accept_id'][0] > self.kiosk.HIGHEST_PREPARE_ID[0] or \
                        ( received_data_json['accept_id'][0] == self.kiosk.HIGHEST_PREPARE_ID[0] \
                            and received_data_json['accept_id'][1] >= self.kiosk.HIGHEST_PREPARE_ID[1]):
                            accept_id_tuple = (received_data_json['accept_id'][0],received_data_json['accept_id'][1])
                            print 'State of ack counter dict...'
                            print self.kiosk.ack_counter_dict
                            if accept_id_tuple in self.kiosk.ack_counter_dict:
                                self.kiosk.ack_counter_dict[accept_id_tuple] += 1
                            else:
                                self.kiosk.ack_counter_dict[accept_id_tuple] = 2
                            #The modified if condition makes sure a delayed ack accept from a process
                            #after the majority does not trigger further message sending
                            #Even if this is not sender, it will get ack accepts from ther processes and calculate majority (Phase 2)
                            if self.kiosk.ack_counter_dict[accept_id_tuple] == self.kiosk.majority_count:
                                #Update the self.kiosk.ACCEPT_BALLT_VAL for the sender
                                #Confirm this with Ishani.
                                #If this ballot number does not exist in log only then go ahead.
                                if not any(d['id'] == accept_id_tuple for d in self.kiosk.log):
                                    self.kiosk.ACCEPT_BALLT_VAL = received_data_json['msg']
                                    self.kiosk.ACCEPTED_BALLT_ID = accept_id_tuple
                                    print 'Received accept from majority'
                                    print 'No need to send commit message. Ack accept is broadcast. So chill'
                                    #Ready to send commit requests now
                                    #for key in self.config.REM_CLIENTS:
                                    #    self.kiosk.send_commit_dict[key]=self.config.client_id

                                    #print 'Send commit dict is '
                                    #print self.kiosk.send_commit_dict
                                    print 'Also add '+str(self.kiosk.ACCEPT_BALLT_VAL)+' to my log right now'
                                    self.kiosk.log.append({'id':self.kiosk.ACCEPTED_BALLT_ID,'val':self.kiosk.ACCEPT_BALLT_VAL})
                                    
                                    tokens = self.kiosk.ACCEPT_BALLT_VAL.split(' ')
                                    #Update ticket balance only if it is a buy message
                                    if tokens[0] == 'buy':
                                        print 'Update balance in tickets'
                                        self.kiosk.TICKETS -= int(tokens[1])
                                        print 'Remaining tickets in system ==========>'+str(self.kiosk.TICKETS)

                                    #Otherwise this is a configuration change kind of message
                                    #Add or remove clients. Make changes in majority accordingly.
                                    elif tokens[0] == 'add_kiosk':
                                        print 'This is a configuration change. Add a new kiosk'
                                        #Append to total_clients array so that this process can connect to new server (only if it does not already exist)
                                        if tokens[1] not in self.config.REM_CLIENTS:
                                            self.config.TOTAL_CLIENTS.append(tokens[1])
                                            self.config.REM_CLIENTS.append(tokens[1])
                                        print 'Added kisok to the system ==========>'+tokens[1]
                                        print 'Updating majority count now'
                                        self.kiosk.majority_count = (len(self.config.REM_CLIENTS) + 1)/2 + 1
                                        print 'New majority count is '+str(self.kiosk.majority_count)
                                        #Sweet. This value is committed. Try to connect to this client now.
                                    elif tokens[0] == 'remove_kiosk':
                                        print 'This is a configuration change. Remove an existing kiosk'
                                        #This is more complicated. Need to close relevant threads and send channels
                                        self.config.REM_CLIENTS.remove(tokens[1])
                                        print 'Removed kisok from the system ==========>'+tokens[1]
                                        print 'Updating majority count now'
                                        self.kiosk.majority_count = (len(self.config.REM_CLIENTS) + 1)/2 + 1
                                        print 'New majority count is '+str(self.kiosk.majority_count)


                                    #Print helper message that current request succeeded or not in processes that made proposals
                                    if (self.kiosk.CURRENT_PREPARE_ID,int(self.config.client_id)) == self.kiosk.ACCEPTED_BALLT_ID:
                                        #Current request was successful!
                                        print '#######################################'
                                        print 'Your request to '+self.kiosk.ACCEPT_BALLT_VAL+' was successfully processed!'
                                        print '#######################################'
                                    elif self.kiosk.CURRENT_PREPARE_ID is not None:
                                        print '#######################################'
                                        print 'Your request to '+self.kiosk.ACCEPT_BALLT_VAL+' could NOT be processed!'
                                        print '#######################################'
                                    #Reset counters
                                    #self.kiosk.ack_counter = 1
                                    #self.kiosk.accept_counter = 1
                                    #Reset acknowledgement array
                                    #self.kiosk.ack_arr = []
                                    print 'Contents of the log========='
                                    print self.kiosk.log
                                    #Current process is the leader!
                                    if accept_id_tuple[1] == int(self.config.client_id):
                                        self.kiosk.CURRENT_LEADER = self.config.client_id
                                        print 'I am the LEADER NOW! WOWS!'
                                        if tokens[0] == 'add_kiosk':
                                            #Add the newly added kiosk to list of total_clients in config.json
                                            # with open('config.json','r') as data_file:    
                                            #     config_data = json.load(data_file)
                                            # #Add kiosk to array
                                            # config_data['total_clients'].append(tokens[1])
                                            # #Write updated json data to config.json
                                            # with open('config.json','w') as data_file:    
                                            #     json.dump(config_data, data_file)
                                            print 'Configuration change is complete. Kiosk added successfully to the system========='

                                    #Inform the other processes that you are the current leader through some broadcast.
                                    print 'Resetting the variables'
                                    self.kiosk.ACCEPTED_BALLT_ID= None #This will be a tuple now
                                    self.kiosk.CURRENT_PREPARE_ID=None #This is just an integer. Represents the proposal number for the sender
                                    self.kiosk.CURRENT_MESSAGE=None
                                    self.kiosk.FINAL_ACCEPT_VALUE_SENT=None
                                    self.kiosk.ACCEPT_BALLT_VAL=None
                                    self.kiosk.ack_counter = 1
                                    self.kiosk.ack_arr = []
                    #The creator of a proposal should also send ack_accept or some sort of commit to other processes
                    #It is a very important requirement. Take note!
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
                        brandnewTime=time.time()
                        #print 'I got leader auth'
                        #print brandnewTime

                        self.kiosk.CURRENT_LEADER=received_data_json['senderID']
                        self.kiosk.lastAuthRcvTime=brandnewTime

                    else:
                        print 'some error'

                    #Time out condition for leader
                    if self.kiosk.CURRENT_LEADER != None and self.kiosk.CURRENT_LEADER != self.config.client_id :
                        # print 'I am printing both received and present for heartbeat from leader..'
                        # print time.time()
                        #print self.kiosk.lastAuthRcvTime 
                        print self.config.timeout_delay
                        if time.time()-self.kiosk.lastAuthRcvTime > self.config.timeout_delay :
                            print 'Time to panic !! No leader in system'
                            self.kiosk.CURRENT_LEADER =None

                    
                    

            except Exception as e:
                #print e
                #self.config.logger.info("Some Exception")
                print "Some Exception"
                traceback.print_exc()
                break
                self.conn.close()
                #serverSocket.close()


#This class will have all the System specific configuration
class SystemConfig():
    def __init__(self):

        #Initialize client_id
        client_id = ' '.join(sys.argv[1:])
        self.client_id = client_id


        #Read data from the file
        with open('config.json','r') as data_file:    
            data = json.load(data_file)
            self.TCP_IP = data['process_dict'][self.client_id]['tcp_ip']
            self.TCP_PORT = data['process_dict'][self.client_id]['tcp_port']
            self.REPLY_DELAY = data['reply_delay']
            self.REQUEST_DELAY = data['request_delay']
            self.TOTAL_CLIENTS = data['total_clients']
            self.PROC_ID_MAPPING = data['process_dict']
            self.per_threshold = data['per_threshold']

        #Initialize logger
        self.logger= logging.getLogger(__name__)
        #Randomize timeout delay
        self.timeout_delay = random.randint(7,13)
        #Initialze channel states, incoming and outgoing channels
        if self.client_id in self.TOTAL_CLIENTS: 
            self.TOTAL_CLIENTS.remove(self.client_id)
        self.channel_states = {}
        self.send_channels = {}
        self.receive_channels = {}
        self.queue_send_markers = []
        self.REM_CLIENTS = []
        print 'Total clients are '
        print self.TOTAL_CLIENTS
        for client in self.TOTAL_CLIENTS:
            self.REM_CLIENTS.append(client)
    
        #Initialize channel states for all clients. We don't iterate over it. So we can populate the dict now
        for key, value in self.PROC_ID_MAPPING.iteritems():
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
        self.majority_count = 2 #This is wrong. Especially when new processes are added. So I need to correctly set majority count. Will do that when server is created.
        self.ack_counter = 1 #I assume the sender i.e. process making the proposal will automatically acknowledge proposal
        #I need to have a counter dictionary for ack acceptance instead of a single value since different
        #processes might be having different proposals and acks are broadcasted so..
        self.ack_counter_dict = {}
        self.accept_counter = 1 #I assume the sender i.e. process making the proposal will automatically accept proposal
        ##Log list
        self.log = []
        ##Introducing 5 Dicts
        self.send_prepare_dict={}; ##Dict for sending prepare
        self.send_ack_prepare_dict={}; ##Dict for sending ack to prepare
        self.send_accept_dict={}; ##Dict for sending accept Phase 2
        self.send_ack_accept_dict={}; ##Dict for sending ack to accept
        self.send_commit_dict={};##Dict for sending commit as distinguished learner
        #Ack accept trying with list. Because we need to broadcast to all other processes on ever accept
        #So for different proposals we might have different messages so dict might not cut it.
        self.send_forward_list = []
        self.send_prepare_list = []
        self.send_ack_prepare_list = []
        self.send_accept_list = []
        self.send_ack_accept_list = []

        self.lastAuthRcvTime= -1


class Server():

    #Constructor for Server Class
    def __init__(self,kiosk_obj,sys_config):
        self.kiosk = kiosk_obj
        self.config = sys_config
        self.threads = []
        self.clients = []
        #Reset the majority count variable for kiosk.
        #Becaus addition of new kiosks changes the config file. 2 is not the right value to start with
        self.kiosk.majority_count = (len(self.config.REM_CLIENTS) + 1)/2 + 1
        #Because the kiosk majority count is set up correctly here ^
        #New clients in client thread initiation will also get the correct value. Hence done.
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
                    key = self.config.TOTAL_CLIENTS[0]
                    process_ip = self.config.PROC_ID_MAPPING[key]['tcp_ip']
                    process_port = self.config.PROC_ID_MAPPING[key]['tcp_port']
                    print 'Trying to connect to '+str(key)                
                    tcpClient.connect((process_ip, process_port))
                    #self.config.logger.info('Key is '+key)
                    self.config.send_channels[key] = tcpClient
                    self.config.logger.debug( self.config.send_channels)
                    #Send a message to the newly connected server with the rem_clients for that server.
                    #This message sending happens only if the new client has an add_kiosk entry in log
                    check_msg = 'add_kiosk '+key
                    log_exist = [item for item in self.kiosk.log if item['val'] == check_msg]
                    if len(log_exist) > 0:
                        print 'Server exists in log and was kiosk was added. Send meta info now.'
                        filtered_rem_clients = [elm for elm in self.config.REM_CLIENTS if elm != key]
                        filtered_rem_clients.append(self.config.client_id)
                        data = json.dumps({'senderID': self.config.client_id, 'type': 'config_meta','msg':filtered_rem_clients })
                        self.config.send_channels[key].send(data)
                    #self.config.logger.info('Length of send channels')
                    print 'Length of send channels'
                    print len(self.config.send_channels)          
                    self.config.logger.debug( len(self.config.send_channels))
                    del self.config.TOTAL_CLIENTS[0]
                    #self.config.logger.info('Remaining clients are ')    
                    print 'Remaining clients are '
                    print self.config.TOTAL_CLIENTS             
                    self.config.logger.debug(self.config.TOTAL_CLIENTS)
            except Exception as e:
                #self.config.logger.info('Unable to connect')
                print 'Some error occured: ' + str(e)
                print 'Unable to connect'
                print 'Try with the next item in loop'
                del(self.config.TOTAL_CLIENTS[0])
                continue


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
            #try:
                #Sending messages and handling dictionaries goes here
                #print "Vacating all the dictionaries !!"
                # dictionaries format== {to:from, to:from, to:from} #can "to" be non unique??
                #send_prep_list = list( self.kiosk.send_prepare_dict.keys() )
            while len(self.kiosk.send_forward_list) > 0:
                try:
                    chan=self.kiosk.send_forward_list[0]
                    print "@@@to_send_forward" + chan['to']
                    to_val=chan['to']
                    from_val=chan['from']
                    print 'Sending forward request to: '+ to_val
                    to_send = json.dumps({'senderID': from_val, 'type': 'forward','msg': chan['msg']})
                    self.config.send_channels[to_val].send(to_send)
                    del(self.kiosk.send_forward_list[0])
                except Exception as e:
                    print 'Error while sending forward request to leader.' + str(e)
                    print 'Try with the next item in loop'
                    del(self.kiosk.send_forward_list[0])
                    print 'Leader is dead may be?'
                    self.kiosk.CURRENT_LEADER = None
                    #Make a fresh proposal
                    self.makeProposal()
                    continue


            while len(self.kiosk.send_prepare_list) > 0:
                try:
                    chan=self.kiosk.send_prepare_list[0]
                    print "@@@to_send_prepare" + chan['to']
                    to_val=chan['to']
                    from_val=chan['from']
                    print 'Sending prepare request to: '+ to_val
                    to_send = json.dumps({'senderID': from_val, 'proposalNum':chan['prop'], 'type': 'prep','msg': 'makeMeLeader'})
                    self.config.send_channels[to_val].send(to_send)
                    del(self.kiosk.send_prepare_list[0])
                except Exception as e:
                    print 'Error while sending prep request' + str(e)
                    print 'Try with the next item in loop'
                    del(self.kiosk.send_prepare_list[0])
                    continue



            #send_ack_prepare_list=list(self.kiosk.send_ack_prepare_dict.keys())
            while len(self.kiosk.send_ack_prepare_list) > 0:
                try:
                    chan=self.kiosk.send_ack_prepare_list[0]
                    to_val=chan['to']
                    from_val=chan['from']
                    print 'Sending acknowled. for prepare to: '+ to_val
                    to_send = json.dumps({'senderID': from_val, 'type': 'ack_prep','last_accept_id': chan['last_accept_id'] , 'last_accept_val' : chan['last_accept_val']})
                    self.config.send_channels[to_val].send(to_send)
                    del(self.kiosk.send_ack_prepare_list[0])
                except Exception as e:
                    print 'Error while sending prep ack' + str(e)
                    print 'Try with the next item in loop'
                    del(self.kiosk.send_ack_prepare_list[0])
                    continue

            #send_accept_list= list(self.kiosk.send_accept_dict.keys())
            while len(self.kiosk.send_accept_list) > 0:
            #for chan in send_accept_list:
                try:
                    chan=self.kiosk.send_accept_list[0]
                    to_val=chan['to']
                    from_val=chan['from']
                    print 'Sending accept request to: '+to_val
                    to_send = json.dumps({'senderID': from_val, 'proposalNum':chan['prop'], 'type': 'accept','accept_val': chan['accept_val'] })
                    self.config.send_channels[to_val].send(to_send)
                    del(self.kiosk.send_accept_list[0])
                except Exception as e:
                    print 'Error while sending accept' + str(e)
                    print 'Try with the next item in loop'
                    del(self.kiosk.send_accept_list[0])
                    continue
            
            #Messages getting joined in received channels, especially messages like accept and ack_accept!
            #Try with some delay. :/
            time.sleep(0.1)

            #send_ack_accept_list=list(self.kiosk.send_ack_accept_dict.keys())
            #This list will contain broadcast info to be sent to all clients!
            len_commit_list = len(self.kiosk.send_ack_accept_list)
            #if len_commit_list > 0:
                #Concat self_ack_accept_list and send_ack_accept_list
                #self.kiosk.send_ack_accept_list.extend(self.kiosk.self_ack_accept_list)
                #After appending, set this list to empty
                #self.kiosk.self_ack_accept_list = []
            while len(self.kiosk.send_ack_accept_list) > 0:
                try:
                    chan = self.kiosk.send_ack_accept_list[0]
                    to_val=chan['to']
                    from_val=chan['from']
                    print 'Sending acknowled. for accept to: '+to_val+' proposal num '+str(chan['accept_id'])
                    to_send = json.dumps({'senderID': from_val, 'type': 'ack_accept', 'accept_id': chan['accept_id'], 'msg': chan['accept_val']})
                    self.config.send_channels[to_val].send(to_send)
                    del(self.kiosk.send_ack_accept_list[0])
                except Exception as e:
                    print 'Error while sending accept acknowledgement' + str(e)
                    print 'Try with the next item in loop'
                    del(self.kiosk.send_ack_accept_list[0])
                    continue

            time.sleep(0.1)
            counter = 0
            #Try sending heartbeats only if you are current leader.
            if self.config.client_id == self.kiosk.CURRENT_LEADER:
                print 'I am the leader. I can send heartbeats!'
                while counter < len(self.config.REM_CLIENTS):
                    try:
                        to_val=self.config.REM_CLIENTS[counter]
                        from_val=self.config.client_id
                        #print 'Sending heartbeat auth to: '+to_val +'from our leader..'+ from_val
                        to_send = json.dumps({'senderID': from_val, 'log':self.kiosk.log, 'type': 'leaderAuth' })
                        self.config.send_channels[to_val].send(to_send)
                        counter=counter+1
                    except Exception as e:
                        print 'Error while sending accept heartbeat' + str(e)
                        print 'Try with the next item in loop'
                        counter=counter+1
                        continue
            # send_commit_list= list(self.kiosk.send_commit_dict.keys())
            # for chan in send_commit_list:
            #     to_val=chan
            #     from_val=self.kiosk.send_commit_dict[chan]
            #     print 'Sending commit request to: '+to_val
            #     to_send = json.dumps({'senderID': from_val, 'type': 'commit', 'accept_id': self.kiosk.ACCEPTED_BALLT_ID, 'msg': self.kiosk.ACCEPT_BALLT_VAL })
            #     self.config.send_channels[to_val].send(to_send)
            #     del(self.kiosk.send_commit_dict[chan])
            #if len_commit_list > 0:
                #Commit message has been sent to all followers
                #reset variables now
                #print 'Resetting the variables. But not actually doing it.'
                # self.kiosk.ACCEPTED_BALLT_ID= None #This will be a tuple now
                # self.kiosk.CURRENT_PREPARE_ID=None #This is just an integer. Represents the proposal number for the sender
                # self.kiosk.CURRENT_MESSAGE=None
                # self.kiosk.FINAL_ACCEPT_VALUE_SENT=None
                # self.kiosk.ACCEPT_BALLT_VAL=None
            time.sleep(5)
                    
            #except Exception as e:
                #print 'Error while sending message (vacating dictionaries)' + str(e)
                #self.config.logger.info('Some error occured: ' + str(e))
                #self.config.logger.info('It got out of the while loop? some prob??')

    ########################
    #  Entry point for leader election  #
    ########################
    def makeProposal(self):
        #No need to set the proposal id or anything. Just forward this request.
        if self.config.client_id != self.kiosk.CURRENT_LEADER and self.kiosk.CURRENT_LEADER is not None:
            print 'I am not the leader. Forwarding this request to current leader.'
            self.kiosk.send_forward_list.append({'to':self.kiosk.CURRENT_LEADER,'from':self.config.client_id,'msg':self.kiosk.CURRENT_MESSAGE})

        else:
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
            
            if self.config.client_id == self.kiosk.CURRENT_LEADER:
                #Set some kiosk params if leader is sending request in Multi Paxos phase. Proposals are not required.
                self.kiosk.FINAL_ACCEPT_VALUE_SENT = self.kiosk.CURRENT_MESSAGE
                self.kiosk.ACCEPTED_BALLT_ID = (self.kiosk.CURRENT_PREPARE_ID,int(self.config.client_id))
                self.kiosk.ACCEPT_BALLT_VAL = self.kiosk.CURRENT_MESSAGE
                current_ballot_id = (self.kiosk.CURRENT_PREPARE_ID,int(self.config.client_id))

            for key in self.config.REM_CLIENTS:
                if self.config.client_id == self.kiosk.CURRENT_LEADER :
                    #Current kiosk is the leader. Send a normal accept** request
                    print 'Directly send accept request.'
                    self.kiosk.send_accept_list.append({'to':key,'from':self.config.client_id,'prop':self.kiosk.CURRENT_PREPARE_ID,'accept_val':self.kiosk.FINAL_ACCEPT_VALUE_SENT})
                    self.kiosk.send_ack_accept_list.append({'to':key,'from':self.config.client_id,'accept_id':current_ballot_id,'accept_val':self.kiosk.FINAL_ACCEPT_VALUE_SENT})
                elif self.kiosk.CURRENT_LEADER is None:
                    print 'Sending prepare request. This will initiate a new election'
                    self.kiosk.send_prepare_list.append({'to':key,'from':self.config.client_id,'prop':self.kiosk.CURRENT_PREPARE_ID})

        print 'Send prepare list '
        print self.kiosk.send_prepare_list
        print 'highest proposal num'+str(self.kiosk.HIGHEST_PREPARE_ID)
        print 'current proposal num'+str(self.kiosk.CURRENT_PREPARE_ID)

        print 'State of variables right now....'
        print 'Current leader id = '
        print self.kiosk.CURRENT_LEADER
        print 'Accept ballot id = '
        print self.kiosk.ACCEPTED_BALLT_ID
        print 'Final accept value to be sent = '
        print self.kiosk.FINAL_ACCEPT_VALUE_SENT
        print 'Accept ballot value = '
        print self.kiosk.ACCEPT_BALLT_VAL
        #send proposal to multiple acceptors ...we will send to ppl in qurom and expect to get reply from all to reach majority !
        #think upon how this qurom is going to be formed ...config file !
        #HIGHEST_PREPARE_ID=proposer_id #it sassums itself in quorum too
        #TO_SEND_DICT_PROPOSER ={#all the qurom ppl }
        #SEND_STATE='Proposer' #acting as a proposer 
    def checkForClientRqst(self):

        while 1==1:
            #time.sleep(1)

            msg = sys.stdin.readline()
            msg = msg[:-1]  # omitting the newline
            if msg == 'exit':
                #Close the server.
                #Close all connections
                for c in self.clients:
                    c.close()
                print 'Ended all client connections'
                self.serverSocket.close()
                os._exit(1) #Exit the whole application. Kill everything in the world.
                #End all threads.
                # for c in self.threads:
                #     #Close the client threads.
                #     c.join()
                # print 'Ended all client threads.'
                # print 'Need to close the server! Exit from the loop then.'
                
            elif msg != '':
                #self.config.logger.info('Value proposed by client...'+msg)
                tokens = msg.split(' ')
                if tokens[0] == 'buy':
                    #Command would be of the form buy 5 which means buy 5 tickets
                    print 'Initiating election'
                   
                    #proposer_id =  #any ballot number greater than it has knowldege of !

                    #This is just a test message send to show how kiosk and config can be used
                    #self.kiosk.CURRENT_LEADER = msg
                    #Populate the send_prepare_dict

                elif tokens[0] == 'add_kiosk':
                    #Command would be of the form add_kiosk 4 which means add kiosk with id 4
                    print 'Initiating configuration change. Adding new kiosk..'
                    kiosk_id = tokens[1]                   
                    #Broadcast this add message as proposal to all clients.

                 
                self.kiosk.CURRENT_MESSAGE = msg
                self.makeProposal()
                
            sys.stdout.write('[Me] ')
            sys.stdout.flush()

if __name__ == "__main__":
    sys_config = SystemConfig()
    kiosk_obj = Kiosk()

    #Start server and pass in configuration information
    server = Server(kiosk_obj,sys_config)

