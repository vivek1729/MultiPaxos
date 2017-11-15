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
                    print 'Received some data..'+received_data
                    print 'Current Leader'
                    #self.kiosk.* will have updated values no matter which class changes it now
                    print "Populating the relevant dictionaries !!"
                    # dictionaries format== {to:from, to:from, to:from}
                    print self.kiosk.CURRENT_LEADER

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
        print 'Total clients is '
        print self.TOTAL_CLIENTS
        for client in self.TOTAL_CLIENTS:
            key = self.PROC_ID_MAPPING[str(client)]['proc_id']
            self.channel_states[key] = {}
            self.channel_states[key]['flag'] = False
            self.channel_states[key]['state'] = []

#This class will have all the Kiosk specific configuration
class Kiosk():
    def __init__(self):
        self.CURRENT_LEADER=None
        self.ACCEPTED_BALLT_ID= 1
        self.HIGHEST_PREPARE_ID=1
        self.CURRENT_PREPARE_ID=None
        self.FINAL_ACCEPT_VALUE_SENT=1
        self.ACCEPT_BALLT_VAL='x'
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
        #sendThread = Thread(target=self.sendData) #thread to send data !!
        #sendThread.setDaemon(True)
        #sendThread.start()
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
                    tcpClient.connect((self.config.TCP_IP, self.config.TOTAL_CLIENTS[0]))
                    key = self.config.PROC_ID_MAPPING[str(self.config.TOTAL_CLIENTS[0])]['proc_id']
                    #self.config.logger.info('Key is '+key)
                    self.config.send_channels[key] = tcpClient
                    self.config.logger.debug( self.config.send_channels) 
                    #self.config.logger.info('Length of send channels')                 
                    self.config.logger.debug( len(self.config.send_channels))
                    del self.config.TOTAL_CLIENTS[0]
                    #self.config.logger.info('Remaining clients are ')                   
                    self.config.logger.debug(self.config.TOTAL_CLIENTS)
                    
                else:
                    #self.config.logger.info('Have successfully connected to all clients!')
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
                print "Vacating all the dictionaries !!"
                # dictionaries format== {to:from, to:from, to:from} #can "to" be non unique??

                for chan in self.kiosk.send_prepare_dict:
                    to_val=chan
                    from_val=self.kiosk.send_prepare_dict[chan]
                    print 'Sending prepare request to: '+ to_val
                    to_send = json.dumps({'senderID': from_val, 'type': 'prep','msg': 'makeMeLeader'})
                    send_channels[to_val].send(to_send)

                for chan in self.kiosk.send_ack_prepare_dict:
                    to_val=chan
                    from_val=self.kiosk.send_ack_prepare_dict[chan]
                    print 'Sending acknowled. for prepare to: '+ to_val
                    to_send = json.dumps({'senderID': from_val, 'type': 'ack_prep','last_accept_id': self.kiosk.ACCEPTED_BALLT_ID , 'last_accept_val' : self.kiosk.ACCEPT_BALLT_VAL})
                    send_channels[to_val].send(to_send)

                for chan in self.kiosk.send_accept_dict:
                    to_val=chan
                    from_val=self.kiosk.send_accept_dict[chan]
                    print 'Sending accept request to: '+to_val
                    to_send = json.dumps({'senderID': from_val, 'type': 'accept','accept_val': self.kiosk.FINAL_ACCEPT_VALUE_SENT })
                    send_channels[to_val].send(to_send)

                for chan in self.kiosk.send_ack_accept_dict:
                    to_val=chan
                    from_val=self.kiosk.send_ack_accept_dict[chan]
                    print 'Sending acknowled. for accept to: '+to_val
                    to_send = json.dumps({'senderID': from_val, 'type': 'ack_accept'})
                    send_channels[to_val].send(to_send)

                for chan in self.kiosk.send_commit_dict:
                    to_val=chan
                    from_val=self.kiosk.send_commit_dict[chan]
                    print 'Sending commit request to: '+to_val
                    to_send = json.dumps({'senderID': from_val, 'type': 'commit', 'msg': self.kiosk.ACCEPT_BALLT_VAL })
                    send_channels[to_val].send(to_send)


                    
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
                print 'Initiating proposal'
               
                #proposer_id =  #any ballot number greater than it has knowldege of !

                #This is just a test message send to show how kiosk and config can be used
                self.kiosk.CURRENT_LEADER = msg
                for key in self.config.send_channels:
                    self.config.send_channels[key].send(msg)
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
