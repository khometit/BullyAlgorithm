"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Avery Dinh (with work provided by Prof. Lundeen)
:Version: 1.0
"""

from enum import Enum
import datetime
import socket
import selectors
import sys
import pickle
import time

"""
This module is to test an application acting as a node in a distributed system. 
The algorithm to choose the system's coordinator is the Bully Algorithm.
The module contains:
> a State class to sum the needed states to be used during the life time of the node.
> a Lab2 class that is the actual node
"""

class State(Enum):
    """
    Enumeration of states a peer can be in for the Lab2 class.

    Referenced from Prof. Kevin Lundeen
    """
    QUIESCENT = 'QUIESCENT'  # Erase any memory of this peer

    # Outgoing message is pending
    SEND_ELECTION = 'ELECTION'
    SEND_VICTORY = 'COORDINATOR'
    SEND_OK = 'OK'
    SEND_JOIN = 'JOIN'
    SEND_PROBE = 'PROBE'

    # Incoming message is pending
    WAITING_FOR_OK = 'WAIT_OK'  # When I've sent them an ELECTION message
    WAITING_FOR_VICTOR = 'WHO IS THE WINNER?'  # This one only applies to myself
    WAITING_FOR_ANY_MESSAGE = 'WAITING'  # When I've done an accept on their connect to my server

    def is_incoming(self):
        """Categorization helper."""
        return self not in (State.SEND_ELECTION, State.SEND_VICTORY, State.SEND_OK)

class Lab2(object):
    """ 
    Class to contain all the needed operations and states for a node to successfully join the distributed group, hold an election and 
    figure out who the biggest bully is. 
    """

    BUF_SZ = 1024
    ASSUME_FAILURE_TIMEOUT = 2.5        #Only wait 4 seconds till something else should happen
    SELECTOR_CHECK = 0.3
    days_to_birthday = 0
    
    def __init__(self, gcd_address, nextBirthday, SUID) -> None:
        """Constructs a Lab2 object to talk to the given GCD """
        self.gcd_address = (gcd_address[0], int(gcd_address[1]))
        days_to_birthday = (nextBirthday - datetime.datetime.now()).days
        self.pid = (days_to_birthday, int(SUID))
        self.members = {}
        self.states= {}
        self.bully = None                   #None when no leader, otherwise it'll be the pid of the leader
        self.selector = selectors.DefaultSelector()
        self.listener, self.listener_address = self.start_a_server()

    def run(self):
        """Start up point for the node. It will talk to the GCD to get the list of members, 
        start an election and keep the listening socket active"""


        #Registering the socket without a callback for this socket
        self.selector.register(self.listener, selectors.EVENT_READ, data=None)
        
        #Talk to the GCD
        self.join_group()

        #Start an election on start up
        self.start_election('Node has just started up.')

        while True:
            events = self.selector.select(self.SELECTOR_CHECK)
            for key, mask in events:
                #When a peer wants to connect to me
                if key.fileobj == self.listener:
                    self.accept_peer()
                #When a peer sent me a message
                elif mask & selectors.EVENT_READ:
                    self.receive_msg(key.fileobj)
                #When I need to send a message out
                else:
                    self.send_msg(key.fileobj)
            self.check_timeouts()

    def check_timeouts(self):
        """
        Function to check if something else needs to happen: start new election or declare victorious
        If state == WAITING FOR VICTOR -> check if nobody sent a COORDINATOR message
        If so, then restart election
        If state == WAITING FOR OK: all timed out -> declare victorious
        """

        if self.get_state(self) == State.WAITING_FOR_VICTOR and self.is_expired(self):
            self.states.clear()
            self.start_election('Have not received COORDINATOR message')
        elif self.get_state(self) == State.WAITING_FOR_OK and self.is_expired(self):
            self.announce_victory()

    def start_a_server(self):
        """Function to start a generic server
        
        :return: a tuple of the socket that's the server, and the socket's name"""
        node_address = ('localhost', 0)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(node_address)
        sock.listen(10)
        sock.setblocking(False)
        return (sock, sock.getsockname())
  
    def connect(self, s, host, port, msg):
        """
        Function to start a connection to the server

        Parameters: 
        s : socket - the socket connection 
        host : str - name of the server
        port : int - port number to use
        msg: str - a msg that might be sent over
        return True if connection is established without any error, False otherwise
        """ 

        try:
            print('\n%s to (%s, %i)' % (msg, host, port))
            s.connect((host, port))
            return True
        except socket.timeout as e:
            s.close()
            print('Timeout error, connection took to long', e)
            return False
        except OSError as e:
            s.close()
            print('Error connecting: ', e)
            return False
    
    def join_group(self):
        """Function to join the group by talking to the GCD and receive the list of members"""
        host, port = self.gcd_address

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connected = self.connect(s, host, port, State.SEND_JOIN)

        if not connected:
            print('Error connecting, please check and try again')
            return
        msg = (State.SEND_JOIN.value, (self.pid, self.listener_address))

        try: 
            data = pickle.dumps(msg)
            s.sendall(data)
            result = s.recv(self.BUF_SZ)
            receivedData = pickle.loads(result)
            print(receivedData)
        except Exception as err:
            print('Encountered error while sending/receiving data from GCD: {}'.format(err))
        else:
            self.update_members(receivedData)
        finally:
            s.close()

    def accept_peer(self):
        """ Function to accept connection from a peer """
        try:
            conn, addr = self.listener.accept()
            print('{}: Accepted connection'.format(self.printSocket(conn)))
            
            #Mark that I'm waiting for any message from this peer
            self.set_state(State.WAITING_FOR_ANY_MESSAGE, conn)
        except Exception as err:
            print('Failure to accept connections from a peer: {}, reason: {}'.format(addr, err))

    def update_members(self, memberList):
        """ Helper function to update the member list with those that aren't already in there, or those that have changed their address
        
        :param: memberList - the new memberList to get checked and update our list accordingly"""
        for m in memberList:
            #update the member list only if they're not there already
            if m not in self.members:
                print('Member updated: ', m, memberList[m])
                self.members[m] = memberList[m]
            #Or when the member lives in a different location
            elif m in self.members and memberList[m] != self.members[m]:
                self.members[m] = memberList[m]

    def set_state(self, state, peer):
        """ Function to set the state associated with a peer connection 
        
        :param: state - the new state
        :param: peer - the socket"""
        #Set the state with a time stamp, which I can use to check timeouts/victory
        self.states[peer] = [state, self.stamp()]

        #Register the new connection with the selector also
        if state == State.WAITING_FOR_ANY_MESSAGE:
            self.selector.register(peer, selectors.EVENT_READ)
        
        #Forget about this peer
        if state == State.QUIESCENT:
            del self.states[peer]
            #Shouldn't unregister my listener from the selector
            if peer != self:
                self.selector.unregister(peer)
                peer.close()

        #Just send the OK right away so I can close the socket
        if state == State.SEND_OK:
            self.send_msg(peer)

    def get_connection(self, member):
        """ Helper function to create a socket connection to a peer
        
        :param: member - the peer to get connected to"""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setblocking(False)
        
        try:
            s.settimeout(self.ASSUME_FAILURE_TIMEOUT)
            s.connect(member)
        except Exception as err:
            print('Failure connecting to member {}, {}'.format(member, err))
            s.close()
        else:
            self.selector.register(s, selectors.EVENT_WRITE)
            return s 
            

    def start_election(self, reason):
        """
        Start an election when someone notices that the leader is down or when the node first joined the group.
        > ELECTION message is sent to processes with higher pid. 
        > If I am the biggest bully, announce victory. 
        > During wait time, the node state will be waiting for ok
        
        :params: reason - the reason why the election was started
        """

        print('\n-------- Starting a new election. Reason: {} ------------'.format(reason))
        self.bully = None                                   #Voting in progress
        self.set_state(State.WAITING_FOR_OK, self)          #Set myself in waiting for ok first in case I am actually the biggest bully
        im_biggest = True

        #Must resolve which peers are of higher pid
        for m in self.members:
            if m > self.pid:          
                #get the connection on a separate socket, then a write event will trigger send msg
                s = self.get_connection(self.members[m])
                #Set pending message
                if s: self.set_state(State.SEND_ELECTION, s)
                im_biggest = False
        print('Finished sending out votes')
       
        if im_biggest: 
            print('I\'m the biggest bully')
            self.announce_victory()

    def is_election_in_progress(self):
        """
        Helper for checking if election is in progress or not. None means it is.
        return: True if no bully is registered, False otherwise.
        """
        return self.bully == None

    def get_state(self, peer=None, detail=False):
        """
        Look up current state in state table.

        :param peer: socket connected to peer process (None means self)
        :param detail: if True, then the state and timestamp are both returned
        :return: either the state or (state, timestamp) depending on detail (not found gives (QUIESCENT, None))
        """
        if peer is None:
            peer = self
        status = self.states[peer] if peer in self.states else (State.QUIESCENT, None)
        return status if detail else status[0]

    def loadPayload(self, state):
        """ 
        Helper function to create the payload for send messages.
        If state is SEND_VICTORY then the payload will only contain processes with pid lower
        than self.
        
        :param: state: the state that decides the payload
        :return: the payload to get sent
        """
        payload = self.members.copy()
        #If sending victory message, I only need those with pid lower than mine
        if state == State.SEND_VICTORY:
            for m in self.members:
                if m > self.pid:
                    del payload[m]

        #Else, the default payload
        return payload

    def send_msg(self, peer):
        """
        Function to send message to a peer. If the message is to send an election, also keep the connection
        open to get a reply and modify the event to EVENT_READ for selector to pick up.
        Otherwise, close the connection.

        :param: peer: the peer to send the message to
        """
        #Attempt to send the message pending for this peer
        msg = self.states[peer][0]
        #print('Sending message to member: {}, msg: {}'.format(peer.getpeername(), msg.value))
        print('{}: {}'.format(self.printSocket(peer), msg))
        
        #Get the correct payload
        payload = self.loadPayload(msg)

        try:   
            data = pickle.dumps((msg.value, payload))
            peer.sendall(data)
        except Exception as err:
            print('Failure occured while trying to send message to peer', err) 
        else:
            #Mark that I'm waiting for a response from the nodes I sent election msg to
            if msg == State.SEND_ELECTION: 
                self.set_state(State.WAITING_FOR_OK, peer)
                self.selector.modify(peer, selectors.EVENT_READ)
        finally:
            #I can close the socket if I'm not expecting any response from the peer afterwards
            if msg != State.SEND_ELECTION:
                self.selector.unregister(peer)
                peer.close()

    def receive_msg(self, peer):
        """
        Function to receive message from a peer. If the message is election, set state to send an OK to peer.
        Also start an election myself if I am not in an election.

        :param: peer: the peer to receive the message
        """
        try:
            data = peer.recv(self.BUF_SZ)
            msg = pickle.loads(data)
            print('{}: Received: {}\n'.format(self.printSocket(peer), msg[0]))
            #print(msg)
        except Exception as err:
            print('Failure accepting data from peer: {}'.format(err))
        else:
            #update members
            self.update_members(msg[1])
            #Received election message
            if msg[0] == State.SEND_ELECTION.value:
                self.set_state(State.SEND_OK, peer)
                
                #start an election myself if I'm not in an election
                if self.is_election_in_progress() == False: 
                    self.start_election('Received ELECTION from lower pid')
            
            #Received coordination message
            if msg[0] == State.SEND_VICTORY.value:
                #Found a new leader, should wipe everything now
                #Assuming at this point, all the nodes I sent election to have replied and was unregistered
                self.set_state(State.QUIESCENT, peer)
                self.set_state(State.QUIESCENT, self)
                self.note_bully(msg[1])
            
            #Received OK message
            if msg[0] == State.SEND_OK.value:
                
                #Only need 1 ok to change state to waitng for victor
                if self.get_state() == State.WAITING_FOR_OK:
                    self.set_state(State.WAITING_FOR_VICTOR, self)

                #Either way, forget about the peer that sent the message
                self.set_state(State.QUIESCENT, peer)
        #just in case 
        finally:
            if peer.fileno() > 0 :
                print('closing connection to peer: {}'.format(peer.getpeername()) )
                self.selector.unregister(peer)
                peer.close()
                print('closed')

    def note_bully(self, memberList):
        """Helper function to note down the bully. Assuming coordinator message's content includes no process with higher pid than herself, hence the highest pid is the bully.
        
        :param: memberList: the current member's list under this new leader"""

        pids = list(memberList.keys())
        highest = pids[0]

        for i in range(1, len(pids)):
            if pids[i] > highest:
                highest = pids[i]

        self.bully = highest
        print('Found new leader: {}'.format(self.bully))

    def is_expired(self, peer, threshold=ASSUME_FAILURE_TIMEOUT):
        """Helper function to check if the time has passed the threshold
        
        :param: peer: the peer to get time checked
        :param: threshold: the threshold to check against
        :return: True if timepassed is greater than threshold. False otherwise"""

        state, stampedTime = self.get_state(peer, detail=True)
        timepassed = (datetime.datetime.now() - stampedTime).total_seconds()    #get the time delta in seconds
        return timepassed > threshold

    @staticmethod
    def stamp():
        """Static helper function to give the current time
        
        :return: current time as a Datetime object"""
        return datetime.datetime.now()

    @staticmethod
    def print_timestamp():
        """Static helper function to give the current time as a string
        
        :return: current time as a string"""
        return datetime.datetime.now().strftime('%H:%M:%S.%f')
   
    def printSocket(self, sock):
        """Static helper function to print a given socket
        
        :return: current time as a string"""
        if sock is None or sock == self or sock == self.listener:
            return 'self'
        return self.cpr_sock(sock) 

    @staticmethod
    def cpr_sock(sock):
        """Static function to print a given socket, formatted into local port and remote port"""

        PEER_DIGITS = 100
        lport = sock.getsockname()[1] % PEER_DIGITS
        try:
            rport = sock.getpeername()[1] % PEER_DIGITS
        except OSError:
            rport = '???'
        return '{}->{} ({})'.format(lport, rport, id(sock))

    @staticmethod
    def print_leader(self):
        """Printing helper for current leader's name"""
        return 'unknown' if self.bully is None else ('self' if self.bully == self.pid else self.bully)

    
    def announce_victory(self):
        """
        Announce to all processes that this node won the election and is currently the bully:
        > send COORDINATOR message
        """
        #Election is over, stop check_timeouts
        self.set_state(State.QUIESCENT, self)

        for m in self.members:
            if m != self.pid:
                #get the connection on a separate socket, then a write event will trigger send msg
                s = self.get_connection(self.members[m])
                #Set pending message
                if s: self.set_state(State.SEND_VICTORY, s)
        
        self.bully = self.pid
        print('I, {}, won the election woooo\n'.format(self.pid))
         
if __name__ == '__main__':
    """ The entry point of the application """

    if len(sys.argv) != 5:
        print('Usage: python client.py BIRTHDAY SUID GCD_HOST GCD_PORT')
        exit(1)
    
    nextBirthday = sys.argv[0] #datetime.datetime(2023, 8, 26)
    SUID = sys.argv[1]
    gcd_address = (sys.argv[2], sys.argv[3])

    node = Lab2(gcd_address, nextBirthday, SUID)
    node.run()