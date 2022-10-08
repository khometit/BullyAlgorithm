from enum import Enum
import datetime
import socket
import selectors
import sys
import pickle
import time

from gcd2 import BUF_SZ

"""
TODO add module and class documentations
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

    # Incoming message is pending
    WAITING_FOR_OK = 'WAIT_OK'  # When I've sent them an ELECTION message
    WAITING_FOR_VICTOR = 'WHO IS THE WINNER?'  # This one only applies to myself
    WAITING_FOR_ANY_MESSAGE = 'WAITING'  # When I've done an accept on their connect to my server

    def is_incoming(self):
        """Categorization helper."""
        return self not in (State.SEND_ELECTION, State.SEND_VICTORY, State.SEND_OK)

class Lab2(object):
    BUF_SZ = 1024
    ASSUME_FAILURE_TIMEOUT = 10
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
        #Registering the socket without a callback for this socket
        self.selector.register(self.listener, selectors.EVENT_READ, data=None)

        #Talk to the GCD
        self.join_group()

        #Start an election on start up
        self.start_election()

        while True:
            events = self.selector.select(10)
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
        #If state == WAITING FOR VICTOR -> check if nobody sent a COORDINATOR message
        #If so, then restart election
        #If state == None + all timed out -> declare victorious

        stillWaiting = len(self.states)
        for s in self.states:
            if self.is_expired(s):
                stillWaiting -= 1
        
        if stillWaiting == 0 and self.listener in self.states and self.states[self.listener][0] == State.WAITING_FOR_VICTOR:
            self.states.clear()
            #TODO clear sockets registered too
            self.start_election()
        elif stillWaiting == 0 and self.listener not in self.states:
            self.announce_victory()


    def start_a_server(self):
        node_address = ('localhost', 0)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(node_address)
        sock.listen(10)
        sock.setblocking(False)
        return (sock, sock.getsockname())

        """
    Function to start a connection to the server

    Parameters: 
    s : socket - the socket connection 
    host : str - name of the server
    port : int - port number to use
    msg: str - a msg that might be sent over
    return True if connection is established without any error, False otherwise
    """    
    
    def connect(self, s, host, port, msg):
        #TODO: review this timeout thing
        socket.setdefaulttimeout(1500)

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
        #time.sleep(10)
        try:
            conn, addr = self.listener.accept()
            print('Accepted connection from peer addr: {}, conn: {}'.format(addr, conn))

            #FIXME: saving conn or addr?
            self.set_state(State.WAITING_FOR_ANY_MESSAGE, conn)
        except Exception as err:
            print('Failure to accept connections from a peer: {}, reason: {}'.format(addr, err))

    #FIXME Is this what update members suppose to do?
    def update_members(self, memberList):
        for m in memberList:
            #update the member list only if they're not there already
            if m not in self.members:
                print(m, memberList[m])
                self.members[m] = memberList[m]
            #Or when the member lives in a different location
            elif m in self.members and memberList[m] != self.members[m]:
                self.members[m] = memberList[m]

    def set_state(self, state, peer, switch_mode=False):
        #Set the state with a time stamp
        print('Time stamp: {}'.format(self.stamp()))
        self.states[peer] = (state, self.stamp())
        print(self.states[peer])

        #Register the new connection with the selector also
        if state == State.WAITING_FOR_ANY_MESSAGE:
            self.selector.register(peer, selectors.EVENT_READ)

    def get_connection(self, member):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.selector.register(s, selectors.EVENT_WRITE)
         
        try:
            s.settimeout(2.0)
            s.connect(member)
        except Exception as err:
            print('Failure connecting to member {}'.format(member))
            self.selector.unregister(s)
        else:
            return s
            
    """
    Start an election when someone notices that the leader is down or when the node first joined the group.
    > ELECTION message is sent to processes with high pid. 
    > If no response received within the given time limit, announce victory. 
    > During wait time, the node state will in election-in-progress
    """
    def start_election(self):
        self.bully = None   #Voting in progress

        #Must resolve which peers are of higher pid
        for m in self.members:
            if m > self.pid:          
                #get the connection on a separate socket, then a write event will trigger send msg
                s = self.get_connection(self.members[m])
                #Set pending message
                if s: self.states[s] = State.SEND_ELECTION

        print('I finished sending messages')
        #set a timeout, then declare victory if nobody replies

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

    def send_msg(self, peer):
        #Attempt to send the message pending for this peer
        print('Sending message to member: {}, msg: {}'.format(peer, self.states[peer]))
        msg, time = self.states[peer]

        try:
            #if msg == State.SEND_OK:
                #time.sleep(5)
            #if msg == State.SEND_ELECTION:
                #peer.settimeout(5)
            data = pickle.dumps((msg.value, self.members))
            peer.sendall(data)
        except Exception as err:
            print('Failure occured while trying to send message to peer') 
        else:
            if msg == State.SEND_ELECTION: 
                self.set_state(State.WAITING_FOR_OK, peer)
                self.selector.modify(peer, selectors.EVENT_READ)
            if msg == State.SEND_OK: self.set_state(State.WAITING_FOR_VICTOR, self.listener)
            if msg == State.SEND_VICTORY: self.set_state(State.QUIESCENT, peer)
        finally:
            #I can close the socket if I'm not expecting any response from the peer afterwards
            if msg != State.SEND_ELECTION:
                self.selector.unregister(peer)
                peer.close()

    def receive_msg(self, peer):
        print('Trying to receive message')
        try:
            data = peer.recv(BUF_SZ)
            msg = pickle.loads(data)
            print(msg)
        except Exception as err:
            print('Failure accepting data from peer: {}'.format(err))
        else:
            #update members
            self.update_members(msg[1])
            if msg[0] == State.SEND_ELECTION.value:
                self.set_state(peer, State.SEND_OK)
                self.send_msg(peer)
                #start an election myself if I'm not in an election
                if self.listener not in self.states: self.start_election()
            if msg[0] == State.SEND_VICTORY.value:
                #FIXME how to extract pid?
                bullyaddr = peer.getpeername()
                for m in self.members:
                    if self.members[m] == bullyaddr:
                        self.bully = m
                print('Found new leader: {}'.format(m))
            if msg[0] == State.SEND_OK.value:
                self.states[self.listener] = State.WAITING_FOR_VICTOR
        finally:
            print('closing connection to peer: {}'.format(peer.getpeername()) )
            if peer.fileno() > 0 :
                self.selector.unregister(peer)
                peer.close()
                print('closed')

    def is_expired(self, peer=None, threshold=ASSUME_FAILURE_TIMEOUT):
        pass

    def stamp(self):
        return datetime.datetime.now().strftime('%H:%M:%S.%f')
    """ 
    def printSocket(self, sock):
        if sock is None or sock == self or sock == self.listener:
            return 'self'
        return self.cpr_sock(sock) 
    """

    def cpr_sock(sock):
        pass

    """
    Announce to all processes that this node won the election and is currently the bully:
    > send COORDINATOR message
    """
    def announce_victory(self):
        for m in self.members:
            #get the connection on a separate socket, then a write event will trigger send msg
            s = self.get_connection(self.members[m])
            #Set pending message
            if s: self.states[s] = State.SEND_VICTORY
        
        self.bully = self.pid

         
if __name__ == '__main__':
    """
    if len(sys.argv) != 3:
        print('Usage: python client.py BIRTHDAY SUID')
        exit(1)
    """
    nextBirthday = datetime.datetime(2023, 8, 26)
    SUID = sys.argv[1]

    #TODO: get this from the command line instead
    gcd_address = ['localhost', 7000]

    node = Lab2(gcd_address, nextBirthday, SUID)
    node.run()

