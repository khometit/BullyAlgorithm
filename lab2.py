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
            events = self.selector.select(1000)
            for key, mask in events:
                if key.fileobj == self.listener:
                    self.accept_peer()
                elif mask & selectors.EVENT_READ:
                    self.receive_msg(key.fileobj)
                else:
                    self.send_msg(key.fileobj)
            self.check_timeouts()

    def check_timeouts(self):
        pass

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

    def set_state(self, state, peer):
        self.states[peer] = state

        #Register the new connection with the selector also
        if state.is_incoming():
            self.selector.register(peer, selectors.EVENT_READ)

    def accept_peer(self):
        try:
            conn, addr = self.listener.accept()
            print('Accepted connection from peer {}'.format(addr))
            self.set_state(State.WAITING_FOR_ANY_MESSAGE, conn)
        except Exception as err:
            print('Failure to accept connections from a peer: {}'.format(addr))

    def get_connection(self, member):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.selector.register(s, selectors.EVENT_WRITE)

        try:
            s.connect(member)
        except Exception as err:
            print('Failure connecting to member {}'.format(member))
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
                s = self.get_connection(self.members[m])
                if s: self.send_msg(s)
        
        print('I finished sending messages')
        #set a timeout, then declare victory if nobody replies

    """
    Announce to all processes that this node won the election and is currently the bully:
    > send COORDINATOR message
    """
    def announce_victory():
        
        pass

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
        #The server becomes a client
        print('Sending message to member')
        if self.bully == None:
            try:
                data = pickle.dumps((State.SEND_ELECTION.value, self.members))
                peer.sendall(data)
            except Exception as err:
                print('Failure occured while trying to send message to peer')
            finally:
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
        finally:
            self.selector.unregister(peer)
            peer.close()

         
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

