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
    ASSUME_FAILURE_TIMEOUT = 10          #Only wait 4 seconds till something else should happen
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
        self.start_election('Node has just started up.')

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
        #If state == WAITING FOR OK: all timed out -> declare victorious

        if self.get_state(self) == State.WAITING_FOR_VICTOR and self.is_expired(self,11):
            self.states.clear()
            #TODO clear sockets registered too?
            self.start_election('Have not received COORDINATOR message')
        elif self.get_state(self) == State.WAITING_FOR_OK and self.is_expired(self):
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
        try:
            conn, addr = self.listener.accept()
            print('Accepted connection from peer addr: {}, conn: {}'.format(addr, conn))
            
            #Mark that I'm waiting for any message from this peer
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
        #Set the state with a time stamp, which I can use to check timeouts/victory
        self.states[peer] = [state, self.stamp()]
        print(self.states[peer])

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

        if state == State.SEND_OK:
            self.send_msg(peer)


    def get_connection(self, member):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setblocking(False)
        self.selector.register(s, selectors.EVENT_WRITE)
         
        try:
            s.settimeout(2.0)
            s.connect(member)
        except Exception as err:
            print('Failure connecting to member {}'.format(member))
            self.selector.unregister(s)
            s.close()
        else:
            return s
            
    """
    Start an election when someone notices that the leader is down or when the node first joined the group.
    > ELECTION message is sent to processes with high pid. 
    > If no response received within the given time limit, announce victory. 
    > During wait time, the node state will in election-in-progress
    """
    def start_election(self, reason):
        print('\n--------Starting a new election. Reason: {}'.format(reason))
        self.bully = None                                   #Voting in progress
        self.set_state(State.WAITING_FOR_OK, self)          #Set myself in waiting for ok first in case I am actually the biggest bully

        #Must resolve which peers are of higher pid
        for m in self.members:
            if m > self.pid:          
                #get the connection on a separate socket, then a write event will trigger send msg
                s = self.get_connection(self.members[m])
                #Set pending message
                if s: self.set_state(State.SEND_ELECTION, s)

        print('I finished sending messages')
        #set a timeout, then declare victory if nobody replies

    def is_election_in_progress(self):
        """
        Helper for checking if election is in progress or not. None means it is.
        return: True if no bully is registered, False otherwise.
        """
        print('Election is in progress: {}'.format(self.bully == None))
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

    def send_msg(self, peer):
        #Attempt to send the message pending for this peer
        msg = self.states[peer][0]
        print('Sending message to member: {}, msg: {}'.format(peer.getpeername(), msg.value))

        try:
            #if msg == State.SEND_OK:
                #time.sleep(5)
            #if msg == State.SEND_ELECTION:
                #peer.settimeout(5)
            data = pickle.dumps((msg.value, self.members))
            peer.sendall(data)
        except Exception as err:
            print('Failure occured while trying to send message to peer', err) 
        else:
            #Mark that I'm waiting for a response from the nodes I sent election msg to
            if msg == State.SEND_ELECTION: 
                self.set_state(State.WAITING_FOR_OK, peer)
                self.selector.modify(peer, selectors.EVENT_READ)
             
            #if msg == State.SEND_OK: 
            #    self.set_state(State.WAITING_FOR_VICTOR, self.listener)
            
            #Taken care of in announce_victory
            #if msg == State.SEND_VICTORY: 
            #    #Probably need to wipe everything
            #    self.set_state(State.QUIESCENT, peer)
        finally:
            #I can close the socket if I'm not expecting any response from the peer afterwards
            if msg != State.SEND_ELECTION:
                self.selector.unregister(peer)
                peer.close()

    def receive_msg(self, peer):    
        try:
            data = peer.recv(BUF_SZ)
            msg = pickle.loads(data)
            print('Trying to receive message: {}\n'.format(msg))
            #print(msg)
        except Exception as err:
            print('Failure accepting data from peer: {}'.format(err))
        else:
            #update members
            self.update_members(msg[1])
            #Received election message
            if msg[0] == State.SEND_ELECTION.value:
                self.set_state(State.SEND_OK, peer)
                #self.send_msg(peer)
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

        finally:
            if peer.fileno() > 0 :
                print('closing connection to peer: {}'.format(peer.getpeername()) )
                self.selector.unregister(peer)
                peer.close()
                print('closed')
            else:
                print('connection closed')

    def note_bully(self, memberList):
        #Assuming coordinator message's content includes no process with higher pid than herself
        pids = list(memberList.keys())
        highest = pids[0]

        for i in range(1, len(pids)):
            if pids[i] > highest:
                highest = pids[i]

        self.bully = highest
        print('Found new leader: {}'.format(self.bully))

    def is_expired(self, peer=None, threshold=ASSUME_FAILURE_TIMEOUT):
        state, stampedTime = self.get_state(peer, detail=True)
        print('state: {}, time: {}'.format(state, stampedTime))
        timepassed = (datetime.datetime.now() - stampedTime).total_seconds()    #get the time delta in seconds
        print('elapsed time: ', timepassed)
        return timepassed > threshold

    def stamp(self):
        return datetime.datetime.now()

    def print_timestamp(self):
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
        #Election is over, stop check_timeouts
        self.set_state(State.QUIESCENT, self)

        for m in self.members:
            if m != self.pid:
                #get the connection on a separate socket, then a write event will trigger send msg
                s = self.get_connection(self.members[m])
                #Set pending message
                if s: self.set_state(State.SEND_VICTORY, s)
        
        self.bully = self.pid
        print('I, {}, won the election woooo\n\n'.format(self.pid))
        for s in self.states:
            print('all states left: ', s, 'state ', self.states[s])

         
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