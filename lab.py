"""
CPSC 5520, Seattle University
Author: Alicia Garcia
Version: 1.0
"""
import types
from datetime import date, datetime
import selectors
import socket
import pickle
import sys
from enum import Enum


class State(Enum):
    """
    Enumeration of states I can be in (copied from professor notes)
    """
    QUIESCENT = 'QUIESCENT'  # default state

    # Outgoing message is pending
    SEND_ELECTION = 'ELECTION'
    SEND_VICTORY = 'COORDINATOR'
    SEND_OK = 'OK'

    # Incoming message is pending
    WAITING_FOR_OK = 'WAIT_OK'  # When I've sent them an ELECTION message
    WAITING_FOR_VICTOR = 'WHO IS THE WINNER?'  # This one only applies to myself
    WAITING_FOR_ANY_MESSAGE = 'WAITING'  # When I've done an accept on their connect to my server

    def is_incoming(self):
        """
        Helper method to return fi the state is available to accept messages
        :return: True if the state is not in the list, else False
        """
        return self not in (State.SEND_ELECTION, State.SEND_VICTORY, State.SEND_OK)


class BullyClient:
    """
    The Bully Client will send and receive messages to determine who is the leader (bully)
    in the server. The process is run synchronously to handle all connections and messages
    """

    def __init__(self, host, port, next_bday, su_id):
        self.gcd_address = (host, int(port))

        days_to_bd = (next_bday - date.today()).days  # determine the number of days till their next bday
        self.process_id = (days_to_bd, su_id)

        self.member_connections = {}  # creating dictionary for holding connected members
        self.connection_states = {}  # creating dictionary for states of the connections with me
        self.leader = None  # election pending as indicated by None
        self.selector = selectors.DefaultSelector()

        self.listening_server, self.listening_address = self.create_listening_server()

        self.BUF_SIZE = 1024
        self.timeout = float(1.500)
        self.failed_msg = 'Failed to connect: '

    def connect(self):
        """
        This method will set up the connection to the GCD server and run the other
        processes for handling outgoing and incoming messages
        :return:
        """
        self.join_group()

        self.start_election()

        try:
            while True:
                events = self.selector.select(timeout=self.timeout)
                for key, mask in events:
                    if key.fileobj == self.listening_server:
                        self.accept_new_connection(key.fileobj)
                    elif mask & selectors.EVENT_READ:
                        self.receive_msg(key.fileobj)
                    else:
                        self.send_msg(key.fileobj)
        except KeyboardInterrupt:
            print('Keyboard interrupt, exiting')
        except socket.timeout as to:
            print(self.failed_msg, repr(to))
        except OSError as err:
            print(self.failed_msg, repr(err))
        finally:
            self.selector.close()

    def join_group(self):
        """
        Create a connection to the GCD server
        :return:
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            print('Contacting GCD Server...\n')
            # check we connect to the server successfully
            if self.contact_server(sock, self.gcd_address[0], self.gcd_address[1], False) is False:
                sys.exit(1)

            print('Successfully connected to GCD Server...\n')

            join_msg = ('JOIN', (self.process_id, self.listening_address))
            sock.sendall(pickle.dumps(join_msg))
            self.member_connections = pickle.loads(sock.recv(self.BUF_SIZE))
            print(self.member_connections)

    def contact_server(self, soc, host, port, register):
        """
        This helper function will handle the connection checks. If connection failed, return false
        else true
        :param register: boolean to determine if we need to register a connection to the selector
        :param soc: the socket stream
        :param host: host address to connect to
        :param port: port to connect to
        :return: false if connection fails, otherwise true
        """
        # First try the connect, if failed, we display the message and exit
        try:
            soc.settimeout(self.timeout)
            soc.connect((host, port))
            if register:
                self.selector.register(soc, selectors.EVENT_READ)  # make sure to register the socket as Read to accept messages
            return True
        except socket.timeout as to:
            print(self.failed_msg, repr(to))
            return False
        except OSError as err:
            print(self.failed_msg, repr(err))
            return False

    def create_listening_server(self):
        """
        Creating a listening server so peers can connect to my client
        :return: listening server and tuple of the listener address
        """
        listening_host = 'localhost'  # get the host name
        port = 0  # using 0 as our port, as this will make the library automatically choose avail. port

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # create instance of a socket
        try:
            server.bind((listening_host, port))  # bind host and port together
        except socket.error as err:  # if the binding fails, we want to be notified
            print('Failed to bind listening_server: {}'.format(err))
            sys.exit(1)  # system should close because client can't receive messages

        # configure how many clients the server can listen to at once, I want 1000 b/c it's a nice number
        server.listen(1000)
        server.setblocking(False)  # set the socket to a non-blocking mode
        self.selector.register(server, selectors.EVENT_READ, data=None)  # register the socket

        return server, server.getsockname()

    def accept_new_connection(self, member_socket):
        """
        This method will accept connection requests when a member tries to contact me.
        If the member doesn't already exist in my list of member_connections, I add them
        and set their state accordingly
        :param member_socket: member in the server attempting to connect with my server
        :return:
        """
        new_conn, new_addr = member_socket.accept()
        print('Accepted connection at address {}\n'.format(new_addr))
        new_conn.setblocking(False)
        events = selectors.EVENT_READ

        # populate the member_states dict with the members(key) and set their state (value) to WAITING
        self.set_state(State.WAITING_FOR_ANY_MESSAGE, new_conn)
        print('New connection was set to {} state\n'.format(self.connection_states[new_conn]))

    def send_msg(self, member_socket):
        """
        This method sends out messages to the member after confirming it's not us
        :param member_socket: the recipient socket of the message
        :return:
        """
        print('{}: sent'.format(self.cpr_sock(member_socket)))
        state = self.get_state(member_socket)
        

        try:
            self.send(member_socket, state.value)
        except ConnectionError as err:
            print(self.failed_msg, repr(err))
        except Exception as err:
            print(self.failed_msg, repr(err))

        if state == State.SEND_ELECTION:
            self.set_state(State.WAITING_FOR_OK, member_socket)
            self.selector.modify(member_socket, selectors.EVENT_READ)
        else:
            self.set_quiescent(member_socket)

    def send(self, member_sock, value):
        """
        This method facilitates the send functionality of the message associated with the member_sock
        :param member_sock: the socket member
        :param value: the state of the member socket
        :return:
        """
        member_sock.send(pickle.dumps(value))
        print('Message {} sent\n'.format(value))

    def receive_msg(self, member_socket):
        """
        This method allows me to receive messages from other members in the server
        :param member_socket: the socket sending us a message
        :return: none
        """

        print('{}: received'.format(self.cpr_sock(member_socket)))

        message = pickle.loads(member_socket.recv(self.BUF_SIZE))

        if message is None:
            print('No message received, closing connection with: {}'.format(member_socket))
            self.set_quiescent(member_socket)
        elif message == 'ELECTION':
            print('Received {}'.format(message))
            self.set_state(State.SEND_OK, member_socket)
            self.send_msg(member_socket)
            self.start_election()
        elif message == 'COORDINATOR':
            print('Received {}'.format(message))
            print('The leader is now {}'.format(self.assign_leader(member_socket)))
            self.set_quiescent(member_socket)
        elif message == 'OK':
            print('Received {}'.format(message))
            self.set_state(State.WAITING_FOR_VICTOR, member_socket)
        else:
            print('Unexpected Received {}'.format(message))

    def start_election(self):
        """
        This method sends a message to all members of the server to determine who's the leader. Message is
        only broadcast to members who are greater than me
        :return:
        """
        for member_pid in self.member_connections.keys():
            if member_pid[0] < self.process_id[0]:
                continue  # we don't need to contact or anyone who is less than us
            if member_pid[1] == self.process_id[1]:
                continue  # we can assume this is us since it's our SUID, so we don't want to contact them
            if member_pid[0] == self.process_id[0] & member_pid[1] < self.process_id[1]:
                continue  # based on requirements, if we have same days_to_bday, check suid. If we are bigger, don't contact

            print('Starting election')
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as member_socket:
                # Call the helper function to contact server and check if false
                self.connection_states[member_socket] = State.SEND_ELECTION
                if self.contact_server(member_socket, self.member_connections[member_pid][0], self.member_connections[member_pid][1], True) is False:
                    self.set_quiescent(member_socket)
                try:
                    self.set_state(State.SEND_ELECTION, member_socket)
                    self.send_msg(member_socket)
                except ConnectionError as err:
                    print(self.failed_msg, repr(err))
                except Exception as err:
                    print(self.failed_msg, repr(err))

    def declare_victory(self):
        """
        This method will set all of the connection_states to Send_Victory and then
        sends the corresponding victory message.
        :return:
        """
        for soc in self.member_connections:
            self.connection_states[soc] = State.SEND_VICTORY
            self.send_msg(soc)
        self.assign_leader(self.process_id)
        print('Leader is self')

    def assign_leader(self, new_leader):
        """
        This method will assign the leader to the member passed
        :param new_leader: member who won the election
        :return: the new leader
        """
        return self.leader == new_leader

    def get_state(self, member_socket):
        """
        Return the state of a given member
        :param member_socket: member in the connections
        :return: return the state value
        """
        return self.connection_states[member_socket]

    def set_state(self, state, member_socket):
        """
        This sets the state of a member_socket. If the message is Send_Election, we want to update the
        selector to EVENT_READ so they can receive messages after sending one
        :param state: The ENUM state of the socket
        :param member_socket: the connection socket used as a key for connection_states
        :param switch_mode: signal to reregister with a different mask in the selector
        :return:
        """
        self.connection_states[member_socket] = state

        if state.is_incoming() and state != State.WAITING_FOR_OK:
            self.selector.register(member_socket, selectors.EVENT_READ)

    def set_quiescent(self, member_socket):
        """
        This method will set the state of a connection to Quiescent and unregister the socket from the
        selector and close the socket
        :param member_socket:
        :return:
        """
        self.connection_states[member_socket] = State.QUIESCENT
        self.selector.unregister(member_socket)
        member_socket.close()

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

if __name__ == '__main__':
    """
    Main function to run bully client program
    """
    if not 4 <= len(sys.argv) <= 5:
        print("Usage: python3 lab2.py GCDHOST GCDPORT SUID [DOB YYYY-MM-DD]")
        exit(1)

    if len(sys.argv) == 5:  # if we receive a dob, format it
        dob = sys.argv[4].split('-')
        curr_date = date.today()
        next_bd = date(curr_date.year, int(dob[1]), int(dob[2]))
        if next_bd < curr_date:
            next_bd = date(next_bd.year + 1, next_bd.month, next_bd.day)
    else:
        next_bd = date(2023, 1, 1)  # if no DOB is provided, set a default one

    print('Next Birthday: ', next_bd)
    suid = int(sys.argv[3])
    print('SU ID: ', suid)
    gcdHost = sys.argv[1]
    gcdPort = sys.argv[2]

    bully = BullyClient(gcdHost, gcdPort, next_bd, suid)
    bully.connect()