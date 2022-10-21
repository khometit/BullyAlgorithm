# Bully Algorithm

An implementation of nodes in a distributed system trying to coordinate by using the 
Bully Algorithm to find a common coordinator for the group. 

Nodes will initiate an election on start-up and/or whenever it notices that the current leader is out of contact.
> send 'VOTE' messages to processes with higher pids.
> higher pids replies with 'OK', to which the lower process will change its state to waiting for coordinator message, within a time limit
> higher pids then start an election themselves if they haven't already
> Either the process with highest pid in the group or the current highest pid in the active group declares victory, to which it sends 'COORDINATOR' message.

Implemented 'PROBE' message as a simulation for other processes to ping the current leader with work. If leader fails to respond then it means the leader is down; thus, triggers a new election. 

Implmented 'FRINGE' feature as a simulation for a server shutdown, which should start an election once it goes back online.
