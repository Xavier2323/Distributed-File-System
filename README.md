Distributed-File-System
============================

This implementation provides a distributed file system with quorum-based replication using Thrift for communication.

Running the System
-----------------
0. Generate the Thrift code:
   ```
   thrift -r --gen py service.thrift
   ```

1. First, create directories for each replica:
   ```
   mkdir replica0 replica1 replica2 replica3 replica4 replica5 replica6
   ```

2. Start the replica servers:
   ```
   # Start coordinator (the one with flag = 1 in compute_nodes.txt)
   python3 replica_server.py 8000 replica0 compute_nodes.txt
   
   # Start other replicas (in separate terminals)
   python3 replica_server.py 8001 replica1 compute_nodes.txt
   python3 replica_server.py 8002 replica2 compute_nodes.txt
   python3 replica_server.py 8003 replica3 compute_nodes.txt
   python3 replica_server.py 8004 replica4 compute_nodes.txt
   python3 replica_server.py 8005 replica5 compute_nodes.txt
   python3 replica_server.py 8006 replica6 compute_nodes.txt
   ```

3. Use the client to interact with the system:
   ```
   # To write a file to the system
   python3 client.py write filename.txt /path/to/local/file.txt csel-kh1250-10 8000
   
   # To read a file from the system
   python3 client.py read filename.txt csel-kh1250-10 8000
   
   # To list all files in the system
   python3 client.py list csel-kh1250-10 8000
   ```

Note that you can connect to any replica for any operation. The system will handle forwarding requests to the coordinator and managing quorums internally.

Troubleshooting
--------------
- Make sure all replica servers are running before executing client commands.
- Check that the compute_nodes.txt file is accessible to all replicas.
- Ensure that the local directories for each replica exist and are writable.