import glob
import sys
import os
import random
import threading
import time
import socket
import queue
from collections import defaultdict

# Add Thrift generated code to path
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('../thrift-0.19.0/lib/py/build/lib*')[0])

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.server import TServer
from thrift.protocol import TMultiplexedProtocol, TBinaryProtocol
from thrift.TMultiplexedProcessor import TMultiplexedProcessor

from service import ReplicaService, CoordinatorService
from service.ttypes import FileChunk, FileMetadata, Response, StatusCode

class ConnectionPool:
    """Connection pool for managing connections to replicas"""
    def __init__(self, max_connections_per_replica=5, connection_timeout=5):
        self.connections = defaultdict(queue.Queue)
        self.max_connections_per_replica = max_connections_per_replica
        self.connection_timeout = connection_timeout
        self.connection_counts = defaultdict(int)
        self.lock = threading.RLock()
        
    def get_connection(self, ip, port, service_name="ReplicaService"):
        """Get a connection from the pool or create a new one if needed"""
        key = (ip, port, service_name)
        
        with self.lock:
            # Try to get an existing connection
            try:
                transport, client = self.connections[key].get_nowait()
                # Check if connection is still valid
                if transport.isOpen():
                    return transport, client
                # Connection is closed, create a new one
                transport.close()
            except queue.Empty:
                pass  # No existing connections available
            
            # Check if we've reached the connection limit
            if self.connection_counts[key] >= self.max_connections_per_replica:
                # Wait for a connection to become available with timeout
                try:
                    start_time = time.time()
                    while time.time() - start_time < self.connection_timeout:
                        try:
                            transport, client = self.connections[key].get(timeout=0.1)
                            if transport.isOpen():
                                return transport, client
                            # Connection is closed, create a new one
                            transport.close()
                            self.connection_counts[key] -= 1
                            break
                        except queue.Empty:
                            continue
                except queue.Empty:
                    pass  # Timed out waiting for a connection
            
            # Create a new connection
            transport = TSocket.TSocket(ip, port)
            transport.setTimeout(self.connection_timeout * 1000)  # Convert to milliseconds
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            
            # Use multiplexed protocol
            multiplexed_protocol = TMultiplexedProtocol.TMultiplexedProtocol(protocol, service_name)
            
            if service_name == "ReplicaService":
                client = ReplicaService.Client(multiplexed_protocol)
            elif service_name == "CoordinatorService":
                client = CoordinatorService.Client(multiplexed_protocol)
            else:
                raise ValueError(f"Unknown service name: {service_name}")
            
            transport.open()
            self.connection_counts[key] += 1
            return transport, client
    
    def release_connection(self, ip, port, service_name, transport, client):
        """Return a connection to the pool"""
        key = (ip, port, service_name)
        
        with self.lock:
            if transport.isOpen():
                self.connections[key].put((transport, client))
            else:
                # Connection is closed, decrement count
                self.connection_counts[key] -= 1
    
    def close_all(self):
        """Close all connections in the pool"""
        with self.lock:
            for connections_queue in self.connections.values():
                while True:
                    try:
                        transport, _ = connections_queue.get_nowait()
                        if transport.isOpen():
                            transport.close()
                    except queue.Empty:
                        break
            self.connections.clear()
            self.connection_counts.clear()

class ReplicaConnection:
    """Context manager for replica connections using the connection pool"""
    def __init__(self, handler, ip, port, service_name="ReplicaService"):
        self.handler = handler
        self.ip = ip
        self.port = port
        self.service_name = service_name
        self.client = None
        self.transport = None
        
    def __enter__(self):
        try:
            # Check if connecting to self
            if self.ip == self.handler.my_ip and self.port == self.handler.my_port:
                if self.service_name == "ReplicaService":
                    return self.handler, None
                elif self.service_name == "CoordinatorService" and self.handler.is_coordinator:
                    return self.handler, None
                
            # Get connection from pool
            self.transport, self.client = self.handler.connection_pool.get_connection(
                self.ip, self.port, self.service_name
            )
            
            return self.client, self.transport
        except Exception as e:
            # Make sure to release the connection if exception occurs
            if self.transport is not None:
                self.handler.connection_pool.release_connection(
                    self.ip, self.port, self.service_name, self.transport, self.client
                )
                self.transport = None
                self.client = None
            raise
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Return connection to pool on exit if it exists
        if self.transport is not None:
            self.handler.connection_pool.release_connection(
                self.ip, self.port, self.service_name, self.transport, self.client
            )
            self.transport = None
            self.client = None

class ReplicaHandler:
    def __init__(self, local_dir, is_coordinator=False, compute_nodes_file="compute_nodes.txt"):
        self.local_dir = local_dir
        self.is_coordinator = is_coordinator
        self.my_ip = None # This will be set when the server starts
        self.my_port = None # This will be set when the server starts
        
        # Create connection pool
        self.connection_pool = ConnectionPool(
            max_connections_per_replica=10,  # Adjust based on your system capacity
            connection_timeout=5  # 5 seconds timeout
        )
        
        # Read compute nodes file to get replica info
        self.compute_nodes = self.read_compute_nodes(compute_nodes_file)
        self.read_quorum_size = self.compute_nodes['read_quorum_size']
        self.write_quorum_size = self.compute_nodes['write_quorum_size']
        self.coordinator_info = self.compute_nodes['coordinator']
        self.replicas = self.compute_nodes['replicas']
        
        # Initialize request queue and processing thread
        self.request_queue = []
        self.queue_lock = threading.Lock()
        self.processing_thread = None
        
        # Create a thread pool for handling concurrent requests
        self.worker_threads = []
        self.max_workers = 20  # Adjust based on your system capacity
        self.request_queue = queue.Queue()
        self.shutdown_event = threading.Event()
        
        # Create local directory if it doesn't exist
        if not os.path.exists(self.local_dir):
            os.makedirs(self.local_dir)
            
        # If this is the coordinator, start the request processing thread
        if self.is_coordinator:
            print(f"Coordinator started")
            print(f"Read quorum size: {self.read_quorum_size}, Write quorum size: {self.write_quorum_size}")
            print(f"Total replicas: {len(self.replicas)}")
    
    def read_compute_nodes(self, filename):
        """Read the compute_nodes.txt file to get info about all replicas and quorum sizes"""
        result = {
            'replicas': [],
            'coordinator': None
        }
        
        with open(filename, 'r') as f:
            # First line contains read_quorum_size,write_quorum_size
            first_line = f.readline().strip()
            nr, nw = map(int, first_line.split(','))
            result['read_quorum_size'] = nr
            result['write_quorum_size'] = nw
            
            # Rest of the lines contain replica info: ip,port,is_coordinator
            for line in f:
                parts = line.strip().split(',')
                if len(parts) == 3:
                    ip, port, is_coordinator = parts
                    port = int(port)
                    is_coordinator = (is_coordinator == '1')
                    
                    if is_coordinator:
                        result['coordinator'] = (ip, port)
                    
                    result['replicas'].append((ip, port))
        
        return result
    
    def start_worker_threads(self):
        """Start worker threads to process requests concurrently"""
        for _ in range(self.max_workers):
            thread = threading.Thread(target=self.worker_thread_function)
            thread.daemon = True
            thread.start()
            self.worker_threads.append(thread)
        print(f"Started {self.max_workers} worker threads")
    
    def worker_thread_function(self):
        """Function executed by worker threads to process requests"""
        while not self.shutdown_event.is_set():
            try:
                # Get a request from the queue with timeout
                request = self.request_queue.get(timeout=0.1)
                
                try:
                    # Process the request
                    request_type = request[0]
                    
                    if request_type == "read":
                        filename, callback = request[1:]
                        self._handle_read_request(filename, callback)
                    elif request_type == "write":
                        filename, new_version, replica, callback = request[1:]
                        self._handle_write_request(filename, new_version, replica, callback)
                except Exception as e:
                    print(f"Error processing request: {e}")
                
                # Mark the request as done
                self.request_queue.task_done()
            
            except queue.Empty:
                # No requests in the queue, just continue
                continue
    
    def connect_to_replica(self, ip, port):
        """Connect to another replica and return the client"""
        with ReplicaConnection(self, ip, port, "ReplicaService") as (client, transport):
            if client == self:
                # Self-connection, no need for transport
                return client, None
            else:
                # Use the active connection but prevent the context manager from closing it
                return client, transport
    
    def connect_to_coordinator(self):
        """Connect to the coordinator and return the client"""
        ip, port = self.coordinator_info
        with ReplicaConnection(self, ip, port, "CoordinatorService") as (client, transport):
            if client == self:
                # Self-connection, no need for transport
                return client, None
            else:
                # Use the active connection but prevent the context manager from closing it
                return client, transport
    
    #-------------------------
    # File Operations
    #-------------------------
    
    def readFile(self, filename):
        """Handle a read file request from a client"""
        try:
            # Forward request to coordinator
            with ReplicaConnection(self, *self.coordinator_info, "CoordinatorService") as (coord_client, _):
                # Call the appropriate method based on whether we're self-connecting
                highest_version = coord_client.getHighestVersionForRead(filename)
            
            # Check if we have the file with the correct version
            local_path = os.path.join(self.local_dir, filename)
            local_version = self.getFileVersion(filename)
            
            if highest_version == -1:
                # File doesn't exist in the system
                return Response(status=StatusCode.FILE_NOT_FOUND, 
                                message=f"File {filename} not found in the system")
            
            if local_version == highest_version and os.path.exists(local_path):
                # We have the latest version
                return Response(status=StatusCode.SUCCESS, 
                                message=f"File available at {local_path}")
            else:
                # We need to copy the file from another replica
                # Find a replica with the highest version
                for ip, port in self.replicas:
                    try:
                        if ip == self.my_ip and port == self.my_port:
                            continue  # Skip ourselves
                            
                        with ReplicaConnection(self, ip, port) as (client, _):
                            if client == self:
                                continue  # Skip self-connection
                                
                            remote_version = client.getFileVersion(filename)
                            
                            if remote_version == highest_version:
                                # Copy file from this replica
                                source_replica = f"{ip}:{port}"
                                result = self.copyFile(filename, source_replica)
                                
                                if result.status == StatusCode.SUCCESS:
                                    return Response(status=StatusCode.SUCCESS, 
                                                    message=f"File available at {local_path}")
                    except Exception as e:
                        print(f"Error connecting to replica {ip}:{port}: {e}")
                
                return Response(status=StatusCode.SERVER_ERROR, 
                                message="Failed to find replica with the latest version")
        except Exception as e:
            print(f"Error in readFile: {e}")
            return Response(status=StatusCode.SERVER_ERROR, message=str(e))
    
    def writeFile(self, filename, clientFilePath):
        """Handle a write file request from a client"""
        try:
            if not os.path.exists(clientFilePath):
                return Response(status=StatusCode.FILE_NOT_FOUND, 
                               message=f"Client file {clientFilePath} not found")
            
            # Forward request to coordinator
            with ReplicaConnection(self, *self.coordinator_info, "CoordinatorService") as (coord_client, _):
                highest_version = coord_client.getHighestVersionForWrite(filename)
            
            # Copy the client file to our local directory
            local_path = os.path.join(self.local_dir, filename)
            new_version = highest_version + 1 if highest_version >= 0 else 0
            
            # Copy the file from client path to local directory using buffered approach
            with open(clientFilePath, 'rb') as src_file:
                with open(local_path, 'wb') as dst_file:
                    buffer_size = 8192  # 8KB buffer
                    while True:
                        buffer = src_file.read(buffer_size)
                        if not buffer:
                            break
                        dst_file.write(buffer)
            
            # Create a version file to track the version
            version_path = os.path.join(self.local_dir, f"{filename}.version")
            with open(version_path, 'w') as vf:
                vf.write(str(new_version))
            
            # Register write with coordinator
            with ReplicaConnection(self, *self.coordinator_info, "CoordinatorService") as (coord_client, _):
                response = coord_client.registerWrite(filename, new_version, f"{self.my_ip}:{self.my_port}")
            
            return response
        except Exception as e:
            print(f"Error in writeFile: {e}")
            return Response(status=StatusCode.SERVER_ERROR, message=str(e))
    
    def listFiles(self, query_others=True):
        """List all files in the system with their versions"""
        all_files = {}
        print(f"Listing files in local directory: {self.local_dir}")
        print(f"Querying other replicas: {query_others}")
        
        # Get local files first
        try:
            for filename in os.listdir(self.local_dir):
                if not filename.endswith('.version'):  # Skip version files
                    filepath = os.path.join(self.local_dir, filename)
                    if os.path.isfile(filepath):
                        version = self.getFileVersion(filename)
                        size = os.path.getsize(filepath)
                        all_files[filename] = FileMetadata(filename=filename, version=version, fileSize=size)
        except Exception as e:
            print(f"Error listing local files: {e}")
        
        # Get files from other replicas only if query_others is True
        if query_others:
            for ip, port in self.replicas:
                if ip == self.my_ip and port == self.my_port:
                    continue  # Skip ourselves
                        
                try:
                    # Use the context manager properly
                    temp_files = []
                    with ReplicaConnection(self, ip, port) as (client, _):
                        if client == self:  # Skip if self-connection
                            continue
                            
                        # Call listFiles with query_others=False to prevent recursion
                        temp_files = client.listFiles(False)
                    
                    # Process the results outside the with block
                    for file_meta in temp_files:
                        filename = file_meta.filename
                        version = file_meta.version
                        size = file_meta.fileSize
                        
                        if filename not in all_files or all_files[filename].version < version:
                            all_files[filename] = FileMetadata(filename=filename, version=version, fileSize=size)
                except Exception as e:
                    print(f"Error connecting to replica {ip}:{port}: {e}")
        
        # Convert dict to list
        return list(all_files.values())
    
    #-------------------------
    # File Transfer Operations
    #-------------------------
    
    def getFileChunk(self, filename, offset, chunkSize):
        """Get a chunk of a file"""
        try:
            filepath = os.path.join(self.local_dir, filename)
            if not os.path.exists(filepath):
                return FileChunk(data=b"", chunkSize=0)
            
            with open(filepath, 'rb') as f:
                f.seek(offset)
                data = f.read(chunkSize)
                return FileChunk(data=data, chunkSize=len(data))
        except Exception as e:
            print(f"Error in getFileChunk: {e}")
            return FileChunk(data=b"", chunkSize=0)
    
    def getFileSize(self, filename):
        """Get the size of a file"""
        try:
            filepath = os.path.join(self.local_dir, filename)
            if not os.path.exists(filepath):
                return -1
            return os.path.getsize(filepath)
        except Exception as e:
            print(f"Error in getFileSize: {e}")
            return -1
    
    #-------------------------
    # Quorum Protocol Operations
    #-------------------------
    
    def getFileVersion(self, filename):
        """Get the version of a file"""
        try:
            version_path = os.path.join(self.local_dir, f"{filename}.version")
            if os.path.exists(version_path):
                with open(version_path, 'r') as f:
                    return int(f.read().strip())
            elif os.path.exists(os.path.join(self.local_dir, filename)):
                # File exists but no version file, assume version 0
                return 0
            else:
                # File doesn't exist
                return -1
        except Exception as e:
            print(f"Error in getFileVersion: {e}")
            return -1
    
    def updateFile(self, filename, version):
        """Update a file to match the specified version"""
        try:
            # Just update the version file
            version_path = os.path.join(self.local_dir, f"{filename}.version")
            with open(version_path, 'w') as f:
                f.write(str(version))
            return Response(status=StatusCode.SUCCESS, message="File version updated")
        except Exception as e:
            print(f"Error in updateFile: {e}")
            return Response(status=StatusCode.SERVER_ERROR, message=str(e))
    
    #-------------------------
    # Inter-replica communication
    #-------------------------
    
    def copyFile(self, filename, sourceReplica):
        """Copy a file from another replica"""
        print(f"Copying file {filename} from {sourceReplica}")
        try:
            ip, port = sourceReplica.split(':')
            port = int(port)
            
            if ip == self.my_ip and port == self.my_port:
                return Response(status=StatusCode.SUCCESS, 
                                message=f"File {filename} already exists on this replica")
            
            with ReplicaConnection(self, ip, port) as (client, _):
                # Get file size
                file_size = client.getFileSize(filename)
                if file_size < 0:
                    return Response(status=StatusCode.FILE_NOT_FOUND, 
                                    message=f"File {filename} not found on source replica")
                
                print(f"Copying file {filename} from {sourceReplica} of size {file_size} bytes")
                
                # Get file version
                version = client.getFileVersion(filename)
                if version < 0:
                    return Response(status=StatusCode.FILE_NOT_FOUND, 
                                    message=f"File {filename} version information not found")
                
                # Download file in chunks
                local_path = os.path.join(self.local_dir, filename)
                with open(local_path, 'wb') as f:
                    offset = 0
                    chunk_size = 2048  # Max chunk size
                    
                    while offset < file_size:
                        chunk = client.getFileChunk(filename, offset, chunk_size)
                        if chunk.chunkSize == 0:
                            break  # No more data
                        
                        f.write(chunk.data)
                        offset += chunk.chunkSize
                
                # Update version
                version_path = os.path.join(self.local_dir, f"{filename}.version")
                with open(version_path, 'w') as vf:
                    vf.write(str(version))
                
                return Response(status=StatusCode.SUCCESS, 
                                message=f"Successfully copied file {filename} from {sourceReplica}")
        except Exception as e:
            print(f"Error in copyFile: {e}")
            return Response(status=StatusCode.SERVER_ERROR, message=str(e))

    #-------------------------
    # Coordinator operations (if this replica is the coordinator) ***
    #-------------------------
    
    def start_processing_thread(self):
        """Start the worker threads for processing requests (coordinator only)"""
        if not self.is_coordinator:
            return
        
        # Start worker threads
        self.start_worker_threads()
    
    def _handle_read_request(self, filename, callback):
        """Internal method to handle a read request (coordinator only)"""
        if not self.is_coordinator:
            return
            
        # Select random replicas for read quorum
        read_quorum = random.sample(self.replicas, self.read_quorum_size)
        print(f"Selected read quorum for {filename}: {read_quorum}")
        
        # Query each replica in the quorum for file version
        highest_version = -1
        
        for ip, port in read_quorum:
            try:
                with ReplicaConnection(self, ip, port) as (client, _):
                    version = client.getFileVersion(filename)
                    
                    if version > highest_version:
                        highest_version = version
            except Exception as e:
                print(f"Error querying replica {ip}:{port}: {e}")
        
        # Return highest version
        callback(highest_version)
    
    def _handle_write_request(self, filename, new_version, replica_str, callback):
        """Internal method to handle a write request (coordinator only)"""
        if not self.is_coordinator:
            return
            
        # If this is a query for highest version (new_version is None)
        if new_version is None:
            # Select random replicas for write quorum
            write_quorum = random.sample(self.replicas, self.write_quorum_size)
            print(f"Selected write quorum for version query of {filename}: {write_quorum}")
            
            # Query each replica in the quorum for file version
            highest_version = -1
            
            for ip, port in write_quorum:
                try:
                    with ReplicaConnection(self, ip, port) as (client, _):
                        version = client.getFileVersion(filename)
                        
                        if version > highest_version:
                            highest_version = version
                except Exception as e:
                    print(f"Error querying replica {ip}:{port}: {e}")
            
            # Return highest version
            callback(highest_version)
            return
        
        # This is a write operation
        # Select random replicas for write quorum
        write_quorum = random.sample(self.replicas, self.write_quorum_size)
        print(f"Selected write quorum for {filename}: {write_quorum}")
        
        # Copy file to all replicas in write quorum
        success = True
        error_messages = []
        
        for ip, port in write_quorum:
            try:
                with ReplicaConnection(self, ip, port) as (client, _):
                    # If we're in the write quorum, copy file to us if we're not the source
                    if ip == self.my_ip and port == self.my_port:
                        if replica_str != f"{self.my_ip}:{self.my_port}":
                            result = self.copyFile(filename, replica_str)
                            if result.status != StatusCode.SUCCESS:
                                success = False
                                error_messages.append(f"Error copying file to self: {result.message}")
                    else:
                        print(f"Copying file {filename} to replica {ip}:{port}")
                        response = client.copyFile(filename, replica_str)
                        
                        if response.status != StatusCode.SUCCESS:
                            success = False
                            error_messages.append(f"Error copying file to replica {ip}:{port}: {response.message}")
            except Exception as e:
                success = False
                error_messages.append(f"Error connecting to replica {ip}:{port}: {e}")
        
        # Return result
        if success:
            callback(Response(status=StatusCode.SUCCESS, 
                              message=f"Successfully wrote file {filename} version {new_version}"))
        else:
            error_msg = "; ".join(error_messages[:3])  # Limit the number of error messages
            if len(error_messages) > 3:
                error_msg += f"; and {len(error_messages) - 3} more errors"
                
            callback(Response(status=StatusCode.SERVER_ERROR, 
                              message=f"Failed to write file to all replicas in quorum: {error_msg}"))

    #-------------------------
    # Coordinator service implementation
    #-------------------------
    
    def getHighestVersionForRead(self, filename):
        """Get the highest version of a file for a read operation (coordinator service)"""
        if not self.is_coordinator:
            raise Exception("This method should only be called on the coordinator")
            
        result = [-1]  # Use list as mutable container
        event = threading.Event()
        
        # Define callback
        def callback(version):
            result[0] = version
            event.set()
        
        # Add request to queue
        self.request_queue.put(("read", filename, callback))
        
        # Wait for result with timeout
        if not event.wait(timeout=10):  # 10 second timeout
            print(f"Timeout waiting for read quorum for {filename}")
        
        return result[0]
    
    def getHighestVersionForWrite(self, filename):
        """Get the highest version of a file for a write operation (coordinator service)"""
        if not self.is_coordinator:
            raise Exception("This method should only be called on the coordinator")
            
        result = [-1]  # Use list as mutable container
        event = threading.Event()
        
        # Define callback
        def callback(version):
            result[0] = version
            event.set()
        
        # Add request to queue with None for new_version and replica to indicate this is just a version query
        self.request_queue.put(("write", filename, None, None, callback))
        
        # Wait for result with timeout
        if not event.wait(timeout=10):  # 10 second timeout
            print(f"Timeout waiting for write quorum for {filename}")
        
        return result[0]
    
    def registerWrite(self, filename, newVersion, replicaStr):
        """Register a write operation (coordinator service)"""
        if not self.is_coordinator:
            raise Exception("This method should only be called on the coordinator")
            
        result = [None]  # Use list as mutable container
        event = threading.Event()
        
        # Define callback
        def callback(response):
            result[0] = response
            event.set()
        
        # Add request to queue with proper parameters
        self.request_queue.put(("write", filename, newVersion, replicaStr, callback))
        
        # Wait for result with timeout
        if not event.wait(timeout=30):  # 30 second timeout for writes
            return Response(status=StatusCode.SERVER_ERROR, 
                           message="Timeout waiting for write operation to complete")
        
        return result[0]

    def shutdown(self):
        """Shutdown the handler and all its threads"""
        # Signal worker threads to exit
        self.shutdown_event.set()
        
        # Wait for all worker threads to finish
        for thread in self.worker_threads:
            thread.join(timeout=1.0)
        
        # Close all connections in the pool
        self.connection_pool.close_all()

def serve_replica(handler, port):
    """Start a Thrift server for a replica"""
    
    handler.my_ip = "localhost"  # For testing purposes, use localhost
    # Get IP address
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("8.8.8.8", 80))
        handler.my_ip = sock.getsockname()[0]
    finally:
        sock.close()
    handler.my_port = port

    # Set up processor
    if handler.is_coordinator:
        # Use TMultiplexedProcessor to serve both services
        processor = TMultiplexedProcessor()
        
        # Register replica service with the same name used in connect_to_replica
        processor.registerProcessor("ReplicaService",
                                   ReplicaService.Processor(handler))
        processor.registerProcessor("CoordinatorService",
                                   CoordinatorService.Processor(handler))
    else:
        # For non-coordinator replicas, we need to use TMultiplexedProcessor too
        # to ensure the service name is registered consistently
        processor = TMultiplexedProcessor()
        processor.registerProcessor("ReplicaService",
                                   ReplicaService.Processor(handler))

    # Set up transport, protocol
    transport = TSocket.TServerSocket(host='0.0.0.0', port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    # Use a threaded server with a larger thread pool
    server = TServer.TThreadPoolServer(
        processor, 
        transport, 
        tfactory, 
        pfactory,
        threads=50  # Increase the thread pool size to handle more concurrent connections
    )

    print(f"Starting {'coordinator' if handler.is_coordinator else 'replica'} server on port {port}")
    
    # Start the worker threads if this is the coordinator
    if handler.is_coordinator:
        handler.start_processing_thread()
        
    try:
        server.serve()
    except KeyboardInterrupt:
        print("Shutting down server...")
    finally:
        # Clean up resources
        handler.shutdown()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 replica_server.py <port> <local_dir> [compute_nodes_file]")
        sys.exit(1)
    
    port = int(sys.argv[1])
    local_dir = sys.argv[2]
    compute_nodes_file = sys.argv[3] if len(sys.argv) > 3 else "compute_nodes.txt"
    
    # Read compute_nodes.txt to determine if this is the coordinator
    with open(compute_nodes_file, 'r') as f:
        # Skip the first line (quorum sizes)
        f.readline()
        
        # Check remaining lines to see if this replica is the coordinator
        is_coordinator = False
        for line in f:
            parts = line.strip().split(',')
            if len(parts) == 3:
                ip, p, coord_flag = parts
                if int(p) == port and coord_flag == '1':
                    is_coordinator = True
                    break
    
    handler = ReplicaHandler(local_dir, is_coordinator, compute_nodes_file)
    serve_replica(handler, port)