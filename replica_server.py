#!/usr/bin/env python3
import glob
import sys
import os
import random
import threading
import time
import socket

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

class ReplicaHandler:
    def __init__(self, local_dir, is_coordinator=False, compute_nodes_file="compute_nodes.txt"):
        self.local_dir = local_dir
        self.is_coordinator = is_coordinator
        self.my_ip = None # This will be set when the server starts
        self.my_port = None # This will be set when the server starts
        
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
    
    def connect_to_replica(self, ip, port):
        """Connect to another replica and return the client"""
        transport = None
        try:
            transport = TSocket.TSocket(ip, port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            
            # Make sure to use the correct service name that matches what's registered on the server
            multiplexed_protocol = TMultiplexedProtocol.TMultiplexedProtocol(protocol, "ReplicaService")
            client = ReplicaService.Client(multiplexed_protocol)
            
            transport.open()
            print(f"Connected to replica {ip}:{port}")
            return client, transport
        except Exception as e:
            print(f"Error connecting to replica {ip}:{port}: {e}")
            if transport and transport.isOpen():
                transport.close()
            raise
    
    def connect_to_coordinator(self):
        """Connect to the coordinator and return the client"""
        ip, port = self.coordinator_info
        transport = None
        
        # Use the correct service based on whether we're connecting to ourselves
        if ip == self.my_ip and port == self.my_port:
            # If we are the coordinator, return self as the client (no transport needed)
            return self, None
        else:
            try:
                # Create a remote connection
                transport = TSocket.TSocket(ip, port)
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                protocol = TMultiplexedProtocol.TMultiplexedProtocol(protocol, "CoordinatorService")
                client = CoordinatorService.Client(protocol)
                transport.open()
                return client, transport
            except Exception as e:
                print(f"Error connecting to coordinator {ip}:{port}: {e}")
                if transport and transport.isOpen():
                    transport.close()
                raise
    
    #-------------------------
    # File Operations
    #-------------------------
    
    def readFile(self, filename):
        """Handle a read file request from a client"""
        try:
            # Forward request to coordinator
            coord_client, transport = self.connect_to_coordinator()
            
            try:
                # If we're not connecting to ourselves, call the remote method
                if coord_client != self:
                    highest_version = coord_client.getHighestVersionForRead(filename)
                else:
                    # If we are the coordinator, call the method directly
                    highest_version = self.getHighestVersionForRead(filename)
            finally:
                # Close transport if it exists
                if transport:
                    transport.close()
            
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
                            
                        client, trans = self.connect_to_replica(ip, port)
                        try:
                            remote_version = client.getFileVersion(filename)
                            
                            if remote_version == highest_version:
                                # Copy file from this replica
                                source_replica = f"{ip}:{port}"
                                result = self.copyFile(filename, source_replica)
                                
                                if result.status == StatusCode.SUCCESS:
                                    return Response(status=StatusCode.SUCCESS, 
                                                    message=f"File available at {local_path}")
                        finally:
                            trans.close()
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
            coord_client, transport = self.connect_to_coordinator()
            
            try:
                # If we're not connecting to ourselves, call the remote method
                if coord_client != self:
                    highest_version = coord_client.getHighestVersionForWrite(filename)
                else:
                    # If we are the coordinator, call the method directly
                    highest_version = self.getHighestVersionForWrite(filename)
            finally:
                # Close transport if it exists
                if transport:
                    transport.close()
            
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
            coord_client, transport = self.connect_to_coordinator()
            
            try:
                # If we're not connecting to ourselves, call the remote method
                if coord_client != self:
                    response = coord_client.registerWrite(filename, new_version, f"{self.my_ip}:{self.my_port}")
                else:
                    # If we are the coordinator, call the method directly
                    response = self.registerWrite(filename, new_version, f"{self.my_ip}:{self.my_port}")
            finally:
                # Close transport if it exists
                if transport:
                    transport.close()
            
            return response
        except Exception as e:
            print(f"Error in writeFile: {e}")
            return Response(status=StatusCode.SERVER_ERROR, message=str(e))
    
    def listFiles(self):
        """List all files in the system with their versions"""
        all_files = {}
        
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
        
        # Get files from other replicas
        for ip, port in self.replicas:
            if ip == self.my_ip and port == self.my_port:
                continue  # Skip ourselves
                
            try:
                client, transport = self.connect_to_replica(ip, port)
                try:
                    replica_files = client.listFiles()
                
                    # Add to our collection, keeping highest version
                    for file_meta in replica_files:
                        filename = file_meta.filename
                        version = file_meta.version
                        size = file_meta.fileSize
                        
                        if filename not in all_files or all_files[filename].version < version:
                            all_files[filename] = FileMetadata(filename=filename, version=version, fileSize=size)
                finally:
                    transport.close()
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
            
            client, transport = self.connect_to_replica(ip, port)
            
            try:
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
            finally:
                transport.close()
        except Exception as e:
            print(f"Error in copyFile: {e}")
            return Response(status=StatusCode.SERVER_ERROR, message=str(e))

    #-------------------------
    # Coordinator operations (if this replica is the coordinator) ***
    #-------------------------
    
    def start_processing_thread(self):
        """Start the request processing thread (coordinator only)"""
        if not self.is_coordinator:
            return
            
        if not self.processing_thread or not self.processing_thread.is_alive():
            self.processing_thread = threading.Thread(target=self.process_request_queue)
            self.processing_thread.daemon = True
            self.processing_thread.start()
            print("Started coordinator request processing thread")
    
    def process_request_queue(self):
        """Process requests from the queue (coordinator only)"""
        if not self.is_coordinator:
            return
            
        while True:
            # Process one request at a time
            request = None
            with self.queue_lock:
                if self.request_queue:
                    request = self.request_queue.pop(0)
            
            if request:
                request_type, args = request
                if request_type == "read":
                    filename, callback = args
                    self._handle_read_request(filename, callback)
                elif request_type == "write":
                    filename, new_version, replica, callback = args
                    self._handle_write_request(filename, new_version, replica, callback)
            
            time.sleep(0.01)  # Small delay to prevent CPU hogging
    
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
                if ip == self.my_ip and port == self.my_port:
                    # If we're in the quorum, just check our own version
                    version = self.getFileVersion(filename)
                else:
                    client, transport = self.connect_to_replica(ip, port)
                    try:
                        version = client.getFileVersion(filename)
                    finally:
                        transport.close()
                
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
                    if ip == self.my_ip and port == self.my_port:
                        # If we're in the quorum, just check our own version
                        version = self.getFileVersion(filename)
                    else:
                        client, transport = self.connect_to_replica(ip, port)
                        try:
                            version = client.getFileVersion(filename)
                        finally:
                            transport.close()
                    
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
                if ip == self.my_ip and port == self.my_port:
                    # If we're in the write quorum, copy file to us if we're not the source
                    if replica_str != f"{self.my_ip}:{self.my_port}":
                        result = self.copyFile(filename, replica_str)
                        if result.status != StatusCode.SUCCESS:
                            success = False
                            error_messages.append(f"Error copying file to self: {result.message}")
                else:
                    client, transport = self.connect_to_replica(ip, port)
                    try:
                        print(f"Copying file {filename} to replica {ip}:{port}")
                        response = client.copyFile(filename, replica_str)
                        
                        if response.status != StatusCode.SUCCESS:
                            success = False
                            error_messages.append(f"Error copying file to replica {ip}:{port}: {response.message}")
                    finally:
                        transport.close()
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
        with self.queue_lock:
            self.request_queue.append(("read", (filename, callback)))
        
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
        with self.queue_lock:
            self.request_queue.append(("write", (filename, None, None, callback)))
        
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
        with self.queue_lock:
            self.request_queue.append(("write", (filename, newVersion, replicaStr, callback)))
        
        # Wait for result with timeout
        if not event.wait(timeout=30):  # 30 second timeout for writes
            return Response(status=StatusCode.SERVER_ERROR, 
                           message="Timeout waiting for write operation to complete")
        
        return result[0]

def serve_replica(handler, port):
    """Start a Thrift server for a replica"""
    handler.my_ip = "localhost"  # Replace with actual IP if needed
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
    transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    # Use a threaded server
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

    print(f"Starting {'coordinator' if handler.is_coordinator else 'replica'} server on port {port}")
    
    # Start the request processing thread if this is the coordinator
    if handler.is_coordinator:
        handler.start_processing_thread()
        
    server.serve()

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