import glob
import sys
import os

# Add Thrift generated code to path
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('../thrift-0.19.0/lib/py/build/lib*')[0])

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TMultiplexedProtocol, TBinaryProtocol

from service import ReplicaService
from service.ttypes import StatusCode

class DFSClient:
    def __init__(self, replica_ip, replica_port):
        """Initialize the client with a connection to a replica"""
        self.replica_ip = replica_ip
        self.replica_port = replica_port
        self.transport = None
        self.client = None
    
    def connect(self):
        """Connect to the replica"""
        try:
            self.transport = TSocket.TSocket(self.replica_ip, self.replica_port)
            self.transport = TTransport.TBufferedTransport(self.transport)
            binary_protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
            protocol = TMultiplexedProtocol.TMultiplexedProtocol(binary_protocol, "ReplicaService")
            self.client = ReplicaService.Client(protocol)
            self.transport.open()
            return True
        except Exception as e:
            print(f"Connection error: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from the replica"""
        if self.transport and self.transport.isOpen():
            self.transport.close()
    
    def read_file(self, filename):
        """Read a file from the DFS"""
        try:
            if not self.connect():
                return False
                
            response = self.client.readFile(filename)
            
            if response.status == StatusCode.SUCCESS:
                print(f"SUCCESS: {response.message}")
            else:
                print(f"ERROR: {response.message}")
            
            self.disconnect()
            return response.status == StatusCode.SUCCESS
        except Exception as e:
            print(f"Error: {e}")
            self.disconnect()
            return False
    
    def write_file(self, filename, local_path):
        """Write a file to the DFS"""
        try:
            if not os.path.exists(local_path):
                print(f"Error: Local file {local_path} does not exist")
                return False
            
            if not self.connect():
                return False
                
            response = self.client.writeFile(filename, local_path)
            
            if response.status == StatusCode.SUCCESS:
                print(f"SUCCESS: {response.message}")
            else:
                print(f"ERROR: {response.message}")
            
            self.disconnect()
            return response.status == StatusCode.SUCCESS
        except Exception as e:
            print(f"Error: {e}")
            self.disconnect()
            return False
    
    def list_files(self):
        """List all files in the DFS"""
        try:
            if not self.connect():
                return False
                
            files = self.client.listFiles()
            
            if not files:
                print("No files found in the system")
            else:
                print("Files in the system:")
                print("Filename\tVersion\tSize (bytes)")
                print("-" * 40)
                for file_meta in files:
                    print(f"{file_meta.filename}\t{file_meta.version}\t{file_meta.fileSize}")
            
            self.disconnect()
            return True
        except Exception as e:
            print(f"Error: {e}")
            self.disconnect()
            return False

def print_usage():
    """Print usage information"""
    print("Usage:")
    print("python3 client.py read <filename> <replica_ip> <replica_port>")
    print("python3 client.py write <filename> <local_path> <replica_ip> <replica_port>")
    print("python3 client.py list <replica_ip> <replica_port>")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print_usage()
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == "read" and len(sys.argv) == 5:
        filename = sys.argv[2]
        replica_ip = sys.argv[3]
        replica_port = int(sys.argv[4])
        
        client = DFSClient(replica_ip, replica_port)
        success = client.read_file(filename)
        sys.exit(0 if success else 1)
    
    elif command == "write" and len(sys.argv) == 6:
        filename = sys.argv[2]
        local_path = sys.argv[3]
        replica_ip = sys.argv[4]
        replica_port = int(sys.argv[5])
        
        client = DFSClient(replica_ip, replica_port)
        success = client.write_file(filename, local_path)
        sys.exit(0 if success else 1)
    
    elif command == "list" and len(sys.argv) == 4:
        replica_ip = sys.argv[2]
        replica_port = int(sys.argv[3])
        
        client = DFSClient(replica_ip, replica_port)
        success = client.list_files()
        sys.exit(0 if success else 1)
    
    else:
        print_usage()
        sys.exit(1)