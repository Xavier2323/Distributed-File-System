// namespace py service

// File chunk representation
struct FileChunk {
  1: binary data,
  2: i32 chunkSize
}

// File metadata
struct FileMetadata {
  1: string filename,
  2: i32 version,
  3: i64 fileSize
}

// Status response
enum StatusCode {
  SUCCESS = 0,
  FILE_NOT_FOUND = 1,
  VERSION_MISMATCH = 2,
  SERVER_ERROR = 3
}

struct Response {
  1: StatusCode status,
  2: string message
}

// Service definition for replica servers
service ReplicaService {
  // File operations
  Response readFile(1: string filename),
  Response writeFile(1: string filename, 2: string clientFilePath),
  Response copyFile(1: string filename, 2: string sourceReplica),
  list<FileMetadata> listFiles(),
  
  // File transfer operations
  FileChunk getFileChunk(1: string filename, 2: i32 offset, 3: i32 chunkSize),
  i64 getFileSize(1: string filename),
  
  // Quorum protocol operations
  i32 getFileVersion(1: string filename),
  Response updateFile(1: string filename, 2: i32 version),
  
  // Inter-replica communication
  Response copyFile(1: string filename, 2: string sourceReplica),
}

// Service definition for coordinator
service CoordinatorService {
  // Quorum management
  i32 getHighestVersionForRead(1: string filename),
  i32 getHighestVersionForWrite(1: string filename),
  Response registerWrite(1: string filename, 2: i32 newVersion, 3: string replica),
}