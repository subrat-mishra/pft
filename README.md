# Parallel File Transmission (PFT)
Parallel File Transmission is a client-server system where a file can be transferred from the server to client. 
Client will initiate the connection with the server and use multiple connections to transmit different parts 
of the file in parallel. Once the client finishes, an intact copy of the file from the server is on the client. 
## Description
- PFT is an application for efficient large file transfer. It's written in JAVA.
- PFT is based on an asynchronous and flexible multi-threaded system which is based on Java NIO Libraries.  
- PFTServer creates a SocketChannel. It's uses java nio to read from sockets,
so that one thread communicates with multiple open connections at once.
- PFTClient opens a SocketChannel to the server running on hostName and port configured.
   * First Client sends a FileRequestMsg containing the filePath to be downloaded to the server
    and get a FileResponseMsg containing the size of the file. If file doesn't exist the fileSize sets to -1.
   * Next based on the FileResponseMsg it creates multiple PFTChunkClient threads which reads the data from
     server using SocketChannel. 
   * PFTChunkClient thread pulls data for specific offset from server and returns a Result object.
   * FileMerger thread merges all the chunked files data into one file as soon as PFTChunkClient
    thread is completed in sequence of chunkId.
- Communication between server and client over the socket is based on a Message class. It's has set of methods
needed to send and receive data over the socket.

## Installing

```mvn clean install```

#### Run PFT-Server
```java -Dlog4j.configuration=file:<dir-name>/log4j.properties -jar <dir-name>/pft/target/pft-server-jar-with-dependencies.jar```

#### Run PFT-Client
```java -Dlog4j.configuration=file:<dir-name>/log4j.properties -jar <dir-name>/pft/target/pft-client-jar-with-dependencies.jar -S <server-file>```


## Future Enhancements
- To support upload file from client.
- Retry of PFTChunkClient currently not supported.
