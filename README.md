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
   * First Client sends a FileRequestMsg containing the filePath to be downloaded from the server
    and get a FileResponseMsg containing the size of the file. If the file doesn't exist client will receive 
    fileSize as -1.
   * Next based on the FileResponseMsg it creates multiple PFTChunkClient threads which reads the data from
     server using SocketChannel. 
   * PFTChunkClient thread pulls data for specific offset of file from server and return a Result Object 
   containing timeTaken for the operation and the job status.
   * FileMerger thread merges all the chunked files data into one file as soon as PFTChunkClient
    thread is completed in sequence of chunkId.
- Communication between server and client over the socket is based on a Message class. It's has set of methods
needed to send and receive data over the socket.

## Prerequisites
- JDK-1.8
- Maven
## Installation

```mvn clean install```

#### Run PFT-Server
```java -Dlog4j.configuration=file:<dir-name>/log4j.properties -jar <dir-name>/pft/target/pft-server-jar-with-dependencies.jar```

``` 
usage: pft-server
    -h,--help                Help usage
    -host,--hostname <arg>   HostName of the server, Default=localhost
    -port,--port <arg>       Server port number, Default=54321
```
#### Run PFT-Client
```java -Dlog4j.configuration=file:<dir-name>/log4j.properties -jar <dir-name>/pft/target/pft-client-jar-with-dependencies.jar -S <server-file>```

```
usage: pft-client
 -C,--clientFilePath <arg>   Client File to be copied,
                             Default=/tmp/<epochTime>/<server-file>
 -H,--hostname <arg>         HostName of the server, Default=localhost
 -h,--help                   Help usage
 -O,--offset <arg>           Max offset per thread, Default=9998336
 -P,--port <arg>             Server port number, Default=54321
 -S,--serverFilePath <arg>   Server File to be downloaded

```
## Future Enhancements
- To support upload file from client.
- Retry of PFTChunkClient currently not supported.
