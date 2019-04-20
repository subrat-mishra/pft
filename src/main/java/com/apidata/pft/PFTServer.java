package com.apidata.pft;

import com.apidata.pft.exception.SocketCloseException;
import com.apidata.pft.message.FileChunkRequestMsg;
import com.apidata.pft.message.FileRequestMsg;
import com.apidata.pft.message.FileResponseMsg;
import com.apidata.pft.message.Message;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.apidata.pft.PFTConstants.END_MESSAGE_MARKER;

/**
 * PFTServer creates a SocketChannel. It's uses java non-blocking io way to read from sockets,
 * so that one thread communicates with multiple open connections at once.
 * */
public class PFTServer {
    private static final Logger LOG = LoggerFactory.getLogger(PFTServer.class);

    private String hostName;
    private int port;
    private Selector selector;
    private Set<SocketChannel> channels;
    private InetSocketAddress listenAddress;

    public PFTServer(String hostname, int port) {
        this.hostName = hostname;
        this.port = port;
        this.listenAddress = new InetSocketAddress(hostName, port);
        this.channels = new HashSet<>();
    }

    public void doWork() {
        LOG.info("Server started on hostname={} and port={}", hostName, port);
        try {
            this.selector = Selector.open();
            ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);

            serverChannel.socket().bind(listenAddress);
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            LOG.info("Server running...");

            while (!Thread.currentThread().isInterrupted()) {
                // waiting for events
                selector.select();

                // work on selected keys
                Iterator keys = selector.selectedKeys().iterator();
                while (keys.hasNext()) {
                    SelectionKey key = (SelectionKey) keys.next();

                    // prevent the same key from coming
                    keys.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    try {
                        if (key.isAcceptable()) {
                            // Channel ready to accept a new socket connection
                            this.accept(key);
                        } else if (key.isReadable()) {
                            // Channel is ready for reading
                            this.read(key);
                        }
                    } catch (IOException e) {
                        LOG.error("IOException occurred", e);
                    }
                }
            }
        } catch (IOException e) {
            LOG.error("IOException occurred", e);
        }
    }

    private void accept(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel channel = serverChannel.accept();
        channel.configureBlocking(false);
        Socket socket = channel.socket();
        SocketAddress remoteAddr = socket.getRemoteSocketAddress();
        LOG.info("Connected to client: " + remoteAddr);

        // register channel with selector for further IO
        channels.add(channel);
        channel.register(this.selector, SelectionKey.OP_READ);
        LOG.info("Total open channels - {}", channels.size());
    }

    private void read(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer buffer = ByteBuffer.allocate(PFTConstants.BUFFER_SIZE);

        try {
            Message msg = Message.nextMsgFromSocket(channel, buffer);
            if (msg instanceof FileRequestMsg) {
                // FileRquestMsg gets a filePath and FileResponseMsg send the length of file.
                LOG.info("Received a FileRequestMsg");
                String filePath = ((FileRequestMsg) msg).getFilePath();

                File file = new File(filePath);
                long length = -1;
                if (file.exists()) {
                    length = file.length();
                }

                LOG.info("FilePath received-{}", filePath);
                FileResponseMsg response = new FileResponseMsg(length);
                Message.sendMessage(channel, response);
            } else if (msg instanceof FileChunkRequestMsg) {
                // Get the FileChunkRequestMsg from client and send the actual payload followed by END_MESSSGE_MARKER
                LOG.info("Received a FileChunkRequestMsg");
                String filePath = ((FileChunkRequestMsg) msg).getFilePath();
                long offset = ((FileChunkRequestMsg) msg).getOffset();
                int chunkId = ((FileChunkRequestMsg) msg).getChunkId();
                long maxBufferSize = ((FileChunkRequestMsg) msg).getMaxBufferSize();
                channel.socket().setSendBufferSize((int)maxBufferSize);

                RandomAccessFile raf = new RandomAccessFile(filePath, "r");
                long seek = chunkId * offset;
                raf.seek(seek);

                FileChannel inChannel = raf.getChannel();

                int capacity = Math.min((int) offset, PFTConstants.BUFFER_SIZE);

                int loop = (int) offset / capacity;
                int remaining = (int) offset;

                // Copy all data.
                buffer = ByteBuffer.allocate(capacity);

                int len;
                int totalBytes=0;
                while ((len = inChannel.read(buffer)) > 0) {
                    totalBytes+=len;
                    buffer.flip();
                    channel.write(buffer);
                    buffer.clear();
                    remaining -= capacity;
                    loop--;
                    if (loop == 0) {
                        break;
                    }
                }
                // Copy remaining data
                if (remaining > 0) {
                    buffer = ByteBuffer.allocate(remaining);
                    while ((len = inChannel.read(buffer)) > 0) {
                        totalBytes+=len;
                        buffer.flip();
                        channel.write(buffer);
                        buffer.clear();
                        break;
                    }
                }
                // Finally add END_MESSAGE_MARKER
                byte[] message = END_MESSAGE_MARKER.getBytes();
                buffer = ByteBuffer.wrap(message);
                channel.write(buffer);
                buffer.clear();
                LOG.info("Total bytes asked {} downloaded {} by thread-{}",offset,totalBytes,chunkId);
            } else {
                LOG.error("Unexpected message " + msg);
                closeConnection(channel, key);
            }
        } catch (SocketCloseException sce) {
            // SocketClose by client
            closeConnection(channel, key);
        }
    }

    private void closeConnection(Channel channel, SelectionKey key) throws IOException {
        this.channels.remove(channel);
        SocketAddress remoteAddr = ((SocketChannel) channel).socket().getRemoteSocketAddress();
        LOG.info("Connection closed by client: " + remoteAddr);
        channel.close();
        key.cancel();
    }

    private static void printUsage(Options options) {
        // print the usage using HelpFormatter
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("pft-server", options);
    }

    public static void main(String[] args) {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("host", "hostname", true, "HostName of the server, Default=localhost");
        options.addOption("port", "port", true, "Server port number, Default=54321");
        options.addOption("h", "help", false, "Help usage");

        try {
            CommandLine cmd = parser.parse(options, args);
            if (cmd.hasOption("h")) {
                printUsage(options);
                System.exit(1);
            }
            String hostName = "localhost";
            if (cmd.hasOption("host")) {
                hostName = cmd.getOptionValue("host");
            }
            int port = 54321;
            if (cmd.hasOption("port")) {
                port = Integer.parseInt(cmd.getOptionValue("port"));
            }
            PFTServer pftServer = new PFTServer(hostName, port);
            pftServer.doWork();
        } catch (ParseException e) {
            LOG.error("Parsing error occurred", e);
            printUsage(options);
        }
    }
}