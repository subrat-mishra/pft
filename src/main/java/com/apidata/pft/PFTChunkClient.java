package com.apidata.pft;

import com.apidata.pft.message.FileChunkRequestMsg;
import com.apidata.pft.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Callable;

import static com.apidata.pft.PFTConstants.BUFFER_SIZE;
import static com.apidata.pft.PFTConstants.END_MESSAGE_MARKER;

/**
 * PFTChunkClient thread pulls data for specific offset from server and returns a Result object.
 */

public class PFTChunkClient implements Callable {
    private static final Logger LOG = LoggerFactory.getLogger(PFTChunkClient.class);

    private int chunkId;
    private String hostName;
    private int port;
    private String serverFilePath;
    private String clientFilePath;
    private long offset;
    private long maxBufferSize;
    private FileChannel channel;

    public PFTChunkClient(int chunkId, String hostName, int port, String serverFilePath,
            long offset, long maxBufferSize, FileChannel channel) {
        this.chunkId = chunkId;
        this.hostName = hostName;
        this.port = port;
        this.serverFilePath = serverFilePath;
        this.offset = offset;
        this.maxBufferSize = maxBufferSize;
        this.channel = channel;
    }

    @Override
    public Result call() {
        Long startTime = System.currentTimeMillis();
        boolean status = false;
        LOG.debug("Started thread-{}", chunkId);
        InetSocketAddress hostAddress = new InetSocketAddress(hostName, port);
        SocketChannel client = null;
        try {
            client = SocketChannel.open(hostAddress);
            client.socket().setReceiveBufferSize((int) maxBufferSize + END_MESSAGE_MARKER.length());
            LOG.info("Connect to server:{}", client.getRemoteAddress());

            if (client.isOpen()) {
                // formulate the request message and send it to the server.
                FileChunkRequestMsg
                        msg =
                        new FileChunkRequestMsg(serverFilePath, offset, chunkId, maxBufferSize);
                Message.sendMessage(client, msg);

                ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);

                int len;
                long totalBytes = 0;
                long bufferChunks = offset / BUFFER_SIZE;
                long remainder = offset % BUFFER_SIZE;
                StringBuilder doneMsg = new StringBuilder();
                int count = 0;
                long startPosition = chunkId * maxBufferSize;
                while ((len = client.read(buffer)) > 0) {
                    totalBytes += len;
                    if (chunkId == 0) {
                        LOG.trace("#{} {} {} {}", chunkId, totalBytes, len, count);
                    }
                    count++;
                    boolean isLastChunk = (bufferChunks-- == 0);
                    if (isLastChunk && remainder > 0) {
                        // If left over data along with END_MESSAGE_MARKER bytes if any
                        buffer.flip();
                        byte[] msgBytes = new byte[len];
                        buffer.get(msgBytes, 0, msgBytes.length);

                        for (int i = (int) (remainder); i < len; i++) {
                            doneMsg.append((char) msgBytes[i]);
                        }

                        buffer = ByteBuffer.allocate((int) remainder);
                        buffer.put(msgBytes, 0, (int) remainder);
                        buffer.flip();
                        channel.write(buffer, startPosition);
                        startPosition += len;
                        buffer.clear();

                        if (doneMsg.toString().equals(END_MESSAGE_MARKER)) {
                            break;
                        }

                    } else if (bufferChunks < 0) {
                        // Finally check the END_MESSAGE_MARKER here
                        buffer.flip();
                        byte[] msgBytes = new byte[len];
                        buffer.get(msgBytes, 0, msgBytes.length);

                        String rawMsg = new String(msgBytes);
                        doneMsg.append(rawMsg.substring(0,
                                END_MESSAGE_MARKER.length() - doneMsg.length()));
                        buffer.clear();
                        if (doneMsg.toString().equals(END_MESSAGE_MARKER)) {
                            break;
                        } else {
                            throw new RuntimeException("Looks like END Marker not received");
                        }
                    } else {
                        // put the data into the fileChannel
                        buffer.flip();
                        channel.write(buffer, startPosition);
                        startPosition += len;
                        buffer.clear();
                    }
                }
                status = true;
                LOG.info("Total bytes asked {} downloaded {} by thread-{}", offset, totalBytes,
                        chunkId);
            }
        } catch (IOException e) {
            LOG.error("IOException occurred", e);
        } finally {
            try {
                if (client != null) {
                    client.close();
                }
            } catch (IOException e) {
                LOG.error("IOException occurred", e);
            }
        }
        Long endTime = System.currentTimeMillis();
        return new Result(chunkId, endTime - startTime, status);
    }
}