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

/**
 * PFTChunkClient thread pulls data for specific offset from server and returns a Result object.
 */

public class PFTChunkClient implements Callable {
    private static final Logger LOG = LoggerFactory.getLogger(PFTChunkClient.class);

    private int chunkId;
    private String hostName;
    private int port;
    private String serverFilePath;
    private long offset;
    private FileChannel channel;
    private long startPosition;

    public PFTChunkClient(int chunkId, String hostName, int port, String serverFilePath,
            long offset, long startPosition, FileChannel channel) {
        this.chunkId = chunkId;
        this.hostName = hostName;
        this.port = port;
        this.serverFilePath = serverFilePath;
        this.offset = offset;
        this.startPosition = startPosition;
        this.channel = channel;
    }

    @Override
    public Result call() {
        Long startTime = System.currentTimeMillis();
        boolean status = false;
        LOG.info("Started PFTChunkClient-{}", chunkId);
        InetSocketAddress hostAddress = new InetSocketAddress(hostName, port);
        SocketChannel client = null;
        try {
            client = SocketChannel.open(hostAddress);
            LOG.info("Connect to server:{}", client.getRemoteAddress());

            if (client.isOpen()) {
                // formulate the request message and send it to the server.

                long bufferChunks = offset / BUFFER_SIZE;
                long remainder = offset % BUFFER_SIZE;

                FileChunkRequestMsg msg = null;
                int len = 0;
                long totalBytes = 0;
                while (bufferChunks > 0) {
                    msg = new FileChunkRequestMsg(serverFilePath, startPosition, chunkId, BUFFER_SIZE);
                    Message.sendMessage(client, msg);
                    ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
                    if ((len = client.read(buffer)) > 0) {
                        totalBytes += len;
                        // put the data into the fileChannel
                        buffer.flip();
                        channel.write(buffer, startPosition);
                        startPosition += len;
                        buffer.clear();
                    }
                    bufferChunks--;
                }

                if (remainder > 0) {
                    msg = new FileChunkRequestMsg(serverFilePath, startPosition, chunkId, (int) remainder);
                    Message.sendMessage(client, msg);
                    ByteBuffer buffer = ByteBuffer.allocate((int) remainder);
                    if ((len = client.read(buffer)) > 0) {
                        totalBytes += len;
                        // put the data into the fileChannel
                        buffer.flip();
                        channel.write(buffer, startPosition);
                        buffer.clear();
                    }
                }
                status = true;
                LOG.info("Total bytes asked {} downloaded {} by PFTChunkClient-{}", offset, totalBytes,
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