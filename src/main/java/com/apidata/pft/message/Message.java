package com.apidata.pft.message;

import com.apidata.pft.PFTConstants;
import com.apidata.pft.exception.SocketCloseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public abstract class Message {
    private static final Logger LOG = LoggerFactory.getLogger(Message.class);

    public abstract void fromBytes(ByteBuffer buffer);

    public abstract void toBytes(ByteBuffer buffer);

    private String messageType() {
        return getClass().getSimpleName();
    }

    public static void stringToMsg(ByteBuffer buffer, String str) {
        byte[] bytes = str.getBytes();
        int len = bytes.length;
        buffer.putShort((short) len);
        buffer.put(bytes);
    }

    public static String stringFromMsg(ByteBuffer buffer) {
        int len = buffer.getShort();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes);
    }

    public static Message nextMsgFromSocket(SocketChannel socket, ByteBuffer dataBuffer)
            throws SocketCloseException, IOException {

        // read first 4 bytes for header
        checkBytesAvailable(socket, dataBuffer, 4);
        int length = dataBuffer.getInt();

        // Read rest of the msg
        checkBytesAvailable(socket, dataBuffer, length);

        String type = stringFromMsg(dataBuffer);
        Message msg = null;
        if (type.equals(FileRequestMsg.class.getSimpleName())) {
            msg = new FileRequestMsg();
        } else if (type.equals(FileResponseMsg.class.getSimpleName())) {
            msg = new FileResponseMsg();
        } else if (type.equals(FileChunkRequestMsg.class.getSimpleName())) {
            msg = new FileChunkRequestMsg();
        }

        if (msg == null) {
            throw new IOException("Unknown message type: " + type);
        }

        // payload data
        msg.fromBytes(dataBuffer);

        LOG.trace("Message read from socket: " + msg);

        return msg;
    }

    public static void sendMessage(SocketChannel channel, Message toSend) throws IOException {

        // need to put the message type into the buffer first.
        ByteBuffer msg = ByteBuffer.allocate(PFTConstants.BUFFER_SIZE);
        stringToMsg(msg, toSend.messageType());

        // and then any extra fields for this type of message
        toSend.toBytes(msg);
        msg.flip();

        // need to encode the length into a different buffer.
        ByteBuffer overall = ByteBuffer.allocate(10);
        overall.putInt(msg.remaining());
        overall.flip();

        // and lastly write the length, followed by the message.
        long written = channel.write(new ByteBuffer[] { overall, msg });

        LOG.trace("Message written to socket: " + toSend + ", length was: " + written);
    }

    private static void checkBytesAvailable(SocketChannel socket, ByteBuffer buffer, int required)
            throws SocketCloseException, IOException {
        // if there's already something in the buffer, then compact it and prepare it for writing again.
        if (buffer.position() != 0) {
            buffer.compact();
        }

        // loop until we have enough data to decode the message
        while (buffer.position() < required) {

            // read from buffer
            int len = socket.read(buffer);
            if (!socket.isOpen() || len <= 0) {
                throw new SocketCloseException("Socket closed while reading");
            }

            LOG.trace("Bytes now in buffer: " + buffer.remaining() + " read from socket: " + len);
        }

        // flip to prepare the buffer for reading.
        buffer.flip();
    }
}