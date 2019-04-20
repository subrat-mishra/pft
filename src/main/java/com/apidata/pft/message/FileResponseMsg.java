package com.apidata.pft.message;

import java.nio.ByteBuffer;

public class FileResponseMsg extends Message {
    private Long fileSize;

    public FileResponseMsg() {

    }

    public FileResponseMsg(Long fileSize) {
        this.fileSize = fileSize;
    }

    public Long getFileSize() {
        return fileSize;
    }

    public void fromBytes(ByteBuffer buffer) {
        int len = buffer.getShort();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        String val = new String(bytes);
        fileSize = Long.parseLong(val);
    }

    public void toBytes(ByteBuffer buffer) {
        byte[] bytes = fileSize.toString().getBytes();
        int len = bytes.length;
        buffer.putShort((short) len);
        buffer.put(bytes);
    }

    @Override
    public String toString() {
        return fileSize.toString();
    }
}