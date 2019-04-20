package com.apidata.pft.message;

import java.nio.ByteBuffer;

public class FileChunkRequestMsg extends Message {
    private String filePath;
    private Long offset;
    private Integer chunkId;
    private Long maxBufferSize;

    public FileChunkRequestMsg() {

    }

    public FileChunkRequestMsg(String filePath, Long offset, Integer chunkId, Long maxBufferSize) {
        this.filePath = filePath;
        this.offset = offset;
        this.chunkId = chunkId;
        this.maxBufferSize = maxBufferSize;
    }

    public String getFilePath() {
        return filePath;
    }

    public Long getOffset() {
        return offset;
    }

    public Integer getChunkId() {
        return chunkId;
    }

    public Long getMaxBufferSize() {
        return maxBufferSize;
    }

    public void fromBytes(ByteBuffer buffer) {
        int len = buffer.getShort();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        String val = new String(bytes);
        String[] arr = val.split(",");
        filePath = arr[0];
        offset = Long.parseLong(arr[1]);
        chunkId = Integer.parseInt(arr[2]);
        maxBufferSize = Long.parseLong(arr[3]);
    }

    public void toBytes(ByteBuffer buffer) {
        byte[] bytes = (filePath + "," + offset + "," + chunkId + "," + maxBufferSize).getBytes();
        int len = bytes.length;
        buffer.putShort((short) len);
        buffer.put(bytes);
    }

    @Override
    public String toString() {
        return filePath + "," + offset + "," + chunkId + "," + maxBufferSize;
    }
}