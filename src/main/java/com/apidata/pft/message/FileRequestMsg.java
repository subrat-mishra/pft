package com.apidata.pft.message;

import java.nio.ByteBuffer;

public class FileRequestMsg extends Message {
    private String filePath;

    public FileRequestMsg() {

    }

    public FileRequestMsg(String filePath) {
        this.filePath = filePath;
    }

    public String getFilePath() {
        return filePath;
    }

    public void fromBytes(ByteBuffer buffer) {
        filePath = stringFromMsg(buffer);
    }

    public void toBytes(ByteBuffer buffer) {
        stringToMsg(buffer, filePath);
    }

    @Override
    public String toString() {
        return filePath;
    }
}