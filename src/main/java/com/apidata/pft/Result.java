package com.apidata.pft;

public class Result {
    private int id;
    private long timeTaken;
    private boolean status;

    public Result(int id, long timeTaken, boolean status) {
        this.id = id;
        this.timeTaken = timeTaken;
        this.status = status;
    }
    public int getId() {
        return id;
    }

    public long getTimeTaken() {
        return timeTaken;
    }

    public boolean isStatus() {
        return status;
    }
}