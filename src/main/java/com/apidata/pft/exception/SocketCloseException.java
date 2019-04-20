package com.apidata.pft.exception;

public class SocketCloseException extends Exception {
    public SocketCloseException() {
        super();
    }

    public SocketCloseException(String message) {
        super(message);
    }

    public SocketCloseException(String message, Throwable cause) {
        super(message, cause);
    }

    public SocketCloseException(Throwable cause) {
        super(cause);
    }
}