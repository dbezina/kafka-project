package com.bezina.ws.email_ntfctn.error;

public class NotRetryableException extends RuntimeException{
    public NotRetryableException(String message) {
        super(message);
    }

    public NotRetryableException(Throwable cause) {
        super(cause);
    }
}
