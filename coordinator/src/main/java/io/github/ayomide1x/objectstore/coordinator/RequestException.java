package io.github.ayomide1x.objectstore.coordinator;

import io.grpc.Status;

/** A failure the caller caused or can act on, carrying the gRPC status to return. */
public class RequestException extends RuntimeException {

    private final Status.Code code;

    public RequestException(Status.Code code, String message) {
        super(message);
        this.code = code;
    }

    public Status.Code code() {
        return code;
    }

    public static RequestException invalid(String message) {
        return new RequestException(Status.Code.INVALID_ARGUMENT, message);
    }
}
