package com.hazelcast.raft.operation;

import com.hazelcast.spi.Operation;

/**
 * TODO: Javadoc Pending...
 *
 * @author mdogan 30.10.2017
 */
public class RaftResponseHandler {

    private final Operation operation;

    public RaftResponseHandler(Operation operation) {
        this.operation = operation;
    }

    public void send(Object r) {
        operation.sendResponse(r);
    }
}
