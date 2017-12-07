package com.hazelcast.raft.impl.service.exception;

import com.hazelcast.raft.exception.RaftException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CannotShutdownException extends RaftException {

    public CannotShutdownException(String message) {
        super(message, null);
    }

}
