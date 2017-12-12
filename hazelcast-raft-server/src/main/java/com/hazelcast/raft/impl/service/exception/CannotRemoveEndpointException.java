package com.hazelcast.raft.impl.service.exception;

import com.hazelcast.raft.exception.RaftException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CannotRemoveEndpointException extends RaftException {
    // TODO RaftException is for internal raft algorithm-related exceptions, isn't so?

    public CannotRemoveEndpointException(String message) {
        super(message, null);
    }

}
