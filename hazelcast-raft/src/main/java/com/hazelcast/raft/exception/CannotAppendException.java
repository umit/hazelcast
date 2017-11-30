package com.hazelcast.raft.exception;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.spi.exception.RetryableException;

/**
 * TODO: Javadoc Pending...
 */
public class CannotAppendException extends RaftException implements RetryableException {

    public CannotAppendException(RaftEndpoint leader) {
        super("Cannot append new operations for now", leader);
    }
}
