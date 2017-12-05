package com.hazelcast.raft.exception;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.spi.exception.RetryableException;

/**
 * TODO: Javadoc Pending...
 */
public class CannotReplicateException extends RaftException implements RetryableException {

    public CannotReplicateException(RaftEndpoint leader) {
        super("Cannot replicate new operations for now", leader);
    }
}
