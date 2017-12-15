package com.hazelcast.raft.exception;

import com.hazelcast.raft.impl.RaftEndpoint;

/**
 * TODO: Javadoc Pending...
 */
public class CannotRunLocalQueryException extends RaftException {
    public CannotRunLocalQueryException(RaftEndpoint leader) {
        super(leader);
    }
}
