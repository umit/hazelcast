package com.hazelcast.raft.exception;

import com.hazelcast.raft.impl.RaftEndpoint;

/**
 * TODO: Javadoc Pending...
 */
public class MemberAlreadyExistsException extends RaftException {

    public MemberAlreadyExistsException(RaftEndpoint member) {
        super("Member already exists: " + member, null);
    }
}
