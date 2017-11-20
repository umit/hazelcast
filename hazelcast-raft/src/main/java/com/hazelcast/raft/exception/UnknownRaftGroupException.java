package com.hazelcast.raft.exception;

/**
 * TODO: Javadoc Pending...
 */
public class UnknownRaftGroupException extends RaftException {

    public UnknownRaftGroupException(String raftName) {
        super("Unknown raft group: " + raftName, null);
    }
}
