package com.hazelcast.raft;

/**
 * TODO: Javadoc Pending...
 *
 */
public interface RaftGroupId {

    String name();

    int commitIndex();
}
