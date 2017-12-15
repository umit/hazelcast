package com.hazelcast.raft;

public enum RaftNodeStatus {
    ACTIVE, CHANGING_MEMBERSHIP, TERMINATING, TERMINATED, STEPPED_DOWN
}
