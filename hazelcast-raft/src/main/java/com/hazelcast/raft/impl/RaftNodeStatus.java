package com.hazelcast.raft.impl;

public enum RaftNodeStatus {
    ACTIVE, CHANGING_MEMBERSHIP, TERMINATING, TERMINATED, STEPPED_DOWN
}
