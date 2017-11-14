package com.hazelcast.raft;

public interface SnapshotAwareService {

    Object takeSnapshot(int commitIndex);

    void restoreSnapshot(int commitIndex, Object snapshot);

}
