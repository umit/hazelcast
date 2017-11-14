package com.hazelcast.raft;

public interface SnapshotAwareService<T> {

    T takeSnapshot(String raftName, int commitIndex);

    void restoreSnapshot(String raftName, int commitIndex, T snapshot);

}
