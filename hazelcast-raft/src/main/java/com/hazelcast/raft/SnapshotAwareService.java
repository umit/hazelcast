package com.hazelcast.raft;

public interface SnapshotAwareService<T> {

    // TODO: should use groupId instead of raftName
    T takeSnapshot(String raftName, int commitIndex);

    // TODO: should use groupId instead of raftName
    void restoreSnapshot(String raftName, int commitIndex, T snapshot);

}
