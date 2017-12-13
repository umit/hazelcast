package com.hazelcast.raft;

public interface SnapshotAwareService<T> {

    T takeSnapshot(RaftGroupId raftGroupId, int commitIndex);

    void restoreSnapshot(RaftGroupId raftGroupId, int commitIndex, T snapshot);

}
