package com.hazelcast.raft.impl.operation;

import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.SnapshotAwareService;

public class RestoreSnapshotOp extends RaftOperation {

    private int commitIndex;

    private Object snapshot;

    public RestoreSnapshotOp(String serviceName, int commitIndex, Object snapshot) {
        setServiceName(serviceName);
        this.commitIndex = commitIndex;
        this.snapshot = snapshot;
    }

    @Override
    public Object doRun(int commitIndex) {
        assert this.commitIndex == commitIndex : " expected restore commit index: " + this.commitIndex + " given commit index: "
                + commitIndex;

        SnapshotAwareService service = getService();
        service.restoreSnapshot(commitIndex, snapshot);
        return null;
    }

    @Override
    public String toString() {
        return "RestoreSnapshotOp{" + "commitIndex=" + commitIndex + ", snapshot=" + snapshot + '}';
    }

}
