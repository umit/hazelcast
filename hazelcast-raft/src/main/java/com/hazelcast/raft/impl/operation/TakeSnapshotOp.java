package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.SnapshotAwareService;

import java.io.IOException;

public class TakeSnapshotOp extends RaftOperation {

    public TakeSnapshotOp(String serviceName) {
        setServiceName(serviceName);
    }

    @Override
    public Object doRun(int commitIndex) {
        SnapshotAwareService service = getService();
        return service.takeSnapshot(commitIndex);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

}
