package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.operation.RaftOperation;
import com.hazelcast.raft.SnapshotAwareService;

import java.io.IOException;

public class TakeSnapshotOp extends RaftOperation {

    private final String name;

    public TakeSnapshotOp(String serviceName, String name) {
        this.name = name;
        setServiceName(serviceName);
    }

    @Override
    public Object doRun(int commitIndex) {
        SnapshotAwareService service = getService();
        return service.takeSnapshot(name, commitIndex);
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
