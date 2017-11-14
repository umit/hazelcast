package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.service.RaftService;

import java.io.IOException;

public class InstallSnapshotOp extends AsyncRaftOp {

    private InstallSnapshot installSnapshot;

    public InstallSnapshotOp() {
    }

    @Override
    public void run() throws Exception {
        RaftService service = getService();
        service.handleSnapshot(name, installSnapshot);
    }

    public InstallSnapshotOp(String name, InstallSnapshot installSnapshot) {
        super(name);
        this.installSnapshot = installSnapshot;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.INSTALL_SNAPSHOT_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        installSnapshot.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        installSnapshot = new InstallSnapshot();
        installSnapshot.readData(in);
    }
}
