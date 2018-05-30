package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.service.RaftGroupInfo;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

import java.io.IOException;

public class CreateRaftNodeOp extends Operation implements IdentifiedDataSerializable, AllowedDuringPassiveState {

    private RaftGroupInfo group;

    public CreateRaftNodeOp() {
    }

    public CreateRaftNodeOp(RaftGroupInfo group) {
        this.group = group;
    }

    @Override
    public void run() {
        RaftService service = getService();
        service.createRaftNode(group);
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(group);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        group = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.CREATE_RAFT_NODE_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", group=").append(group);
    }
}
