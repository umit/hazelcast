package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftEndpointImpl;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.RaftOp;

import java.io.IOException;

public class CheckRemovedEndpointOp extends RaftOp implements IdentifiedDataSerializable {

    private RaftEndpointImpl endpoint;

    public CheckRemovedEndpointOp() {
    }

    public CheckRemovedEndpointOp(RaftEndpointImpl endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftService service = getService();
        return service.getMetadataManager().isRemoved(endpoint);
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(endpoint);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        endpoint = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.CHECK_REMOVED_ENDPOINT_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", endpoint=").append(endpoint);
    }

}
