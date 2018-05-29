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
import java.util.ArrayList;

public class GetActiveEndpointsOp extends RaftOp implements IdentifiedDataSerializable {

    public GetActiveEndpointsOp() {
    }

    @Override
    protected Object doRun(RaftGroupId groupId, long commitIndex) {
        RaftService service = getService();
        // returning arraylist to be able to serialize response
        return new ArrayList<RaftEndpointImpl>(service.getMetadataManager().getActiveEndpoints());
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.GET_ACTIVE_ENDPOINTS_OP;
    }

}
