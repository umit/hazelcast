package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.operation.RaftOperation;

import java.io.IOException;

public class GetRaftGroupIfMemberOp
        extends RaftOperation implements IdentifiedDataSerializable {

    private RaftGroupId groupId;

    private RaftEndpoint endpoint;

    public GetRaftGroupIfMemberOp() {
    }

    public GetRaftGroupIfMemberOp(RaftGroupId groupId, RaftEndpoint endpoint) {
        this.groupId = groupId;
        this.endpoint = endpoint;
    }

    @Override
    protected Object doRun(int commitIndex) {
        RaftService service = getService();
        return service.getMetadataManager().getRaftGroupIfMember(groupId, endpoint);
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(groupId);
        out.writeObject(endpoint);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        groupId = in.readObject();
        endpoint = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.GET_RAFT_GROUP_IF_MEMBER_OP;
    }
}
