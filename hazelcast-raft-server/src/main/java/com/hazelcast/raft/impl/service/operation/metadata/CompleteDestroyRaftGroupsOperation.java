package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.impl.service.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CompleteDestroyRaftGroupsOperation extends RaftOperation implements IdentifiedDataSerializable {

    private Set<RaftGroupId> groupIds;

    public CompleteDestroyRaftGroupsOperation() {
    }

    public CompleteDestroyRaftGroupsOperation(Set<RaftGroupId> groupIds) {
        this.groupIds = groupIds;
    }

    @Override
    protected Object doRun(int commitIndex) {
        RaftService service = getService();
        service.completeDestroy(groupIds);
        return null;
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(groupIds.size());
        for (RaftGroupId groupId : groupIds) {
            groupId.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int count = in.readInt();
        groupIds = new HashSet<RaftGroupId>();
        for (int i = 0; i < count; i++) {
            RaftGroupId groupId = new RaftGroupId();
            groupId.readData(in);
            groupIds.add(groupId);
        }
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.COMPLETE_DESTROY_RAFT_GROUPS_OP;
    }

}
