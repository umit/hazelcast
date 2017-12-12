package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.service.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftMetadataManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.operation.RaftOperation;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class CompleteRemoveEndpointOperation extends RaftOperation implements IdentifiedDataSerializable {

    private RaftEndpoint endpoint;

    private Map<RaftGroupId, Entry<Integer, Integer>> leftGroups;

    public CompleteRemoveEndpointOperation() {
    }

    public CompleteRemoveEndpointOperation(RaftEndpoint endpoint, Map<RaftGroupId, Entry<Integer, Integer>> leftGroups) {
        this.endpoint = endpoint;
        this.leftGroups = leftGroups;
    }

    @Override
    protected Object doRun(int commitIndex) {
        RaftService service = getService();
        RaftMetadataManager metadataManager = service.getMetadataManager();
        metadataManager.completeRemoveEndpoint(endpoint, leftGroups);
        return endpoint;
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(endpoint);
        out.writeInt(leftGroups.size());
        for (Entry<RaftGroupId, Entry<Integer, Integer>> e : leftGroups.entrySet()) {
            out.writeObject(e.getKey());
            out.writeInt(e.getValue().getKey());
            out.writeInt(e.getValue().getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        endpoint = in.readObject();
        int count = in.readInt();
        leftGroups = new HashMap<RaftGroupId, Entry<Integer, Integer>>(count);
        for (int i = 0; i < count; i++) {
            RaftGroupId groupId = in.readObject();
            int currMembersCommitIndex = in.readInt();
            int newMembersCommitIndex = in.readInt();
            leftGroups.put(groupId, new SimpleEntry<Integer, Integer>(currMembersCommitIndex, newMembersCommitIndex));
        }
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.COMPLETE_REMOVE_ENDPOINT_OP;
    }
}
