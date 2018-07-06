package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftMetadataManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO: Javadoc Pending...
 */
public class CreateMetadataRaftGroupOp extends RaftOp implements IdentifiedDataSerializable {

    private List<RaftMemberImpl> initialMembers;
    private int metadataMembersCount;

    public CreateMetadataRaftGroupOp() {
    }

    public CreateMetadataRaftGroupOp(List<RaftMemberImpl> initialMembers, int metadataMembersCount) {
        this.initialMembers = initialMembers;
        this.metadataMembersCount = metadataMembersCount;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftService service = getService();
        RaftMetadataManager metadataManager = service.getMetadataManager();
        metadataManager.createInitialMetadataRaftGroup(initialMembers, metadataMembersCount);
        return null;
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(initialMembers.size());
        for (RaftMemberImpl member : initialMembers) {
            out.writeObject(member);
        }
        out.writeInt(metadataMembersCount);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        initialMembers = new ArrayList<RaftMemberImpl>(len);
        for (int i = 0; i < len; i++) {
            RaftMemberImpl member = in.readObject();
            initialMembers.add(member);
        }
        metadataMembersCount = in.readInt();
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.CREATE_METADATA_RAFT_GROUP_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append("members=").append(metadataMembersCount);
    }
}
