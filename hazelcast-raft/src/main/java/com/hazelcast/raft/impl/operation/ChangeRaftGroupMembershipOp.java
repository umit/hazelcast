package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class ChangeRaftGroupMembershipOp extends InternalRaftOp implements IdentifiedDataSerializable {

    public enum MembershipChangeType {
        ADD, REMOVE
    }

    private RaftEndpoint member;

    private MembershipChangeType changeType;

    public ChangeRaftGroupMembershipOp() {
    }

    public ChangeRaftGroupMembershipOp(RaftEndpoint member, MembershipChangeType changeType) {
        this.member = member;
        this.changeType = changeType;
    }

    public RaftEndpoint getMember() {
        return member;
    }

    public MembershipChangeType getChangeType() {
        return changeType;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(member);
        out.writeUTF(changeType.name());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        member = in.readObject();
        changeType = MembershipChangeType.valueOf(in.readUTF());
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.CHANGE_RAFT_GROUP_MEMBERSHIP_OP;
    }

    @Override
    public String toString() {
        return "ChangeRaftGroupMembershipOp{" + "member=" + member + ", changeType=" + changeType + '}';
    }
}
