package com.hazelcast.raft.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class ChangeRaftGroupMembersOp extends RaftCommandOperation {

    public enum MembershipChangeType {
        ADD, REMOVE
    }

    private RaftEndpoint member;

    private MembershipChangeType changeType;

    public ChangeRaftGroupMembersOp(RaftEndpoint member, MembershipChangeType changeType) {
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
        throw new UnsupportedOperationException();
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "ChangeRaftGroupMembersOp{" + "member=" + member + ", changeType=" + changeType + '}';
    }
}
