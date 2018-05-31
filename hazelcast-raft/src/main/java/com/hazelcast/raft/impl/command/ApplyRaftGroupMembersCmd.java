package com.hazelcast.raft.impl.command;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.command.RaftGroupCmd;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.RaftMember;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * A {@code RaftGroupCmd} to update members of an existing Raft group.
 * This command is generated as a result of member add or remove request.
 */
public class ApplyRaftGroupMembersCmd extends RaftGroupCmd implements IdentifiedDataSerializable {

    private Collection<RaftMember> members;

    public ApplyRaftGroupMembersCmd() {
    }

    public ApplyRaftGroupMembersCmd(Collection<RaftMember> members) {
        this.members = members;
    }

    public Collection<RaftMember> getMembers() {
        return members;
    }

    @Override
    public String toString() {
        return "ApplyRaftGroupMembersCmd{" + "members=" + members + '}';
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.APPLY_RAFT_GROUP_MEMBERS_COMMAND;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(members.size());
        for (RaftMember member : members) {
            out.writeObject(member);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int count = in.readInt();
        Collection<RaftMember> members = new LinkedHashSet<RaftMember>();
        for (int i = 0; i < count; i++) {
            RaftMember member = in.readObject();
            members.add(member);
        }
        this.members = members;
    }
}
