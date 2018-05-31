package com.hazelcast.raft.impl.service;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftMemberImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class MembershipChangeContext implements IdentifiedDataSerializable {

    private final Map<RaftGroupId, List<RaftMemberImpl>> memberMissingGroups = new HashMap<RaftGroupId, List<RaftMemberImpl>>();
    private RaftMemberImpl leavingMember;
    private final Map<RaftGroupId, RaftGroupMembershipChangeContext> changes = new HashMap<RaftGroupId, RaftGroupMembershipChangeContext>();

    public MembershipChangeContext() {
    }

    public MembershipChangeContext(Map<RaftGroupId, List<RaftMemberImpl>> memberMissingGroups) {
        this.memberMissingGroups.putAll(memberMissingGroups);
    }

    public MembershipChangeContext(RaftMemberImpl leavingMember, Map<RaftGroupId, RaftGroupMembershipChangeContext> changes) {
        this.leavingMember = leavingMember;
        this.changes.putAll(changes);
    }

    public Map<RaftGroupId, List<RaftMemberImpl>> getMemberMissingGroups() {
        return memberMissingGroups;
    }

    public boolean hasNoPendingMembersToAdd() {
        return memberMissingGroups.isEmpty();
    }

    public RaftMemberImpl getLeavingMember() {
        return leavingMember;
    }

    public Map<RaftGroupId, RaftGroupMembershipChangeContext> getChanges() {
        return changes;
    }

    public boolean hasNoPendingChange() {
        return changes.isEmpty();
    }

    public void exclude(Collection<RaftGroupId> groupIds) {
        for (RaftGroupId leftGroupId : groupIds) {
            changes.remove(leftGroupId);
        }
    }

    public void setChanges(Map<RaftGroupId, RaftGroupMembershipChangeContext> newChanges) {
        if (newChanges.isEmpty()) {
            throw new IllegalArgumentException();
        }

        if (leavingMember != null) {
            throw new IllegalStateException("Cannot set changes: " + newChanges + " because there is a leaving member: " + leavingMember);
        }

        if (changes.size() > 0) {
            throw new IllegalStateException("Cannot set changes: " + newChanges + " because there are pending membership changes: " + changes);
        }

        memberMissingGroups.clear();
        changes.putAll(newChanges);
    }

    public static class RaftGroupMembershipChangeContext implements DataSerializable {

        private long membersCommitIndex;

        private Collection<RaftMemberImpl> members;

        private RaftMemberImpl memberToAdd;

        private RaftMemberImpl memberToRemove;

        public RaftGroupMembershipChangeContext() {
        }

        public RaftGroupMembershipChangeContext(long membersCommitIndex, Collection<RaftMemberImpl> members,
                                                RaftMemberImpl memberToAdd, RaftMemberImpl memberToRemove) {
            this.membersCommitIndex = membersCommitIndex;
            this.members = members;
            this.memberToAdd = memberToAdd;
            this.memberToRemove = memberToRemove;
        }

        public long getMembersCommitIndex() {
            return membersCommitIndex;
        }

        public Collection<RaftMemberImpl> getMembers() {
            return members;
        }

        public RaftMemberImpl getMemberToAdd() {
            return memberToAdd;
        }

        public RaftMemberImpl getMemberToRemove() {
            return memberToRemove;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(membersCommitIndex);
            out.writeInt(members.size());
            for (RaftMemberImpl member : members) {
                out.writeObject(member);
            }
            out.writeObject(memberToAdd);
            out.writeObject(memberToRemove);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            membersCommitIndex = in.readLong();
            int len = in.readInt();
            members = new HashSet<RaftMemberImpl>(len);
            for (int i = 0; i < len; i++) {
                RaftMemberImpl member = in.readObject();
                members.add(member);
            }
            memberToAdd = in.readObject();
            memberToRemove = in.readObject();
        }

        @Override
        public String toString() {
            return "RaftGroupMembershipChangeContext{" + "membersCommitIndex=" + membersCommitIndex
                    + ", memberToAdd=" + memberToAdd + ", memberToRemove=" + memberToRemove + '}';
        }
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.MEMBERSHIP_CHANGE_CTX;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(memberMissingGroups.size());
        for (Entry<RaftGroupId, List<RaftMemberImpl>> e : memberMissingGroups.entrySet()) {
            out.writeObject(e.getKey());
            List<RaftMemberImpl> members = e.getValue();
            out.writeInt(members.size());
            for (RaftMemberImpl member : members) {
                out.writeObject(member);
            }
        }
        out.writeObject(leavingMember);
        out.writeInt(changes.size());
        for (Entry<RaftGroupId, RaftGroupMembershipChangeContext> entry : changes.entrySet()) {
            out.writeObject(entry.getKey());
            entry.getValue().writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int missingMemberGroupCount = in.readInt();
        for (int i = 0; i < missingMemberGroupCount; i++) {
            RaftGroupId id = in.readObject();
            int memberCount = in.readInt();
            List<RaftMemberImpl> members = new ArrayList<RaftMemberImpl>(memberCount);
            for (int j = 0; j < memberCount; j++) {
                RaftMemberImpl member = in.readObject();
                members.add(member);
            }
            memberMissingGroups.put(id, members);
        }
        leavingMember = in.readObject();
        int groupCount = in.readInt();
        for (int i = 0; i < groupCount; i++) {
            RaftGroupId groupId = in.readObject();
            RaftGroupMembershipChangeContext context = new RaftGroupMembershipChangeContext();
            context.readData(in);
            changes.put(groupId, context);
        }
    }

    @Override
    public String toString() {
        return "MembershipChangeContext{" + "memberMissingGroups=" + memberMissingGroups + ", leavingMember="
                + leavingMember + ", changes=" + changes + '}';
    }
}
