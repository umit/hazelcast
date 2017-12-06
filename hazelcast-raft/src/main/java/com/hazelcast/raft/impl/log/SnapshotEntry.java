package com.hazelcast.raft.impl.log;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.operation.RaftOperation;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

/**
 * TODO: Javadoc Pending...
 *
 */
public class SnapshotEntry extends LogEntry implements IdentifiedDataSerializable {
    private int groupMembersLogIndex;
    private Collection<RaftEndpoint> groupMembers;

    public SnapshotEntry() {
    }

    public SnapshotEntry(int term, int index, RaftOperation operation,
            int groupMembersLogIndex, Collection<RaftEndpoint> groupMembers) {
        super(term, index, operation);
        this.groupMembersLogIndex = groupMembersLogIndex;
        this.groupMembers = groupMembers;
    }

    public int groupMembersLogIndex() {
        return groupMembersLogIndex;
    }

    public Collection<RaftEndpoint> groupMembers() {
        return groupMembers;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(groupMembersLogIndex);
        out.writeInt(groupMembers.size());
        for (RaftEndpoint endpoint : groupMembers) {
            out.writeObject(endpoint);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        groupMembersLogIndex = in.readInt();
        int count = in.readInt();
        groupMembers = new HashSet<RaftEndpoint>(count);
        for (int i = 0; i < count; i++) {
            RaftEndpoint endpoint = in.readObject();
            groupMembers.add(endpoint);
        }
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.SNAPSHOT_ENTRY;
    }

    @Override
    public String toString() {
        return "SnapshotEntry{" + "term=" + term() + ", index=" + index() + ", operation=" + operation()
                + ", groupMembersLogIndex=" + groupMembersLogIndex + ", groupMembers=" + groupMembers + '}';
    }
}
