package com.hazelcast.raft.exception;

import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashSet;

public class MismatchingGroupMembersCommitIndexException extends RaftException {

    private transient int commitIndex;

    private transient Collection<RaftEndpoint> members;

    public MismatchingGroupMembersCommitIndexException(int commitIndex, Collection<RaftEndpoint> members) {
        super(null);
        this.commitIndex = commitIndex;
        this.members = members;
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public Collection<RaftEndpoint> getMembers() {
        return members;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeInt(commitIndex);
        out.writeInt(members.size());
        for (RaftEndpoint endpoint : members) {
            out.writeUTF(endpoint.getUid());
            Address address = endpoint.getAddress();
            out.writeUTF(address.getHost());
            out.writeInt(address.getPort());
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        commitIndex = in.readInt();
        int count = in.readInt();
        members = new HashSet<RaftEndpoint>(count);
        for (int i = 0; i < count; i++) {
            String uuid = in.readUTF();
            String host = in.readUTF();
            int port = in.readInt();
            members.add(new RaftEndpoint(uuid, new Address(host, port)));
        }
    }
}
