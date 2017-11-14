package com.hazelcast.raft;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftException extends HazelcastException {

    private transient RaftEndpoint leader;

    public RaftException(RaftEndpoint leader) {
        this.leader = leader;
    }

    public RaftException(String message, RaftEndpoint leader) {
        super(message);
        this.leader = leader;
    }

    public RaftEndpoint getLeader() {
        return leader;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();

        if (leader == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(leader.getUid());
            Address address = leader.getAddress();
            out.writeUTF(address.getHost());
            out.writeInt(address.getPort());
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        if (in.readBoolean()) {
            String uid = in.readUTF();
            String host = in.readUTF();
            int port = in.readInt();
            leader = new RaftEndpoint(uid, new Address(host, port));
        }
    }
}
