package com.hazelcast.raft.impl.dto;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class VoteRequest implements DataSerializable {

    public int term;
    public Address candidate;
    public int lastLogTerm;
    public int lastLogIndex;

    public VoteRequest() {
    }

    public VoteRequest(int term, Address candidate, int lastLogTerm, int lastLogIndex) {
        this.term = term;
        this.candidate = candidate;
        this.lastLogTerm = lastLogTerm;
        this.lastLogIndex = lastLogIndex;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(term);
        out.writeObject(candidate);
        out.writeInt(lastLogTerm);
        out.writeInt(lastLogIndex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        term = in.readInt();
        candidate = in.readObject();
        lastLogTerm = in.readInt();
        lastLogIndex = in.readInt();
    }

    @Override
    public String toString() {
        return "VoteRequest{" + "term=" + term + ", candidate=" + candidate + ", lastLogTerm=" + lastLogTerm
                + ", lastLogIndex=" + lastLogIndex + '}';
    }
}
