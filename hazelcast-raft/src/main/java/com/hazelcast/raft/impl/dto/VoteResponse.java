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
public class VoteResponse implements DataSerializable {

    public int term;
    public boolean granted;
    public Address voter;

    public VoteResponse() {
    }

    public VoteResponse(int term, boolean granted, Address voter) {
        this.term = term;
        this.granted = granted;
        this.voter = voter;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(term);
        out.writeBoolean(granted);
        out.writeObject(voter);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        term = in.readInt();
        granted = in.readBoolean();
        voter = in.readObject();
    }

    @Override
    public String toString() {
        return "VoteResponse{" + "term=" + term + ", granted=" + granted + ", voter=" + voter + '}';
    }
}
