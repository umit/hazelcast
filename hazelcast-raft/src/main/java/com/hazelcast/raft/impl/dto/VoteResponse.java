package com.hazelcast.raft.impl.dto;

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

    public VoteResponse() {
    }

    public VoteResponse(int term, boolean granted) {
        this.term = term;
        this.granted = granted;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(term);
        out.writeBoolean(granted);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        term = in.readInt();
        granted = in.readBoolean();
    }

    @Override
    public String toString() {
        return "VoteResponse{" + "term=" + term + ", granted=" + granted + '}';
    }
}
