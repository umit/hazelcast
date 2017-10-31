package com.hazelcast.raft.impl.dto;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class AppendResponse implements DataSerializable {

    public boolean success;
    public int term;

    public int lastLogIndex;

    public AppendResponse() {
    }

    public AppendResponse(boolean success, int term, int lastLogIndex) {
        this.success = success;
        this.term = term;
        this.lastLogIndex = lastLogIndex;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(success);
        out.writeInt(term);
        out.writeInt(lastLogIndex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        success = in.readBoolean();
        term = in.readInt();
        lastLogIndex = in.readInt();
    }

    @Override
    public String toString() {
        return "AppendResponse{" + "success=" + success + ", term=" + term + ", lastLogIndex=" + lastLogIndex + '}';
    }
}
