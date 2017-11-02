package com.hazelcast.raft.impl.dto;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class VoteResponse implements IdentifiedDataSerializable {

    public Address voter;
    public int term;
    public boolean granted;

    public VoteResponse() {
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
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.VOTE_RESPONSE;
    }

    @Override
    public String toString() {
        return "VoteResponse{" + "voter=" + voter + ", term=" + term + ", granted=" + granted + '}';
    }

}
