package com.hazelcast.raft.impl.dto;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;

public class AppendFailureResponse implements IdentifiedDataSerializable {

    public RaftEndpoint follower;
    public int term;
    public int expectedNextIndex;

    public AppendFailureResponse() {
    }

    public AppendFailureResponse(RaftEndpoint follower, int term, int expectedNextIndex) {
        this.follower = follower;
        this.term = term;
        this.expectedNextIndex = expectedNextIndex;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(term);
        out.writeObject(follower);
        out.writeInt(expectedNextIndex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        term = in.readInt();
        follower = in.readObject();
        expectedNextIndex = in.readInt();
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.APPEND_FAILURE_RESPONSE;
    }

    @Override
    public String toString() {
        return "AppendFailureResponse{" + "follower=" + follower + ", term=" + term + ", expectedNextIndex="
                + expectedNextIndex + '}';
    }

}
