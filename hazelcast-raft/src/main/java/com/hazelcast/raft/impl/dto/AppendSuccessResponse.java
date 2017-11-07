package com.hazelcast.raft.impl.dto;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class AppendSuccessResponse implements IdentifiedDataSerializable {

    public RaftEndpoint follower;
    public int term;
    public int lastLogIndex;

    public AppendSuccessResponse() {
    }

    public AppendSuccessResponse(RaftEndpoint follower, int term, int lastLogIndex) {
        this.follower = follower;
        this.term = term;
        this.lastLogIndex = lastLogIndex;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(term);
        out.writeObject(follower);
        out.writeInt(lastLogIndex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        term = in.readInt();
        follower = in.readObject();
        lastLogIndex = in.readInt();
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.APPEND_SUCCESS_RESPONSE;
    }

    @Override
    public String toString() {
        return "AppendResponse{" + "follower=" + follower + ", term=" + term  + ", lastLogIndex="
                + lastLogIndex + '}';
    }

}
