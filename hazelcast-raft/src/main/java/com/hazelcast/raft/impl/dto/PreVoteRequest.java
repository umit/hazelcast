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
public class PreVoteRequest implements IdentifiedDataSerializable {

    private RaftEndpoint candidate;
    private int nextTerm;
    private int lastLogTerm;
    private int lastLogIndex;

    public PreVoteRequest() {
    }

    public PreVoteRequest(RaftEndpoint candidate, int nextTerm, int lastLogTerm, int lastLogIndex) {
        this.nextTerm = nextTerm;
        this.candidate = candidate;
        this.lastLogTerm = lastLogTerm;
        this.lastLogIndex = lastLogIndex;
    }

    public RaftEndpoint candidate() {
        return candidate;
    }

    public int nextTerm() {
        return nextTerm;
    }

    public int lastLogTerm() {
        return lastLogTerm;
    }

    public int lastLogIndex() {
        return lastLogIndex;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.PRE_VOTE_REQUEST;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(nextTerm);
        out.writeObject(candidate);
        out.writeInt(lastLogTerm);
        out.writeInt(lastLogIndex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        nextTerm = in.readInt();
        candidate = in.readObject();
        lastLogTerm = in.readInt();
        lastLogIndex = in.readInt();
    }

    @Override
    public String toString() {
        return "PreVoteRequest{" + "candidate=" + candidate + ", nextTerm=" + nextTerm + ", lastLogTerm=" + lastLogTerm
                + ", lastLogIndex=" + lastLogIndex + '}';
    }

}
