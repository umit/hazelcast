package com.hazelcast.raft.impl.dto;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.LogEntry;
import com.hazelcast.raft.impl.RaftDataSerializerHook;

import java.io.IOException;
import java.util.Arrays;

/**
 * TODO: Javadoc Pending...
 *
 */
public class AppendRequest implements IdentifiedDataSerializable {

    public Address leader;
    public int term;
    public int prevLogTerm;
    public int prevLogIndex;
    public int leaderCommitIndex;
    public LogEntry[] entries;

    public AppendRequest() {
    }

    public AppendRequest(Address leader, int term, int prevLogTerm, int prevLogIndex, int leaderCommitIndex, LogEntry[] entries) {
        this.term = term;
        this.leader = leader;
        this.prevLogTerm = prevLogTerm;
        this.prevLogIndex = prevLogIndex;
        this.leaderCommitIndex = leaderCommitIndex;
        this.entries = entries;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(term);
        out.writeObject(leader);
        out.writeInt(prevLogTerm);
        out.writeInt(prevLogIndex);
        out.writeInt(leaderCommitIndex);

        out.writeInt(entries.length);
        for (LogEntry entry : entries) {
            out.writeObject(entry);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        term = in.readInt();
        leader = in.readObject();
        prevLogTerm = in.readInt();
        prevLogIndex = in.readInt();
        leaderCommitIndex = in.readInt();

        int len = in.readInt();
        entries = new LogEntry[len];
        for (int i = 0; i < len; i++) {
            entries[i] = in.readObject();
        }
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.APPEND_REQUEST;
    }

    @Override
    public String toString() {
        return "AppendRequest{" + "leader=" + leader + ", term=" + term + ", prevLogTerm=" + prevLogTerm + ", prevLogIndex="
                + prevLogIndex + ", leaderCommitIndex=" + leaderCommitIndex + ", entries=" + Arrays.toString(entries) + '}';
    }

}
