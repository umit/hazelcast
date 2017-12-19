package com.hazelcast.raft.impl;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;

import java.io.IOException;

/**
 * Implementation of {@link RaftGroupId}.
 */
public final class RaftGroupIdImpl implements RaftGroupId, IdentifiedDataSerializable {

    private String name;
    private int commitIndex;

    public RaftGroupIdImpl() {
    }

    public RaftGroupIdImpl(String name, int commitIndex) {
        assert name != null;
        this.name = name;
        this.commitIndex = commitIndex;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int commitIndex() {
        return commitIndex;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(commitIndex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        commitIndex = in.readInt();
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.GROUP_ID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RaftGroupIdImpl)) {
            return false;
        }

        RaftGroupIdImpl that = (RaftGroupIdImpl) o;
        return commitIndex == that.commitIndex && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + commitIndex;
        return result;
    }

    @Override
    public String toString() {
        return "RaftGroupId{" + "name='" + name + '\'' + ", commitIndex=" + commitIndex + '}';
    }

    // ----- CLIENT CONVENIENCE METHODS -----
    public static RaftGroupId readFrom(ClientMessage message) {
        String name = message.getStringUtf8();
        int commitIndex = message.getInt();
        return new RaftGroupIdImpl(name, commitIndex);
    }

    public static void writeTo(RaftGroupId groupId, ClientMessage message) {
        message.set(groupId.name());
        message.set(groupId.commitIndex());
    }

    public static int dataSize(RaftGroupId groupId) {
        return ParameterUtil.calculateDataSize(groupId.name()) + Bits.INT_SIZE_IN_BYTES;
    }
}
