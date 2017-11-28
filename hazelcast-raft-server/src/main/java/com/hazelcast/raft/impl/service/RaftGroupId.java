package com.hazelcast.raft.impl.service;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public final class RaftGroupId implements IdentifiedDataSerializable {

    private String name;
    private int commitIndex;

    public RaftGroupId(String name, int commitIndex) {
        assert name != null;
        this.name = name;
        this.commitIndex = commitIndex;
    }

    public RaftGroupId() {
    }

    public String name() {
        return name;
    }

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
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.GROUP_ID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RaftGroupId)) {
            return false;
        }

        RaftGroupId that = (RaftGroupId) o;
        return commitIndex == that.commitIndex && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + commitIndex;
        return result;
    }

    public static RaftGroupId readFrom(ClientMessage message) {
        String name = message.getStringUtf8();
        int commitIndex = message.getInt();
        return new RaftGroupId(name, commitIndex);
    }

    public static void writeTo(RaftGroupId groupId, ClientMessage message) {
        message.set(groupId.name);
        message.set(groupId.commitIndex);
    }

    @Override
    public String toString() {
        return "RaftGroupId{" + "name='" + name + '\'' + ", commitIndex=" + commitIndex + '}';
    }

    public static int dataSize(RaftGroupId groupId) {
        return ParameterUtil.calculateDataSize(groupId.name()) + Bits.INT_SIZE_IN_BYTES;
    }
}
