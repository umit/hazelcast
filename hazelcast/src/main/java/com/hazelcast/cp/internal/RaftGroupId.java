/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cp.internal;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.cp.CPGroupId;

import java.io.IOException;

/**
 * Implementation of {@link CPGroupId}.
 */
public final class RaftGroupId implements CPGroupId, IdentifiedDataSerializable {

    private String name;
    private long term;
    private long commitIndex;

    public RaftGroupId() {
    }

    public RaftGroupId(String name, long term, long commitIndex) {
        assert name != null;
        this.name = name;
        this.term = term;
        this.commitIndex = commitIndex;
    }

    @Override
    public String name() {
        return name;
    }

    public long term() {
        return term;
    }

    @Override
    public long id() {
        return commitIndex;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeLong(term);
        out.writeLong(commitIndex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        term = in.readLong();
        commitIndex = in.readLong();
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
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RaftGroupId that = (RaftGroupId) o;

        if (term != that.term) {
            return false;
        }
        if (commitIndex != that.commitIndex) {
            return false;
        }
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (int) (term ^ (term >>> 32));
        result = 31 * result + (int) (commitIndex ^ (commitIndex >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "CPGroupId{" + "name='" + name + '\'' + ", term= " + term + ", commitIndex=" + commitIndex + '}';
    }

    // ----- CLIENT CONVENIENCE METHODS -----
    public static RaftGroupId readFrom(ClientMessage message) {
        String name = message.getStringUtf8();
        long term = message.getLong();
        long commitIndex = message.getLong();
        return new RaftGroupId(name, term, commitIndex);
    }

    public static void writeTo(RaftGroupId groupId, ClientMessage message) {
        message.set(groupId.name());
        message.set(groupId.term);
        message.set(groupId.id());
    }

    public static int dataSize(RaftGroupId groupId) {
        return ParameterUtil.calculateDataSize(groupId.name()) + 2 * Bits.LONG_SIZE_IN_BYTES;
    }
}
