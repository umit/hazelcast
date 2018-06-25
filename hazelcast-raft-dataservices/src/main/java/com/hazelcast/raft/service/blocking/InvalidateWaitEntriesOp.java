/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.blocking;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.service.RaftDataServiceDataSerializerHook;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 */
public class InvalidateWaitEntriesOp<W extends WaitKey> extends RaftOp implements IdentifiedDataSerializable {

    private String serviceName;
    private Collection<W> keys;

    public InvalidateWaitEntriesOp() {
    }

    public InvalidateWaitEntriesOp(String serviceName, Collection<W> keys) {
        this.serviceName = serviceName;
        this.keys = keys;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        AbstractBlockingService<W, BlockingResource<W>, ResourceRegistry<W, BlockingResource<W>>> service = getService();
        service.invalidateWaitEntries(groupId, keys);
        return null;
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    @Override
    public int getFactoryId() {
        return RaftDataServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataServiceDataSerializerHook.INVALIDATE_WAIT_ENTRIES_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(serviceName);
        out.writeInt(keys.size());
        for (WaitKey key : keys) {
            out.writeObject(key);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        serviceName = in.readUTF();
        int size = in.readInt();
        keys = new ArrayList<W>();
        for (int i = 0; i < size; i++) {
            W key = in.readObject();
            keys.add(key);
        }
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", keys=").append(keys);
    }
}
