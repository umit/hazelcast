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

package com.hazelcast.raft.service.countdownlatch;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * TODO: Javadoc Pending...
 */
public class CountDownLatchRegistrySnapshot implements IdentifiedDataSerializable {

    private List<RaftCountDownLatchSnapshot> latches = new ArrayList<RaftCountDownLatchSnapshot>();
    private Collection<String> destroyedLatchNames = new ArrayList<String>();

    public CountDownLatchRegistrySnapshot() {
    }

    CountDownLatchRegistrySnapshot(List<RaftCountDownLatchSnapshot> latches, Collection<String> destroyedLatchNames) {
        this.latches.addAll(latches);
        this.destroyedLatchNames.addAll(destroyedLatchNames);
    }

    List<RaftCountDownLatchSnapshot> getLatches() {
        return latches;
    }

    Collection<String> getDestroyedLatchNames() {
        return destroyedLatchNames;
    }

    @Override
    public int getFactoryId() {
        return RaftCountDownLatchDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftCountDownLatchDataSerializerHook.COUNT_DOWN_LATCH_REGISTRY_SNAPSHOT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(latches.size());
        for (RaftCountDownLatchSnapshot latch : latches) {
            out.writeObject(latch);
        }
        out.writeInt(destroyedLatchNames.size());
        for (String name : destroyedLatchNames) {
            out.writeUTF(name);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            RaftCountDownLatchSnapshot latch = in.readObject();
            latches.add(latch);
        }
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            String name = in.readUTF();
            destroyedLatchNames.add(name);
        }
    }
}
