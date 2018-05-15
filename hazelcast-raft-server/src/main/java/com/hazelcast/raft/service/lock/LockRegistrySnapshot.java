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

package com.hazelcast.raft.service.lock;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * TODO: Javadoc Pending...
 */
public class LockRegistrySnapshot implements IdentifiedDataSerializable {

    private List<RaftLockSnapshot> locks;
    private Map<LockInvocationKey, Long> tryLockTimeouts;

    public LockRegistrySnapshot() {
    }

    LockRegistrySnapshot(Collection<RaftLock> locks, Map<LockInvocationKey, Long> tryLockTimeouts) {
        this.locks = new ArrayList<RaftLockSnapshot>();
        for (RaftLock lock : locks) {
            this.locks.add(lock.toSnapshot());
        }
        this.tryLockTimeouts = new HashMap<LockInvocationKey, Long>(tryLockTimeouts);
    }

    List<RaftLockSnapshot> getLocks() {
        return locks;
    }

    Map<LockInvocationKey, Long> getTryLockTimeouts() {
        return tryLockTimeouts;
    }

    @Override
    public int getFactoryId() {
        return RaftLockDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftLockDataSerializerHook.LOCK_REGISTRY_SNAPSHOT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(locks.size());
        for (RaftLockSnapshot lock : locks) {
            out.writeObject(lock);
        }
        out.writeInt(tryLockTimeouts.size());
        for (Entry<LockInvocationKey, Long> e : tryLockTimeouts.entrySet()) {
            out.writeObject(e.getKey());
            out.writeLong(e.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int lockCount = in.readInt();
        locks = new ArrayList<RaftLockSnapshot>(lockCount);
        for (int i = 0; i < lockCount; i++) {
            RaftLockSnapshot lock = in.readObject();
            locks.add(lock);
        }

        int tryLockTimeoutCount = in.readInt();
        tryLockTimeouts = new HashMap<LockInvocationKey, Long>(tryLockTimeoutCount);
        for (int i = 0; i < tryLockTimeoutCount; i++) {
            LockInvocationKey key = in.readObject();
            long timestamp = in.readLong();
            tryLockTimeouts.put(key, timestamp);
        }
    }
}
