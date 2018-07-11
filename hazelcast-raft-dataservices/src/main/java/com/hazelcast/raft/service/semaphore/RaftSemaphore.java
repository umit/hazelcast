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

package com.hazelcast.raft.service.semaphore;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.blocking.BlockingResource;
import com.hazelcast.util.collection.Long2LongHashMap;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.hazelcast.raft.service.session.AbstractSessionManager.NO_SESSION_ID;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * TODO: Javadoc Pending...
 */
public class RaftSemaphore extends BlockingResource<SemaphoreInvocationKey> implements IdentifiedDataSerializable {

    private static final long MISSING_VALUE = NO_SESSION_ID;

    private boolean initialized;
    private int available;
    private final Long2LongHashMap acquires = new Long2LongHashMap(MISSING_VALUE);

    public RaftSemaphore() {
    }

    RaftSemaphore(RaftGroupId groupId, String name) {
        super(groupId, name);
    }

    @Override
    protected void onInvalidateSession(long sessionId, Long2ObjectHashMap<Object> result) {
        long acquired = acquires.get(sessionId);
        if (acquired != NO_SESSION_ID) {
            Collection<SemaphoreInvocationKey> keys = release(sessionId, (int) acquired);
            for (SemaphoreInvocationKey key : keys) {
                result.put(key.commitIndex(), Boolean.TRUE);
            }
        }
    }

    boolean init(int permits) {
        if (initialized || available != 0) {
            return false;
        }

        available = permits;
        initialized = true;

        return true;
    }

    int getAvailable() {
        return available;
    }

    boolean isAvailable(int permits) {
        checkPositive(permits, "Permits should be positive!");
        return available >= permits;
    }

    boolean acquire(long commitIndex, String name, long sessionId, int permits, boolean wait) {
        if (!isAvailable(permits)) {
            if (wait) {
                waitKeys.add(new SemaphoreInvocationKey(name, commitIndex, sessionId, permits));
            }
            return false;
        }

        doAcquire(sessionId, permits);

        return true;
    }

    private long getAcquired(long sessionId) {
        long acquired = acquires.get(sessionId);
        if (acquired == MISSING_VALUE) {
            acquired = 0;
        }

        return acquired;
    }

    private void doAcquire(long sessionId, int permits) {
        available -= permits;

        if (sessionId == NO_SESSION_ID) {
            return;
        }

        long acquired = getAcquired(sessionId);
        acquires.put(sessionId, acquired + permits);
    }

    Collection<SemaphoreInvocationKey> release(long sessionId, int permits) {
        checkPositive(permits, "Permits should be positive!");

        long acquired = getAcquired(sessionId);

        if (sessionId != NO_SESSION_ID && acquired < permits) {
            throw new IllegalArgumentException("Cannot release " + permits
                    + " permits. Session has acquired only " + acquired + " permits!");
        }

        available += permits;
        if (sessionId != NO_SESSION_ID) {
            acquired -= permits;
            if (acquired == 0) {
                acquires.remove(sessionId);
            } else {
                acquires.put(sessionId, acquired);
            }
        }

        return assingPermitsToWaitKeys();
    }

    private Collection<SemaphoreInvocationKey> assingPermitsToWaitKeys() {
        List<SemaphoreInvocationKey> keys = new ArrayList<SemaphoreInvocationKey>();
        Iterator<SemaphoreInvocationKey> iterator = waitKeys.iterator();
        while (iterator.hasNext()) {
            SemaphoreInvocationKey key = iterator.next();
            if (key.permits() > available) {
                break;
            }

            iterator.remove();
            keys.add(key);
            doAcquire(key.sessionId(), key.permits());
        }

        return keys;
    }

    int drain(long sessionId) {
        int drained = available;
        if (drained > 0) {
            doAcquire(sessionId, drained);
        }
        available = 0;

        return drained;
    }

    Tuple2<Boolean, Collection<SemaphoreInvocationKey>> change(int permits) {
        if (permits == 0) {
            Collection<SemaphoreInvocationKey> c = Collections.emptyList();
            return Tuple2.of(false, c);
        }

        available += permits;
        initialized = true;

        Collection<SemaphoreInvocationKey> keys =
                permits > 0 ? assingPermitsToWaitKeys() : Collections.<SemaphoreInvocationKey>emptyList();

        return Tuple2.of(true, keys);
    }

    @Override
    public int getFactoryId() {
        return RaftSemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSemaphoreDataSerializerHook.RAFT_SEMAPHORE;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        out.writeBoolean(initialized);
        out.writeInt(available);
        out.writeInt(acquires.size());
        for (Map.Entry<Long, Long> e : acquires.entrySet()) {
            out.writeLong(e.getKey());
            out.writeLong(e.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        initialized = in.readBoolean();
        available = in.readInt();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            long key = in.readLong();
            long value = in.readLong();
            acquires.put(key, value);
        }
    }

    @Override
    public String toString() {
        return "RaftSemaphore{" + "groupId=" + groupId + ", name='" + name + '\'' + ", initialized=" + initialized
                + ", available=" + available + ", acquires=" + acquires + ", waitKeys=" + waitKeys + '}';
    }
}
