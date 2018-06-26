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

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.blocking.BlockingResource;
import com.hazelcast.util.collection.Long2LongHashMap;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.util.Iterator;

import static com.hazelcast.raft.service.session.AbstractSessionManager.NO_SESSION_ID;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * TODO: Javadoc Pending...
 */
public class RaftSemaphore extends BlockingResource<SemaphoreInvocationKey> {

    private static final int MISSING_VALUE = NO_SESSION_ID;

    private boolean initialized;
    private int available;
    private final Long2LongHashMap acquires = new Long2LongHashMap(MISSING_VALUE);

    protected RaftSemaphore(RaftGroupId groupId, String name) {
        super(groupId, name);
    }

    @Override
    protected void onInvalidateSession(long sessionId, Long2ObjectHashMap<Object> result) {
        // TODO:
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
        return available - permits >= 0;
    }

    boolean acquire(long commitIndex, String name, long sessionId, int permits, long timeoutMs) {
        if (!isAvailable(permits)) {
            if (timeoutMs != 0) {
                waitKeys.add(new SemaphoreInvocationKey(commitIndex, name, sessionId, permits));
            }
            return false;
        }
        available -= permits;
        long acquired = getAcquired(sessionId);
        acquires.put(sessionId, acquired + permits);
        return true;
    }

    private long getAcquired(long sessionId) {
        long acquired = acquires.get(sessionId);
        if (acquired == MISSING_VALUE) {
            acquired = 0;
        }
        return acquired;
    }

    SemaphoreInvocationKey release(long sessionId, int permits) {
        checkPositive(permits, "Permits should be positive!");

        long acquired = getAcquired(sessionId);
        if (acquired < permits) {
            throw new IllegalStateException("Cannot release " + permits
                    + " permits. Session has acquired only " + acquired + " permits!");
        }

        available += permits;
        if (acquired == permits) {
            acquires.remove(sessionId);
        } else {
            acquires.put(sessionId, acquired - permits);
        }

        Iterator<SemaphoreInvocationKey> iterator = waitKeys.iterator();
        if (iterator.hasNext()) {
            SemaphoreInvocationKey key = iterator.next();
            if (key.permits() <= available) {
                iterator.remove();
                available -= key.permits();
                acquired = getAcquired(sessionId);
                acquires.put(key.sessionId(), acquired + key.permits());
            }
            return key;
        }
        return null;
    }
}
