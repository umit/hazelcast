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

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.blocking.ResourceRegistry;

import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 */
public class SemaphoreRegistry extends ResourceRegistry<SemaphoreInvocationKey, RaftSemaphore>
        implements IdentifiedDataSerializable {

    public SemaphoreRegistry() {
    }

    protected SemaphoreRegistry(RaftGroupId groupId) {
        super(groupId);
    }

    @Override
    protected RaftSemaphore createNewResource(RaftGroupId groupId, String name) {
        return new RaftSemaphore(groupId, name);
    }

    boolean init(String name, int permits) {
        return getOrInitResource(name).init(permits);
    }

    int availablePermits(String name) {
        RaftSemaphore semaphore = getResourceOrNull(name);
        return semaphore != null ? semaphore.getAvailable() : 0;
    }

    boolean acquire(long commitIndex, String name, long sessionId, int permits, long timeoutMs) {
        boolean wait = (timeoutMs != 0);
        boolean acquired = getOrInitResource(name).acquire(commitIndex, name, sessionId, permits, wait);
        if (!acquired && timeoutMs > 0) {
            addWaitKey(new SemaphoreInvocationKey(name, commitIndex, sessionId, permits), timeoutMs);
        }

        return acquired;
    }

    Collection<SemaphoreInvocationKey> release(String name, long sessionId, int permits) {
        Collection<SemaphoreInvocationKey> keys = getOrInitResource(name).release(sessionId, permits);
        for (SemaphoreInvocationKey key : keys) {
            removeWaitKey(key);
        }

        return keys;
    }

    int drainPermits(String name, long sessionId) {
        RaftSemaphore semaphore = getResourceOrNull(name);
        if (semaphore == null) {
            return 0;
        }

        return semaphore.drain(sessionId);
    }

    Tuple2<Boolean, Collection<SemaphoreInvocationKey>> changePermits(String name, int permits) {
        Tuple2<Boolean, Collection<SemaphoreInvocationKey>> t = getOrInitResource(name).change(permits);
        for (SemaphoreInvocationKey key : t.element2) {
            removeWaitKey(key);
        }

        return t;
    }

    @Override
    public int getFactoryId() {
        return RaftSemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSemaphoreDataSerializerHook.SEMAPHORE_REGISTRY;
    }
}
