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
import com.hazelcast.raft.service.blocking.ResourceRegistry;

import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 */
public class SemaphoreRegistry extends ResourceRegistry<SemaphoreInvocationKey, RaftSemaphore> {

    protected SemaphoreRegistry(RaftGroupId groupId) {
        super(groupId);
    }

    @Override
    protected RaftSemaphore createNewResource(RaftGroupId groupId, String name) {
        return new RaftSemaphore(groupId, name);
    }

    boolean init(String name, int permits) {
        RaftSemaphore semaphore = getOrInitResource(name);
        return semaphore.init(permits);
    }

    int availablePermits(String name) {
        RaftSemaphore semaphore = getResourceOrNull(name);
        if (semaphore == null) {
            return 0;
        }
        return semaphore.getAvailable();
    }

    boolean acquire(long commitIndex, String name, long sessionId, int permits, long timeoutMs) {
        RaftSemaphore semaphore = getOrInitResource(name);
        boolean wait = (timeoutMs != 0);
        boolean acquired = semaphore.acquire(commitIndex, name, sessionId, permits, wait);
        if (!acquired && timeoutMs > 0) {
            SemaphoreInvocationKey key = new SemaphoreInvocationKey(commitIndex, name, sessionId, permits);
            scheduleWaitTimeout(key, timeoutMs);
        }
        return acquired;
    }

    int release(String name, long sessionId, int sessionPermits, int permits) {
        RaftSemaphore semaphore = getOrInitResource(name);
        return semaphore.release(sessionId, sessionPermits, permits);
    }

    Collection<SemaphoreInvocationKey> pollWaitingKeys(String name) {
        RaftSemaphore semaphore = getOrInitResource(name);
        return semaphore.pollWaitingKeys();
    }

    int drainPermits(String name, long sessionId) {
        RaftSemaphore semaphore = getResourceOrNull(name);
        if (semaphore == null) {
            return 0;
        }
        return semaphore.drain(sessionId);
    }

    boolean changePermits(String name, int permits) {
        RaftSemaphore semaphore = getOrInitResource(name);
        return semaphore.change(permits);
    }
}
