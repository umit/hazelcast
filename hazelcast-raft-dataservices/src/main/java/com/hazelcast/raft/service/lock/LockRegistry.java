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

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.blocking.ResourceRegistry;
import com.hazelcast.util.Clock;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
class LockRegistry extends ResourceRegistry<LockInvocationKey, RaftLock> {

    LockRegistry(RaftGroupId groupId) {
        super(groupId);
    }

    @Override
    protected RaftLock createNewResource(RaftGroupId groupId, String name) {
        return new RaftLock(groupId, name);
    }

    boolean acquire(String name, LockEndpoint endpoint, long commitIndex, UUID invocationUid) {
        return getOrInitResource(name).acquire(endpoint, commitIndex, invocationUid, true);
    }

    boolean tryAcquire(String name, LockEndpoint endpoint, long commitIndex, UUID invocationUid, long timeoutMs) {
        boolean wait = (timeoutMs > 0);
        boolean acquired = getOrInitResource(name).acquire(endpoint, commitIndex, invocationUid, wait);
        if (wait && !acquired) {
            LockInvocationKey key = new LockInvocationKey(name, endpoint, commitIndex, invocationUid);
            waitTimeouts.put(key, Tuple2.of(timeoutMs, Clock.currentTimeMillis() + timeoutMs));
        }

        return acquired;
    }

    Collection<LockInvocationKey> release(String name, LockEndpoint endpoint, UUID invocationUid) {
        RaftLock lock = getResourceOrNull(name);
        if (lock == null) {
            return Collections.emptyList();
        }

        Collection<LockInvocationKey> waitKeys = lock.release(endpoint, invocationUid);
        for (LockInvocationKey waitKey : waitKeys) {
            waitTimeouts.remove(waitKey);
        }

        return waitKeys;
    }

    Collection<LockInvocationKey> forceRelease(String name, long expectedFence, UUID invocationUid) {
        RaftLock lock = getResourceOrNull(name);
        if (lock == null) {
            return Collections.emptyList();
        }

        Collection<LockInvocationKey> waitKeys = lock.forceRelease(expectedFence, invocationUid);
        for (LockInvocationKey waitKey : waitKeys) {
            waitTimeouts.remove(waitKey);
        }

        return waitKeys;
    }

    int getLockCount(String name, LockEndpoint endpoint) {
        RaftLock lock = getResourceOrNull(name);
        if (lock == null) {
            return 0;
        }

        if (endpoint != null) {
            LockInvocationKey owner = lock.owner();
            return (owner != null && endpoint.equals(owner.endpoint())) ? lock.lockCount() : 0;
        }

        return lock.lockCount();
    }

    long getLockFence(String name) {
        RaftLock lock = getResourceOrNull(name);
        if (lock == null) {
            throw new IllegalMonitorStateException();
        }

        LockInvocationKey owner = lock.owner();
        if (owner == null) {
            throw new IllegalMonitorStateException("Lock[" + name + "] has no owner!");
        }

        return owner.commitIndex();
    }

    LockRegistrySnapshot toSnapshot() {
        return new LockRegistrySnapshot(resources.values(), waitTimeouts, destroyedNames);
    }

    Map<LockInvocationKey, Long> restore(LockRegistrySnapshot snapshot) {
        for (RaftLockSnapshot lockSnapshot : snapshot.getLocks()) {
            resources.put(lockSnapshot.getName(), new RaftLock(lockSnapshot));
        }

        destroyedNames.addAll(snapshot.getDestroyedLockNames());

        long now = Clock.currentTimeMillis();
        Map<LockInvocationKey, Long> added = new HashMap<LockInvocationKey, Long>();
        for (Entry<LockInvocationKey, Long> e : snapshot.getTryLockTimeouts().entrySet()) {
            LockInvocationKey key = e.getKey();
            if (!waitTimeouts.containsKey(key)) {
                long timeout = e.getValue();
                waitTimeouts.put(key, Tuple2.of(timeout, now + timeout));
                added.put(key, timeout);
            }
        }

        return added;
    }
}
