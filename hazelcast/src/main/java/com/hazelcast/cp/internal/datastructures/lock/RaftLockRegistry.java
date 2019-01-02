/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.lock;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.lock.RaftLock.AcquireResult;
import com.hazelcast.cp.internal.datastructures.lock.RaftLock.ReleaseResult;
import com.hazelcast.cp.internal.datastructures.spi.blocking.ResourceRegistry;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.Map.Entry;
import java.util.UUID;

/**
 * Contains {@link RaftLock} resources and manages wait timeouts
 * based on lock / unlock requests
 */
class RaftLockRegistry extends ResourceRegistry<LockInvocationKey, RaftLock> implements IdentifiedDataSerializable {

    RaftLockRegistry() {
    }

    RaftLockRegistry(CPGroupId groupId) {
        super(groupId);
    }

    @Override
    protected RaftLock createNewResource(CPGroupId groupId, String name) {
        return new RaftLock(groupId, name);
    }

    @Override
    protected RaftLockRegistry cloneForSnapshot() {
        RaftLockRegistry clone = new RaftLockRegistry();
        clone.groupId = this.groupId;
        for (Entry<String, RaftLock> e : this.resources.entrySet()) {
            clone.resources.put(e.getKey(), e.getValue().cloneForSnapshot());
        }
        clone.destroyedNames.addAll(this.destroyedNames);
        clone.waitTimeouts.putAll(this.waitTimeouts);

        return clone;
    }

    AcquireResult acquire(String name, LockEndpoint endpoint, long commitIndex, UUID invocationUid) {
        AcquireResult result = getOrInitResource(name).acquire(endpoint, commitIndex, invocationUid, true);

        for (LockInvocationKey waitKey : result.cancelled) {
            removeWaitKey(waitKey);
        }

        return result;
    }

    AcquireResult tryAcquire(String name, LockEndpoint endpoint, long commitIndex, UUID invocationUid, long timeoutMs) {
        boolean wait = (timeoutMs > 0);
        AcquireResult result = getOrInitResource(name).acquire(endpoint, commitIndex, invocationUid, wait);

        for (LockInvocationKey waitKey : result.cancelled) {
            removeWaitKey(waitKey);
        }

        if (wait && !result.ownership.isLocked()) {
            addWaitKey(new LockInvocationKey(name, endpoint, commitIndex, invocationUid), timeoutMs);
        }

        return result;
    }

    ReleaseResult release(String name, LockEndpoint endpoint, UUID invocationUid, int lockCount) {
        RaftLock lock = getResourceOrNull(name);
        if (lock == null) {
            return ReleaseResult.FAILED;
        }

        ReleaseResult result = lock.release(endpoint, invocationUid, lockCount);
        for (LockInvocationKey key : result.notifications) {
            removeWaitKey(key);
        }

        return result;
    }

    ReleaseResult forceRelease(String name, long expectedFence, UUID invocationUid) {
        RaftLock lock = getResourceOrNull(name);
        if (lock == null) {
            return ReleaseResult.FAILED;
        }

        ReleaseResult result = lock.forceRelease(expectedFence, invocationUid);
        for (LockInvocationKey key : result.notifications) {
            removeWaitKey(key);
        }

        return result;
    }

    RaftLockOwnershipState getLockOwnershipState(String name) {
        RaftLock lock = getResourceOrNull(name);
        return lock != null ? lock.lockOwnershipState() : RaftLockOwnershipState.NOT_LOCKED;
    }

    @Override
    public int getFactoryId() {
        return RaftLockDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftLockDataSerializerHook.RAFT_LOCK_REGISTRY;
    }
}
