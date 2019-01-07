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

    AcquireResult acquire(long commitIndex, String name, LockEndpoint endpoint, UUID invocationUid) {
        AcquireResult result = getOrInitResource(name).acquire(commitIndex, endpoint, invocationUid, true);

        for (LockInvocationKey waitKey : result.cancelled) {
            removeWaitKey(name, waitKey.invocationUid());
        }

        return result;
    }

    AcquireResult tryAcquire(long commitIndex, String name, LockEndpoint endpoint, UUID invocationUid, long timeoutMs) {
        AcquireResult result = getOrInitResource(name).acquire(commitIndex, endpoint, invocationUid, (timeoutMs > 0));

        for (LockInvocationKey waitKey : result.cancelled) {
            removeWaitKey(name, waitKey.invocationUid());
        }

        if (!result.ownership.isLockedBy(endpoint.sessionId(), endpoint.threadId())) {
            addWaitKey(name, invocationUid, timeoutMs);
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
            removeWaitKey(name, key.invocationUid());
        }

        return result;
    }

    ReleaseResult forceRelease(String name, UUID invocationUid, long expectedFence) {
        RaftLock lock = getResourceOrNull(name);
        if (lock == null) {
            return ReleaseResult.FAILED;
        }

        ReleaseResult result = lock.forceRelease(expectedFence, invocationUid);
        for (LockInvocationKey key : result.notifications) {
            removeWaitKey(name, key.invocationUid());
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
