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

import com.hazelcast.core.DistributedObject;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.exception.WaitKeyCancelledException;
import com.hazelcast.cp.internal.datastructures.lock.RaftLock.AcquireResult;
import com.hazelcast.cp.internal.datastructures.lock.RaftLock.ReleaseResult;
import com.hazelcast.cp.internal.datastructures.lock.proxy.RaftFencedLockProxy;
import com.hazelcast.cp.internal.datastructures.spi.blocking.AbstractBlockingService;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.cp.internal.RaftService.getObjectNameForProxy;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Contains Raft-based lock instances
 */
public class RaftLockService extends AbstractBlockingService<LockInvocationKey, RaftLock, RaftLockRegistry> {

    /**
     * Name of the service
     */
    public static final String SERVICE_NAME = "hz:raft:lockService";

    public RaftLockService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    protected void initImpl() {
        super.initImpl();
    }

    public RaftLockOwnershipState acquire(CPGroupId groupId, long commitIndex, String name, LockEndpoint endpoint,
                                          UUID invocationUid) {
        heartbeatSession(groupId, endpoint.sessionId());
        RaftLockRegistry registry = getOrInitRegistry(groupId);
        AcquireResult result = registry.acquire(commitIndex, name, endpoint, invocationUid);

        if (logger.isFineEnabled()) {
            if (result.ownership.isLockedBy(endpoint.sessionId(), endpoint.threadId())) {
                logger.fine("Lock[" + name + "] in " + groupId + " acquired by <" + endpoint + ", " + invocationUid
                        + "> at commit index: " + commitIndex + ". new lock state: " + result.ownership);
            } else {
                logger.fine("Lock[" + name + "] in " + groupId + " wait key added for <" + endpoint + ", " + invocationUid
                        + "> at commit index: " + commitIndex + ". lock state: " + registry.getLockOwnershipState(name));
            }
        }

        if (!result.ownership.isLocked()) {
            notifyWaitKeys(groupId, name, result.cancelled, new WaitKeyCancelledException());
        }

        return result.ownership;
    }

    public RaftLockOwnershipState tryAcquire(CPGroupId groupId, long commitIndex, String name, LockEndpoint endpoint,
                                             UUID invocationUid, long timeoutMs) {
        heartbeatSession(groupId, endpoint.sessionId());
        RaftLockRegistry registry = getOrInitRegistry(groupId);
        AcquireResult result = registry.tryAcquire(commitIndex, name, endpoint, invocationUid, timeoutMs);
        boolean success = result.ownership.isLockedBy(endpoint.sessionId(), endpoint.threadId());

        if (logger.isFineEnabled()) {
            if (success) {
                logger.fine("Lock[" + name + "] in " + groupId + " acquired by <" + endpoint + ", " + invocationUid
                        + "> at commit index: " + commitIndex + ". new lock state: " + result.ownership);
            } else if (timeoutMs > 0) {
                logger.fine("Lock[" + name + "] in " + groupId + " wait key added for <" + endpoint + ", " + invocationUid
                        + "> at commit index: " + commitIndex + ". lock state: " + registry.getLockOwnershipState(name));
            } else {
                logger.fine("Lock[" + name + "] in " + groupId + " not acquired by <" + endpoint + ", " + invocationUid
                        + "> at commit index: " + commitIndex + ". lock state: " + registry.getLockOwnershipState(name));
            }
        }

        if (!success) {
            scheduleTimeout(groupId, name, invocationUid, timeoutMs);
            notifyCancelledWaitKeys(groupId, name, result.cancelled);
        }

        return result.ownership;
    }

    public void release(CPGroupId groupId, long commitIndex, String name, LockEndpoint endpoint, UUID invocationUid,
                        int lockCount) {
        heartbeatSession(groupId, endpoint.sessionId());
        RaftLockRegistry registry = getLockRegistryOrFail(groupId, name);
        ReleaseResult result = registry.release(name, endpoint, invocationUid, lockCount);

        if (logger.isFineEnabled()) {
            if (result.success) {
                logger.fine("Lock[" + name + "] in " + groupId + " released by <" + endpoint + ", " + invocationUid
                        + "> at commit index: " + commitIndex + ". new lock state: " + result.ownership);
            } else {
                logger.fine("Lock[" + name + "] in " + groupId + " not released by <" + endpoint + ", " + invocationUid
                        + "> at commit index: " + commitIndex + ". lock state: " + registry.getLockOwnershipState(name));
            }
        }

        if (result.success) {
            notifyWaitKeys(groupId, name, result.notifications, result.ownership);
        } else {
            notifyCancelledWaitKeys(groupId, name, result.notifications);
            throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
        }
    }

    public void forceRelease(CPGroupId groupId, long commitIndex, String name, UUID invocationUid, long expectedFence) {
        RaftLockRegistry registry = getLockRegistryOrFail(groupId, name);
        ReleaseResult result = registry.forceRelease(name, invocationUid, expectedFence);

        if (logger.isFineEnabled()) {
            if (result.success) {
                logger.fine("Lock[" + name + "] in " + groupId + " force-released by <" + invocationUid
                        + "> at commit index: " + commitIndex + ". new lock state: " + result.ownership);
            } else {
                logger.fine("Lock[" + name + "] in " + groupId + " not force-released by <" + invocationUid
                        + "> at commit index: " + commitIndex + ". lock state: " + registry.getLockOwnershipState(name));
            }
        }

        if (result.success) {
            notifyWaitKeys(groupId, name, result.notifications, result.ownership);
        } else {
            notifyCancelledWaitKeys(groupId, name, result.notifications);
            throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
        }
    }

    private void notifyCancelledWaitKeys(CPGroupId groupId, String name, Collection<LockInvocationKey> keys) {
        if (keys.isEmpty()) {
            return;
        }

        notifyWaitKeys(groupId, name, keys, new WaitKeyCancelledException());
    }

    public RaftLockOwnershipState getLockOwnershipState(CPGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);

        RaftLockRegistry registry = getRegistryOrNull(groupId);
        return registry != null ? registry.getLockOwnershipState(name) : RaftLockOwnershipState.NOT_LOCKED;
    }

    private RaftLockRegistry getLockRegistryOrFail(CPGroupId groupId, String name) {
        checkNotNull(groupId);
        RaftLockRegistry registry = getRegistryOrNull(groupId);
        if (registry == null) {
            throw new IllegalMonitorStateException("Lock registry of " + groupId + " not found for Lock[" + name + "]");
        }

        return registry;
    }

    @Override
    protected RaftLockRegistry createNewRegistry(CPGroupId groupId) {
        return new RaftLockRegistry(groupId);
    }

    @Override
    protected Object expiredWaitKeyResponse() {
        return RaftLockOwnershipState.NOT_LOCKED;
    }

    @Override
    protected String serviceName() {
        return SERVICE_NAME;
    }

    @Override
    public DistributedObject createDistributedObject(String proxyName) {
        try {
            CPGroupId groupId = raftService.createRaftGroupForProxy(proxyName);
            return new RaftFencedLockProxy(nodeEngine, groupId, proxyName, getObjectNameForProxy(proxyName));
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public void destroyDistributedObject(String objectName) {
    }
}
