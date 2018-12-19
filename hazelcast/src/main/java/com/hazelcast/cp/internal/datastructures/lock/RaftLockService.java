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

package com.hazelcast.cp.internal.datastructures.lock;

import com.hazelcast.cp.FencedLock;
import com.hazelcast.cp.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.datastructures.exception.WaitKeyCancelledException;
import com.hazelcast.cp.internal.datastructures.lock.RaftLock.AcquireResult;
import com.hazelcast.cp.internal.datastructures.lock.RaftLock.ReleaseResult;
import com.hazelcast.cp.internal.datastructures.lock.proxy.RaftFencedLockProxy;
import com.hazelcast.cp.internal.datastructures.spi.blocking.AbstractBlockingService;
import com.hazelcast.cp.internal.session.ProxySessionManagerService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.cp.internal.RaftService.getObjectNameForProxy;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Contains Raft-based lock instances
 */
public class RaftLockService extends AbstractBlockingService<LockInvocationKey, RaftLock, RaftLockRegistry> {

    /**
     * Representation of a failed lock request
     */
    public static final long INVALID_FENCE = 0L;

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

    @Override
    public FencedLock createRaftObjectProxy(String name) {
        try {
            RaftGroupId groupId = raftService.createRaftGroupForProxy(name);
            RaftInvocationManager invocationManager = raftService.getInvocationManager();
            ProxySessionManagerService sessionManager = nodeEngine.getService(ProxySessionManagerService.SERVICE_NAME);
            return new RaftFencedLockProxy(invocationManager, sessionManager, groupId, getObjectNameForProxy(name));
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public RaftLockOwnershipState acquire(RaftGroupId groupId, String name, LockEndpoint endpoint, long commitIndex,
                                          UUID invocationUid) {
        heartbeatSession(groupId, endpoint.sessionId());
        AcquireResult result = getOrInitRegistry(groupId).acquire(name, endpoint, commitIndex, invocationUid);

        if (logger.isFineEnabled()) {
            logger.fine("Lock[" + name + "] in " + groupId + " acquired: " + result.ownership.isLocked() + " by <" + endpoint
                    + ", " + invocationUid + ">");
        }

        if (!result.ownership.isLocked()) {
            notifyCancelledWaitKeys(groupId, name, result.cancelled);
        }

        return result.ownership;
    }

    public RaftLockOwnershipState tryAcquire(RaftGroupId groupId, String name, LockEndpoint endpoint, long commitIndex,
                                             UUID invocationUid, long timeoutMs) {
        heartbeatSession(groupId, endpoint.sessionId());
        AcquireResult result = getOrInitRegistry(groupId).tryAcquire(name, endpoint, commitIndex, invocationUid, timeoutMs);

        if (logger.isFineEnabled()) {
            logger.fine("Lock[" + name + "] in " + groupId + " acquired: " + result.ownership.isLocked() + " by <" + endpoint
                    + ", " + invocationUid + ">");
        }

        if (!result.ownership.isLocked()) {
            scheduleTimeout(groupId, new LockInvocationKey(name, endpoint, commitIndex, invocationUid), timeoutMs);
            notifyCancelledWaitKeys(groupId, name, result.cancelled);
        }

        return result.ownership;
    }

    public void release(RaftGroupId groupId, String name, LockEndpoint endpoint, UUID invocationUid, int lockCount) {
        heartbeatSession(groupId, endpoint.sessionId());
        RaftLockRegistry registry = getLockRegistryOrFail(groupId, name);
        ReleaseResult result = registry.release(name, endpoint, invocationUid, lockCount);

        if (result.success) {
            if (logger.isFineEnabled()) {
                logger.fine("Lock[" + name + "] in " + groupId + " is released by <" + endpoint + ", " + invocationUid + ">");
            }

            notifySucceededWaitKeys(groupId, name, result.ownership, result.notifications);
        } else {
            notifyCancelledWaitKeys(groupId, name, result.notifications);
            throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
        }
    }

    public void forceRelease(RaftGroupId groupId, String name, long expectedFence, UUID invocationUid) {
        RaftLockRegistry registry = getLockRegistryOrFail(groupId, name);
        ReleaseResult result = registry.forceRelease(name, expectedFence, invocationUid);

        if (result.success) {
            if (logger.isFineEnabled()) {
                logger.fine("Lock[" + name + "] in " + groupId + " is force-released by " + invocationUid + " for fence: "
                        + expectedFence);
            }

            notifySucceededWaitKeys(groupId, name, result.ownership, result.notifications);
        } else {
            notifyCancelledWaitKeys(groupId, name, result.notifications);
            throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
        }
    }

    private void notifySucceededWaitKeys(RaftGroupId groupId, String name, RaftLockOwnershipState ownership,
                                         Collection<LockInvocationKey> waitKeys) {
        if (waitKeys.isEmpty()) {
            return;
        }

        assert ownership.isLocked();

        LockInvocationKey waitKey = waitKeys.iterator().next();
        if (logger.isFineEnabled()) {
            logger.fine("Lock[" + name + "] in " + groupId + " is acquired by <" + waitKey.endpoint() + ", "
                    + waitKey.invocationUid() + ">");
        }

        notifyWaitKeys(groupId, waitKeys, ownership);
    }

    private void notifyCancelledWaitKeys(RaftGroupId groupId, String name, Collection<LockInvocationKey> waitKeys) {
        if (waitKeys.isEmpty()) {
            return;
        }

        logger.warning("Wait keys: " + waitKeys +  " for Lock[" + name + "] in " + groupId + " are notifications.");

        notifyWaitKeys(groupId, waitKeys, new WaitKeyCancelledException());
    }

    public RaftLockOwnershipState getLockOwnershipState(RaftGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);

        RaftLockRegistry registry = getRegistryOrNull(groupId);
        return registry != null ? registry.getLockOwnershipState(name) : RaftLockOwnershipState.NOT_LOCKED;
    }

    private RaftLockRegistry getLockRegistryOrFail(RaftGroupId groupId, String name) {
        checkNotNull(groupId);
        RaftLockRegistry registry = getRegistryOrNull(groupId);
        if (registry == null) {
            throw new IllegalMonitorStateException("Lock registry of " + groupId + " not found for Lock[" + name + "]");
        }

        return registry;
    }

    @Override
    protected RaftLockRegistry createNewRegistry(RaftGroupId groupId) {
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
}
