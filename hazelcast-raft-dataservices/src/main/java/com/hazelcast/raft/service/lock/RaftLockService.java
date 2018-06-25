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

import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.config.raft.RaftLockConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.ILock;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftGroupLifecycleAwareService;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.session.SessionAwareService;
import com.hazelcast.raft.service.blocking.AbstractBlockingService;
import com.hazelcast.raft.service.lock.proxy.RaftLockProxy;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.raft.service.spi.RaftRemoteService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * TODO: Javadoc Pending...
 */
public class RaftLockService extends AbstractBlockingService<LockInvocationKey, RaftLock, LockRegistry>
        implements ManagedService, SnapshotAwareService<LockRegistrySnapshot>,
        RaftRemoteService, RaftGroupLifecycleAwareService, SessionAwareService {

    public static final long INVALID_FENCE = 0L;
    public static final String SERVICE_NAME = "hz:raft:lockService";

    public RaftLockService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    protected void initImpl() {
    }

    @Override
    public void reset() {
    }

    @Override
    protected void shutdownImpl(boolean terminate) {
    }

    @Override
    public LockRegistrySnapshot takeSnapshot(RaftGroupId groupId, long commitIndex) {
        LockRegistry registry = getResourceRegistryOrNull(groupId);
        return registry != null ? registry.toSnapshot() : null;
    }

    @Override
    public void restoreSnapshot(RaftGroupId groupId, long commitIndex, LockRegistrySnapshot snapshot) {
        if (snapshot != null) {
            LockRegistry registry = getOrInitResourceRegistry(groupId);
            Map<LockInvocationKey, Long> timeouts = registry.restore(snapshot);
            for (Entry<LockInvocationKey, Long> e : timeouts.entrySet()) {
                LockInvocationKey key = e.getKey();
                long waitTimeMs = e.getValue();
                onWait(groupId, key, waitTimeMs);
            }
        }
    }

    @Override
    public ILock createRaftObjectProxy(String name) {
        try {
            RaftGroupId groupId = createRaftGroup(name).get();
            SessionManagerService sessionManager = nodeEngine.getService(SessionManagerService.SERVICE_NAME);
            return new RaftLockProxy(name, groupId, sessionManager, raftService.getInvocationManager());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public boolean destroyRaftObject(RaftGroupId groupId, String name) {
        LockRegistry registry = getOrInitResourceRegistry(groupId);
        Collection<Long> indices = registry.destroyResource(name);
        if (indices == null) {
            return false;
        }
        completeFutures(groupId, indices, new DistributedObjectDestroyedException("Lock[" + name + "] is destroyed"));
        return true;
    }

    public ICompletableFuture<RaftGroupId> createRaftGroup(String name) {
        String raftGroupRef = getRaftGroupRef(name);

        RaftInvocationManager invocationManager = raftService.getInvocationManager();
        return invocationManager.createRaftGroup(raftGroupRef);
    }

    private String getRaftGroupRef(String name) {
        RaftLockConfig config = getConfig(name);
        return config != null ? config.getRaftGroupRef() : RaftGroupConfig.DEFAULT_GROUP;
    }

    private RaftLockConfig getConfig(String name) {
        return nodeEngine.getConfig().findRaftLockConfig(name);
    }

    @Override
    protected LockRegistry createNewRegistry(RaftGroupId groupId) {
        return new LockRegistry(groupId);
    }

    public boolean acquire(RaftGroupId groupId, String name, LockEndpoint endpoint, long commitIndex, UUID invocationUid) {
        heartbeatSession(groupId, endpoint.sessionId());
        boolean acquired = getOrInitResourceRegistry(groupId).acquire(name, endpoint, commitIndex, invocationUid);
        if (logger.isFineEnabled()) {
            logger.fine("Lock[" + name + "] in " + groupId + " acquired: " + acquired + " by <" + endpoint + ", "
                    + invocationUid + ">");
        }
        return acquired;
    }

    public boolean tryAcquire(RaftGroupId groupId, String name, LockEndpoint endpoint, long commitIndex, UUID invocationUid,
                          long timeoutMs) {
        heartbeatSession(groupId, endpoint.sessionId());
        boolean acquired = getOrInitResourceRegistry(groupId).tryAcquire(name, endpoint, commitIndex, invocationUid, timeoutMs);
        if (logger.isFineEnabled()) {
            logger.fine("Lock[" + name + "] in " + groupId + " acquired: " + acquired + " by <" + endpoint + ", "
                    + invocationUid + ">");
        }

        if (!acquired) {
            LockInvocationKey key = new LockInvocationKey(name, endpoint, commitIndex, invocationUid);
            onWait(groupId, key, timeoutMs);
        }
        return acquired;
    }

    public void release(RaftGroupId groupId, String name, LockEndpoint endpoint, UUID invocationUid) {
        heartbeatSession(groupId, endpoint.sessionId());
        LockRegistry registry = getLockRegistryOrFail(groupId, name);
        Collection<LockInvocationKey> waitEntries = registry.release(name, endpoint, invocationUid);

        if (logger.isFineEnabled()) {
            logger.fine("Lock[" + name + "] in " + groupId + " is released by <" + endpoint + ", " + invocationUid + ">");
        }

        notifyLockAcquiredWaitEntries(groupId, name, waitEntries);
    }

    public void forceRelease(RaftGroupId groupId, String name, long expectedFence, UUID invocationUid) {
        LockRegistry registry = getLockRegistryOrFail(groupId, name);
        Collection<LockInvocationKey> waitEntries = registry.forceRelease(name, expectedFence, invocationUid);

        if (logger.isFineEnabled()) {
            logger.fine("Lock[" + name + "] in " + groupId + " is force-released by " + invocationUid + " for fence: "
                    + expectedFence);
        }

        notifyLockAcquiredWaitEntries(groupId, name, waitEntries);
    }

    private void notifyLockAcquiredWaitEntries(RaftGroupId groupId, String name, Collection<LockInvocationKey> waitEntries) {
        if (waitEntries.isEmpty()) {
            return;
        }

        LockInvocationKey waitKey = waitEntries.iterator().next();
        if (logger.isFineEnabled()) {
            logger.fine("Lock[" + name + "] in " + groupId + " is acquired by <" + waitKey.endpoint() + ", "
                    + waitKey.invocationUid() + ">");
        }

        notifyWaitEntries(groupId, waitEntries, waitKey.commitIndex());
    }

    @Override
    protected Object invalidatedResult() {
        return INVALID_FENCE;
    }

    @Override
    protected String serviceName() {
        return SERVICE_NAME;
    }

    public int getLockCount(RaftGroupId groupId, String name, LockEndpoint endpoint) {
        checkNotNull(groupId);
        checkNotNull(name);

        LockRegistry registry = getResourceRegistryOrNull(groupId);
        return registry != null ? registry.getLockCount(name, endpoint) : 0;
    }

    public long getLockFence(RaftGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);

        LockRegistry registry = getLockRegistryOrFail(groupId, name);
        return registry.getLockFence(name);
    }

    private LockRegistry getLockRegistryOrFail(RaftGroupId groupId, String name) {
        checkNotNull(groupId);
        LockRegistry registry = getResourceRegistryOrNull(groupId);
        if (registry == null) {
            throw new IllegalMonitorStateException("Resource registry of " + groupId + " not found for Resource[" + name + "]");
        }
        return registry;
    }
}
