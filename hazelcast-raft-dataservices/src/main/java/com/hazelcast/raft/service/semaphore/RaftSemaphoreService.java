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

package com.hazelcast.raft.service.semaphore;

import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.config.raft.RaftSemaphoreConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.blocking.AbstractBlockingService;
import com.hazelcast.raft.service.exception.WaitKeyCancelledException;
import com.hazelcast.raft.service.semaphore.RaftSemaphore.AcquireResult;
import com.hazelcast.raft.service.semaphore.RaftSemaphore.ReleaseResult;
import com.hazelcast.raft.service.semaphore.proxy.RaftSessionAwareSemaphoreProxy;
import com.hazelcast.raft.service.semaphore.proxy.RaftSessionlessSemaphoreProxy;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
public class RaftSemaphoreService extends AbstractBlockingService<SemaphoreInvocationKey, RaftSemaphore, SemaphoreRegistry> {

    public static final String SERVICE_NAME = "hz:raft:semaphoreService";

    public RaftSemaphoreService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    public ISemaphore createRaftObjectProxy(String name) {
        try {
            RaftGroupId groupId = createRaftGroup(name).get();
            RaftSemaphoreConfig config = getConfig(name);
            SessionManagerService sessionManager = nodeEngine.getService(SessionManagerService.SERVICE_NAME);
            RaftInvocationManager invocationManager = raftService.getInvocationManager();
            return config != null && config.isStrictModeEnabled()
                    ? new RaftSessionAwareSemaphoreProxy(invocationManager, sessionManager, groupId, name)
                    : new RaftSessionlessSemaphoreProxy(invocationManager, groupId, name);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public ICompletableFuture<RaftGroupId> createRaftGroup(String name) {
        String groupRef = getRaftGroupRef(name);
        return raftService.getInvocationManager().createRaftGroup(groupRef);
    }

    private String getRaftGroupRef(String name) {
        RaftSemaphoreConfig config = getConfig(name);
        return config != null ? config.getRaftGroupRef() : RaftGroupConfig.DEFAULT_GROUP;
    }

    private RaftSemaphoreConfig getConfig(String name) {
        return nodeEngine.getConfig().findRaftSemaphoreConfig(name);
    }

    public boolean initSemaphore(RaftGroupId groupId, String name, int permits) {
        try {
            Collection<SemaphoreInvocationKey> acquired = getOrInitRegistry(groupId).init(name, permits);
            notifyWaitKeys(groupId, acquired, true);

            if (acquired.size() > 0 && logger.isFineEnabled()) {
                logger.fine("Semaphore[" + name + "] in " + groupId + " acquired permits: " + acquired
                        + " after initialized with " + permits + " permits");
            }

            return true;
        } catch (IllegalStateException ignored) {
            return false;
        }
    }

    public int availablePermits(RaftGroupId groupId, String name) {
        SemaphoreRegistry registry = getRegistryOrNull(groupId);
        return registry != null ? registry.availablePermits(name) : 0;
    }

    public boolean acquirePermits(RaftGroupId groupId, long commitIndex, String name, long sessionId, long threadId,
                                  UUID invocationUid, int permits, long timeoutMs) {
        heartbeatSession(groupId, sessionId);
        SemaphoreInvocationKey key = new SemaphoreInvocationKey(name, commitIndex, sessionId, threadId, invocationUid, permits);
        AcquireResult result = getOrInitRegistry(groupId).acquire(name, key, timeoutMs);

//        if (logger.isFineEnabled()) {
        if (result.acquired > 0) {
            logger.warning("Semaphore[" + name + "] in " + groupId + " acquired permits: " + result.acquired
                    + " by <" + sessionId + ", " + threadId + ", " + invocationUid + ">");
        } else {
            logger.warning("Semaphore[" + name + "] in " + groupId + " NOT acquired permits by <" + sessionId + ", " + threadId
                    + ", " + invocationUid + ">");
        }
//        }

        notifyCancelledWaitKeys(groupId, name, result.cancelled);

        if (result.acquired == 0) {
            scheduleTimeout(groupId, key, timeoutMs);
        }

        return (result.acquired != 0);
    }

    public void releasePermits(RaftGroupId groupId, String name, long sessionId, long threadId, UUID invocationUid, int permits) {
        heartbeatSession(groupId, sessionId);
        ReleaseResult result = getOrInitRegistry(groupId).release(name, sessionId, threadId, invocationUid, permits);
        notifyCancelledWaitKeys(groupId, name, result.cancelled);
        notifyWaitKeys(groupId, result.acquired, true);

        if (!result.success) {
            logger.warning("Semaphore[" + name + "] in " + groupId + " NOT released permits: " + permits
                    + " by <" + sessionId + ", " + threadId + ", " + invocationUid + ">");
            throw new IllegalArgumentException();
        } else {
            logger.warning("Semaphore[" + name + "] in " + groupId + " released permits: " + permits
                    + " by <" + sessionId + ", " + threadId + ", " + invocationUid + ">");
            logger.warning("Semaphore[" + name + "] in " + groupId + " acquired permits: " + result.acquired);
        }
    }

    public int drainPermits(RaftGroupId groupId, String name, long sessionId, long threadId, UUID invocationUid) {
        AcquireResult result = getOrInitRegistry(groupId).drainPermits(name, sessionId, threadId, invocationUid);
        notifyCancelledWaitKeys(groupId, name, result.cancelled);

        return result.acquired;
    }

    public boolean changePermits(RaftGroupId groupId, String name, int permits) {
        Tuple2<Boolean, Collection<SemaphoreInvocationKey>> t = getOrInitRegistry(groupId).changePermits(name, permits);
        notifyWaitKeys(groupId, t.element2, true);

        return t.element1;
    }

    private void notifyCancelledWaitKeys(RaftGroupId groupId, String name, Collection<SemaphoreInvocationKey> waitKeys) {
        if (waitKeys.isEmpty()) {
            return;
        }

        logger.warning("Wait keys: " + waitKeys +  " for Semaphore[" + name + "] in " + groupId + " are cancelled.");

        notifyWaitKeys(groupId, waitKeys, new WaitKeyCancelledException());
    }

    @Override
    protected SemaphoreRegistry createNewRegistry(RaftGroupId groupId) {
        return new SemaphoreRegistry(groupId);
    }

    @Override
    protected Object invalidatedWaitKeyResponse() {
        return false;
    }

    @Override
    protected String serviceName() {
        return SERVICE_NAME;
    }

}
