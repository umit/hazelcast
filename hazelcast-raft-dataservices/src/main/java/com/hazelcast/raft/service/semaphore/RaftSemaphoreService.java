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

import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.config.raft.RaftSemaphoreConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.service.blocking.AbstractBlockingService;
import com.hazelcast.raft.service.semaphore.proxy.RaftSessionAwareSemaphoreProxy;
import com.hazelcast.raft.service.semaphore.proxy.RaftSessionlessSemaphoreProxy;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.raft.service.spi.RaftRemoteService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 */
public class RaftSemaphoreService extends AbstractBlockingService<SemaphoreInvocationKey, RaftSemaphore, SemaphoreRegistry>
        implements SnapshotAwareService<SemaphoreRegistrySnapshot>, RaftRemoteService {

    public static final String SERVICE_NAME = "hz:raft:semaphoreService";

    public RaftSemaphoreService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    public SemaphoreRegistrySnapshot takeSnapshot(RaftGroupId groupId, long commitIndex) {
        return null;
    }

    @Override
    public void restoreSnapshot(RaftGroupId groupId, long commitIndex, SemaphoreRegistrySnapshot snapshot) {
    }

    @Override
    protected SemaphoreRegistry createNewRegistry(RaftGroupId groupId) {
        return new SemaphoreRegistry(groupId);
    }

    @Override
    protected Object invalidatedResult() {
        return null;
    }

    @Override
    protected String serviceName() {
        return SERVICE_NAME;
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
                    : new RaftSessionlessSemaphoreProxy(invocationManager, sessionManager, groupId, name);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private ICompletableFuture<RaftGroupId> createRaftGroup(String name) {
        String raftGroupRef = getRaftGroupRef(name);

        RaftInvocationManager invocationManager = raftService.getInvocationManager();
        return invocationManager.createRaftGroup(raftGroupRef);
    }

    private String getRaftGroupRef(String name) {
        RaftSemaphoreConfig config = getConfig(name);
        return config != null ? config.getRaftGroupRef() : RaftGroupConfig.DEFAULT_GROUP;
    }

    private RaftSemaphoreConfig getConfig(String name) {
        return nodeEngine.getConfig().findRaftSemaphoreConfig(name);
    }

    @Override
    public boolean destroyRaftObject(RaftGroupId groupId, String name) {
        SemaphoreRegistry registry = getOrInitResourceRegistry(groupId);
        Collection<Long> indices = registry.destroyResource(name);
        if (indices == null) {
            return false;
        }
        completeFutures(groupId, indices, new DistributedObjectDestroyedException("Semaphore[" + name + "] is destroyed"));
        return true;
    }

    public boolean initSemaphore(RaftGroupId groupId, String name, int permits) {
        SemaphoreRegistry registry = getOrInitResourceRegistry(groupId);
        return registry.init(name, permits);
    }

    public int availablePermits(RaftGroupId groupId, String name) {
        SemaphoreRegistry registry = getResourceRegistryOrNull(groupId);
        if (registry == null) {
            return 0;
        }
        return registry.availablePermits(name);
    }

    public boolean acquirePermits(RaftGroupId groupId, long commitIndex, String name, long sessionId, int permits, long timeoutMs) {
        heartbeatSession(groupId, sessionId);
        SemaphoreRegistry registry = getOrInitResourceRegistry(groupId);
        return registry.acquire(commitIndex, name, sessionId, permits, timeoutMs);
    }

    public void releasePermits(RaftGroupId groupId, String name, long sessionId, int permits) {
        heartbeatSession(groupId, sessionId);
        SemaphoreRegistry registry = getOrInitResourceRegistry(groupId);
        Collection<SemaphoreInvocationKey> keys = registry.release(name, sessionId, permits);
        notifyWaitKeys(groupId, keys, true);
    }

    public int drainPermits(RaftGroupId groupId, String name, long sessionId) {
        SemaphoreRegistry registry = getResourceRegistryOrNull(groupId);
        if (registry == null) {
            return 0;
        }
        return registry.drainPermits(name, sessionId);
    }

    public boolean changePermits(RaftGroupId groupId, String name, int permits) {
        SemaphoreRegistry registry = getOrInitResourceRegistry(groupId);
        return registry.changePermits(name, permits);
    }
}
