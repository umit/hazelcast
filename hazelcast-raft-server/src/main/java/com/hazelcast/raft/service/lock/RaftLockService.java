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
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.session.SessionAccessor;
import com.hazelcast.raft.impl.session.SessionAwareService;
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.raft.impl.util.PostponedResponse;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.lock.operation.InvalidateWaitOp;
import com.hazelcast.raft.service.lock.proxy.RaftLockProxy;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * TODO: Javadoc Pending...
 */
public class RaftLockService implements ManagedService, SnapshotAwareService<LockRegistrySnapshot>, SessionAwareService {

    public static final String SERVICE_NAME = "hz:raft:lockService";

    private final ConcurrentMap<RaftGroupId, LockRegistry> registries = new ConcurrentHashMap<RaftGroupId, LockRegistry>();
    private final NodeEngine nodeEngine;
    private final ILogger logger;
    private volatile RaftService raftService;
    private volatile SessionAccessor sessionAccessor;

    public RaftLockService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {

    }

    @Override
    public LockRegistrySnapshot takeSnapshot(RaftGroupId groupId, long commitIndex) {
        LockRegistry registry = registries.get(groupId);
        return registry != null ? registry.toSnapshot() : null;
    }

    @Override
    public void restoreSnapshot(RaftGroupId groupId, long commitIndex, LockRegistrySnapshot snapshot) {
        if (snapshot != null) {
            LockRegistry registry = getLockRegistry(groupId);
            Map<LockInvocationKey, Long> timeouts = registry.restore(snapshot);
            ExecutionService executionService = nodeEngine.getExecutionService();
            for (Entry<LockInvocationKey, Long> e : timeouts.entrySet()) {
                LockInvocationKey key = e.getKey();
                long waitTimeNanos = e.getValue();
                Runnable task = new InvalidateWaitTask(groupId, key);
                executionService.schedule(task, waitTimeNanos, TimeUnit.NANOSECONDS);
            }
        }
    }

    @Override
    public void setSessionAccessor(SessionAccessor accessor) {
        this.sessionAccessor = accessor;
    }

    @Override
    public void onSessionInvalidated(RaftGroupId groupId, long sessionId) {
        LockRegistry registry = registries.get(groupId);
        if (registry == null) {
            logger.fine("No lock registry for " + groupId + " and session: " + sessionId);
            return;
        }

        Tuple2<Collection<Long>, Collection<Long>> t = registry.invalidateSession(sessionId);
        if (t != null) {
            Collection<Long> invalidations = t.element1;
            Collection<Long> acquires = t.element2;
            completeFutures(groupId, invalidations, new SessionExpiredException(sessionId));
            completeFutures(groupId, acquires, true);
        }
    }

    public ILock createNew(String name) throws ExecutionException, InterruptedException {
        RaftGroupId groupId = createNewAsync(name).get();
        SessionManagerService sessionManager = nodeEngine.getService(SessionManagerService.SERVICE_NAME);
        return new RaftLockProxy(name, groupId, sessionManager, raftService.getInvocationManager());
    }

    public ICompletableFuture<RaftGroupId> createNewAsync(String name) {
        RaftLockConfig config = getConfig(name);
        checkNotNull(config);

        RaftInvocationManager invocationManager = raftService.getInvocationManager();
        RaftGroupConfig groupConfig = config.getRaftGroupConfig();
        if (groupConfig != null) {
            return invocationManager.createRaftGroup(groupConfig);
        } else {
            return invocationManager.createRaftGroup(config.getRaftGroupRef());
        }
    }

    private RaftLockConfig getConfig(String name) {
        return nodeEngine.getConfig().findRaftLockConfig(name);
    }

    public RaftLockProxy newProxy(String name, RaftGroupId groupId, String sessionId) {
        throw new UnsupportedOperationException();
    }

    private LockRegistry getLockRegistry(RaftGroupId groupId) {
        checkNotNull(groupId);
        LockRegistry registry = registries.get(groupId);
        if (registry == null) {
            registry = new LockRegistry(groupId);
            registries.put(groupId, registry);
        }
        return registry;
    }

    public boolean acquire(RaftGroupId groupId, String name, LockEndpoint endpoint, long commitIndex, UUID invocationUid) {
        if (sessionAccessor.isValid(groupId, endpoint.sessionId)) {
            sessionAccessor.heartbeat(groupId, endpoint.sessionId);
            boolean acquired = getLockRegistry(groupId).acquire(name, endpoint, commitIndex, invocationUid);
            if (logger.isFineEnabled()) {
                logger.fine("Lock: " + name + " in " + groupId + " acquired: " + acquired + " by <" + endpoint + ", "
                        + invocationUid + ">");
            }
            return acquired;
        }
        throw new SessionExpiredException(endpoint.sessionId);
    }

    public Object tryAcquire(RaftGroupId groupId, String name, LockEndpoint endpoint, long commitIndex, UUID invocationUid,
                          long waitTimeNanos) {
        if (sessionAccessor.isValid(groupId, endpoint.sessionId)) {
            sessionAccessor.heartbeat(groupId, endpoint.sessionId);
            boolean acquired = getLockRegistry(groupId).tryAcquire(name, endpoint, commitIndex, invocationUid, waitTimeNanos);
            if (logger.isFineEnabled()) {
                logger.fine("Lock: " + name + " in " + groupId + " acquired: " + acquired + " by <" + endpoint + ", "
                        + invocationUid + ">");
            }
            if (waitTimeNanos > 0 && !acquired) {
                LockInvocationKey key = new LockInvocationKey(name, endpoint, commitIndex, invocationUid);
                Runnable task = new InvalidateWaitTask(groupId, key);
                ExecutionService executionService = nodeEngine.getExecutionService();
                executionService.schedule(task, waitTimeNanos, TimeUnit.NANOSECONDS);
                return PostponedResponse.INSTANCE;
            }

            return acquired;
        }
        throw new SessionExpiredException(endpoint.sessionId);
    }

    public void release(RaftGroupId groupId, String name, LockEndpoint endpoint, UUID invocationUid) {
        if (sessionAccessor.isValid(groupId, endpoint.sessionId)) {
            sessionAccessor.heartbeat(groupId, endpoint.sessionId);

            LockRegistry registry = registries.get(groupId);
            if (registry == null) {
                logger.severe("No LockRegistry is found for " + groupId + " to release lock: " + name + " by <" + endpoint
                        + ", " + invocationUid + ">");
                return;
            }

            Collection<LockInvocationKey> waitEntries = registry.release(name, endpoint, invocationUid);

            if (logger.isFineEnabled()) {
                logger.fine("Lock: " + name + " in " + groupId + " is released by " + endpoint);
                if (waitEntries.size() > 0) {
                    LockInvocationKey newOwner = waitEntries.iterator().next();
                    logger.fine("Lock: " + name + " in " + groupId + " is acquired by " + newOwner.endpoint);
                }
            }

            List<Long> indices = new ArrayList<Long>(waitEntries.size());
            for (LockInvocationKey waitEntry : waitEntries) {
                indices.add(waitEntry.commitIndex);
            }
            completeFutures(groupId, indices, true);
        } else {
            throw new IllegalMonitorStateException();
        }
    }

    public void invalidateWait(RaftGroupId groupId, LockInvocationKey key) {
        // no need to validate the session. if the session is invalid, the corresponding wait entry is gone already
        LockRegistry registry = registries.get(groupId);
        if (registry == null) {
            logger.severe("No LockRegistry is found for " + groupId + " to invalidate wait of lock: " + key);
            return;
        }

        if (registry.invalidateWait(key)) {
            if (logger.isFineEnabled()) {
                logger.fine("Wait entry of " + key + " is invalidated.");
            }

            completeFutures(groupId, Collections.singleton(key.commitIndex), false);
        }
    }

    private void completeFutures(RaftGroupId groupId, Collection<Long> indices, Object result) {
        if (!indices.isEmpty()) {
            RaftNodeImpl raftNode = (RaftNodeImpl) raftService.getRaftNode(groupId);
            for (Long index : indices) {
                raftNode.completeFuture(index, result);
            }
        }
    }

    public Tuple2<LockEndpoint, Integer> lockCount(RaftGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);

        LockRegistry registry = registries.get(groupId);
        if (registry == null) {
            return Tuple2.of(null, 0);
        }

        return registry.lockCount(name);
    }

    private class InvalidateWaitTask implements Runnable {
        final RaftGroupId groupId;
        final LockInvocationKey key;

        InvalidateWaitTask(RaftGroupId groupId, LockInvocationKey key) {
            this.groupId = groupId;
            this.key = key;
        }

        @Override
        public void run() {
            RaftNode raftNode = raftService.getRaftNode(groupId);
            if (raftNode != null) {
                // we should handle CannotReplicateException, LeaderDemotedException, and StaleAppendRequestException
                Future f = raftNode.replicate(new InvalidateWaitOp(key));
                try {
                    f.get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
