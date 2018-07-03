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

package com.hazelcast.raft.service.blocking;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftGroupLifecycleAwareService;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.session.SessionAccessor;
import com.hazelcast.raft.impl.session.SessionAwareService;
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.raft.service.session.AbstractSessionManager;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * TODO: Javadoc Pending...
 */
public abstract class AbstractBlockingService<W extends WaitKey, R extends BlockingResource<W>, RR extends ResourceRegistry<W, R>>
        implements ManagedService, RaftGroupLifecycleAwareService, SessionAwareService {

    public static final long WAIT_TIMEOUT_TASK_UPPER_BOUND_MILLIS = 1500;
    private static final long WAIT_TIMEOUT_TASK_PERIOD_MILLIS = 500;

    private final ConcurrentMap<RaftGroupId, RR> registries = new ConcurrentHashMap<RaftGroupId, RR>();
    protected final NodeEngine nodeEngine;
    protected final ILogger logger;
    protected volatile RaftService raftService;
    private volatile SessionAccessor sessionAccessor;

    protected AbstractBlockingService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public final void init(NodeEngine nodeEngine, Properties properties) {
        this.raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(new InvalidateExpiredWaitEntriesPeriodicTask(),
                WAIT_TIMEOUT_TASK_PERIOD_MILLIS, WAIT_TIMEOUT_TASK_PERIOD_MILLIS, MILLISECONDS);

        initImpl();
    }

    protected void initImpl() {
    }

    @Override
    public void reset() {
    }

    @Override
    public final void shutdown(boolean terminate) {
        registries.clear();
        shutdownImpl(terminate);
    }

    protected void shutdownImpl(boolean terminate) {
    }

    @Override
    public void setSessionAccessor(SessionAccessor accessor) {
        this.sessionAccessor = accessor;
    }

    @Override
    public void onSessionInvalidated(RaftGroupId groupId, long sessionId) {
        ResourceRegistry<W, R> registry = registries.get(groupId);
        if (registry == null) {
            logger.warning("Resource registry of " + groupId + " not found to handle invalidated Session[" + sessionId + "]");
            return;
        }

        Map<Long, Object> result = registry.invalidateSession(sessionId);
        RaftNodeImpl raftNode = (RaftNodeImpl) raftService.getRaftNode(groupId);
        for (Entry<Long, Object> entry : result.entrySet()) {
            raftNode.completeFuture(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void onGroupDestroy(final RaftGroupId groupId) {
        ResourceRegistry<W, R> registry = registries.get(groupId);
        if (registry != null) {
            Collection<Long> indices = registry.destroy();
            completeFutures(groupId, indices, new DistributedObjectDestroyedException(groupId + " is destroyed"));
        }
    }

    protected RR getOrInitResourceRegistry(RaftGroupId groupId) {
        checkNotNull(groupId);
        RR registry = registries.get(groupId);
        if (registry == null) {
            registry = createNewRegistry(groupId);
            registries.put(groupId, registry);
        }
        return registry;
    }

    protected abstract RR createNewRegistry(RaftGroupId groupId);

    // queried locally in tests
    public RR getResourceRegistryOrNull(RaftGroupId groupId) {
        return registries.get(groupId);
    }

    protected void scheduleWaitTimeout(RaftGroupId groupId, W waitEntry, long timeoutMs) {
        if (timeoutMs > 0 && timeoutMs <= WAIT_TIMEOUT_TASK_UPPER_BOUND_MILLIS) {
            Runnable task = new InvalidateExpiredWaitEntriesTask(groupId, waitEntry);
            ExecutionService executionService = nodeEngine.getExecutionService();
            executionService.schedule(task, timeoutMs, MILLISECONDS);
        }
    }

    protected void heartbeatSession(RaftGroupId groupId, long sessionId) {
        if (sessionId == AbstractSessionManager.NO_SESSION_ID) {
            throw new IllegalStateException("No valid session id provided for " + groupId);
        }
        if (sessionAccessor.isValid(groupId, sessionId)) {
            sessionAccessor.heartbeat(groupId, sessionId);
            return;
        }

        throw new SessionExpiredException();
    }

    protected void notifyWaitEntries(RaftGroupId groupId, Collection<W> keys, Object result) {
        if (keys.isEmpty()) {
            return;
        }

        List<Long> indices = new ArrayList<Long>(keys.size());
        for (W entry : keys) {
            indices.add(entry.commitIndex());
        }

        completeFutures(groupId, indices, result);
    }

    protected void invalidateWaitEntries(RaftGroupId groupId, Collection<W> keys) {
        // no need to validate the session. if the session is invalid, the corresponding wait entry is gone already
        ResourceRegistry<W, R> registry = registries.get(groupId);
        if (registry == null) {
            logger.severe("Lock registry of " + groupId + " not found to invalidate wait entries: " + keys);
            return;
        }

        List<Long> invalidated = new ArrayList<Long>();
        for (W key : keys) {
            if (registry.invalidateWaitEntry(key)) {
                invalidated.add(key.commitIndex());
                if (logger.isFineEnabled()) {
                    logger.fine("Wait key of " + key + " is invalidated.");
                }
            }
        }

        completeFutures(groupId, invalidated, invalidatedResult());
    }

    protected abstract Object invalidatedResult();

    protected void completeFutures(RaftGroupId groupId, Collection<Long> indices, Object result) {
        if (!indices.isEmpty()) {
            RaftNodeImpl raftNode = (RaftNodeImpl) raftService.getRaftNode(groupId);
            for (Long index : indices) {
                raftNode.completeFuture(index, result);
            }
        }
    }

    // queried locally
    private Map<RaftGroupId, Collection<W>> getExpiredWaitEntries() {
        Map<RaftGroupId, Collection<W>> timeouts = new HashMap<RaftGroupId, Collection<W>>();
        long now = Clock.currentTimeMillis();
        for (ResourceRegistry<W, R> registry : registries.values()) {
            Collection<W> t = registry.getExpiredWaitEntries(now);
            if (t.size() > 0) {
                timeouts.put(registry.groupId(), t);
            }
        }
        return timeouts;
    }

    private class InvalidateExpiredWaitEntriesTask implements Runnable {
        final RaftGroupId groupId;
        final Collection<W> keys;

        InvalidateExpiredWaitEntriesTask(RaftGroupId groupId, W key) {
            this.groupId = groupId;
            this.keys = Collections.singleton(key);
        }

        InvalidateExpiredWaitEntriesTask(RaftGroupId groupId, Collection<W> keys) {
            this.groupId = groupId;
            this.keys = keys;
        }

        @Override
        public void run() {
            try {
                RaftNode raftNode = raftService.getRaftNode(groupId);
                if (raftNode != null) {
                    Future f = raftNode.replicate(new InvalidateWaitEntriesOp<W>(serviceName(), keys));
                    f.get();
                }
            } catch (Exception e) {
                if (logger.isFineEnabled()) {
                    logger.fine("Could not invalidate wait entries: " + keys + " in " + groupId, e);
                }
            }
        }
    }

    protected abstract String serviceName();

    private class InvalidateExpiredWaitEntriesPeriodicTask implements Runnable {
        @Override
        public void run() {
            for (Entry<RaftGroupId, Collection<W>> e : getExpiredWaitEntries().entrySet()) {
                new InvalidateExpiredWaitEntriesTask(e.getKey(), e.getValue()).run();
            }
        }
    }
}
