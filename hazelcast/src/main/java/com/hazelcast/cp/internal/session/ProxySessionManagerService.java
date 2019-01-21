/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.session;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.GenerateThreadIdOp;
import com.hazelcast.cp.internal.session.operation.CloseSessionOp;
import com.hazelcast.cp.internal.session.operation.CreateSessionOp;
import com.hazelcast.cp.internal.session.operation.HeartbeatSessionOp;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.GracefulShutdownAwareService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.session.CPSession.CPSessionOwnerType.SERVER;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Server-side implementation of Raft proxy session manager
 */
public class ProxySessionManagerService extends AbstractProxySessionManager implements GracefulShutdownAwareService {

    /**
     * Name of the service
     */
    public static final String SERVICE_NAME = "hz:raft:proxySessionManagerService";

    private static final long SHUTDOWN_TASK_PERIOD_IN_MILLIS = SECONDS.toMillis(1);


    private final NodeEngine nodeEngine;

    public ProxySessionManagerService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    protected long generateThreadId(CPGroupId groupId) {
        InternalCompletableFuture<Long> f = getInvocationManager()
                .invoke(groupId, new GenerateThreadIdOp(System.currentTimeMillis()));
        return f.join();
    }

    @Override
    protected SessionResponse requestNewSession(CPGroupId groupId) {
        String instanceName = nodeEngine.getConfig().getInstanceName();
        long creationTime = System.currentTimeMillis();
        RaftOp op = new CreateSessionOp(nodeEngine.getThisAddress(), instanceName, SERVER, creationTime);
        ICompletableFuture<SessionResponse> future = getInvocationManager().invoke(groupId, op);
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    protected ICompletableFuture<Object> heartbeat(CPGroupId groupId, long sessionId) {
        return getInvocationManager().invoke(groupId, new HeartbeatSessionOp(sessionId));
    }

    @Override
    protected ICompletableFuture<Object> closeSession(CPGroupId groupId, Long sessionId) {
        return getInvocationManager().invoke(groupId, new CloseSessionOp(sessionId));
    }

    @Override
    protected ScheduledFuture<?> scheduleWithRepetition(Runnable task, long period, TimeUnit unit) {
        return nodeEngine.getExecutionService().scheduleWithRepetition(task, period, period, unit);
    }

    @Override
    public boolean onShutdown(long timeout, TimeUnit unit) {
        ILogger logger = nodeEngine.getLogger(getClass());

        Map<CPGroupId, ICompletableFuture<Object>> futures = shutdown();
        long remainingTimeNanos = unit.toNanos(timeout);
        boolean successful = true;

        while (remainingTimeNanos > 0 && futures.size() > 0) {
            Iterator<Entry<CPGroupId, ICompletableFuture<Object>>> it = futures.entrySet().iterator();
            while (it.hasNext()) {
                Entry<CPGroupId, ICompletableFuture<Object>> entry = it.next();
                CPGroupId groupId = entry.getKey();
                ICompletableFuture<Object> f = entry.getValue();
                if (f.isDone()) {
                    it.remove();
                    try {
                        f.get();
                        logger.fine("Session closed for " + groupId);
                    } catch (Exception e) {
                        logger.warning("Close session failed for " + groupId, e);
                        successful = false;
                    }
                }
            }

            try {
                Thread.sleep(SHUTDOWN_TASK_PERIOD_IN_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }

            remainingTimeNanos -= MILLISECONDS.toNanos(SHUTDOWN_TASK_PERIOD_IN_MILLIS);
        }

        return successful && futures.isEmpty();
    }

    private RaftInvocationManager getInvocationManager() {
        RaftService raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        return raftService.getInvocationManager();
    }
}
