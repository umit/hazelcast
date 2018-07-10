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

package com.hazelcast.raft.service.semaphore.proxy;

import com.hazelcast.core.ISemaphore;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.raft.service.semaphore.RaftSemaphoreService;
import com.hazelcast.raft.service.semaphore.operation.AcquirePermitsOp;
import com.hazelcast.raft.service.semaphore.operation.AvailablePermitsOp;
import com.hazelcast.raft.service.semaphore.operation.ChangePermitsOp;
import com.hazelcast.raft.service.semaphore.operation.DrainPermitsOp;
import com.hazelcast.raft.service.semaphore.operation.InitSemaphoreOp;
import com.hazelcast.raft.service.semaphore.operation.ReleasePermitsOp;
import com.hazelcast.raft.service.session.SessionAwareProxy;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.raft.service.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.spi.InternalCompletableFuture;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkPositive;
import static java.lang.Math.max;

/**
 * TODO: Javadoc Pending...
 */
public class RaftSessionAwareSemaphoreProxy extends SessionAwareProxy implements ISemaphore {

    private static final int DRAIN_SESSION_ACQ_COUNT = 1024;

    private final String name;
    private final RaftInvocationManager raftInvocationManager;

    public RaftSessionAwareSemaphoreProxy(RaftInvocationManager invocationManager, SessionManagerService sessionManager,
                                          RaftGroupId groupId, String name) {
        super(sessionManager, groupId);
        this.name = name;
        this.raftInvocationManager = invocationManager;
    }

    @Override
    public boolean init(int permits) {
        checkNotNegative(permits, "Permits must be non-negative!");
        return raftInvocationManager.<Boolean>invoke(groupId, new InitSemaphoreOp(name, permits)).join();
    }

    @Override
    public void acquire() {
        acquire(1);
    }

    @Override
    public void acquire(int permits) {
        checkPositive(permits, "Permits must be positive!");
        for (;;) {
            long sessionId = acquireSession(permits);
            AcquirePermitsOp op = new AcquirePermitsOp(name, sessionId, permits, -1L);
            InternalCompletableFuture<Long> f = raftInvocationManager.invoke(groupId, op);
            try {
                f.join();
                break;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
            }
        }
    }

    @Override
    public boolean tryAcquire() {
        return tryAcquire(1);
    }

    @Override
    public boolean tryAcquire(int permits) {
        return tryAcquire(permits, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean tryAcquire(long timeout, TimeUnit unit) {
        return tryAcquire(1, timeout, unit);
    }

    @Override
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) {
        checkPositive(permits, "Permits must be positive!");
        long timeoutMs = max(0, unit.toMillis(timeout));
        for (;;) {
            long sessionId = acquireSession(permits);
            AcquirePermitsOp op = new AcquirePermitsOp(name, sessionId, permits, timeoutMs);
            InternalCompletableFuture<Boolean> f = raftInvocationManager.invoke(groupId, op);
            try {
                boolean acquired = f.join();
                if (!acquired) {
                    releaseSession(sessionId, permits);
                }
                return acquired;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
            }
        }
    }

    @Override
    public void release() {
        release(1);
    }

    @Override
    public void release(int permits) {
        checkPositive(permits, "Permits must be positive!");
        long sessionId = getSession();
        if (sessionId < 0) {
            throw new IllegalStateException("No valid session!");
        }
        InternalCompletableFuture f = raftInvocationManager.invoke(groupId, new ReleasePermitsOp(name, sessionId, permits));
        try {
            f.join();
        } finally {
            releaseSession(sessionId, permits);
        }
    }

    @Override
    public int availablePermits() {
        return raftInvocationManager.<Integer>invoke(groupId, new AvailablePermitsOp(name)).join();
    }

    @Override
    public int drainPermits() {
        for (;;) {
            long sessionId = acquireSession(DRAIN_SESSION_ACQ_COUNT);
            RaftOp op = new DrainPermitsOp(name, sessionId);
            try {
                int count = raftInvocationManager.<Integer>invoke(groupId, op).join();
                releaseSession(sessionId, DRAIN_SESSION_ACQ_COUNT - count);
                return count;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
            }
        }
    }

    @Override
    public void reducePermits(int reduction) {
        checkNotNegative(reduction, "Reduction must be non-negative!");
        if (reduction == 0) {
            return;
        }
        raftInvocationManager.invoke(groupId, new ChangePermitsOp(name, -reduction)).join();
    }

    @Override
    public void increasePermits(int increase) {
        checkNotNegative(increase, "Increase must be non-negative!");
        if (increase == 0) {
            return;
        }
        raftInvocationManager.invoke(groupId, new ChangePermitsOp(name, increase)).join();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServiceName() {
        return RaftSemaphoreService.SERVICE_NAME;
    }

    @Override
    public void destroy() {
        raftInvocationManager.invoke(groupId, new DestroyRaftObjectOp(getServiceName(), name)).join();
    }

}