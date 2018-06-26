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
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.raft.service.semaphore.RaftSemaphoreService;
import com.hazelcast.raft.service.semaphore.operation.AcquirePermitsOp;
import com.hazelcast.raft.service.semaphore.operation.AvailablePermitsOp;
import com.hazelcast.raft.service.semaphore.operation.InitSemaphoreOp;
import com.hazelcast.raft.service.semaphore.operation.ReleasePermitsOp;
import com.hazelcast.raft.service.session.SessionAwareProxy;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.raft.service.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftSemaphoreProxy extends SessionAwareProxy implements ISemaphore {

    private final String name;
    private final RaftInvocationManager raftInvocationManager;

    public RaftSemaphoreProxy(String name, RaftGroupId groupId, SessionManagerService sessionManager,
            RaftInvocationManager invocationManager) {
        super(sessionManager, groupId);
        this.name = name;
        this.raftInvocationManager = invocationManager;
    }

    @Override
    public boolean init(int permits) {
        return join(raftInvocationManager.<Boolean>invoke(groupId, new InitSemaphoreOp(name, permits)));
    }

    @Override
    public void acquire() throws InterruptedException {
        acquire(1);
    }

    @Override
    public void acquire(int permits) throws InterruptedException {
        for (;;) {
            long sessionId = acquireSession();
            AcquirePermitsOp op = new AcquirePermitsOp(name, sessionId, permits, -1L);
            Future<Long> f = raftInvocationManager.invoke(groupId, op);
            try {
                join(f);
                break;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
            }
        }
    }

    @Override
    public boolean tryAcquire() {
        return false;
    }

    @Override
    public boolean tryAcquire(int permits) {
        return false;
    }

    @Override
    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public void release() {
        release(1);
    }

    @Override
    public void release(int permits) {
        final long sessionId = getSession();
        if (sessionId < 0) {
            // TODO: exception type?
            throw new IllegalStateException();
        }
        Future f = raftInvocationManager.invoke(groupId, new ReleasePermitsOp(name, sessionId, permits));
        try {
            join(f);
        } finally {
            releaseSession(sessionId);
        }
    }

    @Override
    public int availablePermits() {
        return join(raftInvocationManager.<Integer>invoke(groupId, new AvailablePermitsOp(name)));
    }

    @Override
    public int drainPermits() {
        return 0;
    }

    @Override
    public void reducePermits(int reduction) {

    }

    @Override
    public void increasePermits(int increase) {

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
        join(raftInvocationManager.invoke(groupId, new DestroyRaftObjectOp(getServiceName(), name)));
    }

    private <T> T join(Future<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

}
