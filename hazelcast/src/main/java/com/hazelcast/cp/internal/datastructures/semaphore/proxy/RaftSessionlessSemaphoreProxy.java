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

package com.hazelcast.cp.internal.datastructures.semaphore.proxy;

import com.hazelcast.core.ISemaphore;
import com.hazelcast.cp.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphoreService;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.AcquirePermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.AvailablePermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.ChangePermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.DrainPermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.InitSemaphoreOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.ReleasePermitsOp;
import com.hazelcast.cp.internal.datastructures.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.cp.internal.session.SessionAwareProxy;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.lang.Math.max;

/**
 * Server-side sessionless proxy of Raft-based {@link ISemaphore} API
 */
public class RaftSessionlessSemaphoreProxy extends SessionAwareProxy implements ISemaphore {

    private final RaftInvocationManager invocationManager;
    private final RaftGroupId groupId;
    private final String name;

    public RaftSessionlessSemaphoreProxy(RaftInvocationManager invocationManager, AbstractProxySessionManager sessionManager, RaftGroupId groupId, String name) {
        super(sessionManager, groupId);
        this.invocationManager = invocationManager;
        this.groupId = groupId;
        this.name = name;
    }

    @Override
    public boolean init(int permits) {
        checkNotNegative(permits, "Permits must be non-negative!");
        return invocationManager.<Boolean>invoke(groupId, new InitSemaphoreOp(name, permits)).join();
    }

    @Override
    public void acquire() {
        acquire(1);
    }

    @Override
    public void acquire(int permits) {
        checkPositive(permits, "Permits must be positive!");
        long clusterWideThreadId = getOrCreateUniqueThreadId(groupId);
        RaftOp op = new AcquirePermitsOp(name, NO_SESSION_ID, clusterWideThreadId, newUnsecureUUID(), permits, -1L);
        invocationManager.invoke(groupId, op).join();
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
        long clusterWideThreadId = getOrCreateUniqueThreadId(groupId);
        long timeoutMs = max(0, unit.toMillis(timeout));
        RaftOp op = new AcquirePermitsOp(name, NO_SESSION_ID, clusterWideThreadId, newUnsecureUUID(), permits, timeoutMs);
        return invocationManager.<Boolean>invoke(groupId, op).join();
    }

    @Override
    public void release() {
        release(1);
    }

    @Override
    public void release(int permits) {
        checkPositive(permits, "Permits must be positive!");
        long clusterWideThreadId = getOrCreateUniqueThreadId(groupId);
        RaftOp op = new ReleasePermitsOp(name, NO_SESSION_ID, clusterWideThreadId, newUnsecureUUID(), permits);
        invocationManager.invoke(groupId, op).join();
    }

    @Override
    public int availablePermits() {
        return invocationManager.<Integer>invoke(groupId, new AvailablePermitsOp(name)).join();
    }

    @Override
    public int drainPermits() {
        long clusterWideThreadId = getOrCreateUniqueThreadId(groupId);
        RaftOp op = new DrainPermitsOp(name, NO_SESSION_ID, clusterWideThreadId, newUnsecureUUID());
        return invocationManager.<Integer>invoke(groupId, op).join();
    }

    @Override
    public void reducePermits(int reduction) {
        checkNotNegative(reduction, "Reduction must be non-negative!");
        if (reduction == 0) {
            return;
        }
        long clusterWideThreadId = getOrCreateUniqueThreadId(groupId);
        RaftOp op = new ChangePermitsOp(name, NO_SESSION_ID, clusterWideThreadId, newUnsecureUUID(), -reduction);
        invocationManager.invoke(groupId, op).join();
    }

    @Override
    public void increasePermits(int increase) {
        checkNotNegative(increase, "Increase must be non-negative!");
        if (increase == 0) {
            return;
        }
        long clusterWideThreadId = getOrCreateUniqueThreadId(groupId);
        RaftOp op = new ChangePermitsOp(name, NO_SESSION_ID, clusterWideThreadId, newUnsecureUUID(), increase);
        invocationManager.invoke(groupId, op).join();
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
        invocationManager.invoke(groupId, new DestroyRaftObjectOp(getServiceName(), name)).join();
    }

}
