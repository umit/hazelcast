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

package com.hazelcast.cp.internal.datastructures.lock.proxy;

import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.cp.RaftGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockOwnershipState;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockService;
import com.hazelcast.cp.internal.datastructures.lock.operation.LockOp;
import com.hazelcast.cp.internal.datastructures.session.AbstractSessionManager;
import com.hazelcast.cp.internal.datastructures.session.SessionManagerService;
import com.hazelcast.cp.internal.datastructures.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.cp.internal.session.SessionExpiredException;
import com.hazelcast.cp.internal.datastructures.exception.WaitKeyCancelledException;
import com.hazelcast.cp.internal.datastructures.lock.operation.ForceUnlockOp;
import com.hazelcast.cp.internal.datastructures.lock.operation.GetLockOwnershipStateOp;
import com.hazelcast.cp.internal.datastructures.lock.operation.TryLockOp;
import com.hazelcast.cp.internal.datastructures.lock.operation.UnlockOp;
import com.hazelcast.cp.internal.datastructures.session.SessionAwareProxy;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.ThreadUtil.getThreadId;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;

/**
 * Server-side proxy of Raft-based {@link ILock} API
 */
public class RaftLockProxy extends SessionAwareProxy implements ILock {

    private final String name;
    private final RaftInvocationManager invocationManager;

    public RaftLockProxy(RaftInvocationManager invocationManager, SessionManagerService sessionManager, RaftGroupId groupId,
                         String name) {
        super(sessionManager, groupId);
        this.name = name;
        this.invocationManager = invocationManager;
    }

    @Override
    public void lock() {
        long threadId = getThreadId();
        UUID invUid = newUnsecureUUID();
        for (;;) {
            long sessionId = acquireSession();
            try {
                invocationManager.invoke(groupId, new LockOp(name, sessionId, threadId, invUid)).join();
                break;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
            }
        }
    }

    @Override
    public boolean tryLock() {
        return tryLock(0, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) {
        checkNotNull(unit);
        long threadId = getThreadId();
        UUID invUid = newUnsecureUUID();
        long timeoutMs = Math.max(0, unit.toMillis(time));
        for (;;) {
            long sessionId = acquireSession();
            RaftOp op = new TryLockOp(name, sessionId, threadId, invUid, timeoutMs);
            try {
                RaftLockOwnershipState ownership = invocationManager.<RaftLockOwnershipState>invoke(groupId, op).join();
                if (!ownership.isLocked()) {
                    releaseSession(sessionId);
                }
                return ownership.isLocked();
            } catch (WaitKeyCancelledException e) {
                return false;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
            }
        }
    }

    @Override
    public void unlock() {
        long sessionId = getSession();
        if (sessionId == AbstractSessionManager.NO_SESSION_ID) {
            throw new IllegalMonitorStateException();
        }
        try {
            invocationManager.invoke(groupId, new UnlockOp(name, sessionId, getThreadId(), newUnsecureUUID(), 1)).join();
        } catch (SessionExpiredException e) {
            invalidateSession(sessionId);
            throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
        } finally {
            releaseSession(sessionId);
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit, long leaseTime, TimeUnit leaseUnit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void lock(long leaseTime, TimeUnit timeUnit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forceUnlock() {
        RaftOp op = new GetLockOwnershipStateOp(name);
        RaftLockOwnershipState ownership = invocationManager.<RaftLockOwnershipState>invoke(groupId, op).join();
        if (!ownership.isLocked()) {
            throw new IllegalMonitorStateException("Lock[" + name + "] has no owner!");
        }

        invocationManager.invoke(groupId, new ForceUnlockOp(name, ownership.getFence(), newUnsecureUUID())).join();
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICondition newCondition(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLocked() {
        return getLockCount() > 0;
    }

    @Override
    public boolean isLockedByCurrentThread() {
        long sessionId = getSession();
        if (sessionId == AbstractSessionManager.NO_SESSION_ID) {
            return false;
        }

        RaftOp op = new GetLockOwnershipStateOp(name);
        RaftLockOwnershipState ownership = invocationManager.<RaftLockOwnershipState>invoke(groupId, op).join();
        return (ownership.getSessionId() == sessionId && ownership.getThreadId() == getThreadId());
    }

    @Override
    public int getLockCount() {
        RaftOp op = new GetLockOwnershipStateOp(name);
        RaftLockOwnershipState ownership = invocationManager.<RaftLockOwnershipState>invoke(groupId, op).join();
        return ownership.getLockCount();
    }

    @Override
    public long getRemainingLeaseTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void lockInterruptibly() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return RaftLockService.SERVICE_NAME;
    }

    @Override
    public void destroy() {
        invocationManager.invoke(groupId, new DestroyRaftObjectOp(getServiceName(), name)).join();
    }
}
