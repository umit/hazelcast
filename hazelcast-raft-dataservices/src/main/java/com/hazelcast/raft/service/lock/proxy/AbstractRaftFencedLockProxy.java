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

package com.hazelcast.raft.service.lock.proxy;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.raft.service.lock.FencedLock;
import com.hazelcast.raft.service.lock.RaftLockService;
import com.hazelcast.raft.service.session.AbstractSessionManager;
import com.hazelcast.raft.service.session.SessionAwareProxy;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.util.Clock;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.service.lock.RaftLockService.INVALID_FENCE;
import static com.hazelcast.raft.service.session.AbstractSessionManager.NO_SESSION_ID;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.ThreadUtil.getThreadId;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;

/**
 * TODO: Javadoc Pending...
 */
public abstract class AbstractRaftFencedLockProxy extends SessionAwareProxy implements FencedLock {

    // thread id -> lock state
    private final ConcurrentMap<Long, LockState> lockStates = new ConcurrentHashMap<Long, LockState>();
    protected final String name;

    public AbstractRaftFencedLockProxy(AbstractSessionManager sessionManager, RaftGroupId groupId, String name) {
        super(sessionManager, groupId);
        this.name = name;
    }

    @Override
    public final long lock() {
        long threadId = getThreadId();
        long fence = tryReentrantLock(threadId);
        if (fence != INVALID_FENCE) {
            return fence;
        }

        UUID invocationUid = newUnsecureUUID();
        for (;;) {
            long sessionId = acquireSession();
            try {
                fence = doLock(groupId, name, sessionId, threadId, invocationUid).join();
                lockStates.put(threadId, new LockState(sessionId, fence));
                return fence;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
            }
        }
    }

    @Override
    public final long tryLock() {
        return tryLock(0, TimeUnit.MILLISECONDS);
    }

    @Override
    public final long tryLock(long time, TimeUnit unit) {
        checkNotNull(unit);

        long threadId = getThreadId();
        long fence = tryReentrantLock(threadId);
        if (fence != INVALID_FENCE) {
            return fence;
        }

        UUID invocationUid = newUnsecureUUID();
        long timeoutMillis = Math.max(0, unit.toMillis(time));
        long start;
        for (;;) {
            start = Clock.currentTimeMillis();
            long sessionId = acquireSession();
            try {
                fence = doTryLock(groupId, name, sessionId, threadId, invocationUid, timeoutMillis).join();
                if (fence != INVALID_FENCE) {
                    lockStates.put(threadId, new LockState(sessionId, fence));
                } else {
                    releaseSession(sessionId);
                }
                return fence;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
                timeoutMillis -= (Clock.currentTimeMillis() - start);
                if (timeoutMillis <= 0) {
                    return INVALID_FENCE;
                }
            }
        }
    }

    private long tryReentrantLock(long threadId) {
        LockState lockState = lockStates.get(threadId);
        if (lockState != null) {
            if (lockState.sessionId == getSession()) {
                lockState.lockCount++;
                return lockState.fence;
            }
            lockStates.remove(threadId);
            throw new IllegalMonitorStateException("Current thread is not owner of the Lock[" + name + "] because Session["
                    + lockState.sessionId + "] is closed by server!");
        }
        return INVALID_FENCE;
    }

    @Override
    public final void unlock() {
        long sessionId = getSession();
        if (sessionId == NO_SESSION_ID) {
            throw new IllegalMonitorStateException("Current thread is not owner of the Lock[" + name
                    + "] because session not found!");
        }
        long threadId = getThreadId();
        LockState lockState = lockStates.get(threadId);
        if (lockState == null) {
            throw new IllegalMonitorStateException("Current thread is not owner of the Lock[" + name + "]");
        }
        if (lockState.sessionId != sessionId) {
            lockStates.remove(threadId);
            throw new IllegalMonitorStateException("Current thread is not owner of the Lock[" + name + "] because Session["
                    + lockState.sessionId + "] is closed by server!");
        }
        if (lockState.lockCount > 1) {
            lockState.lockCount--;
            return;
        }

        try {
            doUnlock(groupId, name, sessionId, threadId, newUnsecureUUID()).join();
        } catch (SessionExpiredException e) {
            invalidateSession(sessionId);
            throw new IllegalMonitorStateException("Current thread is not owner of the Lock[" + name + "] because Session["
                    + sessionId + "] is closed by server!");
        } finally {
            lockStates.remove(threadId);
            releaseSession(sessionId);
        }
    }

    @Override
    public final void forceUnlock() {
        try {
            long fence = doGetLockFence(groupId, name, NO_SESSION_ID, 0).join();
            doForceUnlock(groupId, name, fence, newUnsecureUUID()).join();
        } finally {
            lockStates.remove(getThreadId());
        }
    }

    @Override
    public final long getFence() {
        long sessionId = getSession();
        if (sessionId == NO_SESSION_ID) {
            throw new IllegalMonitorStateException();
        }

        LockState lockState = lockStates.get(getThreadId());
        if (lockState == null) {
            throw new IllegalMonitorStateException();
        }

        return lockState.fence;
    }

    @Override
    public final boolean isLocked() {
        return getLockCount() > 0;
    }

    @Override
    public final boolean isLockedByCurrentThread() {
        LockState lockState = lockStates.get(getThreadId());
        return (lockState != null && lockState.sessionId == getSession());
    }

    @Override
    public final int getLockCount() {
        LockState lockState = lockStates.get(getThreadId());
        if (lockState != null && lockState.sessionId == getSession()) {
            return lockState.lockCount;
        }

        return doGetLockCount(groupId, name, NO_SESSION_ID, 0).join();
    }

    @Override
    public final String getName() {
        return name;
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServiceName() {
        return RaftLockService.SERVICE_NAME;
    }

    protected abstract InternalCompletableFuture<Long> doLock(RaftGroupId groupId, String name, long sessionId, long threadId,
                                                              UUID invocationUid);

    protected abstract InternalCompletableFuture<Long> doTryLock(RaftGroupId groupId, String name, long sessionId, long threadId,
                                                                 UUID invocationUid, long timeoutMillis);

    protected abstract InternalCompletableFuture<Object> doUnlock(RaftGroupId groupId, String name, long sessionId, long threadId,
                                                                  UUID invocationUid);

    protected abstract InternalCompletableFuture<Object> doForceUnlock(RaftGroupId groupId, String name, long expectedFence,
                                                                       UUID invocationUid);

    protected abstract InternalCompletableFuture<Long> doGetLockFence(RaftGroupId groupId, String name, long sessionId,
                                                                      long threadId);

    protected abstract InternalCompletableFuture<Integer> doGetLockCount(RaftGroupId groupId, String name, long sessionId,
                                                                         long threadId);

    private static class LockState {
        final long sessionId;
        final long fence;
        int lockCount;

        LockState(long sessionId, long fence) {
            this.sessionId = sessionId;
            this.fence = fence;
            this.lockCount = 1;
        }
    }
}
