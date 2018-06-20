package com.hazelcast.raft.service.lock.proxy;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.raft.service.session.AbstractSessionManager;
import com.hazelcast.raft.service.lock.FencedLock;
import com.hazelcast.raft.service.session.SessionAwareProxy;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.UuidUtil;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ThreadUtil.getThreadId;

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
        if (fence > 0) {
            return fence;
        }

        UUID invocationUid = UuidUtil.newUnsecureUUID();
        for (;;) {
            long sessionId = acquireSession();
            Future<Long> f = doLock(groupId, name, sessionId, threadId, invocationUid);
            try {
                fence = join(f);
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
        long threadId = getThreadId();
        long fence = tryReentrantLock(threadId);
        if (fence > 0) {
            return fence;
        }

        UUID invocationUid = UuidUtil.newUnsecureUUID();
        long timeoutMillis = Math.max(0, unit.toMillis(time));

        for (;;) {
            long sessionId = acquireSession();
            Future<Long> f = doTryLock(groupId, name, sessionId, threadId, invocationUid, timeoutMillis);
            try {
                fence = join(f);
                if (fence > 0) {
                    lockStates.put(threadId, new LockState(sessionId, fence));
                }

                return fence;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
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

        return 0;
    }

    @Override
    public final void unlock() {
        long sessionId = getSession();
        if (sessionId < 0) {
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

        UUID invocationUid = UuidUtil.newUnsecureUUID();
        Future f = doUnlock(groupId, name, sessionId, threadId, invocationUid);
        try {
            join(f);
        } catch (SessionExpiredException e) {
            throw new IllegalMonitorStateException("Current thread is not owner of the Lock[" + name + "] because Session["
                    + sessionId + "] is closed by server!");
        } finally {
            lockStates.remove(threadId);
            releaseSession(sessionId);
        }
    }

    @Override
    public final void forceUnlock() {
        long threadId = getThreadId();
        try {
            long fence;
            LockState lockState = lockStates.get(threadId);
            if (lockState != null) {
                fence = lockState.fence;
            } else {
                Future<Long> f = doGetLockFence(groupId, name);
                fence = join(f);
            }

            UUID invocationUid = UuidUtil.newUnsecureUUID();
            Future f = doForceUnlock(groupId, name, fence, invocationUid);
            join(f);
        } finally {
            lockStates.remove(threadId);
        }
    }

    @Override
    public final long getFence() {
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

        Future<Integer> f = doGetLockCount(groupId, name);
        return join(f);
    }

    @Override
    public final RaftGroupId getRaftGroupId() {
        return groupId;
    }

    @Override
    public final String getName() {
        return name;
    }

    protected abstract Future<Long> doLock(RaftGroupId groupId, String name, long sessionId, long threadId, UUID invocationUid);

    protected abstract Future<Long> doTryLock(RaftGroupId groupId, String name, long sessionId, long threadId, UUID invocationUid, long timeoutMillis);

    protected abstract Future<Object> doUnlock(RaftGroupId groupId, String name, long sessionId, long threadId, UUID invocationUid);

    protected abstract Future<Object> doForceUnlock(RaftGroupId groupId, String name, long expectedFence, UUID invocationUid);

    protected abstract Future<Long> doGetLockFence(RaftGroupId groupId, String name);

    protected abstract Future<Integer> doGetLockCount(RaftGroupId groupId, String name);

    private <T> T join(Future<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

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
