package com.hazelcast.raft.service.lock.proxy;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.raft.service.lock.RaftLockService;
import com.hazelcast.raft.service.lock.operation.GetLockCountOp;
import com.hazelcast.raft.service.lock.operation.LockOp;
import com.hazelcast.raft.service.lock.operation.TryLockOp;
import com.hazelcast.raft.service.lock.operation.UnlockOp;
import com.hazelcast.raft.service.session.SessionAwareProxy;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ThreadUtil;
import com.hazelcast.util.UuidUtil;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static com.hazelcast.raft.service.lock.RaftLockService.SERVICE_NAME;

public class RaftLockProxy extends SessionAwareProxy implements ILock {

    private final String name;
    private final RaftInvocationManager raftInvocationManager;

    public static ILock create(HazelcastInstance instance, String name) {
        NodeEngine nodeEngine = getNodeEngine(instance);
        try {
            RaftLockService service = nodeEngine.getService(SERVICE_NAME);
            return service.createNew(name);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private static NodeEngine getNodeEngine(HazelcastInstance instance) {
        HazelcastInstanceImpl instanceImpl;
        if (instance instanceof HazelcastInstanceProxy) {
            instanceImpl = ((HazelcastInstanceProxy) instance).getOriginal();
        } else if (instance instanceof HazelcastInstanceImpl) {
            instanceImpl = (HazelcastInstanceImpl) instance;
        } else {
            throw new IllegalArgumentException("Unknown instance! " + instance);
        }
        return instanceImpl.node.getNodeEngine();
    }

    public RaftLockProxy(String name, RaftGroupId groupId, SessionManagerService sessionManager,
            RaftInvocationManager invocationManager) {
        super(sessionManager, groupId);
        this.name = name;
        this.raftInvocationManager = invocationManager;
    }

    @Override
    public void lock() {
        UUID invUid = UuidUtil.newUnsecureUUID();
        for (;;) {
            long sessionId = acquireSession();
            ICompletableFuture<Object> future =
                    raftInvocationManager.invoke(groupId, new LockOp(name, sessionId, ThreadUtil.getThreadId(), invUid));
            try {
                join(future);
                break;
            } catch (SessionExpiredException e) {
                invalidateSession(e.getSessionId());
            }
        }
    }

    @Override
    public boolean tryLock() {
        UUID invUid = UuidUtil.newUnsecureUUID();
        for (;;) {
            long sessionId = acquireSession();
            ICompletableFuture<Boolean> future =
                    raftInvocationManager.invoke(groupId, new TryLockOp(name, sessionId, ThreadUtil.getThreadId(), invUid));
            try {
                return join(future);
            } catch (SessionExpiredException e) {
                invalidateSession(e.getSessionId());
            }
        }
    }

    @Override
    public void unlock() {
        final long sessionId = getSession();
        if (sessionId < 0) {
            throw new IllegalMonitorStateException();
        }
        UUID invUid = UuidUtil.newUnsecureUUID();
        ICompletableFuture future =
                raftInvocationManager.invoke(groupId, new UnlockOp(name, sessionId, ThreadUtil.getThreadId(), invUid));
        try {
            join(future);
        } finally {
            releaseSession(sessionId);
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit, long leaseTime, TimeUnit leaseUnit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void lock(long leaseTime, TimeUnit timeUnit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forceUnlock() {
        throw new UnsupportedOperationException();
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
        if (sessionId < 0) {
            return false;
        }
        ICompletableFuture<Integer> future = raftInvocationManager
                .invoke(groupId, new GetLockCountOp(name, sessionId, ThreadUtil.getThreadId()));
        return  join(future) > 0;
    }

    @Override
    public int getLockCount() {
        ICompletableFuture<Integer> future = getLockCountAsync();
        return join(future);
    }

    public ICompletableFuture<Integer> getLockCountAsync() {
        return raftInvocationManager.invoke(groupId, new GetLockCountOp(name));
    }

    @Override
    public long getRemainingLeaseTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    private <T> T join(ICompletableFuture<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
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
        return SERVICE_NAME;
    }

    @Override
    public void destroy() {
        join(raftInvocationManager.triggerDestroyRaftGroup(groupId));
    }

    public RaftGroupId getGroupId() {
        return groupId;
    }
}
