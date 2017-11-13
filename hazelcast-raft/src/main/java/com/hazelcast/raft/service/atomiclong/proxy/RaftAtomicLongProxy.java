package com.hazelcast.raft.service.atomiclong.proxy;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.raft.impl.service.proxy.RaftReplicatingOperation;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.function.Supplier;

import static com.hazelcast.raft.impl.service.RaftInvocationHelper.invokeOnLeader;
import static com.hazelcast.raft.service.atomiclong.RaftAtomicLongService.PREFIX;

public class RaftAtomicLongProxy implements IAtomicLong {

    private final String name;
    private final String raftName;
    private final NodeEngine nodeEngine;

    public RaftAtomicLongProxy(String name, NodeEngine nodeEngine) {
        this.name = name;
        this.raftName = PREFIX + name;
        this.nodeEngine = nodeEngine;
    }

    @Override
    public long addAndGet(long delta) {
        ICompletableFuture<Long> future = addAndGetAsync(delta);
        return join(future);
    }

    @Override
    public long incrementAndGet() {
        return addAndGet(1);
    }

    @Override
    public long decrementAndGet() {
        return addAndGet(-1);
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        ICompletableFuture<Boolean> future = compareAndSetAsync(expect, update);
        return join(future);
    }

    @Override
    public long getAndAdd(long delta) {
        ICompletableFuture<Long> future = getAndAddAsync(delta);
        return join(future);
    }

    @Override
    public long get() {
        return getAndAdd(0);
    }

    @Override
    public long getAndIncrement() {
        return getAndAdd(1);
    }

    @Override
    public long getAndSet(long newValue) {
        ICompletableFuture<Long> future = getAndSetAsync(newValue);
        return join(future);
    }

    @Override
    public void set(long newValue) {
        getAndSet(newValue);
    }

    private <T> T join(ICompletableFuture<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public ICompletableFuture<Long> addAndGetAsync(final long delta) {
        return invokeOnLeader(nodeEngine, new Supplier<RaftReplicatingOperation>() {
            @Override
            public RaftReplicatingOperation get() {
                return new AddAndGetReplicatingOperation(name, delta);
            }
        }, raftName);
    }

    @Override
    public ICompletableFuture<Long> incrementAndGetAsync() {
        return addAndGetAsync(1);
    }

    @Override
    public ICompletableFuture<Long> decrementAndGetAsync() {
        return addAndGetAsync(-1);
    }

    @Override
    public ICompletableFuture<Boolean> compareAndSetAsync(final long expect, final long update) {
        return invokeOnLeader(nodeEngine, new Supplier<RaftReplicatingOperation>() {
            @Override
            public RaftReplicatingOperation get() {
                return new CompareAndSetReplicatingOperation(name, expect, update);
            }
        }, raftName);
    }

    @Override
    public ICompletableFuture<Long> getAndAddAsync(final long delta) {
        return invokeOnLeader(nodeEngine, new Supplier<RaftReplicatingOperation>() {
            @Override
            public RaftReplicatingOperation get() {
                return new GetAndAddReplicatingOperation(name, delta);
            }
        }, raftName);
    }

    @Override
    public ICompletableFuture<Long> getAsync() {
        return getAndAddAsync(0);
    }

    @Override
    public ICompletableFuture<Long> getAndIncrementAsync() {
        return getAndAddAsync(1);
    }

    @Override
    public ICompletableFuture<Long> getAndSetAsync(final long newValue) {
        return invokeOnLeader(nodeEngine, new Supplier<RaftReplicatingOperation>() {
            @Override
            public RaftReplicatingOperation get() {
                return new GetAndSetReplicatingOperation(name, newValue);
            }
        }, raftName);
    }

    @Override
    public ICompletableFuture<Void> setAsync(long newValue) {
        ICompletableFuture future = getAndSetAsync(newValue);
        return future;
    }

    @Override
    public void alter(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long alterAndGet(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getAndAlter(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> R apply(IFunction<Long, R> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICompletableFuture<Void> alterAsync(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICompletableFuture<Long> alterAndGetAsync(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICompletableFuture<Long> getAndAlterAsync(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> ICompletableFuture<R> applyAsync(IFunction<Long, R> function) {
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
        return RaftAtomicLongService.SERVICE_NAME;
    }

    @Override
    public void destroy() {
        throw new UnsupportedOperationException();
    }

    public String getRaftName() {
        return raftName;
    }
}
