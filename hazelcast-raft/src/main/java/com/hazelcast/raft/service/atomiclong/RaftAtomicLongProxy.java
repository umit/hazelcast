package com.hazelcast.raft.service.atomiclong;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;

public class RaftAtomicLongProxy implements IAtomicLong {

    private final String name;
    private final String raftName;

    RaftAtomicLongProxy(String name, String raftName) {
        this.name = name;
        this.raftName = raftName;
    }

    @Override
    public long addAndGet(long delta) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long decrementAndGet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long get() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getAndAdd(long delta) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getAndSet(long newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long incrementAndGet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getAndIncrement() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void set(long newValue) {
        throw new UnsupportedOperationException();
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
    public ICompletableFuture<Long> addAndGetAsync(long delta) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICompletableFuture<Boolean> compareAndSetAsync(long expect, long update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICompletableFuture<Long> decrementAndGetAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICompletableFuture<Long> getAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICompletableFuture<Long> getAndAddAsync(long delta) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICompletableFuture<Long> getAndSetAsync(long newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICompletableFuture<Long> incrementAndGetAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICompletableFuture<Long> getAndIncrementAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICompletableFuture<Void> setAsync(long newValue) {
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
