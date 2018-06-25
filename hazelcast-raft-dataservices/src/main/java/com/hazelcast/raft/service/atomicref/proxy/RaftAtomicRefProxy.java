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

package com.hazelcast.raft.service.atomicref.proxy;

import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.service.atomicref.RaftAtomicRefService;
import com.hazelcast.raft.service.atomicref.operation.ApplyOp;
import com.hazelcast.raft.service.atomicref.operation.CompareAndSetOp;
import com.hazelcast.raft.service.atomicref.operation.ContainsOp;
import com.hazelcast.raft.service.atomicref.operation.GetOp;
import com.hazelcast.raft.service.atomicref.operation.SetOp;
import com.hazelcast.raft.service.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;

import static com.hazelcast.raft.service.atomicref.operation.ApplyOp.RETURN_VALUE_TYPE.NO_RETURN_VALUE;
import static com.hazelcast.raft.service.atomicref.operation.ApplyOp.RETURN_VALUE_TYPE.RETURN_NEW_VALUE;
import static com.hazelcast.raft.service.atomicref.operation.ApplyOp.RETURN_VALUE_TYPE.RETURN_PREVIOUS_VALUE;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * TODO: Javadoc Pending...
 */
public class RaftAtomicRefProxy<T> implements IAtomicReference<T> {

    private final String name;
    private final RaftGroupId groupId;
    private final RaftInvocationManager raftInvocationManager;
    private final SerializationService serializationService;

    public RaftAtomicRefProxy(String name, RaftGroupId groupId, RaftInvocationManager invocationManager,
                              SerializationService serializationService) {
        this.name = name;
        this.groupId = groupId;
        this.raftInvocationManager = invocationManager;
        this.serializationService = serializationService;
    }

    @Override
    public boolean compareAndSet(T expect, T update) {
        return join(compareAndSetAsync(expect, update));
    }

    @Override
    public T get() {
        return join(getAsync());
    }

    @Override
    public void set(T newValue) {
        join(setAsync(newValue));
    }

    @Override
    public T getAndSet(T newValue) {
        return join(getAndSetAsync(newValue));
    }

    @Override
    public T setAndGet(T update) {
        set(update);
        return update;
    }

    @Override
    public boolean isNull() {
        return join(isNullAsync());
    }

    @Override
    public void clear() {
        join(clearAsync());
    }

    @Override
    public boolean contains(T value) {
        return join(containsAsync(value));
    }

    @Override
    public void alter(IFunction<T, T> function) {
        join(alterAsync(function));
    }

    @Override
    public T alterAndGet(IFunction<T, T> function) {
        return join(alterAndGetAsync(function));
    }

    @Override
    public T getAndAlter(IFunction<T, T> function) {
        return join(getAndAlterAsync(function));
    }

    @Override
    public <R> R apply(IFunction<T, R> function) {
        return join(applyAsync(function));
    }

    @Override
    public ICompletableFuture<Boolean> compareAndSetAsync(T expect, T update) {
        return raftInvocationManager.invoke(groupId, new CompareAndSetOp(name, toData(expect), toData(update)));
    }

    @Override
    public ICompletableFuture<T> getAsync() {
        return raftInvocationManager.invoke(groupId, new GetOp(name));
    }

    @Override
    public ICompletableFuture<Void> setAsync(T newValue) {
        return raftInvocationManager.invoke(groupId, new SetOp(name, toData(newValue), false));
    }

    @Override
    public ICompletableFuture<T> getAndSetAsync(T newValue) {
        return raftInvocationManager.invoke(groupId, new SetOp(name, toData(newValue), true));
    }

    @Override
    public ICompletableFuture<Boolean> isNullAsync() {
        return raftInvocationManager.invoke(groupId, new ContainsOp(name, null));
    }

    @Override
    public ICompletableFuture<Void> clearAsync() {
        return raftInvocationManager.invoke(groupId, new SetOp(name, null, false));
    }

    @Override
    public ICompletableFuture<Boolean> containsAsync(T expected) {
        return raftInvocationManager.invoke(groupId, new ContainsOp(name, toData(expected)));
    }

    @Override
    public ICompletableFuture<Void> alterAsync(IFunction<T, T> function) {
        checkTrue(function != null, "Function cannot be null");
        RaftOp op = new ApplyOp(name, toData(function), NO_RETURN_VALUE, true);
        return raftInvocationManager.invoke(groupId, op);
    }

    @Override
    public ICompletableFuture<T> alterAndGetAsync(IFunction<T, T> function) {
        checkTrue(function != null, "Function cannot be null");
        RaftOp op = new ApplyOp(name, toData(function), RETURN_NEW_VALUE, true);
        return raftInvocationManager.invoke(groupId, op);
    }

    @Override
    public ICompletableFuture<T> getAndAlterAsync(IFunction<T, T> function) {
        checkTrue(function != null, "Function cannot be null");
        RaftOp op = new ApplyOp(name, toData(function), RETURN_PREVIOUS_VALUE, true);
        return raftInvocationManager.invoke(groupId, op);
    }

    @Override
    public <R> ICompletableFuture<R> applyAsync(IFunction<T, R> function) {
        checkTrue(function != null, "Function cannot be null");
        RaftOp op = new ApplyOp(name, toData(function), RETURN_NEW_VALUE, false);
        return raftInvocationManager.invoke(groupId, op);
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
        return RaftAtomicRefService.SERVICE_NAME;
    }

    @Override
    public void destroy() {
        join(raftInvocationManager.invoke(groupId, new DestroyRaftObjectOp(getServiceName(), name)));
    }

    public RaftGroupId getGroupId() {
        return groupId;
    }

    private Data toData(Object value) {
        return serializationService.toData(value);
    }

    private <T> T join(ICompletableFuture<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }
}
