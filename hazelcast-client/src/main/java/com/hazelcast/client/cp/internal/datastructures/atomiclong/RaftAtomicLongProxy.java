/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cp.internal.datastructures.atomiclong;

import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPAtomicLongAddAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.CPAtomicLongAlterCodec;
import com.hazelcast.client.impl.protocol.codec.CPAtomicLongApplyCodec;
import com.hazelcast.client.impl.protocol.codec.CPAtomicLongCompareAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.CPAtomicLongGetAndAddCodec;
import com.hazelcast.client.impl.protocol.codec.CPAtomicLongGetAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.CPAtomicLongGetCodec;
import com.hazelcast.client.impl.protocol.codec.CPGroupDestroyCPObjectCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IFunction;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.atomiclong.RaftAtomicLongService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;

import static com.hazelcast.cp.internal.datastructures.atomiclong.operation.AlterOp.AlterResultType.NEW_VALUE;
import static com.hazelcast.cp.internal.datastructures.atomiclong.operation.AlterOp.AlterResultType.OLD_VALUE;

/**
 * Client-side Raft-based proxy implementation of {@link IAtomicLong}
 */
@SuppressWarnings("checkstyle:methodcount")
class RaftAtomicLongProxy extends ClientProxy implements IAtomicLong {

    private static final ClientMessageDecoder ADD_AND_GET_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return CPAtomicLongAddAndGetCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder COMPARE_AND_SET_DECODER = new ClientMessageDecoder() {
        @Override
        public Boolean decodeClientMessage(ClientMessage clientMessage) {
            return CPAtomicLongCompareAndSetCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder GET_AND_ADD_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return CPAtomicLongGetAndAddCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder GET_AND_SET_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return CPAtomicLongGetAndSetCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder GET_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return CPAtomicLongGetCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder APPLY_DECODER = new ClientMessageDecoder() {
        @Override
        public Object decodeClientMessage(ClientMessage clientMessage) {
            return CPAtomicLongApplyCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder ALTER_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return CPAtomicLongAlterCodec.decodeResponse(clientMessage).response;
        }
    };


    private final CPGroupId groupId;
    private final String objectName;

    RaftAtomicLongProxy(ClientContext context, CPGroupId groupId, String proxyName, String objectName) {
        super(RaftAtomicLongService.SERVICE_NAME, proxyName, context);
        this.groupId = groupId;
        this.objectName = objectName;
    }

    @Override
    public long addAndGet(long delta) {
        return addAndGetAsync(delta).join();
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        return compareAndSetAsync(expect, update).join();
    }

    @Override
    public long decrementAndGet() {
        return decrementAndGetAsync().join();
    }

    @Override
    public long get() {
        return getAsync().join();
    }

    @Override
    public long getAndAdd(long delta) {
        return getAndAddAsync(delta).join();
    }

    @Override
    public long getAndSet(long newValue) {
        return getAndSetAsync(newValue).join();
    }

    @Override
    public long incrementAndGet() {
        return incrementAndGetAsync().join();
    }

    @Override
    public long getAndIncrement() {
        return getAndIncrementAsync().join();
    }

    @Override
    public void set(long newValue) {
        setAsync(newValue).join();
    }

    @Override
    public void alter(IFunction<Long, Long> function) {
        alterAsync(function).join();
    }

    @Override
    public long alterAndGet(IFunction<Long, Long> function) {
        return alterAndGetAsync(function).join();
    }

    @Override
    public long getAndAlter(IFunction<Long, Long> function) {
        return getAndAlterAsync(function).join();
    }

    @Override
    public <R> R apply(IFunction<Long, R> function) {
        return applyAsync(function).join();
    }

    @Override
    public InternalCompletableFuture<Long> addAndGetAsync(long delta) {
        Data groupId = getSerializationService().toData(this.groupId);
        ClientMessage request = CPAtomicLongAddAndGetCodec.encodeRequest(groupId, objectName, delta);
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<Long>(future, getSerializationService(), ADD_AND_GET_DECODER);
    }

    @Override
    public InternalCompletableFuture<Boolean> compareAndSetAsync(long expect, long update) {
        Data groupId = getSerializationService().toData(this.groupId);
        ClientMessage request = CPAtomicLongCompareAndSetCodec.encodeRequest(groupId, objectName, expect, update);
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<Boolean>(future, getSerializationService(), COMPARE_AND_SET_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> decrementAndGetAsync() {
        return addAndGetAsync(-1);
    }

    @Override
    public InternalCompletableFuture<Long> getAsync() {
        Data groupId = getSerializationService().toData(this.groupId);
        ClientMessage request = CPAtomicLongGetCodec.encodeRequest(groupId, objectName);
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<Long>(future, getSerializationService(), GET_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> getAndAddAsync(long delta) {
        Data groupId = getSerializationService().toData(this.groupId);
        ClientMessage request = CPAtomicLongGetAndAddCodec.encodeRequest(groupId, objectName, delta);
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<Long>(future, getSerializationService(), GET_AND_ADD_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> getAndSetAsync(long newValue) {
        Data groupId = getSerializationService().toData(this.groupId);
        ClientMessage request = CPAtomicLongGetAndSetCodec.encodeRequest(groupId, objectName, newValue);
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<Long>(future, getSerializationService(), GET_AND_SET_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> incrementAndGetAsync() {
        return addAndGetAsync(1);
    }

    @Override
    public InternalCompletableFuture<Long> getAndIncrementAsync() {
        return getAndAddAsync(1);
    }

    @Override
    public InternalCompletableFuture<Void> setAsync(long newValue) {
        InternalCompletableFuture future = getAndSetAsync(newValue);
        return future;
    }

    @Override
    public InternalCompletableFuture<Void> alterAsync(IFunction<Long, Long> function) {
        InternalCompletableFuture future = alterAndGetAsync(function);
        return future;
    }

    @Override
    public InternalCompletableFuture<Long> alterAndGetAsync(IFunction<Long, Long> function) {
        Data groupId = getSerializationService().toData(this.groupId);
        Data f = getSerializationService().toData(function);
        ClientMessage request = CPAtomicLongAlterCodec.encodeRequest(groupId, objectName, f, NEW_VALUE.value());
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<Long>(future, getSerializationService(), ALTER_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> getAndAlterAsync(IFunction<Long, Long> function) {
        Data groupId = getSerializationService().toData(this.groupId);
        Data f = getSerializationService().toData(function);
        ClientMessage request = CPAtomicLongAlterCodec.encodeRequest(groupId, objectName, f, OLD_VALUE.value());
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<Long>(future, getSerializationService(), ALTER_DECODER);
    }

    @Override
    public <R> InternalCompletableFuture<R> applyAsync(IFunction<Long, R> function) {
        Data groupId = getSerializationService().toData(this.groupId);
        Data f = getSerializationService().toData(function);
        ClientMessage request = CPAtomicLongApplyCodec.encodeRequest(groupId, objectName, f);
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<R>(future, getSerializationService(), APPLY_DECODER);
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onDestroy() {
        Data groupId = getSerializationService().toData(this.groupId);
        ClientMessage request = CPGroupDestroyCPObjectCodec.encodeRequest(groupId, getServiceName(), objectName);
        new ClientInvocation(getClient(), request, name).invoke().join();
    }

    public CPGroupId getGroupId() {
        return groupId;
    }

}
