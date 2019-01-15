/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IFunction;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.atomiclong.RaftAtomicLongService;
import com.hazelcast.nio.Bits;
import com.hazelcast.spi.InternalCompletableFuture;

import static com.hazelcast.client.impl.protocol.util.ParameterUtil.calculateDataSize;
import static com.hazelcast.cp.internal.datastructures.atomiclong.client.AtomicLongMessageTaskFactoryProvider.ADD_AND_GET_TYPE;
import static com.hazelcast.cp.internal.datastructures.atomiclong.client.AtomicLongMessageTaskFactoryProvider.COMPARE_AND_SET_TYPE;
import static com.hazelcast.cp.internal.datastructures.atomiclong.client.AtomicLongMessageTaskFactoryProvider.DESTROY_TYPE;
import static com.hazelcast.cp.internal.datastructures.atomiclong.client.AtomicLongMessageTaskFactoryProvider.GET_AND_ADD_TYPE;
import static com.hazelcast.cp.internal.datastructures.atomiclong.client.AtomicLongMessageTaskFactoryProvider.GET_AND_SET_TYPE;

/**
 * Client-side Raft-based proxy implementation of {@link IAtomicLong}
 */
@SuppressWarnings("checkstyle:methodcount")
class RaftAtomicLongProxy extends ClientProxy implements IAtomicLong {

    private static final ClientMessageDecoder LONG_RESPONSE_DECODER = new LongResponseDecoder();
    private static final ClientMessageDecoder BOOLEAN_RESPONSE_DECODER = new BooleanResponseDecoder();


    private final RaftGroupId groupId;
    private final String objectName;

    RaftAtomicLongProxy(ClientContext context, RaftGroupId groupId, String proxyName, String objectName) {
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
        ClientMessage msg = encodeRequest(groupId, objectName, delta, ADD_AND_GET_TYPE);
        return invoke(msg, LONG_RESPONSE_DECODER);
    }

    @Override
    public InternalCompletableFuture<Boolean> compareAndSetAsync(long expect, long update) {
        ClientMessage msg = encodeRequest(groupId, objectName, expect, update, COMPARE_AND_SET_TYPE);
        return invoke(msg, BOOLEAN_RESPONSE_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> decrementAndGetAsync() {
        return addAndGetAsync(-1);
    }

    @Override
    public InternalCompletableFuture<Long> getAsync() {
        return getAndAddAsync(0);
    }

    @Override
    public InternalCompletableFuture<Long> getAndAddAsync(long delta) {
        ClientMessage msg = encodeRequest(groupId, objectName, delta, GET_AND_ADD_TYPE);
        return invoke(msg, LONG_RESPONSE_DECODER);
    }

    @Override
    public InternalCompletableFuture<Long> getAndSetAsync(long newValue) {
        ClientMessage msg = encodeRequest(groupId, objectName, newValue, GET_AND_SET_TYPE);
        return invoke(msg, LONG_RESPONSE_DECODER);
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
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalCompletableFuture<Long> alterAndGetAsync(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalCompletableFuture<Long> getAndAlterAsync(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> InternalCompletableFuture<R> applyAsync(IFunction<Long, R> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onDestroy() {
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupId.dataSize(groupId) + calculateDataSize(objectName);
        ClientMessage msg = prepareClientMessage(groupId, objectName, dataSize, DESTROY_TYPE);
        msg.updateFrameLength();

        invoke(msg, BOOLEAN_RESPONSE_DECODER).join();
    }

    private <T> InternalCompletableFuture<T> invoke(ClientMessage msg, ClientMessageDecoder decoder) {
        ClientInvocationFuture future = new ClientInvocation(getClient(), msg, name).invoke();
        return new ClientDelegatingFuture<T>(future, getContext().getSerializationService(), decoder);
    }

    private static ClientMessage encodeRequest(RaftGroupId groupId, String name, long value, int messageTypeId) {
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupId.dataSize(groupId) + calculateDataSize(name)
                + Bits.LONG_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, messageTypeId);
        msg.set(value);
        msg.updateFrameLength();
        return msg;
    }

    private static ClientMessage encodeRequest(RaftGroupId groupId, String name, long value1, long value2, int messageTypeId) {
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupId.dataSize(groupId) + calculateDataSize(name)
                + 2 * Bits.LONG_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, messageTypeId);
        msg.set(value1);
        msg.set(value2);
        msg.updateFrameLength();
        return msg;
    }

    private static ClientMessage prepareClientMessage(RaftGroupId groupId, String name, int dataSize, int messageTypeId) {
        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(messageTypeId);
        msg.setRetryable(false);
        msg.setOperationName("");
        RaftGroupId.writeTo(groupId, msg);
        msg.set(name);
        return msg;
    }

    public RaftGroupId getGroupId() {
        return groupId;
    }

    private static class LongResponseDecoder implements ClientMessageDecoder {
        @Override
        public Long decodeClientMessage(ClientMessage msg) {
            return msg.getLong();
        }
    }

    private static class BooleanResponseDecoder implements ClientMessageDecoder {
        @Override
        public Boolean decodeClientMessage(ClientMessage msg) {
            return msg.getBoolean();
        }
    }
}
