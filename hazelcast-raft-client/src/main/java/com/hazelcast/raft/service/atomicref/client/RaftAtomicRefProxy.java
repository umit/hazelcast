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

package com.hazelcast.raft.service.atomicref.client;

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftGroupIdImpl;
import com.hazelcast.raft.service.atomicref.RaftAtomicRefService;
import com.hazelcast.raft.service.atomicref.operation.ApplyOp.RETURN_VALUE_TYPE;
import com.hazelcast.util.ExceptionUtil;

import static com.hazelcast.client.impl.protocol.util.ParameterUtil.calculateDataSize;
import static com.hazelcast.raft.service.atomicref.client.AtomicRefMessageTaskFactoryProvider.APPLY_TYPE;
import static com.hazelcast.raft.service.atomicref.client.AtomicRefMessageTaskFactoryProvider.COMPARE_AND_SET_TYPE;
import static com.hazelcast.raft.service.atomicref.client.AtomicRefMessageTaskFactoryProvider.CONTAINS_TYPE;
import static com.hazelcast.raft.service.atomicref.client.AtomicRefMessageTaskFactoryProvider.CREATE_TYPE;
import static com.hazelcast.raft.service.atomicref.client.AtomicRefMessageTaskFactoryProvider.DESTROY_TYPE;
import static com.hazelcast.raft.service.atomicref.client.AtomicRefMessageTaskFactoryProvider.GET_TYPE;
import static com.hazelcast.raft.service.atomicref.client.AtomicRefMessageTaskFactoryProvider.SET_TYPE;
import static com.hazelcast.raft.service.atomicref.operation.ApplyOp.RETURN_VALUE_TYPE.NO_RETURN_VALUE;
import static com.hazelcast.raft.service.atomicref.operation.ApplyOp.RETURN_VALUE_TYPE.RETURN_NEW_VALUE;
import static com.hazelcast.raft.service.atomicref.operation.ApplyOp.RETURN_VALUE_TYPE.RETURN_PREVIOUS_VALUE;
import static com.hazelcast.raft.service.util.ClientAccessor.getClient;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * TODO: Javadoc Pending...
 */
public class RaftAtomicRefProxy<T> implements IAtomicReference<T> {

    private static final ClientMessageDecoder DATA_RESPONSE_DECODER = new DataResponseDecoder();
    private static final ClientMessageDecoder BOOLEAN_RESPONSE_DECODER = new BooleanResponseDecoder();

    public static <T> IAtomicReference<T> create(HazelcastInstance instance, String name) {
        int dataSize = ClientMessage.HEADER_SIZE + calculateDataSize(name);
        ClientMessage clientMessage = ClientMessage.createForEncode(dataSize);
        clientMessage.setMessageType(CREATE_TYPE);
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("");
        clientMessage.set(name);
        clientMessage.updateFrameLength();

        HazelcastClientInstanceImpl client = getClient(instance);
        ClientInvocationFuture f = new ClientInvocation(client, clientMessage, name).invoke();

        ICompletableFuture<RaftGroupId> future = new ClientDelegatingFuture<RaftGroupId>(f, client.getSerializationService(),
                new ClientMessageDecoder() {
                    @Override
                    public RaftGroupId decodeClientMessage(ClientMessage clientMessage) {
                        return RaftGroupIdImpl.readFrom(clientMessage);
                    }
                });

        RaftGroupId groupId = join(future);
        return new RaftAtomicRefProxy<T>(instance, groupId, name);
    }

    private final HazelcastClientInstanceImpl client;
    private final RaftGroupId groupId;
    private final String name;

    private RaftAtomicRefProxy(HazelcastInstance instance, RaftGroupId groupId, String name) {
        client = getClient(instance);
        this.groupId = groupId;
        this.name = name;
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
        join(setAsync(update));
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
        Data expectedData = client.getSerializationService().toData(expect);
        Data newData = client.getSerializationService().toData(update);
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupIdImpl.dataSize(groupId) + calculateDataSize(name)
                + nullableSize(expectedData) + nullableSize(newData);
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, COMPARE_AND_SET_TYPE);
        writeNullableData(msg, expectedData);
        writeNullableData(msg, newData);
        msg.updateFrameLength();

        return invoke(msg, BOOLEAN_RESPONSE_DECODER);
    }

    @Override
    public ICompletableFuture<T> getAsync() {
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupIdImpl.dataSize(groupId) + calculateDataSize(name);
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, GET_TYPE);
        msg.updateFrameLength();

        return invoke(msg, DATA_RESPONSE_DECODER);
    }

    @Override
    public ICompletableFuture<Void> setAsync(T newValue) {
        Data data = client.getSerializationService().toData(newValue);
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupIdImpl.dataSize(groupId) + calculateDataSize(name)
                + nullableSize(data) + Bits.BOOLEAN_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, SET_TYPE);
        writeNullableData(msg, data);
        msg.set(false);
        msg.updateFrameLength();

        return invoke(msg, DATA_RESPONSE_DECODER);
    }

    @Override
    public ICompletableFuture<T> getAndSetAsync(T newValue) {
        Data data = client.getSerializationService().toData(newValue);
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupIdImpl.dataSize(groupId) + calculateDataSize(name)
                + nullableSize(data) + Bits.BOOLEAN_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, SET_TYPE);
        writeNullableData(msg, data);
        msg.set(true);
        msg.updateFrameLength();

        return invoke(msg, DATA_RESPONSE_DECODER);
    }

    @Override
    public ICompletableFuture<Boolean> isNullAsync() {
        return containsAsync(null);
    }

    @Override
    public ICompletableFuture<Void> clearAsync() {
        return setAsync(null);
    }

    @Override
    public ICompletableFuture<Boolean> containsAsync(T expected) {
        Data data = client.getSerializationService().toData(expected);
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupIdImpl.dataSize(groupId) + calculateDataSize(name)
                + nullableSize(data) + Bits.BOOLEAN_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, CONTAINS_TYPE);
        writeNullableData(msg, data);
        msg.set(false);
        msg.updateFrameLength();

        return invoke(msg, BOOLEAN_RESPONSE_DECODER);
    }

    @Override
    public ICompletableFuture<Void> alterAsync(IFunction<T, T> function) {
        return invokeApply(function, NO_RETURN_VALUE, true);
    }

    @Override
    public ICompletableFuture<T> alterAndGetAsync(IFunction<T, T> function) {
        return invokeApply(function, RETURN_NEW_VALUE, true);
    }

    @Override
    public ICompletableFuture<T> getAndAlterAsync(IFunction<T, T> function) {
        return invokeApply(function, RETURN_PREVIOUS_VALUE, true);
    }

    @Override
    public <R> ICompletableFuture<R> applyAsync(IFunction<T, R> function) {
        return invokeApply(function, RETURN_NEW_VALUE, false);
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
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void destroy() {
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupIdImpl.dataSize(groupId) + calculateDataSize(name);
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, DESTROY_TYPE);
        msg.updateFrameLength();

        join(invoke(msg, BOOLEAN_RESPONSE_DECODER));
    }

    public RaftGroupId getGroupId() {
        return groupId;
    }

    private int nullableSize(Data data) {
        return Bits.BOOLEAN_SIZE_IN_BYTES + (data != null ? (Bits.INT_SIZE_IN_BYTES + data.totalSize()) : 0);
    }

    private void writeNullableData(ClientMessage msg, Data data) {
        boolean exists = (data != null);
        msg.set(exists);
        if (exists) {
            msg.set(data);
        }
    }

    private <T2, T3> ICompletableFuture<T3> invokeApply(IFunction<T, T2> function, RETURN_VALUE_TYPE returnValueType, boolean alter) {
        checkTrue(function != null, "Function cannot be null");
        Data data = client.getSerializationService().toData(function);
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupIdImpl.dataSize(groupId) + calculateDataSize(name)
                + nullableSize(data) + calculateDataSize(returnValueType.name()) + Bits.BOOLEAN_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, APPLY_TYPE);
        writeNullableData(msg, data);
        msg.set(returnValueType.name());
        msg.set(alter);
        msg.updateFrameLength();

        return invoke(msg, DATA_RESPONSE_DECODER);
    }

    private <T> ICompletableFuture<T> invoke(ClientMessage clientMessage, ClientMessageDecoder decoder) {
        ClientInvocationFuture future = new ClientInvocation(client, clientMessage, getName()).invoke();
        return new ClientDelegatingFuture<T>(future, client.getSerializationService(), decoder);
    }

    private static <T> T join(ICompletableFuture<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private static ClientMessage prepareClientMessage(RaftGroupId groupId, String name, int dataSize, int messageTypeId) {
        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(messageTypeId);
        msg.setRetryable(false);
        msg.setOperationName("");
        RaftGroupIdImpl.writeTo(groupId, msg);
        msg.set(name);
        return msg;
    }

    private static class DataResponseDecoder implements ClientMessageDecoder {
        @Override
        public Data decodeClientMessage(ClientMessage msg) {
            boolean exists = msg.getBoolean();
            return exists ? msg.getData() : null;
        }
    }

    private static class BooleanResponseDecoder implements ClientMessageDecoder {
        @Override
        public Boolean decodeClientMessage(ClientMessage msg) {
            return msg.getBoolean();
        }
    }

}
