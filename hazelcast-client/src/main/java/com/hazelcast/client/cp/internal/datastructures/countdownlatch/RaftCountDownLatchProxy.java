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

package com.hazelcast.client.cp.internal.datastructures.countdownlatch;

import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.countdownlatch.RaftCountDownLatchService;
import com.hazelcast.nio.Bits;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.util.UuidUtil;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.impl.protocol.util.ParameterUtil.calculateDataSize;
import static com.hazelcast.cp.internal.RaftGroupId.dataSize;
import static com.hazelcast.cp.internal.datastructures.countdownlatch.client.CountDownLatchMessageTaskFactoryProvider.AWAIT_TYPE;
import static com.hazelcast.cp.internal.datastructures.countdownlatch.client.CountDownLatchMessageTaskFactoryProvider.COUNT_DOWN_TYPE;
import static com.hazelcast.cp.internal.datastructures.countdownlatch.client.CountDownLatchMessageTaskFactoryProvider.DESTROY_TYPE;
import static com.hazelcast.cp.internal.datastructures.countdownlatch.client.CountDownLatchMessageTaskFactoryProvider.GET_REMAINING_COUNT_TYPE;
import static com.hazelcast.cp.internal.datastructures.countdownlatch.client.CountDownLatchMessageTaskFactoryProvider.GET_ROUND_TYPE;
import static com.hazelcast.cp.internal.datastructures.countdownlatch.client.CountDownLatchMessageTaskFactoryProvider.TRY_SET_COUNT_TYPE;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * TODO: Javadoc Pending...
 */
class RaftCountDownLatchProxy extends ClientProxy implements ICountDownLatch {

    private static final ClientMessageDecoder INT_RESPONSE_DECODER = new IntResponseDecoder();
    private static final ClientMessageDecoder BOOLEAN_RESPONSE_DECODER = new BooleanResponseDecoder();

    private final CPGroupId groupId;
    private final String objectName;

    RaftCountDownLatchProxy(ClientContext context, CPGroupId groupId, String proxyName, String objectName) {
        super(RaftCountDownLatchService.SERVICE_NAME, proxyName, context);
        this.groupId = groupId;
        this.objectName = objectName;
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) {
        checkNotNull(unit);

        long timeoutMillis = Math.max(0, unit.toMillis(timeout));

        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(objectName) + Bits.LONG_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, objectName, dataSize, AWAIT_TYPE);
        msg.set(timeoutMillis);
        msg.updateFrameLength();

        return this.<Boolean>invoke(msg, BOOLEAN_RESPONSE_DECODER).join();
    }

    @Override
    public void countDown() {
        int round = getRound();
        UUID invocationUid = UuidUtil.newUnsecureUUID();
        for (;;) {
            try {
                countDown(round, invocationUid);
                return;
            } catch (OperationTimeoutException ignored) {
                // I can retry safely because my retry would be idempotent...
            }
        }
    }

    private int getRound() {
        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(objectName);
        ClientMessage msg = prepareClientMessage(groupId, objectName, dataSize, GET_ROUND_TYPE);
        msg.updateFrameLength();

        return this.<Integer>invoke(msg, INT_RESPONSE_DECODER).join();
    }

    private void countDown(int round, UUID invocationUid) {
        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(objectName) + Bits.INT_SIZE_IN_BYTES
                + Bits.LONG_SIZE_IN_BYTES * 2;
        ClientMessage msg = prepareClientMessage(groupId, objectName, dataSize, COUNT_DOWN_TYPE);
        msg.set(round);
        msg.set(invocationUid.getLeastSignificantBits());
        msg.set(invocationUid.getMostSignificantBits());
        msg.updateFrameLength();

        invoke(msg, INT_RESPONSE_DECODER).join();
    }

    @Override
    public int getCount() {
        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(objectName);
        ClientMessage msg = prepareClientMessage(groupId, objectName, dataSize, GET_REMAINING_COUNT_TYPE);
        msg.updateFrameLength();

        return this.<Integer>invoke(msg, INT_RESPONSE_DECODER).join();
    }

    @Override
    public boolean trySetCount(int count) {
        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(objectName) + Bits.INT_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, objectName, dataSize, TRY_SET_COUNT_TYPE);
        msg.set(count);
        msg.updateFrameLength();
        return this.<Boolean>invoke(msg, BOOLEAN_RESPONSE_DECODER).join();
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    public CPGroupId getGroupId() {
        return groupId;
    }

    @Override
    public void onDestroy() {
        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(objectName);
        ClientMessage msg = prepareClientMessage(groupId, objectName, dataSize, DESTROY_TYPE);
        msg.updateFrameLength();

        invoke(msg, BOOLEAN_RESPONSE_DECODER).join();
    }

    private <T> InternalCompletableFuture<T> invoke(ClientMessage clientMessage, ClientMessageDecoder decoder) {
        ClientInvocationFuture future = new ClientInvocation(getClient(), clientMessage, name).invoke();
        return new ClientDelegatingFuture<T>(future, getContext().getSerializationService(), decoder);
    }

    private ClientMessage prepareClientMessage(CPGroupId groupId, String name, int dataSize, int messageTypeId) {
        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(messageTypeId);
        msg.setRetryable(false);
        msg.setOperationName("");
        RaftGroupId.writeTo(groupId, msg);
        msg.set(name);
        return msg;
    }

    private static class BooleanResponseDecoder implements ClientMessageDecoder {
        @Override
        public Boolean decodeClientMessage(ClientMessage msg) {
            return msg.getBoolean();
        }
    }

    private static class IntResponseDecoder implements ClientMessageDecoder {
        @Override
        public Integer decodeClientMessage(ClientMessage msg) {
            return msg.getInt();
        }
    }

}
