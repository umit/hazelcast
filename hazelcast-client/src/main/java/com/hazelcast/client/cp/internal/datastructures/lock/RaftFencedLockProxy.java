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

package com.hazelcast.client.cp.internal.datastructures.lock;

import com.hazelcast.client.cp.internal.session.SessionManagerProvider;
import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.RaftGroupId;
import com.hazelcast.cp.internal.RaftGroupIdImpl;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockOwnershipState;
import com.hazelcast.cp.internal.datastructures.lock.proxy.AbstractRaftFencedLockProxy;
import com.hazelcast.cp.internal.datastructures.spi.client.RaftGroupTaskFactoryProvider;
import com.hazelcast.nio.Bits;
import com.hazelcast.spi.InternalCompletableFuture;

import java.util.UUID;

import static com.hazelcast.client.cp.internal.ClientAccessor.getClient;
import static com.hazelcast.client.impl.protocol.util.ParameterUtil.calculateDataSize;
import static com.hazelcast.cp.internal.RaftGroupIdImpl.dataSize;
import static com.hazelcast.cp.internal.RaftService.getObjectNameForProxy;
import static com.hazelcast.cp.internal.datastructures.lock.client.LockMessageTaskFactoryProvider.DESTROY_TYPE;
import static com.hazelcast.cp.internal.datastructures.lock.client.LockMessageTaskFactoryProvider.FORCE_UNLOCK_TYPE;
import static com.hazelcast.cp.internal.datastructures.lock.client.LockMessageTaskFactoryProvider.LOCK_OWNERSHIP_STATE;
import static com.hazelcast.cp.internal.datastructures.lock.client.LockMessageTaskFactoryProvider.LOCK_TYPE;
import static com.hazelcast.cp.internal.datastructures.lock.client.LockMessageTaskFactoryProvider.TRY_LOCK_TYPE;
import static com.hazelcast.cp.internal.datastructures.lock.client.LockMessageTaskFactoryProvider.UNLOCK_TYPE;

/**
 * TODO: Javadoc Pending...
 */
public class RaftFencedLockProxy extends AbstractRaftFencedLockProxy {

    static final ClientMessageDecoder BOOLEAN_RESPONSE_DECODER = new BooleanResponseDecoder();
    static final ClientMessageDecoder LOCK_OWNERSHIP_STATE_RESPONSE_DECODER = new RaftLockOwnershipStateResponseDecoder();

    public static AbstractRaftFencedLockProxy create(HazelcastInstance instance, String name) {
        int dataSize = ClientMessage.HEADER_SIZE + calculateDataSize(name);
        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(RaftGroupTaskFactoryProvider.CREATE_TYPE);
        msg.setRetryable(false);
        msg.setOperationName("");
        msg.set(name);
        msg.updateFrameLength();

        String objectName = getObjectNameForProxy(name);
        HazelcastClientInstanceImpl client = getClient(instance);
        ClientInvocationFuture f = new ClientInvocation(client, msg, objectName).invoke();

        InternalCompletableFuture<RaftGroupId> future = new ClientDelegatingFuture<RaftGroupId>(f, client.getSerializationService(),
                new ClientMessageDecoder() {
                    @Override
                    public RaftGroupId decodeClientMessage(ClientMessage msg) {
                        return RaftGroupIdImpl.readFrom(msg);
                    }
                });

        RaftGroupId groupId = future.join();
        return new RaftFencedLockProxy(instance, groupId, objectName);
    }

    private final HazelcastClientInstanceImpl client;

    private RaftFencedLockProxy(HazelcastInstance instance, RaftGroupId groupId, String name) {
        super(SessionManagerProvider.get(getClient(instance)), groupId, name);
        this.client = getClient(instance);
    }

    @Override
    protected final InternalCompletableFuture<RaftLockOwnershipState> doLock(RaftGroupId groupId, String name,
                                                                       long sessionId, long threadId,
                                                                       UUID invocationUid) {
        ClientMessage msg = encodeRequest(LOCK_TYPE, groupId, name, sessionId, threadId, invocationUid);
        return invoke(client, name, msg, LOCK_OWNERSHIP_STATE_RESPONSE_DECODER);
    }

    @Override
    protected final InternalCompletableFuture<RaftLockOwnershipState> doTryLock(RaftGroupId groupId, String name,
                                                                                long sessionId, long threadId,
                                                                                UUID invocationUid, long timeoutMillis) {
        ClientMessage msg = encodeRequest(TRY_LOCK_TYPE, groupId, name, sessionId, threadId, invocationUid, timeoutMillis);
        return invoke(client, name, msg, LOCK_OWNERSHIP_STATE_RESPONSE_DECODER);
    }

    @Override
    protected final InternalCompletableFuture<Object> doUnlock(RaftGroupId groupId, String name,
                                                               long sessionId, long threadId,
                                                               UUID invocationUid, int releaseCount) {
        ClientMessage msg = encodeRequest(UNLOCK_TYPE, groupId, name, sessionId, threadId, invocationUid, releaseCount);
        return invoke(client, name, msg, BOOLEAN_RESPONSE_DECODER);
    }

    @Override
    protected final InternalCompletableFuture<Object> doForceUnlock(RaftGroupId groupId, String name,
                                                                    UUID invocationUid, long expectedFence) {
        ClientMessage msg = encodeRequest(FORCE_UNLOCK_TYPE, groupId, name, -1, -1, invocationUid, expectedFence);
        return invoke(client, name, msg, BOOLEAN_RESPONSE_DECODER);
    }

    @Override
    protected final InternalCompletableFuture<RaftLockOwnershipState> doGetLockOwnershipState(RaftGroupId groupId,
                                                                                              String name) {
        ClientMessage msg = encodeRequest(LOCK_OWNERSHIP_STATE, groupId, name, -1, -1);
        return invoke(client, name, msg, LOCK_OWNERSHIP_STATE_RESPONSE_DECODER);
    }

    @Override
    public void destroy() {
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupIdImpl.dataSize(groupId) + calculateDataSize(name);
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, DESTROY_TYPE);
        msg.updateFrameLength();

        invoke(client, name, msg, BOOLEAN_RESPONSE_DECODER).join();
    }

    static <T> InternalCompletableFuture<T> invoke(HazelcastClientInstanceImpl client, String name, ClientMessage msg,
                                                   ClientMessageDecoder decoder) {
        ClientInvocationFuture future = new ClientInvocation(client, msg, name).invoke();
        return new ClientDelegatingFuture<T>(future, client.getSerializationService(), decoder);
    }

    static ClientMessage encodeRequest(int messageTypeId, RaftGroupId groupId, String name, long sessionId,
                                       long threadId, UUID invUid) {
        int dataSize = ClientMessage.HEADER_SIZE
                + dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 4;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, messageTypeId);
        setRequestParams(msg, sessionId, threadId, invUid);
        msg.updateFrameLength();
        return msg;
    }

    static ClientMessage encodeRequest(int messageTypeId, RaftGroupId groupId, String name, long sessionId,
                                       long threadId, UUID invUid, int val) {
        int dataSize = ClientMessage.HEADER_SIZE
                + dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 4 + Bits.INT_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, messageTypeId);
        setRequestParams(msg, sessionId, threadId, invUid);
        msg.set(val);
        msg.updateFrameLength();
        return msg;
    }

    static ClientMessage encodeRequest(int messageTypeId, RaftGroupId groupId, String name, long sessionId,
                                       long threadId, UUID invUid, long val) {

        int dataSize = ClientMessage.HEADER_SIZE
                + dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 5;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, messageTypeId);
        setRequestParams(msg, sessionId, threadId, invUid);
        msg.set(val);
        msg.updateFrameLength();
        return msg;
    }

    private static void setRequestParams(ClientMessage msg, long sessionId, long threadId, UUID invUid) {
        msg.set(sessionId);
        msg.set(threadId);
        msg.set(invUid.getLeastSignificantBits());
        msg.set(invUid.getMostSignificantBits());
    }

    static ClientMessage encodeRequest(int messageTypeId, RaftGroupId groupId, String name, long sessionId, long threadId) {
        int dataSize = ClientMessage.HEADER_SIZE
                + dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 2;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, messageTypeId);
        msg.set(sessionId);
        msg.set(threadId);
        msg.updateFrameLength();
        return msg;
    }

    static ClientMessage prepareClientMessage(RaftGroupId groupId, String name, int dataSize, int messageTypeId) {
        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(messageTypeId);
        msg.setRetryable(false);
        msg.setOperationName("");
        RaftGroupIdImpl.writeTo(groupId, msg);
        msg.set(name);
        return msg;
    }

    private static class BooleanResponseDecoder implements ClientMessageDecoder {
        @Override
        public Boolean decodeClientMessage(ClientMessage msg) {
            return msg.getBoolean();
        }
    }

    private static class RaftLockOwnershipStateResponseDecoder implements ClientMessageDecoder {
        @Override
        public RaftLockOwnershipState decodeClientMessage(ClientMessage msg) {
            long fence = msg.getLong();
            int lockCount = msg.getInt();
            long sessionId = msg.getLong();
            long threadId = msg.getLong();
            return new RaftLockOwnershipState(fence, lockCount, sessionId, threadId);
        }
    }

}
