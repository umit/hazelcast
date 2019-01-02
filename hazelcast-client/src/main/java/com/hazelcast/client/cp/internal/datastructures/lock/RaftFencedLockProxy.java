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

package com.hazelcast.client.cp.internal.datastructures.lock;

import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.FencedLock;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockOwnershipState;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockService;
import com.hazelcast.cp.internal.datastructures.lock.proxy.AbstractRaftFencedLockProxy;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.nio.Bits;
import com.hazelcast.spi.InternalCompletableFuture;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static com.hazelcast.client.impl.protocol.util.ParameterUtil.calculateDataSize;
import static com.hazelcast.cp.internal.RaftGroupId.dataSize;
import static com.hazelcast.cp.internal.datastructures.lock.client.LockMessageTaskFactoryProvider.DESTROY_TYPE;
import static com.hazelcast.cp.internal.datastructures.lock.client.LockMessageTaskFactoryProvider.FORCE_UNLOCK_TYPE;
import static com.hazelcast.cp.internal.datastructures.lock.client.LockMessageTaskFactoryProvider.LOCK_OWNERSHIP_STATE;
import static com.hazelcast.cp.internal.datastructures.lock.client.LockMessageTaskFactoryProvider.LOCK_TYPE;
import static com.hazelcast.cp.internal.datastructures.lock.client.LockMessageTaskFactoryProvider.TRY_LOCK_TYPE;
import static com.hazelcast.cp.internal.datastructures.lock.client.LockMessageTaskFactoryProvider.UNLOCK_TYPE;

/**
 * Client-side proxy of Raft-based {@link FencedLock} API
 */
class RaftFencedLockProxy extends ClientProxy implements FencedLock {

    private static final ClientMessageDecoder BOOLEAN_RESPONSE_DECODER = new BooleanResponseDecoder();
    private static final ClientMessageDecoder LOCK_OWNERSHIP_STATE_RESPONSE_DECODER = new RaftLockOwnershipStateResponseDecoder();


    private final FencedLockImpl lock;

    RaftFencedLockProxy(ClientContext context, CPGroupId groupId, String proxyName, String objectName) {
        super(RaftLockService.SERVICE_NAME, proxyName, context);
        this.lock = new FencedLockImpl(getClient().getProxySessionManager(), groupId, proxyName, objectName);
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        lock.lockInterruptibly();
    }

    @Override
    public long lockAndGetFence() {
        return lock.lockAndGetFence();
    }

    @Override
    public boolean tryLock() {
        return lock.tryLock();
    }

    @Override
    public long tryLockAndGetFence() {
        return lock.tryLockAndGetFence();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) {
        return lock.tryLock(time, unit);
    }

    @Override
    public long tryLockAndGetFence(long time, TimeUnit unit) {
        return lock.tryLockAndGetFence(time, unit);
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void forceUnlock() {
        lock.forceUnlock();
    }

    @Override
    public long getFence() {
        return lock.getFence();
    }

    @Override
    public boolean isLocked() {
        return lock.isLocked();
    }

    @Override
    public boolean isLockedByCurrentThread() {
        return lock.isLockedByCurrentThread();
    }

    @Override
    public int getLockCountIfLockedByCurrentThread() {
        return lock.getLockCountIfLockedByCurrentThread();
    }

    @Override
    public CPGroupId getGroupId() {
        return lock.getGroupId();
    }

    @Override
    public Condition newCondition() {
        return lock.newCondition();
    }

    @Override
    public void onDestroy() {
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupId.dataSize(lock.getGroupId()) + calculateDataSize(lock.getName());
        ClientMessage msg = prepareClientMessage(lock.getGroupId(), lock.getObjectName(), dataSize, DESTROY_TYPE);
        msg.updateFrameLength();

        invoke(lock.getName(), msg, BOOLEAN_RESPONSE_DECODER).join();
    }

    @Override
    protected void postDestroy() {
        super.postDestroy();
        lock.destroy();
    }

    private <T> InternalCompletableFuture<T> invoke(String name, ClientMessage msg, ClientMessageDecoder decoder) {
        ClientInvocationFuture future = new ClientInvocation(getClient(), msg, name).invoke();
        return new ClientDelegatingFuture<T>(future, getContext().getSerializationService(), decoder);
    }

    private static ClientMessage encodeRequest(int messageTypeId, CPGroupId groupId, String name, long sessionId, long threadId,
                                               UUID invUid) {
        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 4;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, messageTypeId);
        setRequestParams(msg, sessionId, threadId, invUid);
        msg.updateFrameLength();
        return msg;
    }

    private static ClientMessage encodeRequest(int messageTypeId, CPGroupId groupId, String name, long sessionId,
                                               long threadId, UUID invUid, int val) {
        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 4
                + Bits.INT_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, messageTypeId);
        setRequestParams(msg, sessionId, threadId, invUid);
        msg.set(val);
        msg.updateFrameLength();
        return msg;
    }

    private static ClientMessage encodeRequest(int messageTypeId, CPGroupId groupId, String name, long sessionId,
                                       long threadId, UUID invUid, long val) {
        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 5;
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

    private static ClientMessage encodeRequest(int messageTypeId, CPGroupId groupId, String name, long sessionId, long threadId) {
        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 2;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, messageTypeId);
        msg.set(sessionId);
        msg.set(threadId);
        msg.updateFrameLength();
        return msg;
    }

    private static ClientMessage prepareClientMessage(CPGroupId groupId, String name, int dataSize, int messageTypeId) {
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

    private class FencedLockImpl extends AbstractRaftFencedLockProxy {
        FencedLockImpl(AbstractProxySessionManager sessionManager, CPGroupId groupId, String proxyName, String objectName) {
            super(sessionManager, groupId, proxyName, objectName);
        }

        @Override
        protected InternalCompletableFuture<RaftLockOwnershipState> doLock(long sessionId, long threadId, UUID invocationUid) {
            ClientMessage msg = encodeRequest(LOCK_TYPE, groupId, objectName, sessionId, threadId, invocationUid);
            return invoke(objectName, msg, LOCK_OWNERSHIP_STATE_RESPONSE_DECODER);
        }

        @Override
        protected InternalCompletableFuture<RaftLockOwnershipState> doTryLock(long sessionId, long threadId, UUID invocationUid,
                                                                              long timeoutMillis) {
            ClientMessage msg = encodeRequest(TRY_LOCK_TYPE, groupId, objectName, sessionId, threadId, invocationUid,
                    timeoutMillis);
            return invoke(objectName, msg, LOCK_OWNERSHIP_STATE_RESPONSE_DECODER);
        }

        @Override
        protected InternalCompletableFuture<Object> doUnlock(long sessionId, long threadId, UUID invocationUid,
                                                             int releaseCount) {
            ClientMessage msg = encodeRequest(UNLOCK_TYPE, groupId, objectName, sessionId, threadId, invocationUid, releaseCount);
            return invoke(objectName, msg, BOOLEAN_RESPONSE_DECODER);
        }

        @Override
        protected InternalCompletableFuture<Object> doForceUnlock(UUID invocationUid, long expectedFence) {
            ClientMessage msg = encodeRequest(FORCE_UNLOCK_TYPE, groupId, objectName, -1, -1, invocationUid, expectedFence);
            return invoke(objectName, msg, BOOLEAN_RESPONSE_DECODER);
        }

        @Override
        protected InternalCompletableFuture<RaftLockOwnershipState> doGetLockOwnershipState() {
            ClientMessage msg = encodeRequest(LOCK_OWNERSHIP_STATE, groupId, objectName, -1, -1);
            return invoke(objectName, msg, LOCK_OWNERSHIP_STATE_RESPONSE_DECODER);
        }
    }

}
