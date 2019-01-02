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

package com.hazelcast.client.cp.internal.session;

import com.hazelcast.client.impl.clientside.ClientExceptionFactory;
import com.hazelcast.client.impl.clientside.ClientExceptionFactory.ExceptionFactory;
import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.exception.WaitKeyCancelledException;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.cp.internal.session.SessionExpiredException;
import com.hazelcast.cp.internal.session.SessionResponse;
import com.hazelcast.cp.internal.session.client.SessionMessageTaskFactoryProvider;
import com.hazelcast.nio.Bits;
import com.hazelcast.spi.InternalCompletableFuture;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.SESSION_EXPIRED_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.WAIT_KEY_CANCELLED_EXCEPTION;
import static com.hazelcast.cp.internal.datastructures.semaphore.client.SemaphoreMessageTaskFactoryProvider.GENERATE_THREAD_ID_TYPE;

/**
 * Client-side implementation of Raft proxy session manager
 */
public class ClientProxySessionManager extends AbstractProxySessionManager {

    private static final ClientMessageDecoder SESSION_RESPONSE_DECODER = new SessionResponseDecoder();
    private static final ClientMessageDecoder BOOLEAN_RESPONSE_DECODER = new BooleanResponseDecoder();

    private final HazelcastClientInstanceImpl client;

    public ClientProxySessionManager(HazelcastClientInstanceImpl client) {
        this.client = client;
        ClientExceptionFactory factory = client.getClientExceptionFactory();
        factory.register(SESSION_EXPIRED_EXCEPTION, SessionExpiredException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new SessionExpiredException(message, cause);
            }
        });
        factory.register(WAIT_KEY_CANCELLED_EXCEPTION, WaitKeyCancelledException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new WaitKeyCancelledException(message, cause);
            }
        });
    }

    @Override
    protected long generateThreadId(CPGroupId groupId) {
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupId.dataSize(groupId) + Bits.LONG_SIZE_IN_BYTES;

        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(GENERATE_THREAD_ID_TYPE);
        msg.setRetryable(false);
        msg.setOperationName("");
        RaftGroupId.writeTo(groupId, msg);
        msg.set(System.currentTimeMillis());
        msg.updateFrameLength();

        ClientInvocationFuture future = new ClientInvocation(client, msg, null).invoke();
        return new ClientDelegatingFuture<Long>(future, client.getSerializationService(), new ClientMessageDecoder() {
            @Override
            public Long decodeClientMessage(ClientMessage msg) {
                return msg.getLong();
            }
        }).join();
    }

    @Override
    protected SessionResponse requestNewSession(CPGroupId groupId) {
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupId.dataSize(groupId);
        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(SessionMessageTaskFactoryProvider.CREATE_TYPE);
        msg.setRetryable(false);
        msg.setOperationName("");
        RaftGroupId.writeTo(groupId, msg);
        msg.updateFrameLength();

        InternalCompletableFuture<SessionResponse> future = invoke(msg, SESSION_RESPONSE_DECODER);
        return future.join();
    }

    @Override
    protected ScheduledFuture<?> scheduleWithRepetition(Runnable task, long period, TimeUnit unit) {
        return client.getClientExecutionService().scheduleWithRepetition(task, period, period, unit);
    }

    @Override
    protected ICompletableFuture<Object> heartbeat(CPGroupId groupId, long sessionId) {
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupId.dataSize(groupId) + Bits.LONG_SIZE_IN_BYTES;
        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(SessionMessageTaskFactoryProvider.HEARTBEAT_TYPE);
        msg.setRetryable(false);
        msg.setOperationName("");
        RaftGroupId.writeTo(groupId, msg);
        msg.set(sessionId);
        msg.updateFrameLength();

        return invoke(msg, BOOLEAN_RESPONSE_DECODER);
    }

    @Override
    protected ICompletableFuture<Object> closeSession(CPGroupId groupId, Long sessionId) {
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupId.dataSize(groupId) + Bits.LONG_SIZE_IN_BYTES;
        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(SessionMessageTaskFactoryProvider.CLOSE_SESSION_TYPE);
        msg.setRetryable(false);
        msg.setOperationName("");
        RaftGroupId.writeTo(groupId, msg);
        msg.set(sessionId);
        msg.updateFrameLength();

        return invoke(msg, BOOLEAN_RESPONSE_DECODER);
    }

    private <T> InternalCompletableFuture<T> invoke(ClientMessage msg, ClientMessageDecoder decoder) {
        ClientInvocationFuture future = new ClientInvocation(client, msg, "session").invoke();
        return new ClientDelegatingFuture<T>(future, client.getSerializationService(), decoder);
    }

    private static class BooleanResponseDecoder implements ClientMessageDecoder {
        @Override
        public Boolean decodeClientMessage(ClientMessage msg) {
            return msg.getBoolean();
        }
    }

    private static class SessionResponseDecoder implements ClientMessageDecoder {
        @Override
        public SessionResponse decodeClientMessage(ClientMessage msg) {
            long sessionId = msg.getLong();
            long sessionTTL = msg.getLong();
            long heartbeatInterval = msg.getLong();
            return new SessionResponse(sessionId, sessionTTL, heartbeatInterval);
        }
    }
}
