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

package com.hazelcast.client.cp.internal.session;

import com.hazelcast.client.impl.clientside.ClientExceptionFactory;
import com.hazelcast.client.impl.clientside.ClientExceptionFactory.ExceptionFactory;
import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPSessionCloseSessionCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionCreateSessionCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionGenerateThreadIdCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionHeartbeatSessionCodec;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.exception.WaitKeyCancelledException;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.cp.internal.session.SessionExpiredException;
import com.hazelcast.cp.internal.session.SessionResponse;
import com.hazelcast.cp.lock.exception.LockAcquireLimitExceededException;
import com.hazelcast.cp.lock.exception.LockOwnershipLostException;
import com.hazelcast.logging.ILogger;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.CP_GROUP_DESTROYED_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.LOCK_ACQUIRE_LIMIT_EXCEEDED_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.LOCK_OWNERSHIP_LOST_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.SESSION_EXPIRED_EXCEPTION;
import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.WAIT_KEY_CANCELLED_EXCEPTION;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Client-side implementation of Raft proxy session manager
 */
public class ClientProxySessionManager extends AbstractProxySessionManager {

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 60;
    private static final long SHUTDOWN_WAIT_SLEEP_MILLIS = 10;

    private static final ClientMessageDecoder HEARTBEAT_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public Object decodeClientMessage(ClientMessage clientMessage) {
            return null;
        }
    };

    private static final ClientMessageDecoder CLOSE_SESSION_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public Boolean decodeClientMessage(ClientMessage clientMessage) {
            return CPSessionCloseSessionCodec.decodeResponse(clientMessage).response;
        }
    };


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
        factory.register(LOCK_ACQUIRE_LIMIT_EXCEEDED_EXCEPTION, LockAcquireLimitExceededException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new LockAcquireLimitExceededException(message);
            }
        });
        factory.register(LOCK_OWNERSHIP_LOST_EXCEPTION, LockOwnershipLostException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new LockOwnershipLostException(message);
            }
        });
        factory.register(CP_GROUP_DESTROYED_EXCEPTION, CPGroupDestroyedException.class, new ExceptionFactory() {
            @Override
            public Throwable createException(String message, Throwable cause) {
                return new CPGroupDestroyedException();
            }
        });
    }

    @Override
    protected long generateThreadId(RaftGroupId groupId) {
        ClientMessage request = CPSessionGenerateThreadIdCodec.encodeRequest(groupId);
        ClientMessage response = new ClientInvocation(client, request, "sessionManager").invoke().join();

        return CPSessionGenerateThreadIdCodec.decodeResponse(response).response;
    }

    @Override
    protected SessionResponse requestNewSession(RaftGroupId groupId) {
        ClientMessage request = CPSessionCreateSessionCodec.encodeRequest(groupId, client.getName());
        ClientMessage response = new ClientInvocation(client, request, "sessionManager").invoke().join();
        return CPSessionCreateSessionCodec.decodeResponse(response).session;
    }

    @Override
    protected ScheduledFuture<?> scheduleWithRepetition(Runnable task, long period, TimeUnit unit) {
        return client.getClientExecutionService().scheduleWithRepetition(task, period, period, unit);
    }

    @Override
    protected ICompletableFuture<Object> heartbeat(RaftGroupId groupId, long sessionId) {
        ClientMessage request = CPSessionHeartbeatSessionCodec.encodeRequest(groupId, sessionId);
        ClientInvocationFuture future = new ClientInvocation(client, request, "sessionManager").invoke();
        return new ClientDelegatingFuture<Object>(future, client.getSerializationService(), HEARTBEAT_RESPONSE_DECODER);
    }

    @Override
    protected ICompletableFuture<Object> closeSession(RaftGroupId groupId, Long sessionId) {
        ClientMessage request = CPSessionCloseSessionCodec.encodeRequest(groupId, sessionId);
        ClientInvocationFuture future = new ClientInvocation(client, request, "sessionManager").invoke();
        return new ClientDelegatingFuture<Object>(future, client.getSerializationService(), CLOSE_SESSION_RESPONSE_DECODER);
    }

    @Override
    public Map<RaftGroupId, ICompletableFuture<Object>> shutdown() {
        Map<RaftGroupId, ICompletableFuture<Object>> futures = super.shutdown();

        ILogger logger = client.getLoggingService().getLogger(getClass());

        long remainingTimeNanos = TimeUnit.SECONDS.toNanos(SHUTDOWN_TIMEOUT_SECONDS);

        while (remainingTimeNanos > 0) {
            int closed = 0;

            for (Entry<RaftGroupId, ICompletableFuture<Object>> entry : futures.entrySet()) {
                CPGroupId groupId = entry.getKey();
                ICompletableFuture<Object> f = entry.getValue();
                if (f.isDone()) {
                    closed++;
                    try {
                        f.get();
                        logger.fine("Session closed for " + groupId);
                    } catch (Exception e) {
                        logger.warning("Close session failed for " + groupId, e);

                    }
                }
            }

            if (closed == futures.size()) {
                break;
            }

            try {
                Thread.sleep(SHUTDOWN_WAIT_SLEEP_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return futures;
            }

            remainingTimeNanos -= MILLISECONDS.toNanos(SHUTDOWN_WAIT_SLEEP_MILLIS);
        }

        return futures;
    }

}
