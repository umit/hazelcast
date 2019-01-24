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

package com.hazelcast.cp.internal.session;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.internal.util.Tuple2;
import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.hazelcast.util.ExceptionUtil.peel;
import static com.hazelcast.util.Preconditions.checkState;
import static com.hazelcast.util.ThreadUtil.getThreadId;

/**
 * Implements session management APIs for Raft-based server and client proxies
 */
public abstract class AbstractProxySessionManager {

    /**
     * Represents absence of a Raft session
     */
    public static final long NO_SESSION_ID = -1;

    private final ConcurrentMap<CPGroupId, Object> mutexes = new ConcurrentHashMap<CPGroupId, Object>();
    private final ConcurrentMap<CPGroupId, SessionState> sessions = new ConcurrentHashMap<CPGroupId, SessionState>();
    private final ConcurrentMap<Tuple2<CPGroupId, Long>, Long> threadIds
            = new ConcurrentHashMap<Tuple2<CPGroupId, Long>, Long>();
    private final AtomicBoolean scheduleHeartbeat = new AtomicBoolean(false);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private boolean running = true;

    /**
     * Generates a cluster-wide unique thread id for the caller
     */
    protected abstract long generateThreadId(CPGroupId groupId);

    /**
     * Creates a new session on the Raft group
     */
    protected abstract SessionResponse requestNewSession(CPGroupId groupId);

    /**
     * Commits a heartbeat for the session on the Raft group
     */
    protected abstract ICompletableFuture<Object> heartbeat(CPGroupId groupId, long sessionId);

    /**
     * Closes the given session on the Raft group
     */
    protected abstract ICompletableFuture<Object> closeSession(CPGroupId groupId, Long sessionId);

    /**
     * Schedules the given task for repeating execution
     */
    protected abstract ScheduledFuture<?> scheduleWithRepetition(Runnable task, long period, TimeUnit unit);

    protected final void resetInternalState() {
        lock.writeLock().lock();
        try {
            mutexes.clear();
            sessions.clear();
            threadIds.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }


    public final Long getOrCreateUniqueThreadId(CPGroupId groupId) {
        lock.readLock().lock();
        try {
            Tuple2<CPGroupId, Long> key = Tuple2.of(groupId, getThreadId());
            Long globalThreadId = threadIds.get(key);
            if (globalThreadId != null) {
                return globalThreadId;
            }

            globalThreadId = generateThreadId(groupId);
            Long existing = threadIds.putIfAbsent(key, globalThreadId);

            return existing != null ? existing : globalThreadId;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Increments acquire count of the session.
     * Creates a new session if there is no session yet.
     */
    public final long acquireSession(CPGroupId groupId) {
        return getOrCreateSession(groupId).acquire(1);
    }

    /**
     * Increments acquire count of the session.
     * Creates a new session if there is no session yet.
     */
    public final long acquireSession(CPGroupId groupId, int count) {
        return getOrCreateSession(groupId).acquire(count);
    }

    /**
     * Decrements acquire count of the session.
     * Returns silently if no session exists for the given id.
     */
    public final void releaseSession(CPGroupId groupId, long id) {
        releaseSession(groupId, id, 1);
    }

    /**
     * Decrements acquire count of the session.
     * Returns silently if no session exists for the given id.
     */
    public final void releaseSession(CPGroupId groupId, long id, int count) {
        SessionState session = sessions.get(groupId);
        if (session != null && session.id == id) {
            session.release(count);
        }
    }

    /**
     * Invalidates the given session.
     * No more heartbeats will be sent for the given session.
     */
    public final void invalidateSession(CPGroupId groupId, long id) {
        SessionState session = sessions.get(groupId);
        if (session != null && session.id == id) {
            sessions.remove(groupId, session);
        }
    }

    /**
     * Returns id of the session opened for the given Raft group.
     * Returns {@link #NO_SESSION_ID} if no session exists.
     */
    public final long getSession(CPGroupId groupId) {
        SessionState session = sessions.get(groupId);
        return session != null ? session.id : NO_SESSION_ID;
    }

    /**
     * Invokes a shutdown call on server to close all existing sessions.
     */
    public Map<CPGroupId, ICompletableFuture<Object>> shutdown() {
        lock.writeLock().lock();
        try {
            Map<CPGroupId, ICompletableFuture<Object>> futures = new HashMap<CPGroupId, ICompletableFuture<Object>>();
            for (Entry<CPGroupId, SessionState> e : sessions.entrySet()) {
                CPGroupId groupId = e.getKey();
                long sessionId = e.getValue().id;
                ICompletableFuture<Object> f = closeSession(groupId, sessionId);
                futures.put(groupId, f);
            }
            sessions.clear();
            running = false;
            return futures;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private SessionState getOrCreateSession(CPGroupId groupId) {
        lock.readLock().lock();
        try {
            checkState(running, "Session manager is already shut down!");

            SessionState session = sessions.get(groupId);
            if (session == null || !session.isValid()) {
                synchronized (mutex(groupId)) {
                    session = sessions.get(groupId);
                    if (session == null || !session.isValid()) {
                        session = createNewSession(groupId);
                    }
                }
            }
            return session;
        } finally {
            lock.readLock().unlock();
        }
    }

    private SessionState createNewSession(CPGroupId groupId) {
        synchronized (mutex(groupId)) {
            SessionResponse response = requestNewSession(groupId);
            SessionState session = new SessionState(response.getSessionId(), response.getTtlMillis());
            sessions.put(groupId, session);
            scheduleHeartbeatTask(response.getHeartbeatMillis());
            return session;
        }
    }

    private Object mutex(CPGroupId groupId) {
        Object mutex = mutexes.get(groupId);
        if (mutex != null) {
            return mutex;
        }
        mutex = new Object();
        Object current = mutexes.putIfAbsent(groupId, mutex);
        return current != null ? current : mutex;
    }

    private void scheduleHeartbeatTask(long heartbeatMillis) {
        if (scheduleHeartbeat.compareAndSet(false, true)) {
            scheduleWithRepetition(new HeartbeatTask(), heartbeatMillis, TimeUnit.MILLISECONDS);
        }
    }

    // For testing
    public final long getSessionAcquireCount(CPGroupId groupId, long sessionId) {
        SessionState session = sessions.get(groupId);
        return session != null && session.id == sessionId ? session.acquireCount.get() : 0;
    }


    private static class SessionState {
        private final long id;
        private final AtomicInteger acquireCount = new AtomicInteger();

        private final long ttlMillis;
        private volatile long accessTime;

        SessionState(long id, long ttlMillis) {
            this.id = id;
            this.accessTime = Clock.currentTimeMillis();
            this.ttlMillis = ttlMillis;
        }

        boolean isValid() {
            return isInUse() || !isExpired(Clock.currentTimeMillis());
        }

        boolean isInUse() {
            return acquireCount.get() > 0;
        }

        private boolean isExpired(long timestamp) {
            long expirationTime = accessTime + ttlMillis;
            if (expirationTime < 0) {
                expirationTime = Long.MAX_VALUE;
            }
            return timestamp > expirationTime;
        }

        long acquire(int count) {
            acquireCount.addAndGet(count);
            return id;
        }

        void release(int count) {
            acquireCount.addAndGet(-count);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof SessionState)) {
                return false;
            }

            SessionState that = (SessionState) o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    private class HeartbeatTask implements Runnable {
        // HeartbeatTask executions will not overlap.
        private final Collection<ICompletableFuture<Object>> prevHeartbeats = new ArrayList<ICompletableFuture<Object>>();

        @Override
        public void run() {
            for (ICompletableFuture<Object> future : prevHeartbeats) {
                future.cancel(true);
            }
            prevHeartbeats.clear();

            for (Entry<CPGroupId, SessionState> entry : sessions.entrySet()) {
                final CPGroupId groupId = entry.getKey();
                final SessionState session = entry.getValue();
                if (session.isInUse()) {
                    ICompletableFuture<Object> f = heartbeat(groupId, session.id);
                    f.andThen(new ExecutionCallback<Object>() {
                        @Override
                        public void onResponse(Object response) {
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            Throwable cause = peel(t);
                            if (cause instanceof SessionExpiredException || cause instanceof CPGroupDestroyedException) {
                                invalidateSession(groupId, session.id);
                            }
                        }
                    });
                    prevHeartbeats.add(f);
                }
            }
        }
    }
}
