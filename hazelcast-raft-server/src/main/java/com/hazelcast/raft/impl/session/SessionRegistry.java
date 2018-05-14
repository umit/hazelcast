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

package com.hazelcast.raft.impl.session;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TODO: Javadoc Pending...
 */
public class SessionRegistry {

    private final RaftGroupId groupId;
    private final Map<Long, Session> sessions = new ConcurrentHashMap<Long, Session>();
    private long nextSessionId;

    public SessionRegistry(RaftGroupId groupId) {
        this.groupId = groupId;
    }

    public SessionRegistry(RaftGroupId groupId, SessionRegistrySnapshot snapshot) {
        this(groupId);
        this.nextSessionId = snapshot.getNextSessionId();
        for (Session session : snapshot.getSessions()) {
            this.sessions.put(session.id(), session);
        }
    }

    public RaftGroupId groupId() {
        return groupId;
    }

    public Session getSession(long sessionId) {
        return sessions.get(sessionId);
    }

    public long createNewSession(long sessionTTLMs) {
        long id = nextSessionId++;
        long creationTime = Clock.currentTimeMillis();
        Session session = new Session(id, creationTime, toExpirationTime(creationTime, sessionTTLMs));
        sessions.put(id, session);
        return id;
    }

    public boolean closeSession(long sessionId) {
        return sessions.remove(sessionId) != null;
    }

    public void heartbeat(long sessionId, long sessionTTLMs) {
        Session session = getSessionOrFail(sessionId);
        long currentExpirationTime = session.expirationTime();
        long newExpirationTime = toExpirationTime(Clock.currentTimeMillis(), sessionTTLMs);
        if (newExpirationTime > currentExpirationTime) {
            sessions.put(sessionId, new Session(sessionId, session.creationTime(), newExpirationTime));
        }
    }

    public void shiftExpirationTimes(long durationMs) {
        for (Session session : sessions.values()) {
            long newExpirationTime = toExpirationTime(session.expirationTime(), durationMs);
            sessions.put(session.id(), new Session(session.id(), session.creationTime(), newExpirationTime));
        }
    }

    public Collection<Long> getExpiredSessions() {
        List<Long> expired = new ArrayList<Long>();
        long now = Clock.currentTimeMillis();
        for (Session session : sessions.values()) {
            if (session.isExpired(now)) {
                expired.add(session.id());
            }
        }

        return expired;
    }

    public SessionRegistrySnapshot toSnapshot() {
        return new SessionRegistrySnapshot(nextSessionId, sessions.values());
    }

    private Session getSessionOrFail(long sessionId) {
        Session session = sessions.get(sessionId);
        if (session == null) {
            throw new SessionExpiredException(sessionId);
        }
        return session;
    }

    private static long toExpirationTime(long timestamp, long ttlMillis) {
        long expirationTime = timestamp + ttlMillis;
        return expirationTime > 0 ? expirationTime : Long.MAX_VALUE;
    }

}
