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

package com.hazelcast.cp.internal.session;

import com.hazelcast.cp.RaftGroupId;
import com.hazelcast.cp.Session;
import com.hazelcast.cp.internal.util.Tuple2;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.cp.internal.session.RaftSession.toExpirationTime;

/**
 * Maintains active sessions of a Raft group
 */
class RaftSessionRegistry implements IdentifiedDataSerializable {

    private RaftGroupId groupId;
    private final Map<Long, RaftSession> sessions = new ConcurrentHashMap<Long, RaftSession>();
    private long nextSessionId;

    RaftSessionRegistry() {
    }

    RaftSessionRegistry(RaftGroupId groupId) {
        this.groupId = groupId;
    }

    RaftGroupId groupId() {
        return groupId;
    }

    RaftSession getSession(long sessionId) {
        return sessions.get(sessionId);
    }

    long createNewSession(long sessionTTLMs, Address endpoint) {
        long id = ++nextSessionId;
        long creationTime = Clock.currentTimeMillis();
        sessions.put(id, new RaftSession(id, creationTime, toExpirationTime(creationTime, sessionTTLMs), endpoint));
        return id;
    }

    boolean closeSession(long sessionId) {
        return sessions.remove(sessionId) != null;
    }

    boolean expireSession(long sessionId, long expectedVersion) {
        RaftSession session = sessions.get(sessionId);
        if (session == null) {
            return false;
        }

        if (session.version() != expectedVersion) {
            return false;
        }

        sessions.remove(sessionId);
        return true;
    }

    void heartbeat(long sessionId, long sessionTTLMs) {
        RaftSession session = getSessionOrFail(sessionId);
        sessions.put(sessionId, session.heartbeat(sessionTTLMs));
    }

    void shiftExpirationTimes(long durationMs) {
        for (RaftSession session : sessions.values()) {
            sessions.put(session.id(), session.shiftExpirationTime(durationMs));
        }
    }

    // queried locally
    Collection<Tuple2<Long, Long>> getSessionsToExpire() {
        List<Tuple2<Long, Long>> expired = new ArrayList<Tuple2<Long, Long>>();
        long now = Clock.currentTimeMillis();
        for (RaftSession session : sessions.values()) {
            if (session.isExpired(now)) {
                expired.add(Tuple2.of(session.id(), session.version()));
            }
        }

        return expired;
    }

    private RaftSession getSessionOrFail(long sessionId) {
        RaftSession session = sessions.get(sessionId);
        if (session == null) {
            throw new SessionExpiredException();
        }
        return session;
    }

    Collection<? extends Session> getSessions() {
        return sessions.values();
    }

    RaftSessionRegistry cloneForSnapshot() {
        RaftSessionRegistry clone = new RaftSessionRegistry();
        clone.groupId = this.groupId;
        clone.sessions.putAll(this.sessions);
        clone.nextSessionId = this.nextSessionId;

        return clone;
    }

    @Override
    public int getFactoryId() {
        return RaftSessionServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSessionServiceDataSerializerHook.RAFT_SESSION_REGISTRY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(nextSessionId);
        out.writeInt(sessions.size());
        for (RaftSession session : sessions.values()) {
            out.writeLong(session.id());
            out.writeLong(session.creationTime());
            out.writeLong(session.expirationTime());
            out.writeLong(session.version());
            out.writeObject(session.endpoint());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        nextSessionId = in.readLong();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            long id = in.readLong();
            long creationTime = in.readLong();
            long expirationTime = in.readLong();
            long version = in.readLong();
            Address endpoint = in.readObject();
            sessions.put(id, new RaftSession(id, creationTime, expirationTime, version, endpoint));
        }
    }

}
