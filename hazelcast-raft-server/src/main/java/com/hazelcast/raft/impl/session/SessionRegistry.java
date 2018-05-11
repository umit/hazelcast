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
import com.hazelcast.util.collection.Long2ObjectHashMap;

/**
 * TODO: Javadoc Pending...
 */
public class SessionRegistry {
    private final RaftGroupId groupId;
    private final Long2ObjectHashMap<Session> sessions = new Long2ObjectHashMap<Session>();
    private long nextSessionId;

    public SessionRegistry(RaftGroupId groupId) {
        this.groupId = groupId;
    }

    public long createNewSession() {
        long id = nextSessionId++;
        Session session = new Session();
        sessions.put(id, session);
        return id;
    }

    public boolean invalidateSession(long sessionId) {
        Session session = sessions.get(sessionId);
        if (session == null) {
            return false;
        }
        sessions.remove(sessionId);
        return true;
    }
}
