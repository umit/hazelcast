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

package com.hazelcast.raft.service.session;

import com.hazelcast.raft.RaftGroupId;

/**
 * TODO: Javadoc Pending...
 *
 */
public abstract class SessionAwareProxy {

    protected final RaftGroupId groupId;
    private final AbstractSessionManager sessionManager;

    protected SessionAwareProxy(AbstractSessionManager sessionManager, RaftGroupId groupId) {
        this.sessionManager = sessionManager;
        this.groupId = groupId;
    }

    protected final long getSession() {
        return sessionManager.getSession(groupId);
    }

    protected final long acquireSession() {
        return sessionManager.acquireSession(groupId);
    }

    protected final long acquireSession(int count) {
        return sessionManager.acquireSession(groupId, count);
    }

    protected final void releaseSession(long sessionId) {
        sessionManager.releaseSession(groupId, sessionId);
    }

    protected final void releaseSession(long sessionId, int count) {
        sessionManager.releaseSession(groupId, sessionId, count);
    }

    protected final void invalidateSession(long sessionId) {
        sessionManager.invalidateSession(groupId, sessionId);
    }

    public final RaftGroupId getGroupId() {
        return groupId;
    }
}
