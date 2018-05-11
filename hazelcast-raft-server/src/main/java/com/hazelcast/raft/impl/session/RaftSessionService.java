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
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * TODO: Javadoc Pending...
 */
public class RaftSessionService implements ManagedService, SnapshotAwareService<SessionRegistry> {

    public static String SERVICE_NAME = "hz:core:raftSession";

    private final NodeEngine nodeEngine;
    private volatile RaftService raftService;

    private final Map<RaftGroupId, SessionRegistry> registries = new ConcurrentHashMap<RaftGroupId, SessionRegistry>();

    public RaftSessionService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
        registries.clear();
    }

    @Override
    public SessionRegistry takeSnapshot(RaftGroupId raftGroupId, long commitIndex) {
        return null;
    }

    @Override
    public void restoreSnapshot(RaftGroupId raftGroupId, long commitIndex, SessionRegistry registry) {
    }

    public SessionResponse createNewSession(RaftGroupId groupId) {
        SessionRegistry registry = registries.get(groupId);
        if (registry == null) {
            registry = new SessionRegistry(groupId);
            registries.put(groupId, registry);
        }
        // TODO: schedule expiration
        return new SessionResponse(registry.createNewSession(), getSessionTimeToLiveMillis());
    }

    private long getSessionTimeToLiveMillis() {
        return TimeUnit.SECONDS.toMillis(raftService.getConfig().getSessionTimeToLiveSeconds());
    }

    public boolean invalidateSession(RaftGroupId groupId, long sessionId) {
        SessionRegistry registry = registries.get(groupId);
        if (registry == null) {
            return false;
        }
        if (registry.invalidateSession(sessionId)) {
            notifyServices(groupId, sessionId);
            return true;
        }
        return false;
    }

    private void notifyServices(RaftGroupId groupId, long sessionId) {
        Collection<SessionAwareService> services = nodeEngine.getServices(SessionAwareService.class);
        for (SessionAwareService sessionAwareService : services) {
            sessionAwareService.invalidateSession(groupId, sessionId);
        }
    }
}
