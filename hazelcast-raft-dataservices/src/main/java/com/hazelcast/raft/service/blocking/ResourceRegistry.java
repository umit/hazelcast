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

package com.hazelcast.raft.service.blocking;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.util.Clock;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * TODO: Javadoc Pending...
 */
public abstract class ResourceRegistry<W extends WaitKey, R extends BlockingResource<W>> {

    protected final RaftGroupId groupId;
    protected final Map<String, R> resources = new HashMap<String, R>();
    protected final Set<String> destroyedNames = new HashSet<String>();
    // value.element1: timeout duration, value.element2: deadline (transient)
    protected final Map<W, Tuple2<Long, Long>> waitTimeouts
            = new ConcurrentHashMap<W, Tuple2<Long, Long>>();

    protected ResourceRegistry(RaftGroupId groupId) {
        this.groupId = groupId;
    }

    public RaftGroupId groupId() {
        return groupId;
    }

    protected R getOrInitResource(String name) {
        checkNotDestroyed(name);
        R resource = resources.get(name);
        if (resource == null) {
            resource = createNewResource(groupId, name);
            resources.put(name, resource);
        }
        return resource;
    }

    protected abstract R createNewResource(RaftGroupId groupId, String name);

    protected R getResourceOrNull(String name) {
        checkNotDestroyed(name);
        return resources.get(name);
    }

    private void checkNotDestroyed(String name) {
        checkNotNull(name);
        if (destroyedNames.contains(name)) {
            throw new DistributedObjectDestroyedException("Resource[" + name + "] is already destroyed!");
        }
    }

    protected void scheduleWaitTimeout(W key, long timeoutMs) {
        waitTimeouts.put(key, Tuple2.of(timeoutMs, Clock.currentTimeMillis() + timeoutMs));
    }

    public Map<Long, Object> invalidateSession(long sessionId) {
        Long2ObjectHashMap<Object> result = new Long2ObjectHashMap<Object>();
        for (R resource : resources.values()) {
            result.putAll(resource.invalidateSession(sessionId));
        }
        return result;
    }

    public boolean invalidateWaitKey(W key) {
        BlockingResource<W> resource = getResourceOrNull(key.name());
        if (resource == null) {
            return false;
        }

        waitTimeouts.remove(key);
        return resource.invalidateWaitKey(key);
    }

    public Collection<W> getExpiredWaitKeys(long now) {
        List<W> expired = new ArrayList<W>();
        for (Entry<W, Tuple2<Long, Long>> e : waitTimeouts.entrySet()) {
            long deadline = e.getValue().element2;
            if (deadline <= now) {
                expired.add(e.getKey());
            }
        }

        return expired;
    }

    // queried locally in tests
    public Map<W, Tuple2<Long, Long>> getWaitTimeouts() {
        return waitTimeouts;
    }

    public Collection<Long> destroyResource(String name) {
        destroyedNames.add(name);
        BlockingResource<W> resource = resources.remove(name);
        if (resource == null) {
            return null;
        }

        Collection<W> waitKeys = resource.getWaitKeys();
        Collection<Long> indices = new ArrayList<Long>(waitKeys.size());
        for (W key : waitKeys) {
            indices.add(key.commitIndex());
            waitTimeouts.remove(key);
        }
        return indices;
    }

    public Collection<Long> destroy() {
        destroyedNames.addAll(resources.keySet());
        Collection<Long> indices = new ArrayList<Long>();
        for (BlockingResource<W> raftLock : resources.values()) {
            for (W key : raftLock.getWaitKeys()) {
                indices.add(key.commitIndex());
            }
        }
        resources.clear();
        waitTimeouts.clear();
        return indices;
    }
}
