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
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import static java.util.Collections.unmodifiableCollection;

/**
 * TODO: Javadoc Pending...
 */
public abstract class BlockingResource<W extends WaitKey> {

    protected final RaftGroupId groupId;
    protected final String name;
    protected final LinkedList<W> waitKeys = new LinkedList<W>();

    protected BlockingResource(RaftGroupId groupId, String name) {
        this.groupId = groupId;
        this.name = name;
    }

    protected Map<Long, Object> invalidateSession(long sessionId) {
        Object expired = new SessionExpiredException();
        Long2ObjectHashMap<Object> result = new Long2ObjectHashMap<Object>();

        Iterator<W> iter = waitKeys.iterator();
        while (iter.hasNext()) {
            W entry = iter.next();
            if (sessionId == entry.sessionId()) {
                result.put(entry.commitIndex(), expired);
                iter.remove();
            }
        }

        onInvalidateSession(sessionId, result);

        return result;
    }

    protected abstract void onInvalidateSession(long sessionId, Long2ObjectHashMap<Object> result);

    protected boolean invalidateWaitKey(W key) {
        Iterator<W> iter = waitKeys.iterator();
        while (iter.hasNext()) {
            W waiter = iter.next();
            if (waiter.equals(key)) {
                iter.remove();
                return true;
            }
        }

        return false;
    }

    protected Collection<W> getWaitKeys() {
        return unmodifiableCollection(waitKeys);
    }
}
