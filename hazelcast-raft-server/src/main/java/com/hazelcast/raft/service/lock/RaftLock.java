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

package com.hazelcast.raft.service.lock;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.util.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
class RaftLock {

    private final RaftGroupId groupId;
    private final String name;

    private LockEndpoint owner;
    private int lockCount;
    private UUID refUid;
    private LinkedList<LockInvocationKey> waiters = new LinkedList<LockInvocationKey>();

    RaftLock(RaftGroupId groupId, String name) {
        this.groupId = groupId;
        this.name = name;
    }

    RaftLock(RaftLockSnapshot snapshot) {
        this.groupId = snapshot.getGroupId();
        this.name = snapshot.getName();
        this.owner = snapshot.getOwner();
        this.lockCount = snapshot.getLockCount();
        this.refUid = snapshot.getRefUid();
        this.waiters.addAll(snapshot.getWaiters());
    }

    boolean acquire(LockEndpoint endpoint, long commitIndex, UUID invocationUid, boolean wait) {
        // if acquire() is being retried
        if (invocationUid.equals(refUid)) {
            return true;
        }
        if (owner == null || endpoint.equals(owner)) {
            owner = endpoint;
            lockCount++;
            refUid = invocationUid;
            return true;
        }
        if (wait) {
            waiters.offer(new LockInvocationKey(name, endpoint, commitIndex, invocationUid));
        }
        return false;
    }

    Collection<LockInvocationKey> release(LockEndpoint endpoint, UUID invocationUuid) {
        return release(endpoint, 1, invocationUuid);
    }

    Collection<LockInvocationKey> release(LockEndpoint endpoint, int releaseCount, UUID invocationUid) {
        // if release() is being retried
        if (invocationUid.equals(refUid)) {
            return Collections.emptyList();
        }

        if (endpoint.equals(owner)) {
            refUid = invocationUid;

            lockCount -= Math.min(releaseCount, lockCount);
            if (lockCount > 0) {
                return Collections.emptyList();
            }

            LockInvocationKey next = waiters.poll();
            if (next != null) {
                List<LockInvocationKey> entries = new ArrayList<LockInvocationKey>();
                entries.add(next);

                Iterator<LockInvocationKey> iter = waiters.iterator();
                while (iter.hasNext()) {
                    LockInvocationKey n = iter.next();
                    if (next.invocationUid.equals(n.invocationUid)) {
                        iter.remove();
                        assert next.endpoint.equals(n.endpoint);
                        entries.add(n);
                    }
                }

                owner = next.endpoint;
                lockCount = 1;
                return entries;
            } else {
                owner = null;
            }

            return Collections.emptyList();
        }

        throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
    }

    List<Long> invalidateWaitEntries(long sessionId) {
        List<Long> commitIndices = new ArrayList<Long>();
        Iterator<LockInvocationKey> iter = waiters.iterator();
        while (iter.hasNext()) {
            LockInvocationKey entry = iter.next();
            if (sessionId == entry.endpoint.sessionId) {
                commitIndices.add(entry.commitIndex);
                iter.remove();
            }
        }

        return commitIndices;
    }

    boolean invalidateWaitEntry(LockInvocationKey key) {
        Iterator<LockInvocationKey> iter = waiters.iterator();
        while (iter.hasNext()) {
            LockInvocationKey waiter = iter.next();
            if (waiter.equals(key)) {
                iter.remove();
                return true;
            }
        }

        return false;
    }

    Tuple2<LockEndpoint, Integer> lockCount() {
        return Tuple2.of(owner, lockCount);
    }

    LockEndpoint owner() {
        return owner;
    }

    RaftLockSnapshot toSnapshot() {
        return new RaftLockSnapshot(groupId, name, owner, lockCount, refUid, waiters);
    }

}
