package com.hazelcast.raft.service.lock;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.impl.util.Tuple3;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 *
 */
class RaftLock {

    private final RaftGroupId groupId;
    private final String name;

    private LockEndpoint owner;
    private int lockCount;
    private UUID refUid;
    private LinkedList<Tuple3<LockEndpoint, Long, UUID>> waiters = new LinkedList<Tuple3<LockEndpoint, Long, UUID>>();

    RaftLock(RaftGroupId groupId, String name) {
        this.groupId = groupId;
        this.name = name;
    }

    boolean acquire(LockEndpoint endpoint, long commitIndex, UUID invUid, boolean wait) {
        if (invUid.equals(refUid)) {
            return true;
        }
        if (owner == null || endpoint.equals(owner)) {
            owner = endpoint;
            lockCount++;
            refUid = invUid;
            return true;
        }
        if (wait) {
            waiters.offer(Tuple3.of(endpoint, commitIndex, invUid));
        }
        return false;
    }

    Collection<Long> release(LockEndpoint endpoint, UUID invUid) {
        return release(endpoint, 1, invUid);
    }

    Collection<Long> release(LockEndpoint endpoint, int releaseCount, UUID invUid) {
        if (invUid.equals(refUid)) {
            return Collections.emptyList();
        }
        if (endpoint.equals(owner)) {
            refUid = invUid;

            lockCount -= Math.min(releaseCount, lockCount);
            if (lockCount > 0) {
                return Collections.emptyList();
            }

            Tuple3<LockEndpoint, Long, UUID> next = waiters.poll();
            if (next != null) {
                List<Long> indices = new ArrayList<Long>();
                indices.add(next.element2);

                Iterator<Tuple3<LockEndpoint, Long, UUID>> iter = waiters.iterator();
                while (iter.hasNext()) {
                    Tuple3<LockEndpoint, Long, UUID> n = iter.next();
                    if (next.element3.equals(n.element3)) {
                        iter.remove();
                        assert next.element1.equals(n.element1);
                        indices.add(n.element2);
                    }
                }

                owner = next.element1;
                lockCount = 1;
                return indices;
            } else {
                owner = null;
            }
            return Collections.emptyList();
        }
        throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
    }

    Tuple2<LockEndpoint, Integer> lockCount() {
        return Tuple2.of(owner, lockCount);
    }

    LockEndpoint owner() {
        return owner;
    }
}
