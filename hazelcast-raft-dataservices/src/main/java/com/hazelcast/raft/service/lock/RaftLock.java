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

package com.hazelcast.raft.service.lock;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.blocking.BlockingResource;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.raft.service.lock.RaftLockOwnershipState.NOT_LOCKED;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.util.Collections.unmodifiableCollection;

/**
 * TODO: Javadoc Pending...
 */
class RaftLock extends BlockingResource<LockInvocationKey> implements IdentifiedDataSerializable {

    private LockInvocationKey owner;
    private int lockCount;
    private Map<LockEndpoint, UUID> invocationRefUids = new HashMap<LockEndpoint, UUID>();

    RaftLock() {
    }

    RaftLock(RaftGroupId groupId, String name) {
        super(groupId, name);
    }

    AcquireResult acquire(LockEndpoint endpoint, long commitIndex, UUID invocationUid, boolean wait) {
        // if acquire() is being retried
        if (invocationUid.equals(invocationRefUids.get(endpoint))
                || (owner != null && owner.invocationUid().equals(invocationUid))) {
            RaftLockOwnershipState ownership = new RaftLockOwnershipState(owner.commitIndex(), lockCount, endpoint.sessionId(),
                    endpoint.threadId());
            return AcquireResult.acquired(ownership);
        }

        invocationRefUids.remove(endpoint);

        LockInvocationKey key = new LockInvocationKey(name, endpoint, commitIndex, invocationUid);
        if (owner == null) {
            owner = key;
        }

        if (endpoint.equals(owner.endpoint())) {
            invocationRefUids.put(endpoint, invocationUid);
            lockCount++;
            RaftLockOwnershipState ownership = new RaftLockOwnershipState(owner.commitIndex(), lockCount, endpoint.sessionId(),
                    endpoint.threadId());
            return AcquireResult.acquired(ownership);
        }

        Collection<LockInvocationKey> cancelledWaitKeys = cancelWaitKeys(endpoint, invocationUid);

        if (wait) {
            waitKeys.add(key);
        }

        return AcquireResult.notAcquired(cancelledWaitKeys);
    }

    private Collection<LockInvocationKey> cancelWaitKeys(LockEndpoint endpoint, UUID invocationUid) {
        List<LockInvocationKey> cancelled = new ArrayList<LockInvocationKey>(0);
        Iterator<LockInvocationKey> it = waitKeys.iterator();
        while (it.hasNext()) {
            LockInvocationKey waitKey = it.next();
            if (waitKey.endpoint().equals(endpoint) && !waitKey.invocationUid().equals(invocationUid)) {
                cancelled.add(waitKey);
                it.remove();
            }
        }

        return cancelled;
    }

    ReleaseResult release(LockEndpoint endpoint, UUID invocationUid, int releaseCount) {
        // if release() is being retried
        if (invocationUid.equals(invocationRefUids.get(endpoint))) {
            return ReleaseResult.SUCCESSFUL;
        }

        if (owner != null && endpoint.equals(owner.endpoint())) {
            invocationRefUids.put(endpoint, invocationUid);

            lockCount -= Math.min(releaseCount, lockCount);
            if (lockCount > 0) {
                return ReleaseResult.SUCCESSFUL;
            }

            LockInvocationKey newOwner = waitKeys.poll();
            if (newOwner != null) {
                List<LockInvocationKey> keys = new ArrayList<LockInvocationKey>();
                keys.add(newOwner);

                Iterator<LockInvocationKey> iter = waitKeys.iterator();
                while (iter.hasNext()) {
                    LockInvocationKey key = iter.next();
                    if (newOwner.invocationUid().equals(key.invocationUid())) {
                        assert newOwner.endpoint().equals(key.endpoint());
                        keys.add(key);
                        iter.remove();
                    }
                }

                owner = newOwner;
                lockCount = 1;

                return ReleaseResult.successful(lockOwnershipState(), keys);
            } else {
                owner = null;
            }

            return ReleaseResult.SUCCESSFUL;
        }

        return ReleaseResult.failed(cancelWaitKeys(endpoint, invocationUid));
    }

    ReleaseResult forceRelease(long expectedFence, UUID invocationUid) {
        if (owner == null) {
            return ReleaseResult.FAILED;
        }

        if (owner.commitIndex() == expectedFence) {
            return release(owner.endpoint(), invocationUid, lockCount);
        }

        return ReleaseResult.FAILED;
    }

    RaftLockOwnershipState lockOwnershipState() {
        if (owner == null) {
            return RaftLockOwnershipState.NOT_LOCKED;
        }

        return new RaftLockOwnershipState(owner.commitIndex(), lockCount, owner.sessionId(), owner.endpoint().threadId());
    }

    @Override
    protected void onInvalidateSession(long sessionId, Long2ObjectHashMap<Object> responses) {
        if (owner != null && sessionId == owner.endpoint().sessionId()) {
            Iterator<LockEndpoint> it = invocationRefUids.keySet().iterator();
            while (it.hasNext()) {
                if (it.next().sessionId() == sessionId) {
                    it.remove();
                }
            }

            ReleaseResult result = release(owner.endpoint(), newUnsecureUUID(), Integer.MAX_VALUE);

            if (!result.success) {
                assert result.notifications.isEmpty();
                return;
            }

            Collection<LockInvocationKey> notifications = result.notifications;
            if (notifications.size() > 0) {
                Object newOwnerCommitIndex = notifications.iterator().next().commitIndex();
                for (LockInvocationKey waitKey : notifications) {
                    responses.put(waitKey.commitIndex(), newOwnerCommitIndex);
                }
            }
        }
    }

    @Override
    protected Collection<Long> getOwnerSessions() {
        return owner != null ? Collections.singleton(owner.sessionId()) : Collections.<Long>emptyList();
    }

    @Override
    public int getFactoryId() {
        return RaftLockDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftLockDataSerializerHook.RAFT_LOCK;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        boolean hasOwner = (owner != null);
        out.writeBoolean(hasOwner);
        if (hasOwner) {
            out.writeObject(owner);
        }
        out.writeInt(lockCount);
        out.writeInt(invocationRefUids.size());
        for (Map.Entry<LockEndpoint, UUID> e : invocationRefUids.entrySet()) {
            out.writeObject(e.getKey());
            UUID releaseUid = e.getValue();
            out.writeLong(releaseUid.getLeastSignificantBits());
            out.writeLong(releaseUid.getMostSignificantBits());
        }
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        boolean hasOwner = in.readBoolean();
        if (hasOwner) {
            owner = in.readObject();
        }
        lockCount = in.readInt();
        int releaseRefUidCount = in.readInt();
        for (int i = 0; i < releaseRefUidCount; i++) {
            LockEndpoint endpoint = in.readObject();
            long least = in.readLong();
            long most = in.readLong();
            invocationRefUids.put(endpoint, new UUID(most, least));
        }
    }

    @Override
    public String toString() {
        return "RaftLock{" + "groupId=" + groupId + ", name='" + name + '\'' + ", owner=" + owner + ", lockCount=" + lockCount
                + ", invocationRefUids=" + invocationRefUids + ", waitKeys=" + waitKeys + '}';
    }

    static final class AcquireResult {

        final RaftLockOwnershipState ownership;

        final Collection<LockInvocationKey> cancelled;

        private AcquireResult(RaftLockOwnershipState ownership, Collection<LockInvocationKey> cancelled) {
            this.ownership = ownership;
            this.cancelled = unmodifiableCollection(cancelled);
        }

        private static AcquireResult acquired(RaftLockOwnershipState ownership) {
            return new AcquireResult(ownership, Collections.<LockInvocationKey>emptyList());
        }

        private static AcquireResult notAcquired(Collection<LockInvocationKey> cancelled) {
            return new AcquireResult(NOT_LOCKED, cancelled);
        }

    }

    static final class ReleaseResult {

        static final ReleaseResult FAILED
                = new ReleaseResult(false, null, Collections.<LockInvocationKey>emptyList());

        private static final ReleaseResult SUCCESSFUL
                = new ReleaseResult(true, null, Collections.<LockInvocationKey>emptyList());

        final boolean success;

        final RaftLockOwnershipState ownership;

        final Collection<LockInvocationKey> notifications;

        private ReleaseResult(boolean success, RaftLockOwnershipState ownership, Collection<LockInvocationKey> notifications) {
            this.success = success;
            this.ownership = ownership;
            this.notifications = unmodifiableCollection(notifications);
        }

        private static ReleaseResult successful(RaftLockOwnershipState ownership, Collection<LockInvocationKey> notifications) {
            return new ReleaseResult(true, ownership, notifications);
        }

        private static ReleaseResult failed(Collection<LockInvocationKey> notifications) {
            return new ReleaseResult(false, null, notifications);
        }
    }

}
