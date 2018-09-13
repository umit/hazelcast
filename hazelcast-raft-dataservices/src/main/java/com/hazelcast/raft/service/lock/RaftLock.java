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
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.raft.service.lock.RaftLockService.INVALID_FENCE;
import static java.util.Collections.unmodifiableCollection;

/**
 * TODO: Javadoc Pending...
 */
class RaftLock extends BlockingResource<LockInvocationKey> implements IdentifiedDataSerializable {

    private LockInvocationKey owner;
    private int lockCount;
    private UUID releaseRefUid;

    public RaftLock() {
    }

    RaftLock(RaftGroupId groupId, String name) {
        super(groupId, name);
    }

    @Override
    public Collection<Long> getOwnerSessionIds() {
        return owner != null ? Collections.singleton(owner.sessionId()) : Collections.<Long>emptyList();
    }

    AcquireResult acquire(LockEndpoint endpoint, long commitIndex, UUID invocationUid, boolean wait) {
        // if acquire() is being retried
        if (owner != null && owner.invocationUid().equals(invocationUid)) {
            return AcquireResult.locked(owner.commitIndex());
        }

        LockInvocationKey key = new LockInvocationKey(name, endpoint, commitIndex, invocationUid);
        if (owner == null) {
            owner = key;
        }

        if (endpoint.equals(owner.endpoint())) {
            lockCount++;
            return AcquireResult.locked(owner.commitIndex());
        }

        if (wait) {
            List<LockInvocationKey> cancelledWaitKeys = new ArrayList<LockInvocationKey>();
            Iterator<LockInvocationKey> it = waitKeys.iterator();
            while (it.hasNext()) {
                LockInvocationKey waitKey = it.next();
                if (waitKey.endpoint().equals(endpoint) && !waitKey.invocationUid().equals(invocationUid)) {
                    cancelledWaitKeys.add(waitKey);
                    it.remove();
                }
            }

            waitKeys.add(key);

            return AcquireResult.waitKeysInvalidated(cancelledWaitKeys);
        }

        return AcquireResult.NO_ACQUIRE_NO_WAIT;
    }

    ReleaseResult release(LockEndpoint endpoint, UUID invocationUuid) {
        return release(endpoint, 1, invocationUuid);
    }

    private ReleaseResult release(LockEndpoint endpoint, int releaseCount, UUID invocationUid) {
        // if release() is being retried
        if (invocationUid.equals(releaseRefUid)) {
            return ReleaseResult.RELEASED_NO_NOTIFICATION;
        }

        if (owner != null && endpoint.equals(owner.endpoint())) {
            releaseRefUid = invocationUid;

            lockCount -= Math.min(releaseCount, lockCount);
            if (lockCount > 0) {
                return ReleaseResult.RELEASED_NO_NOTIFICATION;
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

                return ReleaseResult.released(keys);
            } else {
                owner = null;
            }

            return ReleaseResult.RELEASED_NO_NOTIFICATION;
        }

        List<LockInvocationKey> keys = new ArrayList<LockInvocationKey>();
        Iterator<LockInvocationKey> iter = waitKeys.iterator();
        while (iter.hasNext()) {
            LockInvocationKey key = iter.next();
            if (key.endpoint().equals(endpoint)) {
                keys.add(key);
                iter.remove();
            }
        }

        return ReleaseResult.waitKeysInvalidated(keys);
    }

    ReleaseResult forceRelease(long expectedFence, UUID invocationUid) {
        // if forceRelease() is being retried
        if (invocationUid.equals(releaseRefUid)) {
            return ReleaseResult.RELEASED_NO_NOTIFICATION;
        }

        if (owner == null) {
            return ReleaseResult.NOT_RELEASED;
        }

        if (owner.commitIndex() == expectedFence) {
            return release(owner.endpoint(), lockCount, invocationUid);
        }

        return ReleaseResult.NOT_RELEASED;
    }

    int lockCount() {
        return lockCount;
    }

    LockInvocationKey owner() {
        return owner;
    }

    @Override
    protected void onInvalidateSession(long sessionId, Long2ObjectHashMap<Object> responses) {
        if (owner != null && sessionId == owner.endpoint().sessionId()) {
            ReleaseResult result = release(owner.endpoint(), Integer.MAX_VALUE, UuidUtil.newUnsecureUUID());

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
        boolean hasRefUid = (releaseRefUid != null);
        out.writeBoolean(hasRefUid);
        if (hasRefUid) {
            out.writeLong(releaseRefUid.getLeastSignificantBits());
            out.writeLong(releaseRefUid.getMostSignificantBits());
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
        boolean hasRefUid = in.readBoolean();
        if (hasRefUid) {
            long least = in.readLong();
            long most = in.readLong();
            releaseRefUid = new UUID(most, least);
        }
    }

    @Override
    public String toString() {
        return "RaftLock{" + "groupId=" + groupId + ", name='" + name + '\'' + ", owner=" + owner + ", lockCount=" + lockCount
                + ", releaseRefUid=" + releaseRefUid + ", waitKeys=" + waitKeys + '}';
    }

    static class AcquireResult {

        private static final AcquireResult NO_ACQUIRE_NO_WAIT
                = new AcquireResult(INVALID_FENCE, Collections.<LockInvocationKey>emptyList());

        final long fence;
        final Collection<LockInvocationKey> notifications;

        private static AcquireResult locked(long fence) {
            return new AcquireResult(fence, Collections.<LockInvocationKey>emptyList());
        }

        private static AcquireResult waitKeysInvalidated(Collection<LockInvocationKey> notifications) {
            return new AcquireResult(INVALID_FENCE, notifications);
        }

        private AcquireResult(long fence, Collection<LockInvocationKey> notifications) {
            this.fence = fence;
            this.notifications = unmodifiableCollection(notifications);
        }

    }

    static class ReleaseResult {

        static final ReleaseResult NOT_RELEASED
                = new ReleaseResult(false, Collections.<LockInvocationKey>emptyList());

        private static final ReleaseResult RELEASED_NO_NOTIFICATION
                = new ReleaseResult(true, Collections.<LockInvocationKey>emptyList());

        final boolean success;
        final Collection<LockInvocationKey> notifications;

        private static ReleaseResult released(Collection<LockInvocationKey> notifications) {
            return new ReleaseResult(true, notifications);
        }

        private static ReleaseResult waitKeysInvalidated(Collection<LockInvocationKey> notifications) {
            return new ReleaseResult(false, notifications);
        }

        private ReleaseResult(boolean success, Collection<LockInvocationKey> notifications) {
            this.success = success;
            this.notifications = unmodifiableCollection(notifications);
        }
    }

}
