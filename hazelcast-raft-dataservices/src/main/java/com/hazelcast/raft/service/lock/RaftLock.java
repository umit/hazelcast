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
            return AcquireResult.successful(owner.commitIndex());
        }

        LockInvocationKey key = new LockInvocationKey(name, endpoint, commitIndex, invocationUid);
        if (owner == null) {
            owner = key;
        }

        if (endpoint.equals(owner.endpoint())) {
            lockCount++;
            return AcquireResult.successful(owner.commitIndex());
        }

        Collection<LockInvocationKey> cancelledWaitKeys = cancelWaitKeys(endpoint, invocationUid);

        if (wait) {
            waitKeys.add(key);
        }

        return AcquireResult.failed(cancelledWaitKeys);
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

    ReleaseResult release(LockEndpoint endpoint, UUID invocationUuid) {
        return release(endpoint, 1, invocationUuid);
    }

    private ReleaseResult release(LockEndpoint endpoint, int releaseCount, UUID invocationUid) {
        // if release() is being retried
        if (invocationUid.equals(releaseRefUid)) {
            return ReleaseResult.SUCCESSFUL;
        }

        if (owner != null && endpoint.equals(owner.endpoint())) {
            releaseRefUid = invocationUid;

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

                return ReleaseResult.successful(keys);
            } else {
                owner = null;
            }

            return ReleaseResult.SUCCESSFUL;
        }

        return ReleaseResult.failed(cancelWaitKeys(endpoint, invocationUid));
    }

    ReleaseResult forceRelease(long expectedFence, UUID invocationUid) {
        // if forceRelease() is being retried
        if (invocationUid.equals(releaseRefUid)) {
            return ReleaseResult.SUCCESSFUL;
        }

        if (owner == null) {
            return ReleaseResult.FAILED;
        }

        if (owner.commitIndex() == expectedFence) {
            return release(owner.endpoint(), lockCount, invocationUid);
        }

        return ReleaseResult.FAILED;
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

        private static AcquireResult successful(long fence) {
            return new AcquireResult(fence, Collections.<LockInvocationKey>emptyList());
        }

        private static AcquireResult failed(Collection<LockInvocationKey> cancelled) {
            return new AcquireResult(INVALID_FENCE, cancelled);
        }

        final long fence;

        final Collection<LockInvocationKey> cancelled;

        private AcquireResult(long fence, Collection<LockInvocationKey> cancelled) {
            this.fence = fence;
            this.cancelled = unmodifiableCollection(cancelled);
        }

    }

    static class ReleaseResult {

        static final ReleaseResult FAILED
                = new ReleaseResult(false, Collections.<LockInvocationKey>emptyList());

        private static final ReleaseResult SUCCESSFUL
                = new ReleaseResult(true, Collections.<LockInvocationKey>emptyList());

        private static ReleaseResult successful(Collection<LockInvocationKey> notifications) {
            return new ReleaseResult(true, notifications);
        }

        private static ReleaseResult failed(Collection<LockInvocationKey> notifications) {
            return new ReleaseResult(false, notifications);
        }

        final boolean success;

        final Collection<LockInvocationKey> notifications;

        private ReleaseResult(boolean success, Collection<LockInvocationKey> notifications) {
            this.success = success;
            this.notifications = unmodifiableCollection(notifications);
        }
    }

}
