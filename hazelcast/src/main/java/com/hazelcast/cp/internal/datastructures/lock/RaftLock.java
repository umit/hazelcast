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

package com.hazelcast.cp.internal.datastructures.lock;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.spi.blocking.BlockingResource;
import com.hazelcast.cp.internal.datastructures.spi.blocking.WaitKeyContainer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import static com.hazelcast.cp.internal.datastructures.lock.RaftLockOwnershipState.NOT_LOCKED;
import static com.hazelcast.cp.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.cp.internal.util.UUIDSerializationUtil.writeUUID;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.util.Collections.unmodifiableCollection;

/**
 * State-machine implementation of the Raft-based lock
 */
class RaftLock extends BlockingResource<LockInvocationKey> implements IdentifiedDataSerializable {

    /**
     * Current owner of the lock
     */
    private LockInvocationKey owner;

    /**
     * Number of acquires the current lock owner has committed
     */
    private int lockCount;

    /**
     * For each LockEndpoint, uid of its last invocation.
     * Used for preventing duplicate execution of lock / unlock requests.
     */
    private Map<LockEndpoint, UUID> invocationRefUids = new HashMap<LockEndpoint, UUID>();

    RaftLock() {
    }

    RaftLock(CPGroupId groupId, String name) {
        super(groupId, name);
    }

    /**
     * Assigns the lock to the endpoint, if the lock is not held. Lock count is
     * incremented if the endpoint already holds the lock. If some other
     * endpoint holds the lock and the second argument is true, a wait key is
     * created and added to the wait queue. Lock count is not incremented if
     * the lock request is a retry of the lock holder. If the lock request is
     * a retry of a lock endpoint that resides in the wait queue with the same
     * invocation uid, a duplicate wait key is added to the wait queue because
     * cancelling the previous wait key can cause the caller to fail.
     * If the lock request is a new request of a lock endpoint that resides
     * in the wait queue with a different invocation uid, the existing wait key
     * is cancelled because it means the caller has stopped waiting for
     * response of the previous invocation.
     */
    AcquireResult acquire(long commitIndex, LockEndpoint endpoint, UUID invocationUid, boolean wait) {
        // if lock() is being retried
        if (invocationUid.equals(invocationRefUids.get(endpoint))
                || (owner != null && owner.invocationUid().equals(invocationUid))) {
            return AcquireResult.acquired(lockOwnershipState());
        }

        invocationRefUids.remove(endpoint);

        LockInvocationKey key = new LockInvocationKey(endpoint, commitIndex, invocationUid);
        if (owner == null) {
            owner = key;
        }

        if (endpoint.equals(owner.endpoint())) {
            invocationRefUids.put(endpoint, invocationUid);
            lockCount++;
            return AcquireResult.acquired(lockOwnershipState());
        }

        Collection<LockInvocationKey> cancelledWaitKeys = cancelWaitKeys(endpoint, invocationUid);

        if (wait) {
            addWaitKey(endpoint, key);
        }

        return AcquireResult.notAcquired(cancelledWaitKeys);
    }

    private Collection<LockInvocationKey> cancelWaitKeys(LockEndpoint endpoint, UUID invocationUid) {
        List<LockInvocationKey> cancelled = null;
        WaitKeyContainer<LockInvocationKey> container = waitKeys.get(endpoint);
        if (container != null && container.key().isDifferentInvocationOf(endpoint, invocationUid)) {
            cancelled = container.keyAndRetries();
            waitKeys.remove(endpoint);
        }

        return cancelled != null ? cancelled : Collections.<LockInvocationKey>emptyList();
    }

    /**
     * Releases the lock with the given release count. The lock is freed if
     * release count > lock count. If the remaining lock count > 0 after
     * a successful release, the lock is still held by the endpoint.
     * The lock is not released if it is a retry of a previous successful
     * release request of the current lock holder. If the lock is assigned to
     * some other endpoint after this release, wait keys of the new lock holder
     * are returned. If the release request fails because the requesting
     * endpoint does not hold the lock, all wait keys of the endpoint are
     * cancelled because that endpoint has stopped waiting for response of
     * the previous lock() invocation.
     */
    ReleaseResult release(LockEndpoint endpoint, UUID invocationUid, int releaseCount) {
        // if unlock() is being retried
        if (invocationUid.equals(invocationRefUids.get(endpoint))) {
            return ReleaseResult.successful(lockOwnershipState());
        }

        if (owner != null && endpoint.equals(owner.endpoint())) {
            invocationRefUids.put(endpoint, invocationUid);

            lockCount -= Math.min(releaseCount, lockCount);
            if (lockCount > 0) {
                return ReleaseResult.successful(lockOwnershipState());
            }

            List<LockInvocationKey> keys;
            Iterator<Entry<Object, WaitKeyContainer<LockInvocationKey>>> iter = waitKeys.entrySet().iterator();
            if (iter.hasNext()) {
                WaitKeyContainer<LockInvocationKey> container = iter.next().getValue();
                LockInvocationKey newOwner = container.key();
                keys = container.keyAndRetries();

                iter.remove();
                owner = newOwner;
                lockCount = 1;
            } else {
                owner = null;
                keys = Collections.emptyList();
            }

            return ReleaseResult.successful(lockOwnershipState(), keys);
        }

        return ReleaseResult.failed(cancelWaitKeys(endpoint, invocationUid));
    }

    /**
     * Releases the lock if it is current held by the given expected fencing
     * token.
     */
    ReleaseResult forceRelease(long expectedFence, UUID invocationUid) {
        if (owner != null && owner.commitIndex() == expectedFence) {
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

    RaftLock cloneForSnapshot() {
        RaftLock clone = new RaftLock();
        clone.groupId = this.groupId;
        clone.name = this.name;
        clone.waitKeys.putAll(this.waitKeys);
        clone.owner = this.owner;
        clone.lockCount = this.lockCount;
        clone.invocationRefUids.putAll(this.invocationRefUids);

        return clone;
    }

    /**
     * Releases the lock if the current lock holder's session is closed.
     */
    @Override
    protected void onSessionClose(long sessionId, Map<Long, Object> responses) {
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

            for (LockInvocationKey waitKey : result.notifications) {
                responses.put(waitKey.commitIndex(), result.ownership);
            }
        }
    }

    /**
     * Returns session id of the current lock holder or an empty collection if
     * the lock is not held
     */
    @Override
    protected Collection<Long> getActivelyAttachedSessions() {
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
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        boolean hasOwner = (owner != null);
        out.writeBoolean(hasOwner);
        if (hasOwner) {
            out.writeObject(owner);
        }
        out.writeInt(lockCount);
        out.writeInt(invocationRefUids.size());
        for (Entry<LockEndpoint, UUID> e : invocationRefUids.entrySet()) {
            out.writeObject(e.getKey());
            writeUUID(out, e.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        boolean hasOwner = in.readBoolean();
        if (hasOwner) {
            owner = in.readObject();
        }
        lockCount = in.readInt();
        int invocationRefUidCount = in.readInt();
        for (int i = 0; i < invocationRefUidCount; i++) {
            LockEndpoint endpoint = in.readObject();
            UUID invocationUid = readUUID(in);
            invocationRefUids.put(endpoint, invocationUid);
        }
    }

    @Override
    public String toString() {
        return "RaftLock{" + "groupId=" + groupId + ", name='" + name + '\'' + ", owner=" + owner + ", lockCount=" + lockCount
                + ", invocationRefUids=" + invocationRefUids + ", waitKeys=" + waitKeys + '}';
    }

    /**
     * Represents result of a lock() request
     */
    static final class AcquireResult {

        /**
         * If the lock() request is successful, represents new state of the lock ownership.
         * It is {@link RaftLockOwnershipState#NOT_LOCKED} otherwise.
         */
        final RaftLockOwnershipState ownership;

        /**
         * If new a lock() request is send while there are pending wait keys of a previous lock() request,
         * pending wait keys are cancelled. It is because LockEndpoint is a single-threaded entity and
         * a new lock() request implies that the LockEndpoint is no longer interested in its previous lock() call.
         */
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

    /**
     * Represents result of a unlock() request
     */
    static final class ReleaseResult {

        static final ReleaseResult FAILED
                = new ReleaseResult(false, NOT_LOCKED, Collections.<LockInvocationKey>emptyList());

        /**
         * true if the unlock() request is successful
         */
        final boolean success;

        /**
         * If the unlock() request is successful, represents new state of the lock ownership.
         * It can be {@link RaftLockOwnershipState#NOT_LOCKED} if the lock has no new owner after successful release.
         * It is {@link RaftLockOwnershipState#NOT_LOCKED} if the unlock() request is failed.
         */
        final RaftLockOwnershipState ownership;

        /**
         * If the unlock() request is successful and ownership is given to some other endpoint, contains its wait keys.
         * If the unlock() request is failed, can contain cancelled wait keys of the caller, if there is any.
         */
        final Collection<LockInvocationKey> notifications;

        private ReleaseResult(boolean success, RaftLockOwnershipState ownership, Collection<LockInvocationKey> notifications) {
            this.success = success;
            this.ownership = ownership;
            this.notifications = unmodifiableCollection(notifications);
        }

        private static ReleaseResult successful(RaftLockOwnershipState ownership) {
            return new ReleaseResult(true, ownership, Collections.<LockInvocationKey>emptyList());
        }

        private static ReleaseResult successful(RaftLockOwnershipState ownership, Collection<LockInvocationKey> notifications) {
            return new ReleaseResult(true, ownership, notifications);
        }

        private static ReleaseResult failed(Collection<LockInvocationKey> notifications) {
            return new ReleaseResult(false, NOT_LOCKED, notifications);
        }
    }

}
