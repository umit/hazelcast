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

package com.hazelcast.cp;

import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.DistributedObject;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * A reentrant & linearizable & distributed implementation of {@link Lock}.
 * <p>
 * {@link FencedLock} is accessed via {@link CPSubsystem#getLock(String)}.
 * <p>
 * {@link FencedLock} is CP with respect to the CAP principle. It works on top
 * of the Raft consensus algorithm. It offers linearizability during crash
 * failures and network partitions. If a network partition occurs, it remains
 * available on at most one side of the partition.
 * <p>
 * {@link FencedLock} works on top of CP sessions. Please see {@link CPSession}
 * for more information about CP sessions.
 * <p>
 * Distributed locks are unfortunately NOT EQUIVALENT to single-node mutexes
 * because of the complexities in distributed systems, such as uncertain
 * communication patterns and independent process failures. In an asynchronous
 * network, no lock service can guarantee mutual exclusion, because there is
 * no way to distinguish between a slow and a crashed process.
 * Consider the following scenario, where a Hazelcast client acquires
 * a {@link FencedLock}, then hits a long GC pause. Since it will not be able
 * to commit session heartbeats while paused, its CP session will be eventually
 * closed. After this moment, another Hazelcast client can acquire this lock.
 * If the first client wakes up again, it may not immediately notice that it
 * has lost ownership of the lock. In this case, multiple clients think they
 * hold the lock. If they attempt to perform an operation on a shared resource,
 * they can break the system. To prevent such situations, one may choose to use
 * an infinite session timeout, but this time probably she is going to deal
 * with liveliness issues. Even if the first client crashes, requests sent by
 * 2 clients can be re-ordered in the network and hit the external resource in
 * reverse order.
 * <p>
 * There is a simple solution for this problem. Lock holders are ordered by a
 * monotonic fencing token, which increments each time the lock is assigned to
 * a new owner. This fencing token can be passed to external services or
 * resources to ensure sequential execution of side effects performed by lock
 * holders.
 * <p>
 * The following figure illustrates the idea. In this figure, Client-1 acquires
 * the lock first and receives 1 as its fencing token. Then, it passes this
 * token to the external service, which is our shared resource in this scenario.
 * Just after that, Client-1 hits a long GC pause and eventually loses
 * ownership of the lock because it misses to commit CP session heartbeats.
 * Then, Client-2 chimes in and acquires the lock. Similar to Client-1,
 * Client-2 passes its fencing token to the external service as well. After
 * this moment, once Client-1 comes back alive, its write request will be
 * rejected by the external service, and Client-2 will be able to safely talk
 * to the external service.
 * <p>
 *                                                       CLIENT-1's session is expired.
 *                                                                    |
 * |------------------|               LOCK is acquired by CLIENT-1.   |     LOCK is acquired by CLIENT-2.
 * |       LOCK       | . . . . . . . - - - - - - - - - - - - - - - - | . . + + + + + + + + + + + + + + + + + + + + + + + + + + +
 * |------------------|             /\ \ fence = 1                    |   /| \ fence = 2
 *                                 /    \                                /    \
 * |------------------|           /      \                              /      \          CLIENT-1 wakes up.
 * |     CLIENT-1     | . . . . ./. . . . \/. . . _ _ _ _ _ _ _ _ _ _  /_ _ _ _ \ _ _ _ _ . . . . . . . . . . . . . . . . . . . .
 * |------------------|    lock()            \   CLIENT-1 is paused.  /          \    write(A) \
 *                               set_fence(1) \                      /            \             \
 * |------------------|                        \                    /              \             \
 * |     CLIENT-2     | . . . . . . . . . . . . \ . . . . . . . . ./. . . . . . . . \/. . . . . . \ . . . . . . . . . . . . . . .
 * |------------------|                          \           lock()                    \           \      write(B) \
 *                                                \                        set_fence(2) \           \               \
 * |------------------|                            \                                     \           \               \
 * | EXTERNAL SERVICE | . . . . . . . . . . . . . . \/  - - - - - - - - - - - - - - - - - \/  + + + + \/  + + + + + + \/  + + + +
 * |------------------|                                                                         write(A) fails.    write(B) ok.
 *                                                       SERVICE belongs to CLIENT-1.         SERVICE belongs to CLIENT-2.
 * <p>
 * You can read more about the idea in Martin Kleppmann's
 * "How to do distributed locking" blog post and Google's Chubby paper.
 * {@link FencedLock} integrates this fencing token idea with the good old
 * {@link Lock} abstraction.
 * <p>
 * All of the API methods in the new {@link FencedLock} impl offer
 * the exactly-once execution semantics. For instance, even if
 * a {@link #lock()} call is internally retried because of a crashed
 * Hazelcast member, the lock is acquired only once. The same rule
 * also applies to the other methods in the API.
 */
public interface FencedLock extends Lock, DistributedObject {

    /**
     * Representation of a failed lock attempt where
     * the caller thread has not acquired the lock
     */
    long INVALID_FENCE = 0L;

    /**
     * Acquires the lock.
     * <p>
     * If the lock is not available then the current thread becomes disabled
     * for thread scheduling purposes and lies dormant until the lock has been
     * acquired.
     * <p>
     * Consider the following scenario:
     * <pre>
     *     FencedLock lock = ...;
     *     lock.lock();
     *     // JVM of the caller thread hits a long pause
     *     // and its CP session is closed on the CP group.
     *     lock.lock();
     * </pre>
     * In this scenario, a thread acquires the lock, then its JVM instance
     * encounters a long pause, which is longer than
     * {@link CPSubsystemConfig#getSessionTimeToLiveSeconds()}. In this case,
     * its CP session will be closed on the corresponding CP group because
     * it could not commit session heartbeats in the meantime. After the JVM
     * instance wakes up again, the same thread attempts to acquire the lock
     * reentrantly. Before the second lock attempt, if the proxy notices that
     * its CP session is closed, then the second lock() call fails by throwing
     * {@link IllegalMonitorStateException}. However, this is not guaranteed
     * because CP session heartbeats are committed periodically and the second
     * lock attempt could occur before the proxy notices termination of its
     * session on the next session heartbeat commit.
     *
     * @throws IllegalMonitorStateException if the underlying CP session is
     *         closed while locking reentrantly
     */
    void lock();

    /**
     * Acquires the lock unless the current thread is
     * {@linkplain Thread#interrupt interrupted}.
     * <p>
     * If the lock is not available then the current thread becomes disabled
     * for thread scheduling purposes and lies dormant until the lock has been
     * acquired. Interruption may not be possible after the lock request
     * arrives to the CP group, if the proxy does not attempt to retry its
     * lock request because of a failure in the system.
     * <p>
     * Please note that even if {@link InterruptedException} is thrown,
     * the lock may be acquired on the CP group.
     * <p>
     * When {@link InterruptedException} is thrown, the current thread's
     * interrupted status is cleared.
     * <p>
     * Consider the following scenario:
     * <pre>
     *     FencedLock lock = ...;
     *     lock.lockInterruptibly();
     *     // JVM of the caller thread hits a long pause
     *     // and its CP session is closed on the CP group.
     *     lock.lockInterruptibly();
     * </pre>
     * In this scenario, a thread acquires the lock, then its JVM instance
     * encounters a long pause, which is longer than
     * {@link CPSubsystemConfig#getSessionTimeToLiveSeconds()}. In this case,
     * its CP session will be closed on the corresponding CP group because
     * it could not commit session heartbeats in the meantime. After the JVM
     * instance wakes up again, the same thread attempts to acquire the lock
     * reentrantly. Before the second lock attempt, if the proxy notices that
     * its CP session is closed, then the second lock() call fails by throwing
     * {@link IllegalMonitorStateException}. However, this is not guaranteed
     * because CP session heartbeats are committed periodically and the second
     * lock attempt could occur before the proxy notices termination of its
     * session on the next session heartbeat commit.
     *
     * @throws InterruptedException if the current thread is interrupted while
     *         acquiring the lock.
     *
     * @throws IllegalMonitorStateException if the underlying CP session is
     *         closed while locking reentrantly
     */
    void lockInterruptibly() throws InterruptedException;

    /**
     * Acquires the lock and returns the fencing token assigned to the current
     * thread for this lock acquire. If the lock is acquired reentrantly,
     * the same fencing token is returned.
     * <p>
     * This is a convenience method for the following pattern:
     * <pre>
     *     FencedLock lock = ...;
     *     lock.lock();
     *     return lock.getFence();
     * </pre>
     * <p>
     * If the lock is not available then the current thread becomes disabled
     * for thread scheduling purposes and lies dormant until the lock has been
     * acquired.
     * <p>
     * Consider the following scenario where the lock is free initially:
     * <pre>
     *     FencedLock lock = ...; // the lock is free
     *     lock.lockAndGetFence();
     *     // JVM of the caller thread hits a long pause
     *     // and its CP session is closed on the CP group.
     *     lock.lockAndGetFence();
     * </pre>
     * In this scenario, a thread acquires the lock, then its JVM instance
     * encounters a long pause, which is longer than
     * {@link CPSubsystemConfig#getSessionTimeToLiveSeconds()}. In this case,
     * its CP session will be closed on the corresponding CP group because
     * it could not commit session heartbeats in the meantime. After the JVM
     * instance wakes up again, the same thread attempts to acquire the lock
     * reentrantly. Before the second lock attempt, if the proxy notices that
     * its CP session is closed, then the second lock() call fails by throwing
     * {@link IllegalMonitorStateException}. However, this is not guaranteed
     * because CP session heartbeats are committed periodically and the second
     * lock attempt could occur before the proxy notices termination of its
     * session on the next session heartbeat commit.
     * <p>
     * Fencing tokens are monotonic numbers that are incremented each time
     * the lock switches from the not-acquired state to the acquired state.
     * They are simply used for ordering lock holders. A lock holder can pass
     * its fencing to the shared resource to fence off previous lock holders.
     * When this resource receives an operation, it can validate the fencing
     * token in the operation.
     * <p>
     * Consider the following scenario where the lock is free initially:
     * <pre>
     *     FencedLock lock = ...; // the lock is free
     *     long fence1 = lock.lockAndGetFence(); // (1)
     *     long fence2 = lock.lockAndGetFence(); // (2)
     *     assert fence1 == fence2;
     *     lock.unlock();
     *     lock.unlock();
     *     long fence3 = lock.lockAndGetFence(); // (3)
     *     assert fence3 > fence1;
     * </pre>
     * In this scenario, the lock is acquired by a thread in the cluster. Then,
     * the same thread reentrantly acquires the lock again. The fencing token
     * returned from the second acquire is equal to the one returned from the
     * first acquire, because of reentrancy. After the second acquire, the lock
     * is released 2 times, hence becomes free. There is a third lock acquire
     * here, which returns a new fencing token. Because this last lock acquire
     * is not reentrant, its fencing token is guaranteed to be larger than the
     * previous tokens, independent of the thread that has acquired the lock.
     *
     * @throws IllegalMonitorStateException if the underlying CP session is
     *         closed while locking reentrantly
     */
    long lockAndGetFence();

    /**
     * Acquires the lock if it is available or already held by the current
     * thread, and returns immediately with the value {@code true}.
     * If the lock is not available, then this method will return immediately
     * with the value {@code false}.
     * <p>
     * A typical usage idiom for this method would be:
     * <pre>
     *     FencedLock lock = ...;
     *     if (lock.tryLock()) {
     *         try {
     *             // manipulate protected state
     *         } finally {
     *             lock.unlock();
     *         }
     *     } else {
     *         // perform alternative actions
     *     }
     * </pre>
     * This usage ensures that the lock is unlocked if it was acquired,
     * and doesn't try to unlock if the lock was not acquired.
     *
     * @return {@code true} if the lock was acquired and
     *         {@code false} otherwise
     *
     * @throws IllegalMonitorStateException if the underlying CP session is
     *         closed while locking reentrantly
     */
    boolean tryLock();

    /**
     * Acquires the lock only if it is free or already held by the current
     * thread at the time of invocation, and returns the fencing token assigned
     * to the current thread for this lock acquire. If the lock is acquired
     * reentrantly, the same fencing token is returned. If not acquired,
     * then this method will return immediately with {@link #INVALID_FENCE}
     * that represents a failed lock attempt.
     * <p>
     * This is a convenience method for the following pattern:
     * <pre>
     *     FencedLock lock = ...;
     *     if (lock.tryLock()) {
     *         return lock.getFence();
     *     } else {
     *         return FencedLock.INVALID_FENCE;
     *     }
     * </pre>
     * <p>
     * Consider the following scenario where the lock is free initially:
     * <pre>
     *     FencedLock lock = ...; // the lock is free
     *     lock.tryLockAndGetFence();
     *     // JVM of the caller thread hits a long pause
     *     // and its CP session is closed on the CP group.
     *     lock.tryLockAndGetFence();
     * </pre>
     * In this scenario, a thread acquires the lock, then its JVM instance
     * encounters a long pause, which is longer than
     * {@link CPSubsystemConfig#getSessionTimeToLiveSeconds()}. In this case,
     * its CP session will be closed on the corresponding CP group because
     * it could not commit session heartbeats in the meantime. After the JVM
     * instance wakes up again, the same thread attempts to acquire the lock
     * reentrantly. Before the second lock attempt, if the proxy notices that
     * its CP session is closed, then the second lock() call fails by throwing
     * {@link IllegalMonitorStateException}. However, this is not guaranteed
     * because CP session heartbeats are committed periodically and the second
     * lock attempt could occur before the proxy notices termination of its
     * session on the next session heartbeat commit.
     * <p>
     * Fencing tokens are monotonic numbers that are incremented each time
     * the lock switches from the not-acquired state to the acquired state.
     * They are simply used for ordering lock holders. A lock holder can pass
     * its fencing to the shared resource to fence off previous lock holders.
     * When this resource receives an operation, it can validate the fencing
     * token in the operation.
     * <p>
     * Consider the following scenario where the lock is free initially:
     * <pre>
     *     FencedLock lock = ...; // the lock is free
     *     long fence1 = lock.tryLockAndGetFence(); // (1)
     *     long fence2 = lock.tryLockAndGetFence(); // (2)
     *     assert fence1 == fence2;
     *     lock.unlock();
     *     lock.unlock();
     *     long fence3 = lock.tryLockAndGetFence(); // (3)
     *     assert fence3 > fence1;
     * </pre>
     * In this scenario, the lock is acquired by a thread in the cluster. Then,
     * the same thread reentrantly acquires the lock again. The fencing token
     * returned from the second acquire is equal to the one returned from the
     * first acquire, because of reentrancy. After the second acquire, the lock
     * is released 2 times, hence becomes free. There is a third lock acquire
     * here, which returns a new fencing token. Because this last lock acquire
     * is not reentrant, its fencing token is guaranteed to be larger than the
     * previous tokens, independent of the thread that has acquired the lock.
     *
     * @return the fencing token if the lock was acquired and
     *         {@link #INVALID_FENCE} otherwise
     *
     * @throws IllegalMonitorStateException if the underlying CP session is
     *         closed while locking reentrantly
     */
    long tryLockAndGetFence();

    /**
     * Acquires the lock if it is free within the given waiting time,
     * or already held by the current thread.
     * <p>
     * If the lock is available or already held by the current thread, this
     * method returns immediately with the value {@code true}. If the lock is
     * not available then the current thread becomes disabled for thread
     * scheduling purposes and lies dormant until the lock is acquired by the
     * current thread or the specified waiting time elapses.
     * <p>
     * If the lock is acquired, then the value {@code true} is returned.
     * <p>
     * If the specified waiting time elapses, then the value {@code false}
     * is returned. If the time is less than or equal to zero, the method will
     * not wait at all.
     *
     * @param time the maximum time to wait for the lock
     * @param unit the time unit of the {@code time} argument
     * @return {@code true} if the lock was acquired and {@code false}
     *         if the waiting time elapsed before the lock was acquired
     *
     * @throws IllegalMonitorStateException if the underlying CP session is
     *         closed while locking reentrantly
     */
    boolean tryLock(long time, TimeUnit unit);

    /**
     * Acquires the lock if it is free within the given waiting time,
     * or already held by the current thread, and returns the fencing token
     * assigned to the current thread for this lock acquire. If the lock is
     * acquired reentrantly, the same fencing token is returned.
     * <p>
     * If the lock is not available then the current thread becomes disabled
     * for thread scheduling purposes and lies dormant until the lock is
     * acquired by the current thread or the specified waiting time elapses.
     * <p>
     * If the specified waiting time elapses, then {@link #INVALID_FENCE}
     * is returned. If the time is less than or equal to zero, the method will
     * not wait at all.
     * <p>
     * This is a convenience method for the following pattern:
     * <pre>
     *     FencedLock lock = ...;
     *     if (lock.tryLock(time, unit)) {
     *         return lock.getFence();
     *     } else {
     *         return FencedLock.INVALID_FENCE;
     *     }
     * </pre>
     * <p>
     * Consider the following scenario where the lock is free initially:
     * <pre>
     *      FencedLock lock = ...; // the lock is free
     *      lock.tryLockAndGetFence(time, unit);
     *      // JVM of the caller thread hits a long pause and its CP session
     *      is closed on the CP group.
     *      lock.tryLockAndGetFence(time, unit);
     * </pre>
     * In this scenario, a thread acquires the lock, then its JVM instance
     * encounters a long pause, which is longer than
     * {@link CPSubsystemConfig#getSessionTimeToLiveSeconds()}. In this case,
     * its CP session will be closed on the corresponding CP group because
     * it could not commit session heartbeats in the meantime. After the JVM
     * instance wakes up again, the same thread attempts to acquire the lock
     * reentrantly. Before the second lock attempt, if the proxy notices that
     * its CP session is closed, then the second lock() call fails by throwing
     * {@link IllegalMonitorStateException}. However, this is not guaranteed
     * because CP session heartbeats are committed periodically and the second
     * lock attempt could occur before the proxy notices termination of its
     * session on the next session heartbeat commit.
     * <p>
     * Fencing tokens are monotonic numbers that are incremented each time
     * the lock switches from the not-acquired state to the acquired state.
     * They are simply used for ordering lock holders. A lock holder can pass
     * its fencing to the shared resource to fence off previous lock holders.
     * When this resource receives an operation, it can validate the fencing
     * token in the operation.
     * <p>
     * Consider the following scenario where the lock is free initially:
     * <pre>
     *     FencedLock lock = ...; // the lock is free
     *     long fence1 = lock.tryLockAndGetFence(time, unit); // (1)
     *     long fence2 = lock.tryLockAndGetFence(time, unit); // (2)
     *     assert fence1 == fence2;
     *     lock.unlock();
     *     lock.unlock();
     *     long fence3 = lock.tryLockAndGetFence(time, unit); // (3)
     *     assert fence3 > fence1;
     * </pre>
     * In this scenario, the lock is acquired by a thread in the cluster. Then,
     * the same thread reentrantly acquires the lock again. The fencing token
     * returned from the second acquire is equal to the one returned from the
     * first acquire, because of reentrancy. After the second acquire, the lock
     * is released 2 times, hence becomes free. There is a third lock acquire
     * here, which returns a new fencing token. Because this last lock acquire
     * is not reentrant, its fencing token is guaranteed to be larger than the
     * previous tokens, independent of the thread that has acquired the lock.
     *
     * @param time the maximum time to wait for the lock
     * @param unit the time unit of the {@code time} argument
     * @return the fencing token if the lock was acquired and
     *         {@link #INVALID_FENCE} otherwise
     *
     * @throws IllegalMonitorStateException if the underlying CP session is
     *         closed while locking reentrantly
     */
    long tryLockAndGetFence(long time, TimeUnit unit);

    /**
     * Releases the lock if the lock is currently held by the current thread.
     *
     * @throws IllegalMonitorStateException if the lock is not held by
     *         the current thread
     */
    void unlock();

    /**
     * Releases the lock if it is held by any thread in the cluster,
     * not necessarily the current thread.
     *
     * @throws IllegalMonitorStateException if the lock is not held by
     *         any thread in the cluster
     */
    void forceUnlock();

    /**
     * Returns the fencing token if the lock is held by the current thread.
     * <p>
     * Fencing tokens are monotonic numbers that are incremented each time
     * the lock switches from the not-acquired state to the acquired state.
     * They are simply used for ordering lock holders. A lock holder can pass
     * its fencing to the shared resource to fence off previous lock holders.
     * When this resource receives an operation, it can validate the fencing
     * token in the operation.
     *
     * @return the fencing token if the lock is held by the current thread
     *
     * @throws IllegalMonitorStateException if the lock is not held by
     *         the current thread
     */
    long getFence();

    /**
     * Returns whether this lock is locked or not.
     *
     * @return {@code true} if this lock is locked by any thread
     *         in the cluster, {@code false} otherwise.
     */
    boolean isLocked();

    /**
     * Returns whether the lock is held by the current thread or not.
     *
     * @return {@code true} if the lock is held by the current thread or not,
     *         {@code false} otherwise.
     */
    boolean isLockedByCurrentThread();

    /**
     * Returns the reentrant lock count if the lock is held by
     * the current thread, or 0 if the lock is held by some other thread
     * in the cluster, or not held at all.
     *
     * @return the reentrant lock count if the lock is held by the current thread,
     *         0 otherwise
     */
    int getLockCountIfLockedByCurrentThread();

    /**
     * Returns id of the {@link CPGroup} that runs this {@link FencedLock} instance
     *
     * @return id of the {@link CPGroup} that runs this {@link FencedLock} instance
     */
    CPGroupId getGroupId();

    /**
     * NOT IMPLEMENTED. Fails by throwing {@link UnsupportedOperationException}.
     * <p>
     * May the force be the one who dares to implement
     * a linearizable distributed {@link Condition} :)
     *
     * @throws UnsupportedOperationException
     */
    Condition newCondition();
}
