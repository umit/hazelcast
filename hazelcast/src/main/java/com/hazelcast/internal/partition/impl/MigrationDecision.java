/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.nio.Address;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.internal.partition.impl.InternalPartitionImpl.getReplicaIndex;

/**
 * TODO: Javadoc Pending...
 */
class MigrationDecision {

    interface MigrationCallback {
        void migrate(Address source, int sourceCurrentReplicaIndex, int sourceNewReplicaIndex,
                Address destination, int destinationCurrentReplicaIndex, int destinationNewReplicaIndex);
    }

    static void migrate(Address[] oldAddresses, Address[] newAddresses, MigrationCallback callback) {

        if (oldAddresses.length != newAddresses.length) {
            throw new IllegalArgumentException(
                    "replica addresses with different lengths! old: " + Arrays.toString(oldAddresses) + " new: " + Arrays
                            .toString(newAddresses));
        }

        Address[] state = new Address[oldAddresses.length];
        System.arraycopy(oldAddresses, 0, state, 0, oldAddresses.length);

        log("INITIAL STATE: " + Arrays.toString(state));
        log("FINAL STATE: " + Arrays.toString(newAddresses));
        verifyState(oldAddresses, newAddresses, state);
        if (fixCycle(oldAddresses, newAddresses)) {
            log("cycle fixed");
        }
        log("EXPECTED STATE: " + Arrays.toString(newAddresses));

        int currentIndex = 0;
        while (currentIndex < oldAddresses.length) {

            log("STATE: " + Arrays.toString(state));
            verifyState(oldAddresses, newAddresses, state);

            if (newAddresses[currentIndex] == null) {
                if (state[currentIndex] != null) {
                    log("new address is null at index: " + currentIndex);
                    callback.migrate(state[currentIndex], currentIndex, -1, null, -1, -1);
                    state[currentIndex] = null;
                }
                currentIndex++;
                continue;
            }

            if (state[currentIndex] == null) {
                int i = getReplicaIndex(state, newAddresses[currentIndex]);
                if (i == -1) {
                    log("COPY " + newAddresses[currentIndex] + " to index: " + currentIndex);
                    callback.migrate(null, -1, -1, newAddresses[currentIndex], -1, currentIndex);
                    state[currentIndex] = newAddresses[currentIndex];
                    currentIndex++;
                    continue;
                } else if (i > currentIndex) {
                    log("SHIFT UP2 " + state[i] + " from old addresses index: " + i + " to index: " + currentIndex);
                    callback.migrate(null, -1, -1, state[i], i, currentIndex);
                    state[currentIndex] = state[i];
                    state[i] = null;
                    continue;
                } else {
                    throw new AssertionError(
                            "Migration decision algorithm failed during SHIFT UP! INITIAL: " + Arrays.toString(oldAddresses)
                                    + " CURRENT: " + Arrays.toString(state) + " FINAL: " + Arrays.toString(newAddresses));
                }
            }

            if (newAddresses[currentIndex].equals(state[currentIndex])) {
                log("Addresses are same on index: " + currentIndex);
                currentIndex++;
                continue;
            }

            if (getReplicaIndex(newAddresses, state[currentIndex]) == -1
                    && getReplicaIndex(state, newAddresses[currentIndex]) == -1) {
                log("MOVE " + newAddresses[currentIndex] + " to index: " + currentIndex);
                callback.migrate(state[currentIndex], currentIndex, -1, newAddresses[currentIndex], -1, currentIndex);
                state[currentIndex] = newAddresses[currentIndex];
                currentIndex++;
                continue;
            }

            // IT IS A MOVE COPY BACK
            if (getReplicaIndex(state, newAddresses[currentIndex]) == -1) {
                int keepReplicaIndex = getReplicaIndex(newAddresses, state[currentIndex]);

                if (keepReplicaIndex <= currentIndex) {
                    throw new AssertionError(
                            "Migration decision algorithm failed during MOVE COPY BACK! INITIAL: " + Arrays.toString(oldAddresses)
                                    + " CURRENT: " + Arrays.toString(state) + " FINAL: " + Arrays.toString(newAddresses));
                }

                log("MOVE_COPY_BACK " + newAddresses[currentIndex] + " to index: " + currentIndex + " with keepReplicaIndex: "
                        + keepReplicaIndex);

                callback.migrate(state[currentIndex], currentIndex, keepReplicaIndex, newAddresses[currentIndex], -1, currentIndex);

                state[keepReplicaIndex] = state[currentIndex];
                state[currentIndex] = newAddresses[currentIndex];
                currentIndex++;
                continue;
            }

            Address target = newAddresses[currentIndex];
            int i = currentIndex;
            while (true) {
                int j = getReplicaIndex(state, target);

                if (j == -1) {
                    throw new AssertionError("Migration algorithm failed during SHIFT UP! " + target
                            + " is not present in " + Arrays.toString(state) + "INITIAL: " + Arrays.toString(oldAddresses)
                            + " FINAL: " + Arrays.toString(newAddresses));
                } else if (newAddresses[j] == null) {
                    log("SHIFT UP " + state[j] + " from old addresses index: " + j + " to index: " + i);
                    callback.migrate(state[i], i, -1, state[j], j, i);
                    state[i] = state[j];
                    state[j] = null;
                    break;
                } else if (getReplicaIndex(state, newAddresses[j]) == -1) { //
                    log("MOVE2 " + newAddresses[j] + " to index: " + j);
                    callback.migrate(state[j], j, -1, newAddresses[j], -1, j);
                    state[j] = newAddresses[j];
                    break;
                } else {
                    // newAddresses[j] is also present in old partition replicas
                    target = newAddresses[j];
                    i = j;
                }
            }
        }

        if (!Arrays.equals(state, newAddresses)) {
            throw new AssertionError("Migration decisions failed! INITIAL: " + Arrays.toString(oldAddresses)
                    + " CURRENT: " + Arrays.toString(state) + " FINAL: " + Arrays.toString(newAddresses));
        }
    }

    // TODO: assert
    private static void verifyState(Address[] oldAddresses, Address[] newAddresses, Address[] state) {
        Set<Address> verify = new HashSet<Address>();
        for (Address s : state) {
            if (s != null) {
                if (!verify.add(s)) {
                    throw new AssertionError("Migration decision algorithm failed! DUPLICATE REPLICA ADDRESSES! INITIAL: "
                            + Arrays.toString(oldAddresses) + " CURRENT: " + Arrays.toString(state) + " FINAL: "
                            + Arrays.toString(newAddresses));
                }
            }
        }
    }

    static boolean isCyclic(Address[] oldReplicas, Address[] newReplicas) {
        for (int i = 0; i < oldReplicas.length; i++) {
            final Address oldAddress = oldReplicas[i];
            final Address newAddress = newReplicas[i];

            if (oldAddress == null || newAddress == null || oldAddress.equals(newAddress)) {
                continue;
            }

            if (isCyclic(oldReplicas, newReplicas, i)) {
                return true;
            }
        }

        return false;
    }

    static boolean fixCycle(Address[] oldReplicas, Address[] newReplicas) {
        boolean cyclic = false;
        for (int i = 0; i < oldReplicas.length; i++) {
            final Address oldAddress = oldReplicas[i];
            final Address newAddress = newReplicas[i];

            if (oldAddress == null || newAddress == null || oldAddress.equals(newAddress)) {
                continue;
            }

            if (isCyclic(oldReplicas, newReplicas, i)) {
                fixCycle(oldReplicas, newReplicas, i);
                cyclic = true;
            }
        }

        return cyclic;
    }

    private static boolean isCyclic(Address[] oldReplicas, Address[] newReplicas, final int index) {
        final Address newOwner = newReplicas[index];
        int firstIndex = index;

        int k = 0;
        while (true) {
            int nextIndex = InternalPartitionImpl.getReplicaIndex(newReplicas, oldReplicas[firstIndex]);
            if (nextIndex == -1) {
                return false;
            }

            if (firstIndex == nextIndex) {
                return false;
            }

            if (newOwner.equals(oldReplicas[nextIndex])) {
                return true;
            }

            // TODO: assert
            if (k++ > 1000) {
                System.err.println("**************** Old: " + Arrays.toString(oldReplicas) + ", new: " + Arrays.toString(newReplicas) + ", index: " + index
                    + ", firstIndex: " + firstIndex + ", nextIndex: " + nextIndex);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            firstIndex = nextIndex;
        }
    }

    private static void fixCycle(Address[] oldReplicas, Address[] newReplicas, int index) {
        while (true) {
            int nextIndex = InternalPartitionImpl.getReplicaIndex(newReplicas, oldReplicas[index]);
            newReplicas[index] = oldReplicas[index];
            if (nextIndex == -1) {
                return;
            }
            index = nextIndex;
        }
    }

    private static final boolean TRACE = false;
    private static void log(String x) {
        if (TRACE) {
            System.out.println(x);
        }
    }

}
