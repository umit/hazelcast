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

package com.hazelcast.config.cp;

import com.hazelcast.core.IndeterminateOperationStateException;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * Contains configuration options for the CP subsystem.
 */
public class CPSubsystemConfig {

    /**
     * Default value for a CP session to be kept alive after the last heartbeat
     * it has received. See {@link #sessionTimeToLiveSeconds}
     */
    public static final int DEFAULT_SESSION_TTL_SECONDS = 60;

    /**
     * Default value of interval for the periodically-committed CP session
     * heartbeats. See {@link #sessionHeartbeatIntervalSeconds}
     */
    public static final int DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 5;

    /**
     * Minimum number of CP members for CP groups. If set,
     * {@link #cpMemberCount} and {@link #groupSize} cannot be smaller than
     * this value. See {@link #cpMemberCount} and {@link #groupSize}.
     *
     */
    public static final int MIN_GROUP_SIZE = 3;

    /**
     * Maximum number of CP members for CP groups. If set, {@link #groupSize}
     * cannot be larger than this value. See {@link #groupSize}.
     */
    public static final int MAX_GROUP_SIZE = 7;


    /**
     * Number of CP members to will initialize the CP subsystem. It is 0
     * by default and the CP subsystem is disabled. The CP subsystem is enabled
     * when a positive value is set. After the CP subsystem is initialized
     * successfully, more CP members can be added at run-time and number of
     * active CP members can go beyond the configured CP member count.
     */
    private int cpMemberCount;

    /**
     * Number of CP members for running CP groups. If set, it must be an odd
     * number between {@link #MIN_GROUP_SIZE} and {@link #MAX_GROUP_SIZE}.
     * Otherwise, {@link #cpMemberCount} is respected.
     */
    private int groupSize;

    /**
     * Duration for a CP session to be kept alive after the last heartbeat
     * it ha received. The session will be closed if there is no new heartbeat
     * during this duration.
     */
    private int sessionTimeToLiveSeconds = DEFAULT_SESSION_TTL_SECONDS;

    /**
     * Interval for the periodically-committed CP session heartbeats.
     * A CP session is started on a CP group with the first session-based
     * request of a Hazelcast server or a client. After that, heartbeats are
     * periodically committed to the CP group.
     */
    private int sessionHeartbeatIntervalSeconds = DEFAULT_HEARTBEAT_INTERVAL_SECONDS;

    /**
     * Duration to wait before automatically removing a missing CP member from
     * the CP subsystem. It is 0 by default and disabled. When a CP member
     * leaves the cluster, it is not automatically removed from
     * the CP subsystem, since the missing CP member could be still alive
     * and left the cluster because of a network partition. On the other hand,
     * if a missing CP member is actually crashed, it creates a danger for its
     * CP groups, because it will be still part of majority calculations.
     * This situation could lead to losing majority of CP groups if multiple
     * CP members leave the cluster over time.
     * <p>
     * If this configuration is enabled, missing CP members will be
     * automatically removed from the CP subsystem after some time.
     * This feature is very useful in terms of fault tolerance when
     * CP member count is also configured to be larger than group size.
     * In this case, a missing CP member will be safely replaced in its CP
     * groups with other available CP members in the CP subsystem. This
     * configuration also implies that no network partition is expected to be
     * longer than the configured duration.
     */
    private long missingCpMemberAutoRemovalSeconds;

    /**
     * Offers a choice between at-least-once and at-most-once execution
     * of the operations on top of the Raft consensus algorithm.
     * It is disabled by default and offers at-least-once execution guarantee.
     * If enabled, it switches to at-most-once execution guarantee.
     * When you invoke an API method on a CP data structure proxy, it
     * replicates an internal operation to the corresponding CP group. After
     * this operation is committed to majority of this CP group by the Raft
     * leader node, it sends a response for the public API call. If a failure
     * causes loss of the response, then the calling side cannot determine if
     * the operation is committed on the CP group or not. In this case, if this
     * configuration is disabled, the operation is replicated again to the CP
     * group, and hence could be committed multiple times. If it is enabled,
     * the public API call fails with
     * {@link IndeterminateOperationStateException}.
     */
    private boolean failOnIndeterminateOperationState;

    /**
     * Contains configuration options for Hazelcast's Raft consensus algorithm
     * implementation
     */
    private RaftAlgorithmConfig raftAlgorithmConfig = new RaftAlgorithmConfig();

    public CPSubsystemConfig() {
    }

    public CPSubsystemConfig(CPSubsystemConfig config) {
        this.cpMemberCount = config.cpMemberCount;
        this.groupSize = config.groupSize;
        this.raftAlgorithmConfig = new RaftAlgorithmConfig(config.raftAlgorithmConfig);
        this.sessionTimeToLiveSeconds = config.sessionTimeToLiveSeconds;
        this.sessionHeartbeatIntervalSeconds = config.sessionHeartbeatIntervalSeconds;
        this.failOnIndeterminateOperationState = config.failOnIndeterminateOperationState;
        this.missingCpMemberAutoRemovalSeconds = config.missingCpMemberAutoRemovalSeconds;
    }

    /**
     * Returns the number of CP members that will initialize the CP subsystem.
     * The CP subsystem is disabled if 0.
     *
     * @return the number of CP members that will initialize the CP subsystem
     */
    public int getCPMemberCount() {
        return cpMemberCount;
    }

    /**
     * Sets the CP member count. The CP subsystem is disabled if 0.
     * Cannot be smaller than {@link #MIN_GROUP_SIZE} and {@link #groupSize}
     *
     * @return this config instance
     */
    public CPSubsystemConfig setCPMemberCount(int cpMemberCount) {
        checkTrue(cpMemberCount >= MIN_GROUP_SIZE, "CP subsystem must have at least " + MIN_GROUP_SIZE
                + " CP members");
        checkTrue(groupSize <= cpMemberCount, "The group size parameter cannot be bigger than "
                + "the number of the CP member count");
        this.cpMemberCount = cpMemberCount;
        return this;
    }

    /**
     * Returns number of CP members that each CP group will consist of
     * Returns 0 if CP member count is 0.
     * If group size is not set:
     * - returns CP member count if it is an odd number
     * - returns CP member count - 1 if it is an even number
     *
     * @return number of CP members that each CP group will consist of
     */
    public int getGroupSize() {
        if (groupSize > 0 || cpMemberCount == 0) {
            return groupSize;
        }

        int groupSize = cpMemberCount;
        if (groupSize % 2 == 0) {
            groupSize--;
        }

        return Math.min(groupSize, MAX_GROUP_SIZE);
    }

    /**
     * Sets group size. Must be an odd number between {@link #MIN_GROUP_SIZE}
     * and {@link #MAX_GROUP_SIZE}. Must be smaller than or equal to CP member
     * count.
     *
     * @return this config instance
     */
    public CPSubsystemConfig setGroupSize(int groupSize) {
        checkTrue(groupSize == 0 || (groupSize >= MIN_GROUP_SIZE && groupSize <= MAX_GROUP_SIZE
                && (groupSize % 2 == 1)), "Group size must be an odd value between 3 and 7");
        checkTrue(groupSize <= cpMemberCount,
                "Group size cannot be bigger than CP member count");
        this.groupSize = groupSize;
        return this;
    }

    /**
     * Returns duration for a CP session to be kept alive
     * after the last heartbeat
     *
     * @return duration for a CP session to be kept alive
     *         after the last heartbeat
     */
    public long getSessionTimeToLiveSeconds() {
        return sessionTimeToLiveSeconds;
    }

    /**
     * Sets duration for a CP session to be kept alive after the last heartbeat
     *
     * @return this config instance
     */
    public CPSubsystemConfig setSessionTimeToLiveSeconds(int sessionTimeToLiveSeconds) {
        checkPositive(sessionTimeToLiveSeconds, "Session TTL must be a positive value!");
        checkTrue(sessionTimeToLiveSeconds > sessionHeartbeatIntervalSeconds,
                "Session TTL must be greater than session heartbeat interval!");
        checkTrue(missingCpMemberAutoRemovalSeconds == 0
                        || sessionTimeToLiveSeconds <= missingCpMemberAutoRemovalSeconds,
                "Session TTL must be smaller than or equal to missing CP member auto-removal seconds!");
        this.sessionTimeToLiveSeconds = sessionTimeToLiveSeconds;
        return this;
    }

    /**
     * Returns interval for the periodically-committed CP session heartbeats
     *
     * @return interval for the periodically-committed CP session heartbeats
     */
    public int getSessionHeartbeatIntervalSeconds() {
        return sessionHeartbeatIntervalSeconds;
    }

    /**
     * Sets interval for the periodically-committed CP session heartbeats
     *
     * @return this config instance
     */
    public CPSubsystemConfig setSessionHeartbeatIntervalSeconds(int sessionHeartbeatIntervalSeconds) {
        checkPositive(sessionTimeToLiveSeconds, "Session heartbeat interval must be a positive value!");
        checkTrue(sessionTimeToLiveSeconds > sessionHeartbeatIntervalSeconds,
                "Session heartbeat interval must be smaller than session TTL!");
        this.sessionHeartbeatIntervalSeconds = sessionHeartbeatIntervalSeconds;
        return this;
    }

    /**
     * Returns duration to wait before automatically removing
     * a missing CP member from the CP subsystem
     *
     * @return duration to wait before automatically removing
     *         a missing CP member from the CP subsystem
     */
    public long getMissingCpMemberAutoRemovalSeconds() {
        return missingCpMemberAutoRemovalSeconds;
    }

    /**
     * Sets duration to wait before automatically removing a missing CP member
     * from the CP subsystem
     *
     * @return this config instance
     */
    public CPSubsystemConfig setMissingCpMemberAutoRemovalSeconds(long missingCpMemberAutoRemovalSeconds) {
        checkTrue(missingCpMemberAutoRemovalSeconds == 0
                        || missingCpMemberAutoRemovalSeconds >= sessionTimeToLiveSeconds,
                "missingCpMemberAutoRemovalSeconds must be either 0 or greater than or equal to session TTL");
        this.missingCpMemberAutoRemovalSeconds = missingCpMemberAutoRemovalSeconds;
        return this;
    }

    /**
     * Returns the value to determine if CP API calls will fail when result
     * of a replicated operation becomes indeterminate
     *
     * @return the value to determine if CP API calls will fail when result
     *         of a replicated operation becomes indeterminate
     */
    public boolean isFailOnIndeterminateOperationState() {
        return failOnIndeterminateOperationState;
    }

    /**
     * Sets the value to determine if CP API calls will fail when result of a
     * replicated operation becomes indeterminate
     *
     * @return this config instance
     */
    public CPSubsystemConfig setFailOnIndeterminateOperationState(boolean failOnIndeterminateOperationState) {
        this.failOnIndeterminateOperationState = failOnIndeterminateOperationState;
        return this;
    }

    /**
     * Returns configuration options for Hazelcast's Raft consensus algorithm
     * implementation
     *
     * @return configuration options for Hazelcast's Raft consensus algorithm
     *         implementation
     */
    public RaftAlgorithmConfig getRaftAlgorithmConfig() {
        return raftAlgorithmConfig;
    }

    /**
     * Sets configuration options for Hazelcast's Raft consensus algorithm
     * implementation
     *
     * @return this config instance
     */
    public CPSubsystemConfig setRaftAlgorithmConfig(RaftAlgorithmConfig raftAlgorithmConfig) {
        checkNotNull(raftAlgorithmConfig);
        this.raftAlgorithmConfig = raftAlgorithmConfig;
        return this;
    }

}
