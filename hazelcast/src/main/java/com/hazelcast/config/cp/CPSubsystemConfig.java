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

import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CPSubsystemConfig {

    /**
     * Name of the default group if no group name is specified by CP data structures.
     */
    public static final String DEFAULT_GROUP_NAME = "default";

    /**
     * Default value for session time to live duration after its last heartbeat
     */
    public static final int DEFAULT_SESSION_TTL_SECONDS = 30;

    /**
     * Default value for session heartbeat intervals
     */
    public static final int DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 5;


    private int cpMemberCount;

    private int groupSize;

    private RaftAlgorithmConfig raftAlgorithmConfig = new RaftAlgorithmConfig();

    private int sessionTimeToLiveSeconds = DEFAULT_SESSION_TTL_SECONDS;

    private int sessionHeartbeatIntervalSeconds = DEFAULT_HEARTBEAT_INTERVAL_SECONDS;

    /**
     * When enabled, an append request fails if the target member (leader) leaves the cluster.
     * At this point result of append request is indeterminate, it may have been replicated by the leader
     * to some of the followers.
     */
    private boolean failOnIndeterminateOperationState;

    private long missingCpMemberAutoRemovalSeconds;

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

    public int getCpMemberCount() {
        return cpMemberCount;
    }

    public CPSubsystemConfig setCpMemberCount(int cpMemberCount) {
        checkTrue(cpMemberCount >= 2, "CP subsystem must have at least 2 members");
        checkTrue(groupSize <= cpMemberCount,
                "The group size parameter cannot be bigger than the number of the cp member count");
        this.cpMemberCount = cpMemberCount;
        return this;
    }

    public int getGroupSize() {
        return groupSize > 0 ? groupSize : cpMemberCount;
    }

    public CPSubsystemConfig setGroupSize(int groupSize) {
        checkTrue(groupSize == 0 || groupSize >= 2, "Raft groups must have at least 2 members");
        checkTrue(groupSize <= cpMemberCount,
                "The group size parameter cannot be bigger than the number of the cp member count");
        this.groupSize = groupSize;
        return this;
    }

    public RaftAlgorithmConfig getRaftAlgorithmConfig() {
        return raftAlgorithmConfig;
    }

    public CPSubsystemConfig setRaftAlgorithmConfig(RaftAlgorithmConfig raftAlgorithmConfig) {
        this.raftAlgorithmConfig = raftAlgorithmConfig;
        return this;
    }

    public long getSessionTimeToLiveSeconds() {
        return sessionTimeToLiveSeconds;
    }

    public CPSubsystemConfig setSessionTimeToLiveSeconds(int sessionTimeToLiveSeconds) {
        checkPositive(sessionTimeToLiveSeconds, "Session TTL should be greater than zero!");
        checkTrue(sessionTimeToLiveSeconds > sessionHeartbeatIntervalSeconds,
                "Session timeout must be greater than heartbeat interval!");
        this.sessionTimeToLiveSeconds = sessionTimeToLiveSeconds;
        return this;
    }

    public int getSessionHeartbeatIntervalSeconds() {
        return sessionHeartbeatIntervalSeconds;
    }

    public CPSubsystemConfig setSessionHeartbeatIntervalSeconds(int sessionHeartbeatIntervalSeconds) {
        checkPositive(sessionTimeToLiveSeconds, "Session heartbeat interval should be greater than zero!");
        checkTrue(sessionTimeToLiveSeconds > sessionHeartbeatIntervalSeconds,
                "Session TTL must be greater than heartbeat interval!");
        checkTrue(missingCpMemberAutoRemovalSeconds == 0 || sessionTimeToLiveSeconds <= missingCpMemberAutoRemovalSeconds,
                "Session TTL must be smaller than or equal to missingCpMemberAutoRemovalSeconds!");
        this.sessionHeartbeatIntervalSeconds = sessionHeartbeatIntervalSeconds;
        return this;
    }

    public boolean isFailOnIndeterminateOperationState() {
        return failOnIndeterminateOperationState;
    }

    public CPSubsystemConfig setFailOnIndeterminateOperationState(boolean failOnIndeterminateOperationState) {
        this.failOnIndeterminateOperationState = failOnIndeterminateOperationState;
        return this;
    }

    public long getMissingCpMemberAutoRemovalSeconds() {
        return missingCpMemberAutoRemovalSeconds;
    }

    public CPSubsystemConfig setMissingCpMemberAutoRemovalSeconds(long missingCpMemberAutoRemovalSeconds) {
        checkTrue(missingCpMemberAutoRemovalSeconds == 0 || missingCpMemberAutoRemovalSeconds >= sessionTimeToLiveSeconds,
                "missingCpMemberAutoRemovalSeconds must be either 0 or greater than or equal to session TTL");
        this.missingCpMemberAutoRemovalSeconds = missingCpMemberAutoRemovalSeconds;
        return this;
    }
}
