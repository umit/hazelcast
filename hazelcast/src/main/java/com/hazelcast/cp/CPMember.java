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
import com.hazelcast.core.Cluster;
import com.hazelcast.core.EndpointIdentifier;
import com.hazelcast.nio.Address;

/**
 * A CP member is a Hazelcast member that is internally elected to be part of
 * the {@link CPSubsystem}, hence maintain CP data structures. If
 * {@link CPSubsystemConfig#getCPMemberCount()} is configured to be N,
 * first N members of the cluster are assigned as CP members during startup.
 * After the CP subsystem is initialized, other Hazelcast members can be
 * promoted to be a CP member via the
 * {@link CPSubsystemManagementService#promoteToCPMember()} API.
 * <p>
 * When a Hazelcast member is elected as a CP member during the cluster startup
 * process, UUID of the member is assigned as CP member UUID. If this member
 * splits from the cluster, it will get a new member UUID while merging back
 * to the cluster, but its CP member UUID, which is returned by this method,
 * will NOT change. It is because a split-member merges to the cluster
 * as a new Hazelcast member. However, the CP subsystem does not perform any
 * special treatment to network partitions and does not perform any action
 * such as data merging, etc. Therefore, CP member UUID of a merging Hazelcast
 * member remains the same, although its member UUID changes.
 *
 * @see CPSubsystemConfig
 * @see CPSubsystemManagementService
 */
public interface CPMember extends EndpointIdentifier {

    /**
     * Returns the address of this CP member.
     * It is same with the address of {@link Cluster#getLocalMember()}
     *
     * @return the address of this CP member, which is same with the address of
     *         {@link Cluster#getLocalMember()}.
     */
    Address getAddress();

    /**
     * Returns the UUID of this CP member.
     *
     * @return the UUID of this CP member.
     */
    String getUuid();

}
