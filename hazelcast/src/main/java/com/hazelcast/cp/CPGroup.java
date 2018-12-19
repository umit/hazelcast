/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.EndpointIdentifier;

import java.util.Collection;

/**
 * Contains information about a CP group
 */
public interface CPGroup {

    /**
     * Represents status of a CP group
     */
    enum CPGroupStatus {
        /**
         * A CP group is active after it is initialized with the first request it has received,
         * and before its destroy process is initialized.
         */
        ACTIVE,

        /**
         * A CP group moves into this state after its destroy process is initialized but not completed yet.
         */
        DESTROYING,

        /**
         * A CP group moves into this state after its destroy process is completed.
         */
        DESTROYED
    }

    /**
     * Returns unique id of the CP group
     */
    CPGroupId id();

    /**
     * Returns status of the CP group
     */
    CPGroupStatus status();

    /**
     * Returns members that the CP group is initialized with.
     */
    Collection<EndpointIdentifier> initialMembers();

    /**
     * Returns current members of the CP group
     */
    Collection<EndpointIdentifier> members();
}
