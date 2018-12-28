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

/**
 * Identifier for CP groups.
 * <p>
 * Each CP group is denoted by a unique name and a commitIndex in Raft log.
 */
public interface CPGroupId {

    /**
     * Returns the name of the CP group.
     */
    String name();

    /**
     * Returns the commit index of the log when this CP group is created.
     */
    long commitIndex();
}
