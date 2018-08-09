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

package com.hazelcast.config.raft;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftGroupConfig {

    /**
     * Name of the default group if no group name is specified by Raft data structures.
     */
    public static final String DEFAULT_GROUP = "default";

    /**
     * Name of the Raft group
     */
    private String name;

    /**
     * Size of the raft group.
     * Raft groups are recommended to have odd-number of nodes in order to get a bigger degree of fault tolerance.
     * For instance, majority of a 5-node Raft group is 3 and this group can tolerate concurrent failures of 2 nodes.
     * However, majority of a 4-node Raft group is also 3 but this group can tolerate failure of a single node.
     */
    private int size;

    public RaftGroupConfig() {
    }

    public RaftGroupConfig(String name, int size) {
        this.name = name;
        this.size = size;
    }

    public RaftGroupConfig(RaftGroupConfig config) {
        this(config.name, config.size);
    }

    public String getName() {
        return name;
    }

    public RaftGroupConfig setName(String name) {
        this.name = name;
        return this;
    }

    public int getSize() {
        return size;
    }

    public RaftGroupConfig setSize(int size) {
        this.size = size;
        return this;
    }

    @Override
    public String toString() {
        return "RaftGroupConfig{" + "name='" + name + '\'' + ", size=" + size + '}';
    }
}
