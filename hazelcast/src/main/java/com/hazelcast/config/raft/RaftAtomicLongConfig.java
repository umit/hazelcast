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
public class RaftAtomicLongConfig extends AbstractRaftObjectConfig {

    public RaftAtomicLongConfig() {
        super();
    }

    public RaftAtomicLongConfig(String name, String raftGroupRef) {
        super(name, raftGroupRef);
    }

    public RaftAtomicLongConfig setName(String name) {
        this.name = name;
        return this;
    }

    public RaftAtomicLongConfig setRaftGroupRef(String raftGroupRef) {
        this.raftGroupRef = raftGroupRef;
        return this;
    }
}
