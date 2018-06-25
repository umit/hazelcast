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

package com.hazelcast.raft.service;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.service.blocking.InvalidateWaitEntriesOp;

/**
 */
public class RaftDataServiceDataSerializerHook implements DataSerializerHook {

    private static final int FACTORY_ID = -1010;
    private static final String RAFT_DS_FACTORY = "hazelcast.serialization.ds.raft.data";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_DS_FACTORY, FACTORY_ID);

    public static final int INVALIDATE_WAIT_ENTRIES_OP = 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case INVALIDATE_WAIT_ENTRIES_OP:
                        return new InvalidateWaitEntriesOp();
                }
                throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
