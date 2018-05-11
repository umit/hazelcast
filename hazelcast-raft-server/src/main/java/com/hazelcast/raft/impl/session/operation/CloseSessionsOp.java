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

package com.hazelcast.raft.impl.session.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.session.RaftSessionService;
import com.hazelcast.raft.impl.session.RaftSessionServiceDataSerializerHook;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.singletonList;

/**
 * TODO: Javadoc Pending...
 */
public class CloseSessionsOp extends RaftOp implements IdentifiedDataSerializable {

    private Collection<Long> sessionIds;

    public CloseSessionsOp() {
    }

    public CloseSessionsOp(long sessionId) {
        this(singletonList(sessionId));
    }

    public CloseSessionsOp(Collection<Long> sessionIds) {
        this.sessionIds = sessionIds;
    }

    @Override
    protected Object doRun(RaftGroupId groupId, long commitIndex) {
        RaftSessionService service = getService();
        service.closeSessions(groupId, sessionIds);
        return null;
    }

    @Override
    public String getServiceName() {
        return RaftSessionService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftSessionServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSessionServiceDataSerializerHook.CLOSE_SESSIONS;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(sessionIds.size());
        for (long sessionId : sessionIds) {
            out.writeLong(sessionId);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        List<Long> sessionIds = new ArrayList<Long>();
        for (int i = 0; i < size; i++) {
            sessionIds.add(in.readLong());
        }
        this.sessionIds = sessionIds;
    }
}
