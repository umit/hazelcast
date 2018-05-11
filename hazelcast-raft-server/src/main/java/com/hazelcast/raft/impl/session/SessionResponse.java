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

package com.hazelcast.raft.impl.session;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public class SessionResponse implements IdentifiedDataSerializable {

    private long sessionId;

    private long sessionTTL;

    public SessionResponse() {
    }

    public SessionResponse(long sessionId, long sessionTTL) {
        this.sessionId = sessionId;
        this.sessionTTL = sessionTTL;
    }

    public long getSessionId() {
        return sessionId;
    }

    public long getSessionTTL() {
        return sessionTTL;
    }

    @Override
    public int getFactoryId() {
        return RaftSessionServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSessionServiceDataSerializerHook.SESSION_RESPONSE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(sessionId);
        out.writeLong(sessionTTL);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        sessionId = in.readLong();
        sessionTTL = in.readLong();
    }

    @Override
    public String toString() {
        return "SessionResponse{" + "sessionId=" + sessionId + ", sessionTTL=" + sessionTTL + '}';
    }
}
