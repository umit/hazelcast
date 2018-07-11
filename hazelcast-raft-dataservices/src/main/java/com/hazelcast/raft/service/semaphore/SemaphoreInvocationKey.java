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

package com.hazelcast.raft.service.semaphore;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.service.blocking.WaitKey;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public class SemaphoreInvocationKey implements WaitKey, IdentifiedDataSerializable {

    private String name;
    private long commitIndex;
    private long sessionId;
    private int permits;

    public SemaphoreInvocationKey() {
    }

    public SemaphoreInvocationKey(String name, long commitIndex, long sessionId, int permits) {
        this.name = name;
        this.commitIndex = commitIndex;
        this.sessionId = sessionId;
        this.permits = permits;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long commitIndex() {
        return commitIndex;
    }

    @Override
    public long sessionId() {
        return sessionId;
    }

    public int permits() {
        return permits;
    }

    @Override
    public int getFactoryId() {
        return RaftSemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSemaphoreDataSerializerHook.SEMAPHORE_INVOCATION_KEY;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(name);
        out.writeLong(commitIndex);
        out.writeLong(sessionId);
        out.writeInt(permits);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        name = in.readUTF();
        commitIndex = in.readLong();
        sessionId = in.readLong();
        permits = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SemaphoreInvocationKey that = (SemaphoreInvocationKey) o;

        if (commitIndex != that.commitIndex) {
            return false;
        }
        if (sessionId != that.sessionId) {
            return false;
        }
        if (permits != that.permits) {
            return false;
        }
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (int) (commitIndex ^ (commitIndex >>> 32));
        result = 31 * result + (int) (sessionId ^ (sessionId >>> 32));
        result = 31 * result + permits;
        return result;
    }

    @Override
    public String toString() {
        return "SemaphoreInvocationKey{" + "name='" + name + '\'' + ", commitIndex=" + commitIndex + ", sessionId=" + sessionId
                + ", permits=" + permits + '}';
    }

}
