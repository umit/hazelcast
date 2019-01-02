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

package com.hazelcast.cp.internal.datastructures.countdownlatch;

import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.cp.internal.datastructures.spi.blocking.WaitKey;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Represents a {@link ICountDownLatch#await(long, TimeUnit)}} invocation
 */
public class AwaitInvocationKey implements WaitKey, IdentifiedDataSerializable {

    private String name;
    private long commitIndex;

    AwaitInvocationKey() {
    }

    AwaitInvocationKey(String name, long commitIndex) {
        checkNotNull(name);
        this.name = name;
        this.commitIndex = commitIndex;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long sessionId() {
        return NO_SESSION_ID;
    }

    @Override
    public long commitIndex() {
        return commitIndex;
    }

    @Override
    public int getFactoryId() {
        return RaftCountDownLatchDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftCountDownLatchDataSerializerHook.AWAIT_INVOCATION_KEY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeLong(commitIndex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        commitIndex = in.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AwaitInvocationKey that = (AwaitInvocationKey) o;

        if (commitIndex != that.commitIndex) {
            return false;
        }
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (int) (commitIndex ^ (commitIndex >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "AwaitInvocationKey{" + "name='" + name + '\'' + ", commitIndex=" + commitIndex + '}';
    }

}
