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

import com.hazelcast.raft.service.blocking.WaitKey;

/**
 * TODO: Javadoc Pending...
 */
public class SemaphoreInvocationKey implements WaitKey {

    private final long commitIndex;
    private final String name;
    private final long sessionId;
    private final int permits;

    public SemaphoreInvocationKey(long commitIndex, String name, long sessionId, int permits) {
        this.commitIndex = commitIndex;
        this.name = name;
        this.sessionId = sessionId;
        this.permits = permits;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long sessionId() {
        return sessionId;
    }

    @Override
    public long commitIndex() {
        return commitIndex;
    }

    public int permits() {
        return permits;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SemaphoreInvocationKey)) {
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
        int result = (int) (commitIndex ^ (commitIndex >>> 32));
        result = 31 * result + name.hashCode();
        result = 31 * result + (int) (sessionId ^ (sessionId >>> 32));
        result = 31 * result + permits;
        return result;
    }
}
