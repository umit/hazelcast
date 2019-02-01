/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.raft.impl.state;

/**
 * Mutable state maintained by the leader of the Raft group for each follower.
 * In follower state, three variables are stored:
 * <ul>
 * <li>{@code nextIndex}: index of the next log entry to send to that server
 * (initialized to leader's {@code lastLogIndex + 1})</li>
 * <li>{@code matchIndex}: index of highest log entry known to be replicated
 * on server (initialized to 0, increases monotonically)</li>
 * <li>{@code waitingAppendAck}: a boolean flag indicating that leader is still
 * waiting for ACK to an append request</li>
 * </ul>
 */
public class FollowerState {

    private long matchIndex;

    private long nextIndex;

    private boolean waitingAppendAck;

    FollowerState(long matchIndex, long nextIndex) {
        this.matchIndex = matchIndex;
        this.nextIndex = nextIndex;
    }

    /**
     * Returns the match index for follower.
     */
    public long matchIndex() {
        return matchIndex;
    }

    /**
     * Sets the match index for follower.
     */
    public void matchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }

    /**
     * Returns the next index for follower.
     */
    public long nextIndex() {
        return nextIndex;
    }

    /**
     * Sets the next index for follower.
     */
    public void nextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    /**
     * Returns whether leader is waiting for ACK to a append request or not.
     */
    public boolean isWaitingAppendAck() {
        return waitingAppendAck;
    }

    /**
     * Sets the flag for waiting ACK to an append request.
     */
    public void setWaitingAppendAck() {
        this.waitingAppendAck = true;
    }

    /**
     * Clears the flag for waiting ACK to an append request
     * and returns whether or not flag was set previously.
     */
    public boolean clearWaitingAppendAck() {
        if (waitingAppendAck) {
            waitingAppendAck = false;
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "FollowerState{" + "matchIndex=" + matchIndex + ", nextIndex=" + nextIndex + ", waitingAppendAck="
                + waitingAppendAck + '}';
    }
}
