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

package com.hazelcast.raft.service.countdownlatch;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
public class RaftCountDownLatchSnapshot implements IdentifiedDataSerializable {

    private RaftGroupId groupId;
    private String name;
    private final List<CountDownLatchInvocationKey> waitKeys = new ArrayList<CountDownLatchInvocationKey>();
    private int round;
    private int countDownFrom;
    private final Set<UUID> countDownUids = new HashSet<UUID>();

    public RaftCountDownLatchSnapshot() {
    }

    RaftCountDownLatchSnapshot(RaftGroupId groupId, String name, List<CountDownLatchInvocationKey> waitKeys,
                                      int round, int countDownFrom, Set<UUID> countDownUids) {
        this.groupId = groupId;
        this.name = name;
        this.waitKeys.addAll(waitKeys);
        this.round = round;
        this.countDownFrom = countDownFrom;
        this.countDownUids.addAll(countDownUids);
    }

    public RaftGroupId getGroupId() {
        return groupId;
    }

    public String getName() {
        return name;
    }

    List<CountDownLatchInvocationKey> getWaitKeys() {
        return waitKeys;
    }

    int getRound() {
        return round;
    }

    int getCountDownFrom() {
        return countDownFrom;
    }

    Set<UUID> getCountDownUids() {
        return countDownUids;
    }

    @Override
    public int getFactoryId() {
        return RaftCountDownLatchDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftCountDownLatchDataSerializerHook.COUNT_DOWN_LATCH_SNAPSHOT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(groupId);
        out.writeUTF(name);
        out.writeInt(waitKeys.size());
        for (CountDownLatchInvocationKey key : waitKeys) {
            out.writeObject(key);
        }
        out.writeInt(round);
        out.writeInt(countDownFrom);
        for (UUID uid : countDownUids) {
            out.writeLong(uid.getLeastSignificantBits());
            out.writeLong(uid.getMostSignificantBits());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        groupId = in.readObject();
        name = in.readUTF();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            CountDownLatchInvocationKey key = in.readObject();
            waitKeys.add(key);
        }
        round = in.readInt();
        countDownFrom = in.readInt();
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            long least = in.readLong();
            long most = in.readLong();
            UUID uid = new UUID(most, least);
            countDownUids.add(uid);
        }
    }

}
