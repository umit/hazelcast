/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition;

import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public final class PartitionRuntimeState implements IdentifiedDataSerializable {

    private MemberInfo[] members;
    private int[][] minimizedPartitionTable;
    private int version;
    private Collection<MigrationInfo> completedMigrations;
    // used to know ongoing migrations when master changed
    private MigrationInfo activeMigration;

    // transient fields
    private ILogger logger;
    private Address endpoint;

    public PartitionRuntimeState() {
    }

    public PartitionRuntimeState(ILogger logger,
                                 Collection<MemberInfo> memberList,
                                 InternalPartition[] partitions,
                                 Collection<MigrationInfo> migrationInfos,
                                 int version) {
        this.logger = logger;
        this.version = version;

        members = new MemberInfo[memberList.size()];
        Map<Address, Integer> addressIndexes = new HashMap<Address, Integer>(memberList.size());
        int memberIndex = 0;
        for (MemberInfo member : memberList) {
            addMember(member, addressIndexes, memberIndex);
            memberIndex++;
        }
        setPartitions(partitions, addressIndexes);
        completedMigrations = migrationInfos != null ? migrationInfos : new ArrayList<MigrationInfo>(0);
    }

    private void addMember(MemberInfo member, Map<Address, Integer> addressIndexes, int memberIndex) {
        members[memberIndex] = member;
        addressIndexes.put(member.getAddress(), memberIndex);
    }

    private void setPartitions(InternalPartition[] partitions, Map<Address, Integer> addressIndexes) {
        minimizedPartitionTable = new int[partitions.length][InternalPartition.MAX_REPLICA_COUNT];

        List<String> unmatchedAddresses = new LinkedList<String>();
        for (InternalPartition partition : partitions) {
            int[] indexes = minimizedPartitionTable[partition.getPartitionId()];

            for (int replicaIndex = 0; replicaIndex < InternalPartition.MAX_REPLICA_COUNT; replicaIndex++) {
                Address address = partition.getReplicaAddress(replicaIndex);
                if (address == null) {
                    indexes[replicaIndex] = -1;
                } else {
                    Integer knownIndex = addressIndexes.get(address);

                    if (knownIndex == null && replicaIndex == 0) {
                        unmatchedAddresses.add(address + " -> " + partition);
                    }
                    if (knownIndex == null) {
                        indexes[replicaIndex] = -1;
                    } else {
                        indexes[replicaIndex] = knownIndex;
                    }
                }
            }
        }

        if (logger.isFineEnabled() && !unmatchedAddresses.isEmpty()) {
            // it can happen that the primary address at any given moment is not known,
            // most probably because master node has updated/published the partition table yet
            // or partition table update is not received yet.
            logger.fine("Unknown owner addresses in partition state! "
                    + "(Probably they have recently joined to or left the cluster.) " + unmatchedAddresses);
        }
    }

    public Address[][] getPartitionTable() {
        int length = minimizedPartitionTable.length;
        Address[][] result = new Address[length][InternalPartition.MAX_REPLICA_COUNT];
        for (int partitionId = 0; partitionId < length; partitionId++) {
            Address[] replicas = result[partitionId];
            int[] addressIndexes = minimizedPartitionTable[partitionId];
            for (int replicaIndex = 0; replicaIndex < addressIndexes.length; replicaIndex++) {
                int index = addressIndexes[replicaIndex];
                if (index != -1) {
                    replicas[replicaIndex] = members[index].getAddress();
                }
            }
        }
        return result;
    }

    public MemberInfo[] getMembers() {
        return members;
    }

    public Address getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(final Address endpoint) {
        this.endpoint = endpoint;
    }

    public Collection<MigrationInfo> getCompletedMigrations() {
        return completedMigrations != null ? completedMigrations : Collections.<MigrationInfo>emptyList();
    }

    public MigrationInfo getActiveMigration() {
        return activeMigration;
    }

    public void setActiveMigration(MigrationInfo activeMigration) {
        this.activeMigration = activeMigration;
    }

    public void setCompletedMigrations(Collection<MigrationInfo> completedMigrations) {
        this.completedMigrations = completedMigrations;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        version = in.readInt();
        int size = in.readInt();
        members = new MemberInfo[size];
        for (int memberIndex = 0; memberIndex < size; memberIndex++) {
            MemberInfo memberInfo = new MemberInfo();
            memberInfo.readData(in);
            members[memberIndex] = memberInfo;
        }

        int partitionCount = in.readInt();
        minimizedPartitionTable = new int[partitionCount][InternalPartition.MAX_REPLICA_COUNT];
        for (int i = 0; i < partitionCount; i++) {
            int[] indexes = minimizedPartitionTable[i];
            for (int ix = 0; ix < InternalPartition.MAX_REPLICA_COUNT; ix++) {
                indexes[ix] = in.readInt();
            }
        }

        if (in.readBoolean()) {
            activeMigration = new MigrationInfo();
            activeMigration.readData(in);
        }

        int k = in.readInt();
        if (k > 0) {
            completedMigrations = new ArrayList<MigrationInfo>(k);
            for (int i = 0; i < k; i++) {
                MigrationInfo migrationInfo = new MigrationInfo();
                migrationInfo.readData(in);
                completedMigrations.add(migrationInfo);
            }
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(version);
        int memberSize = members.length;
        out.writeInt(memberSize);
        for (MemberInfo memberInfo : members) {
            memberInfo.writeData(out);
        }

        out.writeInt(minimizedPartitionTable.length);
        for (int[] indexes : minimizedPartitionTable) {
            for (int ix = 0; ix < InternalPartition.MAX_REPLICA_COUNT; ix++) {
                out.writeInt(indexes[ix]);
            }
        }

        if (activeMigration != null) {
            out.writeBoolean(true);
            activeMigration.writeData(out);
        } else {
            out.writeBoolean(false);
        }

        if (completedMigrations != null) {
            int k = completedMigrations.size();
            out.writeInt(k);
            for (MigrationInfo migrationInfo : completedMigrations) {
                migrationInfo.writeData(out);
            }
        } else {
            out.writeInt(0);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PartitionRuntimeState [" + version + "]{\n");
        for (MemberInfo address : members) {
            sb.append(address).append('\n');
        }
        sb.append(", completedMigrations=").append(completedMigrations);
        sb.append('}');
        return sb.toString();
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.PARTITION_STATE;
    }
}
