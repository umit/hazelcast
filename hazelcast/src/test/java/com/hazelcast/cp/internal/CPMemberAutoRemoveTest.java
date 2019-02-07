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

package com.hazelcast.cp.internal;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPMember;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static com.hazelcast.test.SplitBrainTestSupport.blockCommunicationBetween;
import static com.hazelcast.test.SplitBrainTestSupport.unblockCommunicationBetween;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CPMemberAutoRemoveTest extends HazelcastRaftTestSupport {

    private int missingRaftMemberRemovalSeconds;

    @Test
    public void when_missingCPNodeDoesNotJoin_then_itIsAutomaticallyRemoved() {
        missingRaftMemberRemovalSeconds = 10;
        final HazelcastInstance[] instances = newInstances(3, 3, 0);

        final CPMember terminatedMember = instances[2].getCPSubsystem().getLocalCPMember();
        instances[2].getLifecycleService().terminate();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                Collection<CPMemberInfo> activeMembers = getRaftService(instances[0]).getMetadataGroupManager()
                                                                                     .getActiveMembers();
                assertFalse(activeMembers.contains(terminatedMember));
                assertTrue(getRaftService(instances[0]).getMissingMembers().isEmpty());
            }
        });
    }

    @Test
    public void when_missingCPNodeJoins_then_itIsNotAutomaticallyRemoved() {
        missingRaftMemberRemovalSeconds = 300;
        final HazelcastInstance[] instances = newInstances(3, 3, 0);

        final CPMember cpMember0 = instances[0].getCPSubsystem().getLocalCPMember();
        final CPMember cpMember1 = instances[1].getCPSubsystem().getLocalCPMember();
        final CPMember cpMember2 = instances[2].getCPSubsystem().getLocalCPMember();

        blockCommunicationBetween(instances[1], instances[2]);
        blockCommunicationBetween(instances[0], instances[2]);

        closeConnectionBetween(instances[1], instances[2]);
        closeConnectionBetween(instances[0], instances[2]);

        assertClusterSizeEventually(2, instances[0], instances[1]);
        assertClusterSizeEventually(1, instances[2]);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(getRaftService(instances[0]).getMissingMembers().contains(cpMember2));
                assertTrue(getRaftService(instances[1]).getMissingMembers().contains(cpMember2));
                assertTrue(getRaftService(instances[2]).getMissingMembers().contains(cpMember0));
                assertTrue(getRaftService(instances[2]).getMissingMembers().contains(cpMember1));
            }
        }, 20);

        unblockCommunicationBetween(instances[1], instances[2]);
        unblockCommunicationBetween(instances[0], instances[2]);

        assertClusterSizeEventually(3, instances);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(getRaftService(instances[0]).getMissingMembers().isEmpty());
                assertTrue(getRaftService(instances[1]).getMissingMembers().isEmpty());
                assertTrue(getRaftService(instances[2]).getMissingMembers().isEmpty());
            }
        });
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5")
              .setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "5")
              .getCPSubsystemConfig()
              .setSessionTimeToLiveSeconds(missingRaftMemberRemovalSeconds)
              .setMissingCPMemberAutoRemovalSeconds(missingRaftMemberRemovalSeconds);

        return config;
    }

}