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

package com.hazelcast.raft.impl.service;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftMember;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.raft.impl.service.operation.metadata.GetActiveMembersOp;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.raft.impl.service.RaftService.METADATA_GROUP_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemberAddRemoveTest extends HazelcastRaftTestSupport {

    @Test
    public void testPromoteToRaftMember() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(2, 2, 1);

        final RaftService service = getRaftService(instances[instances.length - 1]);
        service.triggerRaftMemberPromotion().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotNull(service.getMetadataManager().getLocalMember());
            }
        });
    }

    @Test
    public void testPromoteToRaftMemberIdempotency() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(2, 2, 1);

        final RaftService service = getRaftService(instances[instances.length - 1]);
        service.triggerRaftMemberPromotion().get();
        service.triggerRaftMemberPromotion().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotNull(service.getMetadataManager().getLocalMember());
            }
        });
    }

    @Test
    public void testRemoveRaftMember() throws ExecutionException, InterruptedException {
        final HazelcastInstance[] instances = newInstances(3);

        final RaftGroupId testGroupId = getRaftInvocationManager(instances[0]).createRaftGroup("test", 3).get();

        final RaftService service = getRaftService(instances[1]);
        Member member = instances[0].getCluster().getLocalMember();
        instances[0].getLifecycleService().terminate();

        assertClusterSizeEventually(2, instances[1]);

        final RaftMemberImpl removedEndpoint = new RaftMemberImpl(member);
        service.removeRaftMember(removedEndpoint).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftGroupInfo metadataGroupInfo = getRaftGroup(instances[1], METADATA_GROUP_ID);
                assertEquals(2, metadataGroupInfo.memberCount());
                assertFalse(metadataGroupInfo.containsMember(removedEndpoint));

                RaftGroupInfo testGroupInfo = getRaftGroup(instances[1], testGroupId);
                assertEquals(2, testGroupInfo.memberCount());
                assertFalse(testGroupInfo.containsMember(removedEndpoint));
            }
        });
    }

    @Test
    public void testRemoveRaftMemberIdempotency() throws ExecutionException, InterruptedException {
        final HazelcastInstance[] instances = newInstances(3);

        final RaftGroupId testGroupId = getRaftInvocationManager(instances[0]).createRaftGroup("test", 3).get();

        final RaftService service = getRaftService(instances[1]);
        Member member = instances[0].getCluster().getLocalMember();
        instances[0].getLifecycleService().terminate();

        assertClusterSizeEventually(2, instances[1]);

        final RaftMemberImpl removedEndpoint = new RaftMemberImpl(member);
        service.removeRaftMember(removedEndpoint).get();
        service.removeRaftMember(removedEndpoint).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftGroupInfo metadataGroup = getRaftGroup(instances[1], METADATA_GROUP_ID);
                assertEquals(2, metadataGroup.memberCount());
                assertFalse(metadataGroup.containsMember(removedEndpoint));

                RaftGroupInfo testGroup = getRaftGroup(instances[1], testGroupId);
                assertEquals(2, testGroup.memberCount());
                assertFalse(testGroup.containsMember(removedEndpoint));
            }
        });
    }

    @Test
    public void testMetadataGroupReinitializationAfterLostMajority() {
        HazelcastInstance[] instances = newInstances(3, 3, 1);

        waitAllForLeaderElection(Arrays.copyOf(instances, 3), METADATA_GROUP_ID);

        instances[1].getLifecycleService().terminate();
        instances[2].getLifecycleService().terminate();
        assertClusterSizeEventually(2, instances[3]);

        HazelcastInstance[] newInstances = new HazelcastInstance[3];
        newInstances[0] = instances[0];
        newInstances[1] = instances[3];

        getRaftService(newInstances[0]).resetAndInitRaftState();
        getRaftService(newInstances[1]).resetAndInitRaftState();

        Config config = createConfig(3, 3);
        config.getRaftServiceConfig().getMetadataGroupConfig().setInitialRaftMember(true);
        newInstances[2] = factory.newHazelcastInstance(config);

        waitAllForLeaderElection(newInstances, METADATA_GROUP_ID);

        RaftGroupInfo group = getRaftGroup(newInstances[2], METADATA_GROUP_ID);
        Collection<RaftMemberImpl> endpoints = group.endpointImpls();

        for (HazelcastInstance instance : newInstances) {
            Member localMember = instance.getCluster().getLocalMember();
            RaftMemberImpl endpoint = new RaftMemberImpl(localMember);
            assertTrue(endpoints.contains(endpoint));
        }
    }

    @Test
    public void testRaftInvocationsAfterMetadataGroupReinitialization() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3, 3, 1);

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[3]);
        invocationManager.invoke(METADATA_GROUP_ID, new GetActiveMembersOp()).get();

        instances[0].getLifecycleService().terminate();
        instances[1].getLifecycleService().terminate();
        instances[2].getLifecycleService().terminate();
        assertClusterSizeEventually(1, instances[3]);

        Config config = createConfig(3, 3);
        config.getRaftServiceConfig().getMetadataGroupConfig().setInitialRaftMember(true);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        invocationManager.invoke(METADATA_GROUP_ID, new GetActiveMembersOp()).get();
    }

    @Test
    public void testResetRaftStateWhileMajorityIsReachable() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3, 3, 1);

        waitAllForLeaderElection(Arrays.copyOf(instances, 3), METADATA_GROUP_ID);

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[3]);

        instances[0].getLifecycleService().terminate();
        assertClusterSizeEventually(3, instances[1], instances[2], instances[3]);

        getRaftService(instances[1]).resetAndInitRaftState();
        getRaftService(instances[2]).resetAndInitRaftState();

        Config config = createConfig(3, 3);
        config.getRaftServiceConfig().getMetadataGroupConfig().setInitialRaftMember(true);
        HazelcastInstance newInstance = factory.newHazelcastInstance(config);

        List<RaftMember> newEndpoints = invocationManager.<List<RaftMember>>invoke(METADATA_GROUP_ID, new GetActiveMembersOp()).get();

        assertTrue(newEndpoints.contains(new RaftMemberImpl(instances[1].getCluster().getLocalMember())));
        assertTrue(newEndpoints.contains(new RaftMemberImpl(instances[2].getCluster().getLocalMember())));
        assertTrue(newEndpoints.contains(new RaftMemberImpl(newInstance.getCluster().getLocalMember())));

        invocationManager.invoke(METADATA_GROUP_ID, new GetActiveMembersOp()).get();
    }

}
