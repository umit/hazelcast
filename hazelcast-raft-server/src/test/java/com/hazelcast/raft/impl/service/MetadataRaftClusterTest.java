package com.hazelcast.raft.impl.service;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongService;
import com.hazelcast.raft.service.atomiclong.proxy.RaftAtomicLongProxy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.raft.impl.RaftUtil.getLeaderEndpoint;
import static com.hazelcast.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.raft.impl.service.RaftService.METADATA_GROUP_ID;
import static com.hazelcast.raft.impl.service.RaftServiceUtil.getRaftGroupInfo;
import static com.hazelcast.raft.impl.service.RaftServiceUtil.getRaftNode;
import static com.hazelcast.test.SplitBrainTestSupport.blockCommunicationBetween;
import static com.hazelcast.test.SplitBrainTestSupport.unblockCommunicationBetween;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MetadataRaftClusterTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;

    @Test
    public void when_clusterStarts_then_metadataClusterIsInitialized() {
        int cpNodeCount = 3;
        Address[] raftAddresses = createAddresses(cpNodeCount);
        instances = newInstances(raftAddresses, cpNodeCount + 2);

        final List<Address> raftAddressesList = Arrays.asList(raftAddresses);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    if (raftAddressesList.contains(getAddress(instance))) {
                        assertNotNull(getRaftNode(instance, METADATA_GROUP_ID));
                    }
                }
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    if (!raftAddressesList.contains(getAddress(instance))) {
                        assertNull(getRaftNode(instance, METADATA_GROUP_ID));
                    }
                }
            }
        }, 10);
    }

    @Test
    public void when_raftGroupIsCreatedWithAllCPNodes_then_raftNodeIsCreatedOnAll()  {
        int nodeCount = 5;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        final RaftAtomicLongProxy atomicLong = (RaftAtomicLongProxy) RaftAtomicLongProxy.create(instances[0], "id", nodeCount);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance instance : instances) {
                    assertNotNull(getRaftNode(instance, atomicLong.getGroupId()));
                }
            }
        });
    }

    @Test
    public void when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints() {
        when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints(true);
    }

    @Test
    public void when_raftGroupIsCreatedFromNonCPNode_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints() {
        when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints(false);
    }

    private void when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints(boolean invokeOnCP) {
        int cpNodeCount = 4;
        int nodeCount = 6;
        Address[] raftAddresses = createAddresses(cpNodeCount);
        instances = newInstances(raftAddresses, nodeCount);

        final int newGroupCount = 3;

        HazelcastInstance instance = instances[invokeOnCP ? 0 : instances.length - 1];
        final RaftAtomicLongProxy atomicLong = (RaftAtomicLongProxy) RaftAtomicLongProxy.create(instance, "id", newGroupCount);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int count = 0;
                for (HazelcastInstance instance : instances) {
                    RaftNode raftNode = getRaftNode(instance, atomicLong.getGroupId());
                    if (raftNode != null) {
                        count++;
                    }
                }

                assertEquals(newGroupCount, count);
            }
        });
    }

    @Test
    public void when_sizeOfRaftGroupIsLargerThanCPNodeCount_then_raftGroupCannotBeCreated() {
        int nodeCount = 3;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        try {
            RaftAtomicLongProxy.create(instances[0], "id", nodeCount + 1);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void when_raftGroupIsCreatedWithSameSizeMultipleTimes_then_itSucceeds() {
        int nodeCount = 3;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);
        final RaftAtomicLongProxy atomicLong1 = (RaftAtomicLongProxy) RaftAtomicLongProxy.create(instances[0], "id", nodeCount);
        final RaftAtomicLongProxy atomicLong2 = (RaftAtomicLongProxy) RaftAtomicLongProxy.create(instances[1], "id", nodeCount);
        assertEquals(atomicLong1.getGroupId(), atomicLong2.getGroupId());
    }

    @Test
    public void when_raftGroupIsCreatedWithDifferentSizeMultipleTimes_then_itFails() {
        int nodeCount = 3;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        RaftAtomicLongProxy.create(instances[0], "id", nodeCount);
        try {
            RaftAtomicLongProxy.create(instances[0], "id", nodeCount - 1);
            fail();
        } catch (IllegalStateException ignored) {
        }
    }

    @Test
    public void when_raftGroupTriggerDestroyIsCommitted_then_raftGroupStatusIsUpdated() {
        int nodeCount = 3;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        final RaftAtomicLongProxy atomicLong = (RaftAtomicLongProxy) RaftAtomicLongProxy.create(instances[0], "id", nodeCount);
        atomicLong.destroy();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance instance : instances) {
                    RaftGroupInfo groupInfo = getRaftGroupInfo(instance, atomicLong.getGroupId());
                    assertNotNull(groupInfo);
                    assertEquals(RaftGroupStatus.DESTROYED, groupInfo.status());
                    assertNull(getRaftNode(instance, atomicLong.getGroupId()));
                }
            }
        });
    }

    @Test
    public void when_raftGroupDestroyTriggeredMultipleTimes_then_destroyDoesNotFail() {
        int nodeCount = 3;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        final RaftAtomicLongProxy atomicLong = (RaftAtomicLongProxy) RaftAtomicLongProxy.create(instances[0], "id", nodeCount);
        atomicLong.destroy();
        atomicLong.destroy();
    }

    @Test
    public void when_raftGroupIsDestroyed_then_itCanBeCreatedAgain() {
        int nodeCount = 3;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        final RaftAtomicLongProxy atomicLong = (RaftAtomicLongProxy) RaftAtomicLongProxy.create(instances[0], "id", nodeCount);
        atomicLong.destroy();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance instance : instances) {
                    RaftGroupInfo groupInfo = getRaftGroupInfo(instance, atomicLong.getGroupId());
                    assertNotNull(groupInfo);
                    assertEquals(RaftGroupStatus.DESTROYED, groupInfo.status());
                }
            }
        });

        RaftAtomicLongProxy.create(instances[0], "id", nodeCount - 1);
    }

    @Test
    public void when_metadataClusterNodeFallsFarBehind_then_itInstallsSnapshot() {
        int nodeCount = 3;
        int commitCountToSnapshot = 5;
        Address[] raftAddresses = createAddresses(nodeCount);
        Config config = createConfig(raftAddresses);
        RaftConfig raftConfig = (RaftConfig) config.getServicesConfig().getServiceConfig(RaftService.SERVICE_NAME).getConfigObject();
        raftConfig.setCommitIndexAdvanceCountToSnapshot(commitCountToSnapshot);

        final HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = factory.newHazelcastInstance(raftAddresses[i], config);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance instance : instances) {
                    RaftNode raftNode = getRaftNode(instance, METADATA_GROUP_ID);
                    assertNotNull(raftNode);
                }
            }
        });

        RaftEndpoint leaderEndpoint = getLeaderEndpoint(getRaftNode(instances[0], METADATA_GROUP_ID));
        final HazelcastInstance leader = factory.getInstance(leaderEndpoint.getAddress());
        HazelcastInstance follower = null;
        for (HazelcastInstance instance : instances) {
            if (!getAddress(instance).equals(leaderEndpoint.getAddress())) {
                follower = instance;
                break;
            }
        }

        assertNotNull(follower);
        blockCommunicationBetween(leader, follower);

        final List<RaftGroupId> groupIds = new ArrayList<RaftGroupId>();
        for (int i = 0; i < commitCountToSnapshot; i++) {
            RaftAtomicLongProxy atomicLong = (RaftAtomicLongProxy) RaftAtomicLongProxy.create(leader, "id" + i, nodeCount);
            groupIds.add(atomicLong.getGroupId());
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(getSnapshotEntry(getRaftNode(leader, METADATA_GROUP_ID)).index() > 0);
            }
        });

        unblockCommunicationBetween(leader, follower);

        final HazelcastInstance f = follower;
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftGroupId groupId : groupIds) {
                    assertNotNull(getRaftNode(f, groupId));
                }
            }
        });
    }

    @Test
    public void when_shutdownMember_blabla() {
        int nodeCount = 4;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        RaftAtomicLongProxy atomicLong = (RaftAtomicLongProxy) RaftAtomicLongProxy.create(instances[0], "id", nodeCount - 1);
        instances[0].shutdown();
    }

    @Override
    protected Config createConfig(Address[] raftAddresses) {
        ServiceConfig atomicLongServiceConfig = new ServiceConfig().setEnabled(true)
                                                                   .setName(RaftAtomicLongService.SERVICE_NAME)
                                                                   .setClassName(RaftAtomicLongService.class.getName());

        Config config = super.createConfig(raftAddresses);
        config.getServicesConfig().addServiceConfig(atomicLongServiceConfig);
        return config;
    }

}
