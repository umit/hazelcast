package com.hazelcast.raft.impl.service;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus;
import com.hazelcast.raft.impl.service.operation.metadata.GetActiveEndpointsOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetRaftGroupOp;
import com.hazelcast.raft.impl.service.proxy.DefaultRaftQueryOperation;
import com.hazelcast.raft.impl.service.proxy.RaftQueryOperation;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongService;
import com.hazelcast.raft.service.atomiclong.proxy.RaftAtomicLongProxy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.function.Supplier;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.raft.impl.RaftUtil.getLeaderEndpoint;
import static com.hazelcast.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.raft.impl.service.RaftService.METADATA_GROUP_ID;
import static com.hazelcast.raft.impl.service.RaftServiceUtil.getRaftNode;
import static com.hazelcast.test.SplitBrainTestSupport.blockCommunicationBetween;
import static com.hazelcast.test.SplitBrainTestSupport.unblockCommunicationBetween;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MetadataRaftClusterTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;

    @Test
    public void when_clusterStartsWithNonCPNodes_then_metadataClusterIsInitialized() {
        int cpNodeCount = 3;
        Address[] raftAddresses = createAddresses(cpNodeCount);
        instances = newInstances(raftAddresses, cpNodeCount,2);

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
    public void when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyThem() {
        final int nodeCount = 5;
        final int metadataGroupSize = 3;
        final Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses, metadataGroupSize, 0);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (Address address : Arrays.asList(raftAddresses).subList(0, metadataGroupSize)) {
                    HazelcastInstance instance = factory.getInstance(address);
                    assertNotNull(getRaftNode(instance, METADATA_GROUP_ID));
                }
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                for (Address address : Arrays.asList(raftAddresses).subList(metadataGroupSize, raftAddresses.length)) {
                    HazelcastInstance instance = factory.getInstance(address);
                    assertNull(getRaftNode(instance, METADATA_GROUP_ID));
                }
            }
        }, 10);
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
        int cpNodeCount = 5;
        int metadataGroupSize = 2;
        int nonCpNodeCount = 2;
        Address[] raftAddresses = createAddresses(cpNodeCount);
        instances = newInstances(raftAddresses, metadataGroupSize, nonCpNodeCount);

        final int newGroupCount = metadataGroupSize + 1;

        HazelcastInstance instance = instances[invokeOnCP ? 0 : instances.length - 1];
        final RaftAtomicLongProxy atomicLong = (RaftAtomicLongProxy) RaftAtomicLongProxy.create(instance, "id", newGroupCount);
        atomicLong.set(5);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int count = 0;
                for (HazelcastInstance instance : instances) {
                    RaftNodeImpl raftNode = getRaftNode(instance, atomicLong.getGroupId());
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
    public void when_raftGroupDestroyTriggered_then_raftGroupIsDestroyed() {
        int metadataGroupSize = 3;
        int cpNodeCount = 5;
        Address[] raftAddresses = createAddresses(cpNodeCount);
        instances = newInstances(raftAddresses, metadataGroupSize, 0);

        final RaftAtomicLongProxy atomicLong = (RaftAtomicLongProxy) RaftAtomicLongProxy.create(instances[0], "id", cpNodeCount);
        final RaftGroupId groupId = atomicLong.getGroupId();
        atomicLong.destroy();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    assertNull(getRaftNode(instance, groupId));
                }
            }
        });

        final RaftInvocationManager invocationService = getRaftInvocationService(instances[0]);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Future<RaftGroupInfo> f = invocationService.query(new Supplier<RaftQueryOperation>() {
                    @Override
                    public RaftQueryOperation get() {
                        return new DefaultRaftQueryOperation(METADATA_GROUP_ID, new GetRaftGroupOp(groupId));
                    }
                }, QueryPolicy.LEADER_LOCAL);

                RaftGroupInfo groupInfo = f.get();
                assertEquals(RaftGroupStatus.DESTROYED, groupInfo.status());
            }
        });
    }

    @Test
    public void when_raftGroupDestroyTriggeredMultipleTimes_then_destroyDoesNotFail() {
        int metadataGroupSize = 3;
        int cpNodeCount = 5;
        Address[] raftAddresses = createAddresses(cpNodeCount);
        instances = newInstances(raftAddresses, metadataGroupSize, 0);

        final RaftAtomicLongProxy atomicLong = (RaftAtomicLongProxy) RaftAtomicLongProxy.create(instances[0], "id", cpNodeCount);
        atomicLong.destroy();
        atomicLong.destroy();
    }

    @Test
    public void when_raftGroupIsDestroyed_then_itCanBeCreatedAgain() {
        int metadataGroupSize = 3;
        int cpNodeCount = 5;
        Address[] raftAddresses = createAddresses(cpNodeCount);
        instances = newInstances(raftAddresses, metadataGroupSize, 0);

        final RaftAtomicLongProxy atomicLong = (RaftAtomicLongProxy) RaftAtomicLongProxy.create(instances[0], "id", cpNodeCount);
        atomicLong.destroy();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance instance : instances) {
                    assertNull(getRaftNode(instance, atomicLong.getGroupId()));
                }
            }
        });

        RaftAtomicLongProxy.create(instances[0], "id", cpNodeCount - 1);
    }

    @Test
    public void when_metadataClusterNodeFallsFarBehind_then_itInstallsSnapshot() {
        int nodeCount = 3;
        int commitCountToSnapshot = 5;
        Address[] raftAddresses = createAddresses(nodeCount);
        Config config = createConfig(raftAddresses, raftAddresses.length);
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
                    RaftNodeImpl raftNode = getRaftNode(instance, METADATA_GROUP_ID);
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
    public void when_memberIsShutdown_then_itIsRemovedFromRaftGroups()
            throws ExecutionException, InterruptedException {
        int cpNodeCount = 5;
        int metadataGroupSize = cpNodeCount - 1;
        int atomicLong1GroupSize = cpNodeCount - 2;
        Address[] raftAddresses = createAddresses(cpNodeCount);
        instances = newInstances(raftAddresses, metadataGroupSize, 0);

        RaftAtomicLongProxy atomicLong1 = (RaftAtomicLongProxy) RaftAtomicLongProxy.create(instances[0], "id1", atomicLong1GroupSize);
        final RaftGroupId groupId1 = atomicLong1.getGroupId();
        RaftAtomicLongProxy atomicLong2 = (RaftAtomicLongProxy) RaftAtomicLongProxy.create(instances[0], "id2", cpNodeCount);
        final RaftGroupId groupId2 = atomicLong2.getGroupId();

        RaftEndpoint endpoint = findCommonEndpoint(instances[0], METADATA_GROUP_ID, groupId1);
        assertNotNull(endpoint);

        RaftInvocationManager invocationService = null;
        for (HazelcastInstance instance : instances) {
            if (!getAddress(instance).equals(endpoint.getAddress())) {
                invocationService = getRaftInvocationService(instance);
                break;
            }
        }
        assertNotNull(invocationService);

        factory.getInstance(endpoint.getAddress()).shutdown();

        ICompletableFuture<List<RaftEndpoint>> f1 = invocationService.query(new Supplier<RaftQueryOperation>() {
            @Override
            public RaftQueryOperation get() {
                return new DefaultRaftQueryOperation(METADATA_GROUP_ID, new GetActiveEndpointsOp());
            }
        }, QueryPolicy.LEADER_LOCAL);

        boolean active = false;
        for (RaftEndpoint activeEndpoint : f1.get()) {
            if (activeEndpoint.equals(endpoint)) {
                active = true;
                break;
            }
        }

        assertFalse(active);

        ICompletableFuture<RaftGroupInfo> f2 = invocationService.query(new Supplier<RaftQueryOperation>() {
            @Override
            public RaftQueryOperation get() {
                return new DefaultRaftQueryOperation(METADATA_GROUP_ID, new GetRaftGroupOp(METADATA_GROUP_ID));
            }
        }, QueryPolicy.LEADER_LOCAL);

        ICompletableFuture<RaftGroupInfo> f3 = invocationService.query(new Supplier<RaftQueryOperation>() {
            @Override
            public RaftQueryOperation get() {
                return new DefaultRaftQueryOperation(METADATA_GROUP_ID, new GetRaftGroupOp(groupId1));
            }
        }, QueryPolicy.LEADER_LOCAL);

        ICompletableFuture<RaftGroupInfo> f4 = invocationService.query(new Supplier<RaftQueryOperation>() {
            @Override
            public RaftQueryOperation get() {
                return new DefaultRaftQueryOperation(METADATA_GROUP_ID, new GetRaftGroupOp(groupId2));
            }
        }, QueryPolicy.LEADER_LOCAL);

        RaftGroupInfo metadataGroup = f2.get();
        assertFalse(metadataGroup.containsMember(endpoint));
        assertEquals(metadataGroupSize, metadataGroup.memberCount());
        RaftGroupInfo atomicLongGroup1 = f3.get();
        assertFalse(atomicLongGroup1.containsMember(endpoint));
        assertEquals(atomicLong1GroupSize, atomicLongGroup1.memberCount());
        RaftGroupInfo atomicLongGroup2 = f4.get();
        assertFalse(atomicLongGroup2.containsMember(endpoint));
        assertEquals(cpNodeCount - 1, atomicLongGroup2.memberCount());
    }

    private RaftEndpoint findCommonEndpoint(HazelcastInstance instance, final RaftGroupId groupId1, final RaftGroupId groupId2)
            throws ExecutionException, InterruptedException {
        RaftInvocationManager invocationService = getRaftInvocationService(instance);
        ICompletableFuture<RaftGroupInfo> f1 = invocationService.query(new Supplier<RaftQueryOperation>() {
            @Override
            public RaftQueryOperation get() {
                return new DefaultRaftQueryOperation(METADATA_GROUP_ID, new GetRaftGroupOp(groupId1));
            }
        }, QueryPolicy.LEADER_LOCAL);
        ICompletableFuture<RaftGroupInfo> f2 = invocationService.query(new Supplier<RaftQueryOperation>() {
            @Override
            public RaftQueryOperation get() {
                return new DefaultRaftQueryOperation(METADATA_GROUP_ID, new GetRaftGroupOp(groupId2));
            }
        }, QueryPolicy.LEADER_LOCAL);
        RaftGroupInfo group1 = f1.get();
        RaftGroupInfo group2 = f2.get();

        Set<RaftEndpoint> members = new HashSet<RaftEndpoint>(group1.members());
        members.retainAll(group2.members());

        return members.isEmpty() ? null : members.iterator().next();
    }

    @Override
    protected Config createConfig(Address[] raftAddresses, int metadataGroupSize) {
        ServiceConfig atomicLongServiceConfig = new ServiceConfig().setEnabled(true)
                                                                   .setName(RaftAtomicLongService.SERVICE_NAME)
                                                                   .setClassName(RaftAtomicLongService.class.getName());

        Config config = super.createConfig(raftAddresses, metadataGroupSize);
        RaftConfig raftConfig =
                (RaftConfig) config.getServicesConfig().getServiceConfig(RaftService.SERVICE_NAME).getConfigObject();
        raftConfig.setAppendNopEntryOnLeaderElection(true);

        config.getServicesConfig().addServiceConfig(atomicLongServiceConfig);
        return config;
    }

}
