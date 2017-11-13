package com.hazelcast.raft.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.service.CreateRaftGroupHelper;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongProxy;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.raft.impl.RaftUtil.getLeaderEndpoint;
import static com.hazelcast.raft.impl.RaftUtil.getRaftNode;
import static com.hazelcast.raft.impl.RaftUtil.getRaftService;
import static com.hazelcast.raft.impl.RaftUtil.getRole;
import static com.hazelcast.raft.impl.service.RaftService.METADATA_RAFT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HazelcastRaftTest extends HazelcastRaftTestSupport {

    @Test
    public void crashedLeader_cannotRecoverAndRejoinRaftGroup() throws Exception {
        raftAddresses = createRaftAddresses(2);
        instances = newInstances(raftAddresses);

        RaftNode leader = waitAllForLeaderElection(instances, METADATA_RAFT);

        HazelcastInstance leaderInstance = factory.getInstance(leader.getLocalEndpoint().getAddress());
        final HazelcastInstance followerInstance = getRandomFollowerInstance(instances, leader);

        leaderInstance.shutdown();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                RaftNode raftNode = getRaftNode(followerInstance, METADATA_RAFT);
                assertEquals(RaftRole.CANDIDATE, getRole(raftNode));
            }
        });

        final HazelcastInstance newInstance = factory.newHazelcastInstance(leader.getLocalEndpoint().getAddress(),
                createConfig(raftAddresses));
        assertClusterSizeEventually(2, followerInstance);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                RaftNode raftNode = getRaftNode(followerInstance, METADATA_RAFT);
                assertEquals(RaftRole.CANDIDATE, getRole(raftNode));

                raftNode = getRaftNode(newInstance, METADATA_RAFT);
                assertEquals(RaftRole.CANDIDATE, getRole(raftNode));
            }
        }, 10);
    }

    @Test
    public void crashedFollower_cannotRecoverAndRejoinRaftGroup() throws Exception {
        raftAddresses = createRaftAddresses(2);
        instances = newInstances(raftAddresses);

        final RaftNode leader = waitAllForLeaderElection(instances, METADATA_RAFT);

        final HazelcastInstance leaderInstance = factory.getInstance(leader.getLocalEndpoint().getAddress());
        HazelcastInstance followerInstance = getRandomFollowerInstance(instances, leader);

        Address restartingAddress = getAddress(followerInstance);
        followerInstance.shutdown();

        final HazelcastInstance newInstance = factory.newHazelcastInstance(restartingAddress, createConfig(raftAddresses));
        assertClusterSize(2, leaderInstance);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                RaftNode raftNode = getRaftNode(leaderInstance, METADATA_RAFT);
                assertEquals(RaftRole.LEADER, getRole(raftNode));

                raftNode = getRaftNode(newInstance, METADATA_RAFT);
                assertEquals(RaftRole.CANDIDATE, getRole(raftNode));
            }
        }, 10);
    }

    @Test
    public void createNewRaftGroup() throws Exception {
        raftAddresses = createRaftAddresses(5);
        instances = newInstances(raftAddresses);

        final String name = "atomic";
        final int raftGroupSize = 3;

        CreateRaftGroupHelper.createRaftGroup(getNodeEngineImpl(instances[0]), name, raftGroupSize);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int count = 0;
                for (HazelcastInstance instance : instances) {
                    RaftNode raftNode = getRaftService(instance).getRaftNode(name);
                    if (raftNode != null) {
                        count++;
                        assertNotNull(getLeaderEndpoint(raftNode));
                    }
                }
                assertEquals(raftGroupSize, count);
            }
        });

    }

    @Test
    public void createNewAtomicLong() throws Exception {
        raftAddresses = createRaftAddresses(5);
        instances = newInstances(raftAddresses);

        RaftAtomicLongService service = getNodeEngineImpl(instances[0]).getService(RaftAtomicLongService.SERVICE_NAME);
        final int raftGroupSize = 3;
        final String name = "id";

        final IAtomicLong id = service.createNew(name, raftGroupSize);
        assertNotNull(id);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int count = 0;
                RaftAtomicLongProxy proxy = (RaftAtomicLongProxy) id;
                for (HazelcastInstance instance : instances) {
                    RaftNode raftNode = getRaftService(instance).getRaftNode(proxy.getRaftName());
                    if (raftNode != null) {
                        count++;
                        assertNotNull(getLeaderEndpoint(raftNode));
                    }
                }
                assertEquals(raftGroupSize, count);
            }
        });
    }

    @Override
    protected Config createConfig(Address[] addresses) {
        ServiceConfig atomicLongServiceConfig = new ServiceConfig().setEnabled(true)
                .setName(RaftAtomicLongService.SERVICE_NAME).setClassName(RaftAtomicLongService.class.getName());

        Config config = super.createConfig(addresses);
        config.getServicesConfig().addServiceConfig(atomicLongServiceConfig);
        return config;
    }
}
