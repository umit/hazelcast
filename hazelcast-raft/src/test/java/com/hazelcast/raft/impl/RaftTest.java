package com.hazelcast.raft.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.raft.impl.RaftDataSerializerHook.APPEND_RESPONSE_OP;
import static com.hazelcast.raft.impl.RaftDataSerializerHook.VOTE_RESPONSE_OP;
import static com.hazelcast.raft.impl.RaftService.METADATA_RAFT;
import static com.hazelcast.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.raft.impl.RaftUtil.getLastLogEntry;
import static com.hazelcast.raft.impl.RaftUtil.getRaftNode;
import static com.hazelcast.raft.impl.RaftUtil.waitUntilLeaderElected;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.SplitBrainTestSupport.blockCommunicationBetween;
import static com.hazelcast.test.SplitBrainTestSupport.unblockCommunicationBetween;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class RaftTest extends HazelcastTestSupport {

    private HazelcastInstance[] instances;

    private TestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = createHazelcastInstanceFactory(3);
    }

    @Test
    public void testNextLeaderCommitsPreviousLeadersEntry() throws ExecutionException, InterruptedException {
        Config config = createRaftConfig();
        configureSplitBrainDelay(config);

        instances = factory.newInstances(config);
        assertClusterSizeEventually(3, instances);

        final RaftNode leader = getLeader(METADATA_RAFT);

        Future f1 = leader.replicate("val1");
        f1.get();

        HazelcastInstance leaderInstance = factory.getInstance(leader.getThisAddress());
        final List<HazelcastInstance> followerInstances = new ArrayList<HazelcastInstance>(factory.getAllHazelcastInstances());
        followerInstances.remove(leaderInstance);

        final int commitIndex = getCommitIndex(leader);

        for (HazelcastInstance followerInstance : followerInstances) {
            dropOperationsBetween(followerInstance, leaderInstance, RaftDataSerializerHook.F_ID, asList(APPEND_RESPONSE_OP, VOTE_RESPONSE_OP));
        }

        Future f2 = leader.replicate("val2");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (HazelcastInstance instance : instances) {
                    RaftNode raftNode = getRaftNode(instance, METADATA_RAFT);
                    LogEntry lastLogEntry = getLastLogEntry(raftNode);
                    assertTrue(lastLogEntry.index() > commitIndex);
                }
            }
        });

        // the followers append f2 but the leader does not commit because it does not receive AppendResponse
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (HazelcastInstance instance : instances) {
                    RaftNode raftNode = getRaftNode(instance, METADATA_RAFT);
                    assertEquals(commitIndex, getCommitIndex(raftNode));
                }
            }
        });

        block(leaderInstance, followerInstances);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (HazelcastInstance instance : followerInstances) {
                    RaftNode raftNode = getRaftNode(instance, METADATA_RAFT);
                    assertNotEquals(leader.getThisAddress(), RaftUtil.getLeader(raftNode));
                }
            }
        });

        unblock(leaderInstance, followerInstances);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                RaftNode raftNode = getRaftNode(followerInstances.get(0), METADATA_RAFT);
                assertEquals(RaftUtil.getLeader(leader), RaftUtil.getLeader(raftNode));
            }
        });

        // the new leader appends f3 to the next index of f2, and commits both f2 and f3
        RaftNode newLeader = getLeader(METADATA_RAFT);
        Future f3 = newLeader.replicate("val3");

        assertEquals("val3", f3.get());
        assertEquals("val2", f2.get());
    }

    @Test
    public void testNextLeaderInvalidatesPreviousLeadersEntry() throws ExecutionException, InterruptedException {
        Config config = createRaftConfig();
        configureSplitBrainDelay(config);

        instances = factory.newInstances(config);
        assertClusterSizeEventually(3, instances);

        final RaftNode leader = getLeader(METADATA_RAFT);

        Future f1 = leader.replicate("val1");
        f1.get();

        HazelcastInstance leaderInstance = factory.getInstance(leader.getThisAddress());
        final List<HazelcastInstance> followerInstances = new ArrayList<HazelcastInstance>(factory.getAllHazelcastInstances());
        followerInstances.remove(leaderInstance);

        final int commitIndex = getCommitIndex(leader);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (HazelcastInstance instance : instances) {
                    RaftNode raftNode = getRaftNode(instance, METADATA_RAFT);
                    assertEquals(commitIndex, getCommitIndex(raftNode));
                }
            }
        });

        block(leaderInstance, followerInstances);

        // the alone leader appends f2 but cannot replicate it to the others
        Future f2 = leader.replicate("val2");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (HazelcastInstance instance : followerInstances) {
                    RaftNode raftNode = getRaftNode(instance, METADATA_RAFT);
                    assertNotEquals(leader.getThisAddress(), RaftUtil.getLeader(raftNode));
                }
            }
        });

        unblock(leaderInstance, followerInstances);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                RaftNode raftNode = getRaftNode(followerInstances.get(0), METADATA_RAFT);
                assertEquals(RaftUtil.getLeader(leader), RaftUtil.getLeader(raftNode));
            }
        });

        RaftNode newLeader = getLeader(METADATA_RAFT);

        // the new leader overwrites f2 with the new entry f3 on the same log index
        Future f3 = newLeader.replicate("val3");

        assertEquals("val3", f3.get());
        try {
            f2.get();
            fail();
        } catch (Exception ignored) {
        }
    }

    private Config createRaftConfig() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).clear().addMember("127.0.0.1");

        RaftConfig raftConfig = new RaftConfig();
        raftConfig.setAddresses(asList("127.0.0.1:5001", "127.0.0.1:5002", "127.0.0.1:5003"));

        ServiceConfig raftServiceConfig = new ServiceConfig().setEnabled(true).setName(RaftService.SERVICE_NAME)
                                                             .setClassName(RaftService.class.getName()).setConfigObject(raftConfig);
        config.getServicesConfig().addServiceConfig(raftServiceConfig);
        return config;
    }

    private void configureSplitBrainDelay(Config config) {
        config.setProperty(MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "15")
              .setProperty(MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "5");
    }

    private void block(HazelcastInstance leader, List<HazelcastInstance> followers) {
        for (HazelcastInstance follower : followers) {
            blockCommunicationBetween(leader, follower);
        }

        for (HazelcastInstance follower : followers) {
            closeConnectionBetween(leader, follower);
        }

        assertClusterSizeEventually(2, followers.toArray(new HazelcastInstance[0]));
        assertClusterSizeEventually(1, leader);
    }

    private void unblock(HazelcastInstance leader, List<HazelcastInstance> followers) {
        for (HazelcastInstance follower : followers) {
            unblockCommunicationBetween(leader, follower);
        }
    }

    private RaftNode getLeader(final String raftName) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertNotNull(getRaftNode(instances[0], raftName));
            }
        });

        RaftNode raftNode = getRaftNode(instances[0], raftName);
        waitUntilLeaderElected(raftNode);
        Address leaderAddress = RaftUtil.getLeader(raftNode);

        for (HazelcastInstance instance : instances) {
            if (getAddress(instance).equals(leaderAddress)) {
                return getRaftNode(instance, raftName);
            }
        }

        throw new IllegalStateException();
    }

}
