package com.hazelcast.raft.impl;

import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.service.RaftTestApplyOperation;
import com.hazelcast.raft.impl.service.RaftTestQueryOperation;
import com.hazelcast.raft.impl.testing.LocalRaftGroup;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.raft.impl.RaftUtil.getLeaderEndpoint;
import static com.hazelcast.raft.impl.RaftUtil.newGroupWithService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LocalQueryTest extends HazelcastTestSupport {

    private LocalRaftGroup group;

    @Before
    public void init() {
    }

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_queryWithInvalidPolicy_thenFail() throws Exception {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.query(new RaftTestQueryOperation(), QueryPolicy.LINEARIZABLE).get();
    }

    @Test
    public void when_queryFromLeader_withoutAnyCommit_thenReadNull() throws Exception {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        Object result = leader.query(new RaftTestQueryOperation(), QueryPolicy.LEADER_LOCAL).get();
        assertNull(result);
    }

    @Test
    public void when_queryFromFollower_withoutAnyCommit_thenReadNull() throws Exception {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollowerNode();

        Object result = follower.query(new RaftTestQueryOperation(), QueryPolicy.ANY_LOCAL).get();
        assertNull(result);
    }

    @Test
    public void when_queryFromLeader_onStableCluster_thenReadLatestValue() throws Exception {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        int count = 3;
        for (int i = 1; i <= count; i++) {
            leader.replicate(new RaftTestApplyOperation("value" + i)).get();
        }

        Object result = leader.query(new RaftTestQueryOperation(), QueryPolicy.LEADER_LOCAL).get();
        assertEquals("value" + count, result);
    }

    @Test(expected = NotLeaderException.class)
    public void when_queryFromFollower_withLeaderLocalPolicy_thenFail() throws Exception {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(new RaftTestApplyOperation("value")).get();

        RaftNodeImpl follower = group.getAnyFollowerNode();

        follower.query(new RaftTestQueryOperation(), QueryPolicy.LEADER_LOCAL).get();
    }

    @Test
    public void when_queryFromFollower_onStableCluster_thenReadLatestValue() throws Exception {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        int count = 3;
        for (int i = 1; i <= count; i++) {
            leader.replicate(new RaftTestApplyOperation("value" + i)).get();
        }

        RaftNodeImpl follower = group.getAnyFollowerNode();

        Object result = follower.query(new RaftTestQueryOperation(), QueryPolicy.ANY_LOCAL).get();
        assertEquals("value" + count, result);
    }

    @Test
    public void when_queryFromSlowFollower_thenReadStaleValue() throws Exception {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        final RaftNodeImpl slowFollower = group.getAnyFollowerNode();

        Object firstValue = "value1";
        leader.replicate(new RaftTestApplyOperation(firstValue)).get();
        final int leaderCommitIndex = getCommitIndex(leader);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(leaderCommitIndex, getCommitIndex(slowFollower));
            }
        });

        group.dropMessagesToEndpoint(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendRequest.class);

        leader.replicate(new RaftTestApplyOperation("value2")).get();

        Object result = slowFollower.query(new RaftTestQueryOperation(), QueryPolicy.ANY_LOCAL).get();
        assertEquals(firstValue, result);
    }

    @Test
    public void when_queryFromSlowFollower_thenEventuallyReadLatestValue() throws Exception {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(new RaftTestApplyOperation("value1")).get();

        final RaftNodeImpl slowFollower = group.getAnyFollowerNode();
        group.dropMessagesToEndpoint(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendRequest.class);

        final Object lastValue = "value2";
        leader.replicate(new RaftTestApplyOperation(lastValue)).get();

        group.allowAllMessagesToEndpoint(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Object result = slowFollower.query(new RaftTestQueryOperation(), QueryPolicy.ANY_LOCAL).get();
                assertEquals(lastValue, result);
            }
        });
    }

    @Test
    public void when_queryFromSplitLeader_thenReadStaleValue() throws Exception {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();

        Object firstValue = "value1";
        leader.replicate(new RaftTestApplyOperation(firstValue)).get();
        final int leaderCommitIndex = getCommitIndex(leader);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl node : group.getNodes()) {
                    assertEquals(leaderCommitIndex, getCommitIndex(node));
                }
            }
        });

        final RaftNodeImpl followerNode = group.getAnyFollowerNode();
        group.split(leader.getLocalEndpoint());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftEndpoint leaderEndpoint = getLeaderEndpoint(followerNode);
                assertNotNull(leaderEndpoint);
                assertNotEquals(leader.getLocalEndpoint(), leaderEndpoint);
            }
        });

        RaftNodeImpl newLeader = group.getNode(getLeaderEndpoint(followerNode));
        Object lastValue = "value2";
        newLeader.replicate(new RaftTestApplyOperation(lastValue)).get();

        Object result1 = newLeader.query(new RaftTestQueryOperation(), QueryPolicy.ANY_LOCAL).get();
        assertEquals(lastValue, result1);

        Object result2 = leader.query(new RaftTestQueryOperation(), QueryPolicy.ANY_LOCAL).get();
        assertEquals(firstValue, result2);
    }

    @Test
    public void when_queryFromSplitLeader_thenEventuallyReadLatestValue() throws Exception {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();

        Object firstValue = "value1";
        leader.replicate(new RaftTestApplyOperation(firstValue)).get();
        final int leaderCommitIndex = getCommitIndex(leader);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl node : group.getNodes()) {
                    assertEquals(leaderCommitIndex, getCommitIndex(node));
                }
            }
        });

        final RaftNodeImpl followerNode = group.getAnyFollowerNode();
        group.split(leader.getLocalEndpoint());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftEndpoint leaderEndpoint = getLeaderEndpoint(followerNode);
                assertNotNull(leaderEndpoint);
                assertNotEquals(leader.getLocalEndpoint(), leaderEndpoint);
            }
        });

        RaftNodeImpl newLeader = group.getNode(getLeaderEndpoint(followerNode));
        final Object lastValue = "value2";
        newLeader.replicate(new RaftTestApplyOperation(lastValue)).get();

        group.merge();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Object result = leader.query(new RaftTestQueryOperation(), QueryPolicy.ANY_LOCAL).get();
                assertEquals(lastValue, result);
            }
        });
    }
}
