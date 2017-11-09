package com.hazelcast.raft.impl;

import com.hazelcast.raft.impl.service.RaftAddOperation;
import com.hazelcast.raft.impl.service.RaftDataService;
import com.hazelcast.raft.impl.testing.RaftGroup;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Future;

import static com.hazelcast.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.raft.impl.RaftUtil.getLeaderEndpoint;
import static com.hazelcast.raft.impl.service.RaftDataService.SERVICE_NAME;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LocalRaftTest extends HazelcastTestSupport {

    private RaftGroup group;

    @Before
    public void init() {
    }

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void testLeaderElection_twoNodes() throws Exception {
        testLeaderElection(2);
    }

    @Test
    public void testLeaderElection_threeNodes() throws Exception {
        testLeaderElection(3);
    }

    private void testLeaderElection(int nodeCount) throws Exception {
        group = new RaftGroup(nodeCount);
        group.start();
        group.waitUntilLeaderElected();

        RaftEndpoint leaderEndpoint = group.getLeaderEndpoint();
        assertNotNull(leaderEndpoint);

        int leaderIndex = group.getLeaderIndex();
        assertThat(leaderIndex, greaterThanOrEqualTo(0));

        RaftNode leaderNode = group.getLeaderNode();
        assertNotNull(leaderNode);
    }

    @Test
    public void testSingleCommitEntry_twoNodes() throws Exception {
        testSingleCommitEntry(2);
    }

    @Test
    public void testSingleCommitEntry_threeNodes() throws Exception {
        testSingleCommitEntry(3);
    }

    private void testSingleCommitEntry(final int nodeCount) throws Exception {
        group = newGroupWithService(nodeCount);
        group.start();
        group.waitUntilLeaderElected();

        final Object val = "val";
        Future f = group.getLeaderNode().replicate(new RaftAddOperation(val));
        Object result = f.get();
        assertEquals(result, val);

        final int commitIndex = 1;
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = 0; i < nodeCount; i++) {
                    RaftNode node = group.getNode(i);
                    assertEquals(commitIndex, getCommitIndex(node));
                    RaftDataService service = group.getIntegration(i).getService(SERVICE_NAME);
                    assertEquals(val, service.get(commitIndex));
                }
            }
        });
    }


    @Test
    public void testTerminateFollower() throws Exception {
        group = newGroupWithService(3);
        group.start();
        group.waitUntilLeaderElected();

        int leaderIndex = group.getLeaderIndex();
        for (int i = 0; i < group.size(); i++) {
            if (i != leaderIndex) {
                group.terminateNode(i);
                break;
            }
        }

        String value = "value";
        RaftNode leaderNode = group.getLeaderNode();
        Future future = leaderNode.replicate(new RaftAddOperation(value));
        assertEquals(value, future.get());
    }

    @Test
    public void testTerminateLeader() throws Exception {
        group = newGroupWithService(3);
        group.start();
        group.waitUntilLeaderElected();

        final RaftEndpoint leaderEndpoint = group.getLeaderEndpoint();
        final int leaderIndex = group.getLeaderIndex();
        group.terminateNode(leaderIndex);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = 0; i < group.size(); i++) {
                    if (i != leaderIndex) {
                        assertNotEquals(leaderEndpoint, getLeaderEndpoint(group.getNode(i)));
                    }
                }
            }
        });

        String value = "value";
        RaftNode leaderNode = group.getLeaderNode();
        Future future = leaderNode.replicate(new RaftAddOperation(value));
        assertEquals(value, future.get());
    }

    @Test
    public void split_withLeaderOnMajority_AndMergeBack() throws Exception {
        int nodeCount = 5;
        group = new RaftGroup(nodeCount);
        group.start();
        group.waitUntilLeaderElected();

        final int[] split = group.createMinoritySplitIndexes(false);
        group.split(split);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int ix : split) {
                    assertNull(getLeaderEndpoint(group.getNode(ix)));
                }
            }
        });

        group.merge();
        group.waitUntilLeaderElected();
    }

    @Test
    public void split_withLeaderOnMinority_AndMergeBack() throws Exception {
        final int nodeCount = 5;
        group = new RaftGroup(nodeCount);
        group.start();
        group.waitUntilLeaderElected();

        final RaftEndpoint leaderEndpoint = group.getLeaderEndpoint();

        final int[] split = group.createMajoritySplitIndexes(false);
        group.split(split);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int ix : split) {
                    assertNotEquals(leaderEndpoint, getLeaderEndpoint(group.getNode(ix)));
                }
            }
        });

        for (int i = 0; i < nodeCount; i++) {
            if (Arrays.binarySearch(split, i) < 0) {
                assertEquals(leaderEndpoint, getLeaderEndpoint(group.getNode(i)));
            }
        }

        group.merge();
        group.waitUntilLeaderElected();
    }

    private RaftGroup newGroupWithService(int nodeCount) {
        return new RaftGroup(nodeCount, Collections.<String, Class>singletonMap(SERVICE_NAME, RaftDataService.class));
    }
}
