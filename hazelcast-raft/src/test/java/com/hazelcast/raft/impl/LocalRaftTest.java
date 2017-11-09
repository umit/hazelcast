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

import static com.hazelcast.raft.impl.RaftUtil.getLeaderEndpoint;
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
    public void startGroup() throws Exception {
        int nodeCount = 5;
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
    public void startGroup_withService() throws Exception {
        int nodeCount = 5;
        group = new RaftGroup(nodeCount, Collections.<String, Class>singletonMap(RaftDataService.SERVICE_NAME, RaftDataService.class));
        group.start();
        group.waitUntilLeaderElected();

        RaftNode leaderNode = group.getLeaderNode();

        String value = "value";
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
}
