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

package com.hazelcast.cp.internal.raft.impl;

import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.core.Endpoint;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.cp.exception.StaleAppendRequestException;
import com.hazelcast.cp.internal.raft.MembershipChangeType;
import com.hazelcast.cp.internal.raft.impl.dataservice.ApplyRaftRunnable;
import com.hazelcast.cp.internal.raft.impl.dataservice.RaftDataService;
import com.hazelcast.cp.internal.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshot;
import com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.ACTIVE;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getCommittedGroupMembers;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLastLogOrSnapshotEntry;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getMatchIndex;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getStatus;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.newGroupWithService;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SnapshotTest extends HazelcastTestSupport {

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

    @Test
    public void when_commitLogAdvances_then_snapshotIsTaken() throws ExecutionException, InterruptedException {
        final int entryCount = 50;
        RaftAlgorithmConfig raftAlgorithmConfig = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroupWithService(3, raftAlgorithmConfig);
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl raftNode : group.getNodes()) {
                    assertEquals(entryCount, getCommitIndex(raftNode));
                    assertEquals(entryCount, getSnapshotEntry(raftNode).index());
                    RaftDataService service = group.getService(raftNode);
                    assertEquals(entryCount, service.size());
                    for (int i = 0; i < entryCount; i++) {
                        assertEquals(("val" + i), service.get(i + 1));
                    }
                }
            }
        });
    }

    @Test
    public void when_snapshotIsTaken_then_nextEntryIsCommitted() throws ExecutionException, InterruptedException {
        final int entryCount = 50;
        RaftAlgorithmConfig raftAlgorithmConfig = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroupWithService(3, raftAlgorithmConfig);
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl raftNode : group.getNodes()) {
                    assertEquals(entryCount, getCommitIndex(raftNode));
                    assertEquals(entryCount, getSnapshotEntry(raftNode).index());
                }
            }
        });

        leader.replicate(new ApplyRaftRunnable("valFinal")).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl raftNode : group.getNodes()) {
                    assertEquals(entryCount + 1, getCommitIndex(raftNode));
                    RaftDataService service = group.getService(raftNode);
                    assertEquals(entryCount + 1, service.size());
                    for (int i = 0; i < entryCount; i++) {
                        assertEquals(("val" + i), service.get(i + 1));
                    }
                    assertEquals("valFinal", service.get(51));
                }
            }
        });
    }

    @Test
    public void when_followerFallsTooFarBehind_then_itInstallsSnapshot() throws ExecutionException, InterruptedException {
        final int entryCount = 50;
        RaftAlgorithmConfig raftAlgorithmConfig = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroupWithService(3, raftAlgorithmConfig);
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        final RaftNodeImpl slowFollower = followers[1];

        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), AppendRequest.class);

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(entryCount, getSnapshotEntry(leader).index());
            }
        });

        leader.replicate(new ApplyRaftRunnable("valFinal")).get();

        group.resetAllDropRulesFrom(leader.getLocalMember());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl raftNode : group.getNodes()) {
                    assertEquals(entryCount + 1, getCommitIndex(raftNode));
                    RaftDataService service = group.getService(raftNode);
                    assertEquals(entryCount + 1, service.size());
                    for (int i = 0; i < entryCount; i++) {
                        assertEquals(("val" + i), service.get(i + 1));
                    }
                    assertEquals("valFinal", service.get(51));
                }
            }
        });
    }

    @Test
    public void when_leaderMissesInstallSnapshotResponse_then_itAdvancesMatchIndexWithNextInstallSnapshotResponse()
            throws ExecutionException, InterruptedException {
        final int entryCount = 50;
        RaftAlgorithmConfig raftAlgorithmConfig = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount)
                                                                           .setAppendRequestBackoffTimeoutInMillis(1000);
        group = newGroupWithService(3, raftAlgorithmConfig);
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        final RaftNodeImpl slowFollower = followers[1];

        // the leader cannot send AppendEntriesRPC to the follower
        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), AppendRequest.class);

        // the follower cannot send append response to the leader after installing the snapshot
        group.dropMessagesToMember(slowFollower.getLocalMember(), leader.getLocalMember(), AppendSuccessResponse.class);

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(entryCount, getSnapshotEntry(leader).index());
            }
        });

        leader.replicate(new ApplyRaftRunnable("valFinal")).get();

        group.resetAllDropRulesFrom(leader.getLocalMember());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl raftNode : group.getNodesExcept(slowFollower.getLocalMember())) {
                    assertEquals(entryCount + 1, getCommitIndex(raftNode));
                    RaftDataService service = group.getService(raftNode);
                    assertEquals(entryCount + 1, service.size());
                    for (int i = 0; i < entryCount; i++) {
                        assertEquals(("val" + i), service.get(i + 1));
                    }
                    assertEquals("valFinal", service.get(51));
                }

                assertEquals(entryCount, getCommitIndex(slowFollower));
                RaftDataService service = group.getService(slowFollower);
                assertEquals(entryCount, service.size());
                for (int i = 0; i < entryCount; i++) {
                    assertEquals(("val" + i), service.get(i + 1));
                }
            }
        });

        group.resetAllDropRulesFrom(slowFollower.getLocalMember());

        final long commitIndex = getCommitIndex(leader);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNode raftNode : group.getNodesExcept(leader.getLocalMember())) {
                    assertEquals(commitIndex, getMatchIndex(leader, raftNode.getLocalMember()));
                }
            }
        });
    }

    @Test
    public void when_followerMissesTheLastEntryThatGoesIntoTheSnapshot_then_itInstallsSnapshot() throws ExecutionException, InterruptedException {
        final int entryCount = 50;
        RaftAlgorithmConfig raftAlgorithmConfig = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroupWithService(3, raftAlgorithmConfig);
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        final RaftNodeImpl slowFollower = followers[1];

        for (int i = 0; i < entryCount - 1; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl follower : group.getNodesExcept(leader.getLocalMember())) {
                    assertEquals(entryCount - 1, getMatchIndex(leader, follower.getLocalMember()));
                }
            }
        });

        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), AppendRequest.class);

        leader.replicate(new ApplyRaftRunnable("val" + (entryCount - 1))).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(entryCount, getSnapshotEntry(leader).index());
            }
        });

        leader.replicate(new ApplyRaftRunnable("valFinal")).get();

        group.resetAllDropRulesFrom(leader.getLocalMember());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl raftNode : group.getNodes()) {
                    assertEquals(entryCount + 1, getCommitIndex(raftNode));
                    RaftDataService service = group.getService(raftNode);
                    assertEquals(entryCount + 1, service.size());
                    for (int i = 0; i < entryCount; i++) {
                        assertEquals(("val" + i), service.get(i + 1));
                    }
                    assertEquals("valFinal", service.get(51));
                }
            }
        }, 30);
    }

    @Test
    public void when_isolatedLeaderAppendsEntries_then_itInvalidatesTheirFeaturesUponInstallSnapshot() throws ExecutionException, InterruptedException {
        final int entryCount = 50;
        RaftAlgorithmConfig raftAlgorithmConfig = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroupWithService(3, raftAlgorithmConfig);
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        final RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        for (int i = 0; i < 40; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl raftNode : group.getNodes()) {
                    assertEquals(40, getCommitIndex(raftNode));
                }
            }
        });

        group.split(leader.getLocalMember());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl raftNode : followers) {
                    Endpoint leaderEndpoint = getLeaderMember(raftNode);
                    assertNotNull(leaderEndpoint);
                    assertNotEquals(leader.getLocalMember(), leaderEndpoint);
                }
            }
        });

        List<Future> futures = new ArrayList<Future>();
        for (int i = 40; i < 45; i++) {
            Future f = leader.replicate(new ApplyRaftRunnable("isolated" + i));
            futures.add(f);
        }

        final RaftNodeImpl newLeader = group.getNode(getLeaderMember(followers[0]));

        for (int i = 40; i < 51; i++) {
            newLeader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl raftNode : followers) {
                    assertTrue(getSnapshotEntry(raftNode).index() > 0);
                }
            }
        });

        group.dropMessagesToMember(leader.getLocalMember(), followers[0].getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), followers[1].getLocalMember(), AppendRequest.class);
        group.merge();

        for (Future f : futures) {
            try {
                f.get();
                fail();
            } catch (StaleAppendRequestException ignored) {
            }
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl raftNode : group.getNodes()) {
                    assertEquals(51, getCommitIndex(raftNode));
                    RaftDataService service = group.getService(raftNode);
                    assertEquals(51, service.size());
                    for (int i = 0; i < 51; i++) {
                        assertEquals(("val" + i), service.get(i + 1));
                    }

                }
            }
        });
    }

    @Test
    public void when_followersLastAppendIsMembershipChange_then_itUpdatesRaftNodeStateWithInstalledSnapshot() throws ExecutionException, InterruptedException {
        final int entryCount = 50;
        group = newGroupWithService(5, new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount));
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        final RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        leader.replicate(new ApplyRaftRunnable("val")).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl follower : followers) {
                    assertEquals(1L, getCommitIndex(follower));
                }
            }
        });

        final RaftNodeImpl slowFollower = followers[0];

        for (RaftNodeImpl follower : followers) {
            if (follower != slowFollower) {
                group.dropMessagesToMember(follower.getLocalMember(), follower.getLeader(), AppendSuccessResponse.class);
                group.dropMessagesToMember(follower.getLocalMember(), follower.getLeader(), AppendFailureResponse.class);
            }
        }

        final RaftNodeImpl newRaftNode1 = group.createNewRaftNode();
        final ICompletableFuture f1 = leader.replicateMembershipChange(newRaftNode1.getLocalMember(), MembershipChangeType.ADD);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl follower : followers) {
                    assertEquals(2L, getLastLogOrSnapshotEntry(follower).index());
                }
            }
        });

        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), InstallSnapshot.class);

        for (RaftNodeImpl follower : followers) {
            if (follower != slowFollower) {
                group.allowAllMessagesToMember(follower.getLocalMember(), leader.getLeader());
            }
        }

        f1.get();

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertThat(getSnapshotEntry(leader).index(), greaterThanOrEqualTo((long) entryCount));
            }
        });

        group.allowAllMessagesToMember(leader.getLeader(), slowFollower.getLocalMember());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertThat(getSnapshotEntry(slowFollower).index(), greaterThanOrEqualTo((long) entryCount));
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(getCommittedGroupMembers(leader).index(), getCommittedGroupMembers(slowFollower).index());
                assertEquals(ACTIVE, getStatus(slowFollower));
            }
        });


    }
}