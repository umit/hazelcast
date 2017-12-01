package com.hazelcast.raft.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.RaftNode.RaftNodeStatus;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.state.LeaderState;
import com.hazelcast.util.ExceptionUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static org.junit.Assert.fail;

public class RaftUtil {

    public static RaftRole getRole(final RaftNode node) {
        Callable<RaftRole> task = new Callable<RaftRole>() {
            @Override
            public RaftRole call() throws Exception {
                return node.state().role();
            }
        };
        return readRaftState(node, task);
    }

    public static RaftEndpoint getLeaderEndpoint(final RaftNode node) {
        Callable<RaftEndpoint> task = new Callable<RaftEndpoint>() {
            @Override
            public RaftEndpoint call() throws Exception {
                return node.state().leader();
            }
        };
        return readRaftState(node, task);
    }

    public static LogEntry getLastLogOrSnapshotEntry(final RaftNode node) {
        Callable<LogEntry> task = new Callable<LogEntry>() {
            @Override
            public LogEntry call() throws Exception {
                return node.state().log().lastLogOrSnapshotEntry();
            }
        };

        return readRaftState(node, task);
    }

    public static LogEntry getSnapshotEntry(final RaftNode node) {
        Callable<LogEntry> task = new Callable<LogEntry>() {
            @Override
            public LogEntry call() throws Exception {
                return node.state().log().snapshot();
            }
        };

        return readRaftState(node, task);
    }

    public static int getCommitIndex(final RaftNode node) {
        Callable<Integer> task = new Callable<Integer>() {
            @Override
            public Integer call()
                    throws Exception {
                return node.state().commitIndex();
            }
        };

        return readRaftState(node, task);
    }

    public static int getTerm(final RaftNode node) {
        Callable<Integer> task = new Callable<Integer>() {
            @Override
            public Integer call()
                    throws Exception {
                return node.state().term();
            }
        };

        return readRaftState(node, task);
    }

    public static int getNextIndex(final RaftNode leader, final RaftEndpoint follower) {
        Callable<Integer> task = new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                LeaderState leaderState = leader.state().leaderState();
                return leaderState.getNextIndex(follower);
            }
        };

        return readRaftState(leader, task);
    }

    public static int getMatchIndex(final RaftNode leader, final RaftEndpoint follower) {
        Callable<Integer> task = new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                LeaderState leaderState = leader.state().leaderState();
                return leaderState.getMatchIndex(follower);
            }
        };

        return readRaftState(leader, task);
    }

    public static RaftNodeStatus getStatus(final RaftNode node) {
        Callable<RaftNodeStatus> task = new Callable<RaftNodeStatus>() {
            @Override
            public RaftNodeStatus call() throws Exception {
                return node.getStatus();
            }
        };

        return readRaftState(node, task);
    }

    public static void waitUntilLeaderElected(RaftNode node) {
        while (getLeaderEndpoint(node) == null) {
            sleepSeconds(1);
        }
    }

    private static <T> T readRaftState(RaftNode node, Callable<T> task) {
        FutureTask<T> futureTask = new FutureTask<T>(task);
        node.execute(futureTask);
        try {
            return futureTask.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public static RaftEndpoint newRaftEndpoint(int port) {
        return new RaftEndpoint(randomString(), newAddress(port));
    }

    public static Address newAddress(int port) {
        try {
            return new Address(InetAddress.getByName("127.0.0.1"), port);
        } catch (UnknownHostException e) {
            fail("Could not create new Address: " + e.getMessage());
        }
        return null;
    }

    public static int majority(int count) {
        return count / 2 + 1;
    }

    public static int minority(int count) {
        return count - majority(count);
    }
}
