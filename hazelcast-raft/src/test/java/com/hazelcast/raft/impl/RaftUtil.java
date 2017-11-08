package com.hazelcast.raft.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;

import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;

public class RaftUtil {

    public static RaftService getRaftService(HazelcastInstance instance) {
        return getNodeEngineImpl(instance).getService(RaftService.SERVICE_NAME);
    }

    public static RaftNode getRaftNode(HazelcastInstance instance, String name) {
        return getRaftService(instance).getRaftNode(name);
    }

    public static RaftRole getRole(final RaftNode node) {
        Callable<RaftRole> task = new Callable<RaftRole>() {
            @Override
            public RaftRole call() throws Exception {
                return node.getState().role();
            }
        };
        return readRaftState(node, task);
    }

    public static RaftEndpoint getLeaderEndpoint(final RaftNode node) {
        Callable<RaftEndpoint> task = new Callable<RaftEndpoint>() {
            @Override
            public RaftEndpoint call() throws Exception {
                return node.getState().leader();
            }
        };
        return readRaftState(node, task);
    }

    public static LogEntry getLastLogEntry(final RaftNode node) {
        Callable<LogEntry> task = new Callable<LogEntry>() {
            @Override
            public LogEntry call() throws Exception {
                return node.getState().log().lastLogEntry();
            }
        };

        return readRaftState(node, task);
    }

    public static int getCommitIndex(final RaftNode node) {
        Callable<Integer> task = new Callable<Integer>() {
            @Override
            public Integer call()
                    throws Exception {
                return node.getState().commitIndex();
            }
        };

        return readRaftState(node, task);
    }

    public static int getTerm(final RaftNode node) {
        Callable<Integer> task = new Callable<Integer>() {
            @Override
            public Integer call()
                    throws Exception {
                return node.getState().term();
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
        Executor executor = node.getExecutor();
        FutureTask<T> futureTask = new FutureTask<T>(task);

        executor.execute(futureTask);
        try {
            return futureTask.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }
}
