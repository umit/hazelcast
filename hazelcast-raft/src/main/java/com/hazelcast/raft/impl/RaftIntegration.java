package com.hazelcast.raft.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.TaskScheduler;

import java.util.concurrent.Executor;

/**
 * TODO: Javadoc Pending...
 */
public interface RaftIntegration {

    TaskScheduler getTaskScheduler();

    Executor getExecutor(String name);

    ILogger getLogger(String name);

    ILogger getLogger(Class clazz);

    boolean isJoined();

    boolean isReachable(RaftEndpoint endpoint);

    boolean send(Operation operation, RaftEndpoint target);

    Object runOperation(RaftOperation operation, int commitIndex);

}
