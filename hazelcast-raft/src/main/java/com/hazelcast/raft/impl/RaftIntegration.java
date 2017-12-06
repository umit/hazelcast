package com.hazelcast.raft.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.operation.RaftOperation;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.dto.PreVoteResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;

import java.util.concurrent.TimeUnit;

/**
 * TODO: Javadoc Pending...
 */
public interface RaftIntegration {

    ILogger getLogger(String name);

    boolean isJoined();

    boolean isReachable(RaftEndpoint endpoint);

    boolean send(PreVoteRequest request, RaftEndpoint target);

    boolean send(PreVoteResponse response, RaftEndpoint target);

    boolean send(VoteRequest request, RaftEndpoint target);

    boolean send(VoteResponse response, RaftEndpoint target);

    boolean send(AppendRequest request, RaftEndpoint target);

    boolean send(AppendSuccessResponse response, RaftEndpoint target);

    boolean send(AppendFailureResponse response, RaftEndpoint target);

    boolean send(InstallSnapshot request, RaftEndpoint target);

    Object runOperation(RaftOperation operation, int commitIndex);

    void execute(Runnable task);

    void schedule(Runnable task, long delay, TimeUnit timeUnit);

    SimpleCompletableFuture newCompletableFuture();
}
