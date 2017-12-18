package com.hazelcast.raft.impl;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.raft.MembershipChangeType;
import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.dto.PreVoteResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.operation.RaftOperation;

/**
 * TODO: Javadoc Pending...
 *
 */
public interface RaftNode {

    RaftGroupId getGroupId();

    RaftEndpoint getLocalEndpoint();

    RaftEndpoint getLeader();

    RaftNodeStatus getStatus();

    boolean isTerminatedOrSteppedDown();

    void forceSetTerminatedStatus();

    void start();

    void handlePreVoteRequest(PreVoteRequest request);

    void handlePreVoteResponse(PreVoteResponse response);

    void handleVoteRequest(VoteRequest request);

    void handleVoteResponse(VoteResponse response);

    void handleAppendRequest(AppendRequest request);

    void handleAppendResponse(AppendSuccessResponse response);

    void handleAppendResponse(AppendFailureResponse response);

    void handleInstallSnapshot(InstallSnapshot request);

    ICompletableFuture replicate(RaftOperation operation);

    ICompletableFuture replicateMembershipChange(RaftEndpoint member, MembershipChangeType change);

    ICompletableFuture replicateMembershipChange(RaftEndpoint member, MembershipChangeType change, int groupMembersCommitIndex);

    ICompletableFuture query(RaftOperation operation, QueryPolicy queryPolicy);

}
