package com.hazelcast.raft.impl.async;

import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.RaftState;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.util.AddressableExecutionCallback;

import java.util.HashSet;
import java.util.Set;

/**
 * TODO: Javadoc Pending...
 *
 */
public class LeaderElectionExecutionCallback implements AddressableExecutionCallback<VoteResponse> {
    private final RaftNode raftNode;
    private final int majority;
    private final Set<Address> voters = new HashSet<Address>();

    public LeaderElectionExecutionCallback(RaftNode raftNode, int majority) {
        this.raftNode = raftNode;
        this.majority = majority;
        voters.add(raftNode.getNodeEngine().getThisAddress());
    }

    @Override
    public void onResponse(Address voter, VoteResponse resp) {
        RaftState state = raftNode.state();
        if (RaftRole.CANDIDATE != state.role()) {
            return;
        }

        if (resp.term > state.term()) {
            raftNode.logger.warning("Newer term discovered, fallback to follower");
            state.role(RaftRole.FOLLOWER);
            state.term(resp.term);
            return;
        }

        if (resp.term < state.term()) {
            raftNode.logger.warning("Obsolete vote response received: " + resp + ", current-term: " + state.term());
            return;
        }

        if (resp.granted && resp.term == state.term()) {
            if (voters.add(voter)) {
                raftNode.logger.warning(
                        "Vote granted from " + voter + " for term " + state.term() + ", number of votes: " + voters
                                .size() + ", majority: " + majority);
            }
        }

        if (voters.size() >= majority) {
            raftNode.logger.severe("We are THE leader!");
            state.selfLeader();
            raftNode.scheduleLeaderLoop();
        }
    }

    @Override
    public void onFailure(Address remote, Throwable t) {
    }
}
