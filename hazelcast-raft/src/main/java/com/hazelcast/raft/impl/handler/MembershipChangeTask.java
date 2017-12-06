package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.operation.ApplyRaftGroupMembersOp;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.raft.MembershipChangeType;

import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * TODO: Javadoc Pending...
 *
 */
public class MembershipChangeTask implements Runnable {
    private final RaftNode raftNode;
    private final RaftEndpoint member;
    private final MembershipChangeType changeType;
    private final SimpleCompletableFuture resultFuture;

    public MembershipChangeTask(RaftNode raftNode, RaftEndpoint member, MembershipChangeType changeType,
            SimpleCompletableFuture resultFuture) {
        this.raftNode = raftNode;
        this.member = member;
        this.changeType = changeType;
        this.resultFuture = resultFuture;
    }

    @Override
    public void run() {
        if (!verifyMembershipChange()) {
            return;
        }

        Collection<RaftEndpoint> members = new LinkedHashSet<RaftEndpoint>(raftNode.state().members());
        switch (changeType) {
            case ADD:
                members.add(member);
                break;
            case REMOVE:
                members.remove(member);
                break;
            default:
                resultFuture.setResult(new IllegalArgumentException("Unknown type: " + changeType));
                return;
        }
        new ReplicateTask(raftNode, new ApplyRaftGroupMembersOp(members), resultFuture).run();
    }

    private boolean verifyMembershipChange() {
        if (changeType == null) {
            resultFuture.setResult(new IllegalArgumentException("Unknown type: " + changeType));
            return false;
        }

        Collection<RaftEndpoint> members = raftNode.state().members();
        boolean memberExists = members.contains(member);
        if ((changeType == MembershipChangeType.ADD && memberExists) ||
                (changeType == MembershipChangeType.REMOVE && !memberExists)) {
            Exception e = new IllegalArgumentException("Cannot " + changeType + " member: " + member + " to / from: " + members);
            resultFuture.setResult(e);
            return false;
        }

        return true;
    }
}
