package com.hazelcast.raft.impl.handler;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.exception.MemberAlreadyExistsException;
import com.hazelcast.raft.exception.MemberDoesNotExistException;
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
        if (changeType == null) {
            resultFuture.setResult(new IllegalArgumentException("Null membership change type"));
            return;
        }

        Collection<RaftEndpoint> members = new LinkedHashSet<RaftEndpoint>(raftNode.state().members());
        boolean memberExists = members.contains(member);

        ILogger logger = raftNode.getLogger(getClass());
        logger.severe("Changing membership -> " + changeType + ": " + member);

        switch (changeType) {
            case ADD:
                if (memberExists) {
                    resultFuture.setResult(new MemberAlreadyExistsException());
                    return;
                }
                members.add(member);
                break;

            case REMOVE:
                if (!memberExists) {
                    resultFuture.setResult(new MemberDoesNotExistException());
                    return;
                }
                members.remove(member);
                break;

            default:
                resultFuture.setResult(new IllegalArgumentException("Unknown type: " + changeType));
                return;
        }
        logger.severe("New members after " + changeType + " -> " + members);
        new ReplicateTask(raftNode, new ApplyRaftGroupMembersOp(members), resultFuture).run();
    }
}
