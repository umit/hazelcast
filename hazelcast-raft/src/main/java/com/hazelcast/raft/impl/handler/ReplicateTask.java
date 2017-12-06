package com.hazelcast.raft.impl.handler;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.operation.RaftOperation;
import com.hazelcast.raft.exception.CannotReplicateException;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.exception.RaftGroupTerminatedException;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftNode.RaftNodeStatus;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.operation.ApplyRaftGroupMembersOp;
import com.hazelcast.raft.operation.ChangeRaftGroupMembersOp;
import com.hazelcast.raft.operation.ChangeRaftGroupMembersOp.MembershipChangeType;
import com.hazelcast.raft.operation.RaftCommandOperation;
import com.hazelcast.raft.operation.TerminateRaftGroupOp;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;

import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * TODO: Javadoc Pending...
 *
 */
public class ReplicateTask implements Runnable {
    private final RaftNode raftNode;
    private final RaftOperation operation;
    private final SimpleCompletableFuture resultFuture;
    private final ILogger logger;

    public ReplicateTask(RaftNode raftNode, RaftOperation operation, SimpleCompletableFuture resultFuture) {
        this.raftNode = raftNode;
        this.operation = operation;
        this.logger = raftNode.getLogger(getClass());
        this.resultFuture = resultFuture;
    }

    @Override
    public void run() {
        if (!verifyServiceName()) {
            return;
        }

        if (!verifyRaftNodeStatus()) {
            return;
        }

        RaftState state = raftNode.state();
        if (state.role() != RaftRole.LEADER) {
            resultFuture.setResult(new NotLeaderException(raftNode.getLocalEndpoint(), state.leader()));
            return;
        }

        if (!raftNode.canReplicateNewEntry(operation)) {
            resultFuture.setResult(new CannotReplicateException(raftNode.getLocalEndpoint()));
            return;
        }

        if (!verifyChangeGroupMembersRequestOp()) {
            return;
        }

        RaftOperation operation = getOperationToAppended();

        if (logger.isFineEnabled()) {
            logger.fine("Replicating: " + operation + " in term: " + state.term());
        }

        int newEntryLogIndex = state.log().lastLogOrSnapshotIndex() + 1;
        raftNode.registerFuture(newEntryLogIndex, resultFuture);
        state.log().appendEntries(new LogEntry(state.term(), newEntryLogIndex, operation));

        handleInternalRaftOperation(newEntryLogIndex, operation);

        raftNode.broadcastAppendRequest();
    }

    private boolean verifyServiceName() {
        if (!(operation instanceof RaftCommandOperation || raftNode.getServiceName().equals(operation.getServiceName()))) {
            resultFuture.setResult(new IllegalArgumentException("operation: " + operation + "  service name: "
                    + operation.getServiceName() + " is different than expected service name: " + raftNode.getServiceName()));
            return false;
        }

        return true;
    }

    private boolean verifyRaftNodeStatus() {
        if (raftNode.getStatus() == RaftNodeStatus.TERMINATED) {
            resultFuture.setResult(new RaftGroupTerminatedException());
            return false;
        } else if (raftNode.getStatus() == RaftNodeStatus.STEPPED_DOWN) {
            resultFuture.setResult(new NotLeaderException(raftNode.getLocalEndpoint(), null));
            return false;
        }

        return true;
    }

    private boolean verifyChangeGroupMembersRequestOp() {
        if (operation instanceof ChangeRaftGroupMembersOp) {
            ChangeRaftGroupMembersOp op = (ChangeRaftGroupMembersOp) operation;
            Collection<RaftEndpoint> members = raftNode.state().members();
            boolean memberExists = members.contains(op.getMember());
            MembershipChangeType changeType = op.getChangeType();
            if ((changeType == MembershipChangeType.ADD && memberExists) ||
                    (changeType == MembershipChangeType.REMOVE && !memberExists)) {
                resultFuture.setResult("Cannot " + changeType + " member: " + op.getMember() + " to / from: " + members);
                return false;
            }
        }

        return true;
    }

    private RaftOperation getOperationToAppended() {
        if (!(operation instanceof ChangeRaftGroupMembersOp)) {
            return operation;
        }

        ChangeRaftGroupMembersOp op = (ChangeRaftGroupMembersOp) operation;
        Collection<RaftEndpoint> members = new LinkedHashSet<RaftEndpoint>(raftNode.state().members());
        if (op.getChangeType() == MembershipChangeType.ADD) {
            members.add(op.getMember());
        } else {
            members.remove(op.getMember());
        }

        return new ApplyRaftGroupMembersOp(members);
    }

    private void handleInternalRaftOperation(int logIndex, RaftOperation operation) {
        if (operation instanceof TerminateRaftGroupOp) {
            raftNode.setStatus(RaftNodeStatus.TERMINATING);
        } else if (operation instanceof ApplyRaftGroupMembersOp) {
            raftNode.setStatus(RaftNodeStatus.CHANGING_MEMBERSHIP);
            ApplyRaftGroupMembersOp op = (ApplyRaftGroupMembersOp) operation;
            raftNode.updateGroupMembers(logIndex, op.getMembers());
            // TODO update quorum match indices...
            // TODO if the raft group shrinks, we can move the commit index forward...
        }
    }

}
