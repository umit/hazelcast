package com.hazelcast.raft.impl.task;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.exception.CannotReplicateException;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.exception.RaftGroupTerminatedException;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.RaftNodeStatus;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.command.ApplyRaftGroupMembersCmd;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.raft.command.TerminateRaftGroupCmd;

/**
 * ReplicateTask is executed to append an operation to Raft log
 * and replicate the new entry to followers. It's scheduled by
 * {@link com.hazelcast.raft.impl.RaftNode#replicate(Object)}
 * or by {@link MembershipChangeTask} for membership changes.
 * <p>
 * If this node is not the leader, future is immediately notified with {@link NotLeaderException}.
 * <p>
 * If replication of the operation is not allowed at the moment (see {@link RaftNodeImpl#canReplicateNewEntry(Object)}),
 * future is immediately notified with {@link CannotReplicateException}.
 */
public class ReplicateTask implements Runnable {
    private final RaftNodeImpl raftNode;
    private final Object operation;
    private final SimpleCompletableFuture resultFuture;
    private final ILogger logger;

    public ReplicateTask(RaftNodeImpl raftNode, Object operation, SimpleCompletableFuture resultFuture) {
        this.raftNode = raftNode;
        this.operation = operation;
        this.logger = raftNode.getLogger(getClass());
        this.resultFuture = resultFuture;
    }

    @Override
    public void run() {
        if (!verifyRaftNodeStatus()) {
            return;
        }

        RaftState state = raftNode.state();
        if (state.role() != RaftRole.LEADER) {
            resultFuture.setResult(new NotLeaderException(raftNode.getGroupId(), raftNode.getLocalMember(), state.leader()));
            return;
        }

        if (!raftNode.canReplicateNewEntry(operation)) {
            resultFuture.setResult(new CannotReplicateException(raftNode.getLocalMember()));
            return;
        }

        if (logger.isFineEnabled()) {
            logger.fine("Replicating: " + operation + " in term: " + state.term());
        }

        long newEntryLogIndex = state.log().lastLogOrSnapshotIndex() + 1;
        raftNode.registerFuture(newEntryLogIndex, resultFuture);
        state.log().appendEntries(new LogEntry(state.term(), newEntryLogIndex, operation));

        handleRaftGroupCmd(newEntryLogIndex, operation);

        raftNode.broadcastAppendRequest();
    }

    private boolean verifyRaftNodeStatus() {
        if (raftNode.getStatus() == RaftNodeStatus.TERMINATED) {
            resultFuture.setResult(new RaftGroupTerminatedException());
            logger.fine("Won't run " + operation + ", since raft node is terminated");
            return false;
        } else if (raftNode.getStatus() == RaftNodeStatus.STEPPED_DOWN) {
            logger.fine("Won't run " + operation + ", since raft node is stepped down");
            resultFuture.setResult(new NotLeaderException(raftNode.getGroupId(), raftNode.getLocalMember(), null));
            return false;
        }

        return true;
    }

    private void handleRaftGroupCmd(long logIndex, Object operation) {
        if (operation instanceof TerminateRaftGroupCmd) {
            raftNode.setStatus(RaftNodeStatus.TERMINATING);
        } else if (operation instanceof ApplyRaftGroupMembersCmd) {
            raftNode.setStatus(RaftNodeStatus.CHANGING_MEMBERSHIP);
            ApplyRaftGroupMembersCmd op = (ApplyRaftGroupMembersCmd) operation;
            raftNode.updateGroupMembers(logIndex, op.getMembers());
            // TODO update quorum match indices...
            // TODO if the raft group shrinks, we can move the commit index forward...
        }
    }

}