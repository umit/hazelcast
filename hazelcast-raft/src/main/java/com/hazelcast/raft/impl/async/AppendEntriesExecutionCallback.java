package com.hazelcast.raft.impl.async;

import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.LeaderState;
import com.hazelcast.raft.impl.LogEntry;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendResponse;
import com.hazelcast.raft.impl.util.AddressableExecutionCallback;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;

import java.util.HashSet;
import java.util.Set;

/**
 * TODO: Javadoc Pending...
 *
 */
public class AppendEntriesExecutionCallback implements AddressableExecutionCallback<AppendResponse> {
    private final RaftNode raftNode;
    private final AppendRequest req;
    private final int majority;
    private final Set<Address> addresses = new HashSet<Address>();
    private final Set<Address> erroneousAddresses = new HashSet<Address>();
    private final SimpleCompletableFuture resultFuture;

    AppendEntriesExecutionCallback(RaftNode raftNode, AppendRequest request, int majority, SimpleCompletableFuture resultFuture) {
        this.raftNode = raftNode;
        this.req = request;
        this.majority = majority;
        this.resultFuture = resultFuture;
        addresses.add(raftNode.getNodeEngine().getThisAddress());
    }

    @Override
    public void onResponse(Address follower, AppendResponse resp) {
        // Check for a newer term, stop running
        if (resp.term > req.term) {
            //                r.handleStaleTerm(s)
            return;
        }

        // Abort pipeline if not successful
        if (!resp.success) {
            raftNode.logger.severe("Failure response " + resp);
            // TODO: handle?
            return;
        }

        assert req.entries.length > 0;

        if (addresses.add(follower)) {
            raftNode.logger.warning("Success response " + resp);

            // Update our replication state
            LogEntry last = req.entries[req.entries.length - 1];
            LeaderState leaderState = raftNode.state().leaderState();
            leaderState.nextIndex(follower, last.index() + 1);
            leaderState.matchIndex(follower, last.index());
        }

        if (addresses.size() >= majority) {
            LogEntry last = req.entries[req.entries.length - 1];
            raftNode.state().commitIndex(last.index());
            raftNode.sendHeartbeat();
            raftNode.processLogs(raftNode.state().commitIndex());
            Object result = req.entries[0].data();
            resultFuture.setResult(result);
            return;
        }

        if (addresses.size() + erroneousAddresses.size() >= raftNode.state().members().size()) {
            // TODO: ?
        }
    }

    @Override
    public void onFailure(Address remote, Throwable t) {
        erroneousAddresses.add(remote);
    }
}
