package com.hazelcast.raft.impl.service.proxy;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.spi.Operation;

/**
 * TODO: Javadoc Pending...
 *
 */
public abstract class RaftReplicatingOperation extends Operation implements IdentifiedDataSerializable {

    @Override
    public final void run() throws Exception {
        RaftOperation op = getRaftOperation();
        String raftName = getRaftName();
        replicate(op, raftName);
    }

    protected abstract RaftOperation getRaftOperation();

    protected abstract String getRaftName();

    private void replicate(RaftOperation op, String raftName) {
        RaftService service = getService();
        RaftNode raftNode = service.getRaftNode(raftName);

        ICompletableFuture future = raftNode.replicate(op);
        future.andThen(new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {
                sendResponse(response);
            }

            @Override
            public void onFailure(Throwable t) {
                sendResponse(t);
            }
        });
    }

    @Override
    public final boolean returnsResponse() {
        return false;
    }

    @Override
    public final String getServiceName() {
        return RaftService.SERVICE_NAME;
    }
}
