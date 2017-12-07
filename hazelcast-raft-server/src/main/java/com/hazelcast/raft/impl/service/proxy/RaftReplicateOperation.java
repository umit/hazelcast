package com.hazelcast.raft.impl.service.proxy;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.exception.RaftGroupTerminatedException;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.service.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftGroupInfo;
import com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.operation.RaftOperation;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.TargetNotMemberException;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public abstract class RaftReplicateOperation extends Operation implements IdentifiedDataSerializable {

    private RaftGroupId raftGroupId;

    public RaftReplicateOperation() {
    }

    public RaftReplicateOperation(RaftGroupId raftGroupId) {
        this.raftGroupId = raftGroupId;
    }

    @Override
    public final void run() throws Exception {
        RaftOperation op = getRaftOperation();
        RaftGroupId groupId = getRaftGroupId();
        replicate(op, groupId);
    }

    public RaftGroupId getRaftGroupId() {
        return raftGroupId;
    }

    protected abstract RaftOperation getRaftOperation();

    private void replicate(RaftOperation op, RaftGroupId groupId) {
        RaftService service = getService();
        RaftNode raftNode = service.getRaftNode(groupId);
        if (raftNode == null) {
            RaftGroupInfo raftGroupInfo = service.getRaftGroupInfo(groupId);
            if (raftGroupInfo != null && raftGroupInfo.status() == RaftGroupStatus.DESTROYED) {
                sendResponse(new RaftGroupTerminatedException());
            } else {
                sendResponse(new NotLeaderException(service.getLocalEndpoint(), null));
            }
            return;
        }

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

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(raftGroupId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        raftGroupId = in.readObject();
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException
                || throwable instanceof TargetNotMemberException
                || throwable instanceof CallerNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }
}
