package com.hazelcast.raft.service.lock.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.service.lock.operation.UnlockOp;

import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 *
 */
public class UnlockMessageTask extends AbstractLockMessageTask {

    private long threadId;
    private UUID invocationUid;

    protected UnlockMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        RaftInvocationManager raftInvocationManager = getRaftInvocationManager();
        ICompletableFuture future =
                raftInvocationManager.invoke(groupId, new UnlockOp(name, sessionId, threadId, invocationUid));
        future.andThen(this);
    }

    @Override
    protected Object decodeClientMessage(ClientMessage clientMessage) {
        super.decodeClientMessage(clientMessage);
        threadId = clientMessage.getLong();
        long least = clientMessage.getLong();
        long most = clientMessage.getLong();
        invocationUid = new UUID(most, least);
        return null;
    }
}
