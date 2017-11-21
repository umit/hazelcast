package com.hazelcast.raft.service.atomiclong.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;

import static com.hazelcast.raft.impl.service.CreateRaftGroupHelper.createRaftGroupAsync;
import static com.hazelcast.raft.service.atomiclong.RaftAtomicLongService.PREFIX;
import static com.hazelcast.raft.service.atomiclong.RaftAtomicLongService.SERVICE_NAME;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CreateAtomicLongMessageTask extends AbstractAtomicLongMessageTask {

    private int nodeCount;

    protected CreateAtomicLongMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() throws Throwable {
        String raftName = PREFIX + name;
        ICompletableFuture future = createRaftGroupAsync(nodeEngine, SERVICE_NAME, raftName, nodeCount);
        future.andThen(this);
    }

    @Override
    public void onResponse(Object response) {
        sendResponse(true);
    }

    @Override
    protected Object decodeClientMessage(ClientMessage clientMessage) {
        super.decodeClientMessage(clientMessage);
        nodeCount = clientMessage.getInt();
        return null;
    }
}
