package com.hazelcast.raft.service.lock.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.raft.service.lock.proxy.RaftLockProxy;

/**
 * TODO: Javadoc Pending...
 *
 */
public class GetLockCountMessageTask extends AbstractLockMessageTask {

    protected GetLockCountMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() throws Throwable {
        RaftLockProxy lockProxy = getProxy();
        ICompletableFuture<Integer> future = lockProxy.getLockCountAsync();
        future.andThen(this);
    }
}
