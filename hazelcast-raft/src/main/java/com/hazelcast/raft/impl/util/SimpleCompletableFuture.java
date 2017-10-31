package com.hazelcast.raft.impl.util;

import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.AbstractCompletableFuture;

/**
 * TODO: Javadoc Pending...
 *
 */
public class SimpleCompletableFuture<T> extends AbstractCompletableFuture<T> {

    public SimpleCompletableFuture(NodeEngine nodeEngine) {
        super(nodeEngine, nodeEngine.getLogger(RaftNode.class));
    }

    @Override
    public void setResult(Object result) {
        super.setResult(result);
    }
}
