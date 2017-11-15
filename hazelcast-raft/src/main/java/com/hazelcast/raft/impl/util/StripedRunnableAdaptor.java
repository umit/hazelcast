package com.hazelcast.raft.impl.util;

import com.hazelcast.util.executor.StripedRunnable;

/**
 * TODO: Javadoc Pending...
 *
 */
public final class StripedRunnableAdaptor implements StripedRunnable {

    private final Runnable command;
    private final int key;

    public StripedRunnableAdaptor(Runnable command, int key) {
        this.command = command;
        this.key = key;
    }

    @Override
    public int getKey() {
        return key;
    }

    @Override
    public void run() {
        command.run();
    }
}
