package com.hazelcast.raft.impl.util;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.nio.Address;

/**
 * TODO: Javadoc Pending...
 */
public class ExecutionCallbackAdapter<T> implements ExecutionCallback<T> {

    private final Address address;

    private final AddressableExecutionCallback<T> callback;

    public ExecutionCallbackAdapter(Address address, AddressableExecutionCallback<T> callback) {
        this.address = address;
        this.callback = callback;
    }

    @Override
    public void onResponse(T response) {
        callback.onResponse(address, response);
    }

    @Override
    public void onFailure(Throwable t) {
        callback.onFailure(address, t);
    }
}
