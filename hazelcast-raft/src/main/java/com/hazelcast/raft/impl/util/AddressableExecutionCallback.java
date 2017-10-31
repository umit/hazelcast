package com.hazelcast.raft.impl.util;

import com.hazelcast.nio.Address;

/**
 * TODO: Javadoc Pending...
 */
public interface AddressableExecutionCallback<T> {

    void onResponse(Address address, T response);

    void onFailure(Address address, Throwable t);

}
