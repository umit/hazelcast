package com.hazelcast.raft;

import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftConfig {

    private Collection<String> addresses;

    public Collection<String> getAddresses() {
        return addresses;
    }

    public void setAddresses(Collection<String> addresses) {
        this.addresses = addresses;
    }
}
