package com.hazelcast.raft;

import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 *
 * @author mdogan 30.10.2017
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
