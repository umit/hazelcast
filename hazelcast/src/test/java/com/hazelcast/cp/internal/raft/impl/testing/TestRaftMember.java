package com.hazelcast.cp.internal.raft.impl.testing;

import com.hazelcast.core.EndpointIdentifier;

public class TestRaftMember implements EndpointIdentifier {

    private String uuid;

    private int port;

    public TestRaftMember(String uuid, int port) {
        this.uuid = uuid;
        this.port = port;
    }

    @Override
    public String getUuid() {
        return uuid;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TestRaftMember that = (TestRaftMember) o;

        if (port != that.port) {
            return false;
        }
        return uuid.equals(that.uuid);
    }

    @Override
    public int hashCode() {
        int result = uuid.hashCode();
        result = 31 * result + port;
        return result;
    }

    @Override
    public String toString() {
        return "TestRaftMember{" + "uuid='" + uuid + '\'' + ", port=" + port + '}';
    }

}
