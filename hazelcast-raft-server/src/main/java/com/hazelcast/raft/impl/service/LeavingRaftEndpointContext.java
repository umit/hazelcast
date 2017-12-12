package com.hazelcast.raft.impl.service;

import com.hazelcast.raft.impl.RaftEndpoint;

import java.util.Collection;
import java.util.Map;

public class LeavingRaftEndpointContext {

    private RaftEndpoint endpoint;

    private Map<RaftGroupId, RaftGroupLeavingEndpointContext> groups;

    public LeavingRaftEndpointContext(RaftEndpoint endpoint, Map<RaftGroupId, RaftGroupLeavingEndpointContext> groups) {
        this.endpoint = endpoint;
        this.groups = groups;
    }

    public RaftEndpoint getEndpoint() {
        return endpoint;
    }

    public Map<RaftGroupId, RaftGroupLeavingEndpointContext> getGroups() {
        return groups;
    }

    public static class RaftGroupLeavingEndpointContext {

        private int membersCommitIndex;

        private Collection<RaftEndpoint> members;

        private RaftEndpoint substitute;

        public RaftGroupLeavingEndpointContext(int membersCommitIndex, Collection<RaftEndpoint> members, RaftEndpoint substitute) {
            this.membersCommitIndex = membersCommitIndex;
            this.members = members;
            this.substitute = substitute;
        }

        public int getMembersCommitIndex() {
            return membersCommitIndex;
        }

        public Collection<RaftEndpoint> getMembers() {
            return members;
        }

        public RaftEndpoint getSubstitute() {
            return substitute;
        }

        @Override
        public String toString() {
            return "RaftGroupLeavingEndpointContext{" + "membersCommitIndex=" + membersCommitIndex + ", substitute=" + substitute + '}';
        }
    }

    @Override
    public String toString() {
        return "LeavingRaftEndpointContext{" + "endpoint=" + endpoint + ", groups=" + groups + '}';
    }
}
