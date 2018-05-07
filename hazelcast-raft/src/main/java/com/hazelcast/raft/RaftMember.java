package com.hazelcast.raft;

/**
 * Represents a member of the Raft group.
 * Each member must have a unique address and id in the group.
 */
public class RaftMember {

    private String address;
    private String uid;

    public RaftMember() {
    }

    public RaftMember(RaftMember member) {
        this(member.address, member.uid);
    }

    public RaftMember(String address, String uid) {
        this.address = address;
        this.uid = uid;
    }

    public String getAddress() {
        return address;
    }

    public RaftMember setAddress(String address) {
        this.address = address;
        return this;
    }

    public String getUid() {
        return uid;
    }

    public RaftMember setUid(String uid) {
        this.uid = uid;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RaftMember)) {
            return false;
        }

        RaftMember that = (RaftMember) o;
        return address.equals(that.address) && uid.equals(that.uid);
    }

    @Override
    public int hashCode() {
        int result = address.hashCode();
        result = 31 * result + uid.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "RaftMember{" + "address='" + address + '\'' + ", id='" + uid + '\'' + '}';
    }
}
