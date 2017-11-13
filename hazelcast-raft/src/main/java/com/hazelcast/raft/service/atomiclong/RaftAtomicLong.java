package com.hazelcast.raft.service.atomiclong;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftAtomicLong {

    private final String name;

    private long value;
    private int commitIndex;

    public RaftAtomicLong(String name) {
        this.name = name;
    }

    public long addAndGet(long delta, int commitIndex) {
        this.commitIndex = commitIndex;
        return value += delta;
    }

    public long getAndAdd(long delta, int commitIndex) {
        this.commitIndex = commitIndex;
        long v = value;
        value += delta;
        return v;
    }

    public long getAndSet(long value, int commitIndex) {
        this.commitIndex = commitIndex;
        long v = this.value;
        this.value = value;
        return v;
    }

    public boolean compareAndSet(long currentValue, long newValue, int commitIndex) {
        this.commitIndex = commitIndex;
        if (value == currentValue) {
            value = newValue;
            return true;
        }
        return false;
    }
}
