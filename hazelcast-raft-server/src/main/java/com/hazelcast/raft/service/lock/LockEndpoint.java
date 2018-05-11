package com.hazelcast.raft.service.lock;

/**
 * TODO: Javadoc Pending...
 *
 */
public class LockEndpoint {
    public final long sessionId;
    public final long threadId;

    public LockEndpoint(long sessionId, long threadId) {
        this.sessionId = sessionId;
        this.threadId = threadId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LockEndpoint)) {
            return false;
        }

        LockEndpoint that = (LockEndpoint) o;

        if (sessionId != that.sessionId) {
            return false;
        }
        return threadId == that.threadId;
    }

    @Override
    public int hashCode() {
        int result = (int) (sessionId ^ (sessionId >>> 32));
        result = 31 * result + (int) (threadId ^ (threadId >>> 32));
        return result;
    }
}
