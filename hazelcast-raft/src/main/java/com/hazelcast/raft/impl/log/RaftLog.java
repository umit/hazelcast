package com.hazelcast.raft.impl.log;

import com.hazelcast.raft.RaftOperation;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.reverse;

/**
 * TODO: Javadoc Pending...
 */
public class RaftLog {

    /**
     * !!! Log entry indices start from 1 !!!
     */
    private final ArrayList<LogEntry> logs = new ArrayList<LogEntry>();

    private LogEntry snapshot = new LogEntry();

    public int lastLogIndex() {
        return lastLogEntry().index();
    }

    public int lastLogTerm() {
        return lastLogEntry().term();
    }

    // lastLogEntry returns the last index and term, either from the last log or from the last snapshot
    public LogEntry lastLogEntry() {
        return logs.size() > 0 ? logs.get(logs.size() - 1) : (snapshot != null ? snapshot : new LogEntry());
    }

    public LogEntry getEntry(int entryIndex) {
        if (entryIndex < 1) {
            throw new IllegalArgumentException("Illegal index: " + entryIndex + ". Index starts from 1.");
        } else if (entryIndex > lastLogIndex() || snapshotIndex() >= entryIndex) {
            return null;
        }

        return logs.get(toArrayIndex(entryIndex));
    }

    public List<LogEntry> truncateEntriesFrom(int entryIndex) {
        if (entryIndex <= snapshotIndex()) {
            throw new IllegalArgumentException("Illegal index: " + entryIndex + ", snapshot index: " + snapshotIndex());
        } else if (entryIndex > lastLogIndex()) {
            throw new IllegalArgumentException("Illegal index: " + entryIndex + ", last log index: " + lastLogIndex());
        }

        List<LogEntry> truncated = new ArrayList<LogEntry>();
        for (int i = logs.size() - 1, j = toArrayIndex(entryIndex); i >= j; i--) {
            truncated.add(logs.remove(i));
        }

        reverse(truncated);

        return truncated;
    }

    public void appendEntries(LogEntry... newEntries) {
        int lastTerm = lastLogTerm();
        int lastIndex = lastLogIndex();

        for (LogEntry entry : newEntries) {
            if (entry.term() < lastTerm) {
                throw new IllegalArgumentException("Cannot append " + entry + " since its term is lower than last log term: "
                        + lastTerm);
            } else if (entry.index() != lastIndex + 1) {
                throw new IllegalArgumentException("Cannot append " + entry + "since its index is bigger than (lasLogIndex + 1): "
                        + (lastIndex + 1));
            }
            logs.add(entry);
            lastIndex++;
            lastTerm = Math.max(lastTerm, entry.term());
        }
    }

    // both inclusive
    public LogEntry[] getEntriesBetween(int fromEntryIndex, int toEntryIndex) {
        if (fromEntryIndex > toEntryIndex) {
            throw new IllegalArgumentException("Illegal from entry index: " + fromEntryIndex + ", to entry index: " + toEntryIndex);
        } else if (fromEntryIndex <= snapshotIndex()) {
            throw new IllegalArgumentException("Illegal from entry index: " + fromEntryIndex + ", snapshot index: " + snapshotIndex());
        } else if (fromEntryIndex > lastLogIndex()) {
            throw new IllegalArgumentException("Illegal from entry index: " + fromEntryIndex + ", last log index: " + lastLogIndex());
        } else if (toEntryIndex > lastLogIndex()) {
            throw new IllegalArgumentException("Illegal to entry index: " + toEntryIndex + ", last log index: " + lastLogIndex());
        }

        return logs.subList(toArrayIndex(fromEntryIndex), toArrayIndex(toEntryIndex + 1)).toArray(new LogEntry[0]);
    }

    public void setSnapshot(int entryIndex, RaftOperation operation) {
        if (entryIndex <= snapshotIndex()) {
            throw new IllegalArgumentException("Illegal index: " + entryIndex + ", snapshot index: " + snapshotIndex());
        } else if (entryIndex > lastLogIndex()) {
            throw new IllegalArgumentException("Illegal index: " + entryIndex + ", last log index: " + lastLogIndex());
        }

        LogEntry snapshotLogEntry = getEntry(entryIndex);
        LogEntry snapshot = new LogEntry(snapshotLogEntry.term(), entryIndex, operation);

        reverse(logs);
        int removeCount = (entryIndex - snapshotIndex());
        for (int i = logs.size() - 1, j = i - removeCount; i > j; i--) {
            logs.remove(i);
        }
        reverse(logs);

        this.snapshot = snapshot;
    }

    public int snapshotIndex() {
        return snapshot.index();
    }

    public LogEntry snapshot() {
        return snapshot;
    }

    private int toArrayIndex(int entryIndex) {
        return (entryIndex - snapshotIndex()) - 1;
    }
}
