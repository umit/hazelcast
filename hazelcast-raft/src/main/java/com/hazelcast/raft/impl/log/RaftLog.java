package com.hazelcast.raft.impl.log;

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

    public int lastLogOrSnapshotIndex() {
        return lastLogOrSnapshotEntry().index();
    }

    public int lastLogOrSnapshotTerm() {
        return lastLogOrSnapshotEntry().term();
    }

    // lastLogOrSnapshotEntry returns the last index and term, either from the last log entry or from the last snapshot
    public LogEntry lastLogOrSnapshotEntry() {
        return logs.size() > 0 ? logs.get(logs.size() - 1) : snapshot;
    }

    // returns only from the current log, not from the snapshot entry
    public LogEntry getLogEntry(int entryIndex) {
        if (entryIndex < 1) {
            throw new IllegalArgumentException("Illegal index: " + entryIndex + ". Index starts from 1.");
        }
        if (entryIndex > lastLogOrSnapshotIndex() || snapshotIndex() >= entryIndex) {
            return null;
        }

        return logs.get(toArrayIndex(entryIndex));
    }

    public List<LogEntry> truncateEntriesFrom(int entryIndex) {
        if (entryIndex <= snapshotIndex()) {
            throw new IllegalArgumentException("Illegal index: " + entryIndex + ", snapshot index: " + snapshotIndex());
        }
        if (entryIndex > lastLogOrSnapshotIndex()) {
            throw new IllegalArgumentException("Illegal index: " + entryIndex + ", last log index: " + lastLogOrSnapshotIndex());
        }

        List<LogEntry> truncated = new ArrayList<LogEntry>();
        for (int i = logs.size() - 1, j = toArrayIndex(entryIndex); i >= j; i--) {
            truncated.add(logs.remove(i));
        }

        reverse(truncated);

        return truncated;
    }

    public void appendEntries(LogEntry... newEntries) {
        int lastTerm = lastLogOrSnapshotTerm();
        int lastIndex = lastLogOrSnapshotIndex();

        for (LogEntry entry : newEntries) {
            if (entry.term() < lastTerm) {
                throw new IllegalArgumentException("Cannot append " + entry + " since its term is lower than last log term: "
                        + lastTerm);
            }
            if (entry.index() != lastIndex + 1) {
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
        }
        if (fromEntryIndex <= snapshotIndex()) {
            throw new IllegalArgumentException("Illegal from entry index: " + fromEntryIndex + ", snapshot index: " + snapshotIndex());
        }
        if (fromEntryIndex > lastLogOrSnapshotIndex()) {
            throw new IllegalArgumentException("Illegal from entry index: " + fromEntryIndex + ", last log index: " + lastLogOrSnapshotIndex());
        }
        if (toEntryIndex > lastLogOrSnapshotIndex()) {
            throw new IllegalArgumentException("Illegal to entry index: " + toEntryIndex + ", last log index: " + lastLogOrSnapshotIndex());
        }

        return logs.subList(toArrayIndex(fromEntryIndex), toArrayIndex(toEntryIndex + 1)).toArray(new LogEntry[0]);
    }

    public List<LogEntry> setSnapshot(LogEntry snapshot) {
        if (snapshot.index() <= snapshotIndex()) {
            throw new IllegalArgumentException("Illegal index: " + snapshot.index() + ", current snapshot index: " + snapshotIndex());
        }

        List<LogEntry> truncated = new ArrayList<LogEntry>();
        reverse(logs);
        for (int i = logs.size() - 1; i >= 0; i--) {
            LogEntry logEntry = logs.get(i);
            if (logEntry.index() > snapshot.index()) {
                break;
            }

            logs.remove(i);
        }

        reverse(logs);

        this.snapshot = snapshot;

        return truncated;
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
