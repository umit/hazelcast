package com.hazelcast.raft.impl;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.addAll;
import static java.util.Collections.reverse;

/**
 * TODO: Javadoc Pending...
 */
public class RaftLog {

    /**
     * !!! Log entry indices start from 1 !!!
     */
    private final ArrayList<LogEntry> logs = new ArrayList<LogEntry>();

    public int lastLogIndex() {
        return lastLogEntry().index();
    }

    public int lastLogTerm() {
        return lastLogEntry().term();
    }

    public LogEntry lastLogEntry() {
        return logs.isEmpty() ? new LogEntry() : logs.get(logs.size() - 1);
    }

    public LogEntry getEntry(int entryIndex) {
        return logs.size() >= entryIndex ? logs.get(toArrayIndex(entryIndex)) : null;
    }

    public List<LogEntry> truncateEntriesFrom(int entryIndex) {
        List<LogEntry> truncated = new ArrayList<LogEntry>();
        for (int i = logs.size() - 1; i >= toArrayIndex(entryIndex); i--) {
            truncated.add(logs.remove(i));
        }

        reverse(truncated);

        return truncated;
    }

    public void appendEntries(LogEntry... newEntries) {
        addAll(logs, newEntries);
    }

    // both inclusive
    public LogEntry[] getEntriesBetween(int fromEntryIndex, int toEntryIndex) {
        if (logs.size() < fromEntryIndex) {
            return new LogEntry[0];
        }

        return logs.subList(toArrayIndex(fromEntryIndex), toArrayIndex(toEntryIndex + 1)).toArray(new LogEntry[0]);
    }

    private int toArrayIndex(int entryIndex) {
        return entryIndex - 1;
    }
}
