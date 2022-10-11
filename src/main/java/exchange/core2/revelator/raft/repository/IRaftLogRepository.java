package exchange.core2.revelator.raft.repository;

import exchange.core2.revelator.raft.messages.RaftLogEntry;
import exchange.core2.revelator.raft.messages.RsmRequest;

import java.util.List;

/**
 * RAFT log persistent storage repository
 *
 * @param <T> - request records type for particular Replicated State Machine
 */
public interface IRaftLogRepository<T extends RsmRequest> extends AutoCloseable {

    long getLastLogIndex();

    int getLastLogTerm();

    /**
     * Get entry from log
     *
     * @param indexFrom - RAFT record index (starting from 1)
     * @param limit     - max number of entries to retrieve
     * @return records (if found)
     */
    List<RaftLogEntry<T>> getEntries(long indexFrom, int limit);


    long findLastEntryInTerm(long indexAfter, long indexBeforeIncl, int term);

    /**
     * Append single entry
     *
     * @param logEntry   - RAFT Replicated State Machine entry
     * @param endOfBatch - force writing to disk
     * @return index of added entry
     */
    long appendEntry(RaftLogEntry<T> logEntry, boolean endOfBatch);


    void appendOrOverride(List<RaftLogEntry<T>> newEntries, long prevLogIndex);


}
