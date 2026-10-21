package exchange.core2.revelator.raft.repository;

public class JournalDescriptor {

    private final long timestamp;
    private final long seqFirst;
    private long seqLast = -1; // -1 if not finished yet

    private final SnapshotDescriptor baseSnapshot;

    private final JournalDescriptor prev; // can be null

    private JournalDescriptor next = null; // can be null

    public JournalDescriptor(long timestamp, long seqFirst, SnapshotDescriptor baseSnapshot, JournalDescriptor prev) {
        this.timestamp = timestamp;
        this.seqFirst = seqFirst;
        this.baseSnapshot = baseSnapshot;
        this.prev = prev;
    }
}
