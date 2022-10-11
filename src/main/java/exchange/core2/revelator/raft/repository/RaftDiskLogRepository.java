package exchange.core2.revelator.raft.repository;

import exchange.core2.revelator.raft.RsmRequestFactory;
import exchange.core2.revelator.raft.messages.RaftLogEntry;
import exchange.core2.revelator.raft.messages.RsmRequest;
import org.agrona.collections.MutableLong;
import org.eclipse.collections.api.tuple.primitive.LongLongPair;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class RaftDiskLogRepository<T extends RsmRequest> implements IRaftLogRepository<T> {

    private static final Logger log = LoggerFactory.getLogger(RaftDiskLogRepository.class);

    private final int journalBufferFlushTrigger = 65536;
    private final long journalFileMaxSize = 2_000_000_000;

    private final int indexRecordEveryNBytes = 4096;


    private final String exchangeId = "EC2R-TEST";
    private final Path folder;

    private final int nodeId;

    private RandomAccessFile raf;
    private FileChannel writeChannel;
    private FileChannel readChannel;


    private RandomAccessFile indexRaf;
    private FileChannel indexWriteChannel;

    // index -> position in file // TODO keep term in the index?
    private NavigableMap<Long, Long> currentIndex = new TreeMap<>(); // TODO  use ART ?


    private long baseSnapshotId;

    private int filesCounter = 0;

    private long writtenBytes = 0;
    private long lastIndexWrittenAt = 0;

    private long lastIndex = 0L;
    private int lastLogTerm = 0;

    private final ByteBuffer journalWriteBuffer = ByteBuffer.allocateDirect(512 * 1024);
    private final ByteBuffer indexWriteBuffer = ByteBuffer.allocateDirect(512 * 1024); // TODO Limit index size


    private SnapshotDescriptor lastSnapshotDescriptor = null; // todo implemnt
    private JournalDescriptor lastJournalDescriptor;


    private final RsmRequestFactory<T> rsmRequestFactory;

    public RaftDiskLogRepository(RsmRequestFactory<T> rsmRequestFactory, int nodeId) {
        this.rsmRequestFactory = rsmRequestFactory;
        this.nodeId = nodeId;

        this.folder = Path.of("./raftlogs/node" + nodeId);

        final long timestamp = System.currentTimeMillis();

        baseSnapshotId = timestamp;

        startNewFile(timestamp);
    }


    @Override
    public long findLastEntryInTerm(long indexAfter, long indexBeforeIncl, int term) {
        // TODO Term index

        throw new UnsupportedOperationException();

        //return 0;
    }

    @Override
    public long getLastLogIndex() {
        return lastIndex;
    }

    @Override
    public int getLastLogTerm() {
        return lastLogTerm;
    }

    @Override
    public long appendEntry(RaftLogEntry<T> logEntry, boolean endOfBatch) {

        if (writeChannel == null) {
            startNewFile(logEntry.timestamp());
        }

        final ByteBuffer buffer = journalWriteBuffer;

        lastIndex++;
        lastLogTerm = logEntry.term();

        buffer.putLong(logEntry.timestamp()); // 8 bytes
        buffer.putInt(logEntry.term()); // 4 bytes
        logEntry.serialize(buffer);


        if (endOfBatch || buffer.position() >= journalBufferFlushTrigger) {

            // flushing on end of batch or when buffer is full
            flushBufferSync(false, logEntry.timestamp());
        }

        return lastIndex;
    }

    @Override
    public void appendOrOverride(List<RaftLogEntry<T>> newEntries, long prevLogIndex) {

        log.debug("appendOrOverride(newEntries={} , prevLogIndex={}", newEntries, prevLogIndex);

        try {

            // check for missed records
            if (prevLogIndex > lastIndex) {
                throw new IllegalStateException("Can not accept prevLogIndex=" + prevLogIndex + " because=" + lastIndex);
            }

            // check if leader is overriding some records
            if (prevLogIndex < lastIndex) {

                // TODO loading just to compare term - can be done faster
                final long removeAfter = verifyTerms(newEntries, prevLogIndex);

                if (removeAfter != -1) {

                    final long position = findPosition(removeAfter);
                    log.debug("Removing after position: {}", position);
                    writeChannel.position(position);
                    writeChannel.truncate(position);
                    writtenBytes = position;

                    truncateIndexRecords(removeAfter);

                    lastIndex = removeAfter;
                    lastLogTerm = getEntries(lastIndex, 1).get(0).term();
                }
            }

            // adding missing records
            final int offset = (int) (lastIndex - prevLogIndex);
            if (offset > 0) {
                final int lastIndex = newEntries.size();
                for (int i = offset; i <= lastIndex; i++) {
                    appendEntry(newEntries.get(i), i == lastIndex);
                }
            }

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private long findPosition(long removeAfter) throws IOException {

        final LongLongPair startingIndexPoint = findStartingIndexPoint(removeAfter);
        final long startOffset = startingIndexPoint.getOne();
        final long floorIndex = startingIndexPoint.getTwo();

        readChannel.position(startOffset);

        long idx = floorIndex;

        try (final InputStream is = Channels.newInputStream(readChannel);
             final BufferedInputStream bis = new BufferedInputStream(is);
             final DataInputStream dis = new DataInputStream(bis)) {

            while (dis.available() != 0) {

                RaftLogEntry.create(dis, rsmRequestFactory);

                idx++;

                if (idx == removeAfter) {
                    return readChannel.position();
                }
            }
        }

        throw new RuntimeException("Can not reach index " + removeAfter);
    }

    private void truncateIndexRecords(long removeAfterIndex) throws IOException {

        // clean tail subtree
        currentIndex.tailMap(removeAfterIndex, false).clear();

        final Map.Entry<Long, Long> lastIndexEntry = currentIndex.lastEntry();

        indexWriteChannel.position(0L);

        if (lastIndexEntry == null) {
            // empty tree - just clean all file
            lastIndexWrittenAt = 0L;
            indexWriteChannel.truncate(0L);

        } else {

            // set bytes offset to last known value (maybe not very exact?)
            lastIndexWrittenAt = lastIndexEntry.getValue();

            // remove all records after
            try (final InputStream is = Channels.newInputStream(indexWriteChannel);
                 final BufferedInputStream bis = new BufferedInputStream(is);
                 final DataInputStream dis = new DataInputStream(bis)) {

                while (dis.available() != 0) {

                    // read index record (16 bytes)
                    final long lastIndex = dis.readLong();
                    dis.readLong();

                    if (lastIndex > lastIndexEntry.getKey()) {
                        final long pos = indexWriteChannel.position() - 16;
                        indexWriteChannel.position(pos);
                        indexWriteChannel.truncate(pos);
                        return;
                    }
                }
            }
        }
    }

    private long verifyTerms(List<RaftLogEntry<T>> newEntries, long prevLogIndex) {
        final List<RaftLogEntry<T>> existingEntries = getEntries(prevLogIndex, newEntries.size());
        final int intersectionLength = Math.min(existingEntries.size(), newEntries.size());

        for (int i = 0; i < intersectionLength; i++) {
            if (existingEntries.get(i).term() != newEntries.get(i).term()) {
                return prevLogIndex + i;
            }
        }

        return -1;
    }

    /**
     * @return offset+index to start looking from
     */
    private LongLongPair findStartingIndexPoint(long indexFrom) {
        final Map.Entry<Long, Long> entry = currentIndex.floorEntry(indexFrom);
        final long startOffset = (entry == null) ? 0L : entry.getValue();
        final long floorIndex = (entry == null) ? 1L : entry.getKey();
        return PrimitiveTuples.pair(startOffset, floorIndex);
    }

    @Override
    public List<RaftLogEntry<T>> getEntries(long indexFrom, int limit) {

        if (indexFrom == 0L && limit == 1) {
            return List.of();
        }

        if (indexFrom > lastIndex) {
            return List.of();
        }

        final LongLongPair indexStartingIndex = findStartingIndexPoint(indexFrom);
        final long startOffset = indexStartingIndex.getOne();
        final long floorIndex = indexStartingIndex.getTwo();

        try {
            log.debug("Reading {} - floor idx:{} offset:{}", indexFrom, floorIndex, startOffset);
            readChannel.position(startOffset);
            log.debug("Position ok");
        } catch (IOException ex) {
            throw new RuntimeException("can not read log at offset " + startOffset, ex);
        }

        final List<RaftLogEntry<T>> entries = new ArrayList<>();

        final MutableLong indexCounter = new MutableLong(floorIndex);

        try (final InputStream is = Channels.newInputStream(readChannel);
             final BufferedInputStream bis = new BufferedInputStream(is);
             final DataInputStream dis = new DataInputStream(bis)) {

            final boolean allLoaded = readCommands(dis, entries, indexCounter, indexFrom, limit);
            if (!allLoaded) {
                throw new RuntimeException("not loaded everything");
            }

            return entries;

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            try {
                readChannel.position(0);
            } catch (IOException e) {
                log.error("Can not rewind readChannel position to 0");
            }
        }
    }

    private void startNewFile(final long timestamp) {

        try {

            filesCounter++;

            closeCurrentFiles();

            final Path logFileName = resolveJournalPath(filesCounter, baseSnapshotId);
            final Path indexFileName = resolveIndexPath(filesCounter, baseSnapshotId);
            log.debug("Starting new raft log file: {} index file: {}", logFileName, indexFileName);

            if (Files.exists(logFileName)) {
                throw new IllegalStateException("File already exists: " + logFileName);
            }

            if (Files.exists(indexFileName)) {
                throw new IllegalStateException("File already exists: " + indexFileName);
            }

            raf = new RandomAccessFile(logFileName.toString(), "rwd");
            writeChannel = raf.getChannel();
            readChannel = raf.getChannel();

            indexRaf = new RandomAccessFile(indexFileName.toString(), "rwd");
            indexWriteChannel = raf.getChannel();


            registerNextJournal(baseSnapshotId, timestamp); // TODO fix time


        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }


    /**
     * call only from journal thread
     */
    private void registerNextJournal(long seq, long timestamp) {

        lastJournalDescriptor = new JournalDescriptor(timestamp, seq, lastSnapshotDescriptor, lastJournalDescriptor);
    }


    private Path resolveJournalPath(int partitionId, long snapshotId) {
        return folder.resolve(String.format("%s_log_%d_%04X.ecrl", exchangeId, snapshotId, partitionId));
    }

    private Path resolveIndexPath(int partitionId, long snapshotId) {
        return folder.resolve(String.format("%s_idx_%d_%04X.ridx", exchangeId, snapshotId, partitionId));
    }

    private void flushBufferSync(final boolean forceStartNextFile,
                                 final long timestampNs) {

        try {

//        log.debug("Flushing buffer position={}", buffer.position());

            // uncompressed write for single messages or small batches
            writtenBytes += journalWriteBuffer.position();
            journalWriteBuffer.flip();
            writeChannel.write(journalWriteBuffer);
            journalWriteBuffer.clear();

            //
            if (writtenBytes > lastIndexWrittenAt + indexRecordEveryNBytes) {

                log.debug("Adding index record:{}->{}", lastIndex, writtenBytes);

                currentIndex.put(lastIndex, writtenBytes);

                indexWriteBuffer.putLong(lastIndex);
                indexWriteBuffer.putLong(writtenBytes);
                indexWriteBuffer.flip();
                indexWriteChannel.write(indexWriteBuffer);
                indexWriteBuffer.clear();

            }

            if (forceStartNextFile || writtenBytes >= journalFileMaxSize) {

//            log.info("RAW {}", LatencyTools.createLatencyReportFast(hdrRecorderRaw.getIntervalHistogram()));
//            log.info("LZ4-compression {}", LatencyTools.createLatencyReportFast(hdrRecorderLz4.getIntervalHistogram()));

                // todo start preparing new file asynchronously, but ONLY ONCE
                startNewFile(timestampNs);
                writtenBytes = 0;
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }


    private List<RaftLogEntry<T>> readData(final long baseSnapshotId,
                                           final long indexFrom,
                                           final int limit) throws IOException {


        final List<RaftLogEntry<T>> entries = new ArrayList<>();

        final MutableLong currentIndex = new MutableLong(0L);

        int partitionCounter = 1;

        while (true) {

            final Path path = resolveJournalPath(partitionCounter, baseSnapshotId);

            // TODO Use index

            log.debug("Reading RAFT log file: {}", path.toFile());

            try (final FileInputStream fis = new FileInputStream(path.toFile());
                 final BufferedInputStream bis = new BufferedInputStream(fis);
                 final DataInputStream dis = new DataInputStream(bis)) {

                final boolean done = readCommands(dis, entries, currentIndex, indexFrom, limit - entries.size());
                if (done) {
                    return entries;
                }


                partitionCounter++;
                log.debug("EOF reached, reading next partition {}...", partitionCounter);

            } catch (FileNotFoundException ex) {
                log.debug("FileNotFoundException: currentIndex={}, {}", currentIndex, ex.getMessage());
                throw ex;

            } catch (EOFException ex) {
                // partitionCounter++;
                log.debug("File end reached through exception, currentIndex={} !!!", currentIndex);
                throw ex;
            }
        }

    }


    private boolean readCommands(final DataInputStream dis,
                                 final List<RaftLogEntry<T>> collector,
                                 final MutableLong indexCounter,
                                 final long indexFrom,
                                 final int limit) throws IOException {

        while (dis.available() != 0) {

            final long idx = indexCounter.incrementAndGet();

            final RaftLogEntry<T> logEntry = RaftLogEntry.create(dis, rsmRequestFactory);

            if (idx >= indexFrom) {
                log.debug("Adding record into collection idx={} {}", idx, logEntry);
                collector.add(logEntry);
            }

            if (collector.size() == limit) {
                return true;
            }
        }

        return false;
    }


    @Override
    public void close() throws IOException {
        closeCurrentFiles();
    }

    private void closeCurrentFiles() throws IOException {
        if (writeChannel != null) {
            writeChannel.close();
            readChannel.close();
            raf.close();
        }

        if (indexWriteChannel != null) {
            indexWriteChannel.close();
            indexRaf.close();
        }
    }
}
