package zipkin.collector.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin.Codec;
import zipkin.Component;
import zipkin.Span;
import zipkin.internal.LazyCloseable;
import zipkin.internal.Nullable;
import zipkin.storage.Callback;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static zipkin.collector.kafka.Kafka10CollectorConfig.FirstPollOffsetStrategy.*;

/**
 * Created by huangjian on 2017/5/10.
 */
final class Kafka10CollectorConsumerStream extends LazyCloseable<ExecutorService> {

    private static final Logger LOG = LoggerFactory.getLogger(Kafka10CollectorConsumerStream.class);

    private final Kafka10CollectorConfig kafka10CollectorConfig;

    private transient Kafka10CollectorConfig.FirstPollOffsetStrategy firstPollOffsetStrategy;  // Strategy to determine the fetch offset of the first realized by the collector upon activation
    private transient boolean consumerAutoCommitMode;
    private transient int maxRetries;                                   // Max number of times a tuple is retried
    private transient Kafka10CollectorRetryService retryService;
    private transient Kafka10CollectorTimer commitTimer;
    private transient boolean initialized;                            // Flag indicating that the collector is still undergoing initialization process. Initialization is only complete after the first call to  Kafka10ConsumerRebalancedListener.onPartitionsAssigned()
    private transient boolean closed = true;

    private transient final Kafka10CollectorSpansBuilder spansBuilder;      // Object that contains the logic to build tuples for each ConsumerRecord

    private transient KafkaConsumer<String, String> kafkaConsumer;

    private transient Map<TopicPartition, OffsetEntry> stored;         // Tuples that were successfully stored. These tuples will be committed periodically when the store timer expires, after consumer rebalance, or on close/deactivate
    private transient List<ConsumerRecord<String, String>> waitingToStore;         // Records that have been polled and are queued to be emitted in the nextTuple() call. One record is emitted per nextTuple()
    private transient int maxWaitingToStore = 500;     // max messages to be persisted.
    private transient long numUncommittedOffsets;                       // Number of offsets that have been polled and emitted but not yet been committed

    //    private final int streams;
    private final String[] topics;
    private final Kafka10CollectorSink collector;
    private final AtomicReference<Component.CheckResult> failure = new AtomicReference<>();

    Kafka10CollectorConsumerStream(
            Kafka10Collector.Builder builder,
            Kafka10CollectorConfig kafka10CollectorConfig) {
//        this.streams = builder.streams;
        this.topics = builder.topics;
        this.collector = builder.delegate.build();

        this.kafka10CollectorConfig = kafka10CollectorConfig;

        this.spansBuilder = (Kafka10CollectorSpansBuilder) consumerRecord -> {
            byte[] spansByte = consumerRecord.value().getBytes(Charset.forName("utf-8"));
            if (spansByte.length > 30)
                return Codec.JSON.readSpans(spansByte);
            return null;
        };

        this.numUncommittedOffsets = 0;
        this.firstPollOffsetStrategy = this.kafka10CollectorConfig.getFirstPollOffsetStrategy();
        this.consumerAutoCommitMode = this.kafka10CollectorConfig.isConsumerAutoCommitMode();

        // Retries management
        maxRetries = this.kafka10CollectorConfig.getMaxTupleRetries();
        retryService = this.kafka10CollectorConfig.getRetryService();

        if (!consumerAutoCommitMode) {     // If it is auto store, no need to store offsets manually
            this.commitTimer = new Kafka10CollectorTimer(
                    this.kafka10CollectorConfig.getOffsetsCommitPeriodMs(),
                    this.kafka10CollectorConfig.getOffsetsCommitPeriodMs(),
                    TimeUnit.MILLISECONDS);
        }

        this.stored = new HashMap<>();
        this.waitingToStore = new ArrayList<>(700);
        this.maxWaitingToStore = 500;

        LOG.info("Kafka10 Collector initialized with the following configuration: {}", this.kafka10CollectorConfig);
    }

    @Override
    public void close() {
        this.closed = true;
        this.kafkaConsumer.close();
        ExecutorService maybeNull = maybeNull();
        if (maybeNull != null) maybeNull.shutdown();
    }

    @Override
    protected ExecutorService compute() {
        this.closed = false;
        // TODO: 2017/5/10 to add multithreaded consumers.
        subscribeKafkaConsumer();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.execute(guardFailures(this::collect));
        return pool;
    }

    private void subscribeKafkaConsumer() {
        this.kafkaConsumer = new KafkaConsumer<>(
                kafka10CollectorConfig.getKafkaProps(),
                kafka10CollectorConfig.getKeyDeserializer(),
                kafka10CollectorConfig.getValueDeserializer());
        final List<String> topics = new ArrayList<>(this.topics.length);
        Collections.addAll(topics, this.topics);
        this.kafkaConsumer.subscribe(topics, new Kafka10ConsumerRebalancedListener());
        LOG.info("Kafka consumer subscribed topics {}", topics);
        // ConsumerRebalancedListener will be called following this poll, upon partition registration
        this.kafkaConsumer.poll(0);
    }

    private Runnable guardFailures(final Runnable delegate) {
        return () -> {
            while (!closed) try {
                delegate.run();
            } catch (RuntimeException e) {
                failure.set(Component.CheckResult.failed(e));
            }
        };
    }

    private void setStored(TopicPartition tp, long fetchOffset) {
        // If this partition was previously assigned to this collector,
        // leave the stored offsets as they were to resume where it left off
        if (!consumerAutoCommitMode && !stored.containsKey(tp)) {
            stored.put(tp, new OffsetEntry(tp, fetchOffset));
        }
    }

    // ======== collect (like storm spout . nextTupe) ======= //
    private void collect() {
        if (initialized) {
            if (store()) {
                storeAndCommitOffset();
            }
            if (poll()) {
                addWaitingToStore(pollKafkaBroker());
            }
        } else {
            LOG.debug("Spout not initialized. Not sending tuples until initialization completes");
        }
    }
    // ======== collect (like storm spout . nextTupe) ======= //

    // ======== codes for storing ========= //
    private boolean store() {
        return !consumerAutoCommitMode
                && (waitingToStore.size() > 0 && commitTimer.isExpiredResetOnTrue())
                && (waitingToStore.size() > this.maxWaitingToStore);
    }

    private void storeAndCommitOffset() {
        final List<Span> spans = new ArrayList<>(this.maxWaitingToStore);
        final Set<Kafka10CollectorMessageId> messageIds = new HashSet<>();
        for (ConsumerRecord<String, String> record : waitingToStore) {
            try {
                spans.addAll(spansBuilder.buildSpans(record));
                messageIds.add(new Kafka10CollectorMessageId(record));
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
        if (spans.size() > 0) {
            collector.accept(spans,
                    new Callback<Void>() {
                        @Override
                        public void onSuccess(@Nullable Void value) {
                            for (Kafka10CollectorMessageId messageId : messageIds) {
                                setStored(messageId.getTopicPartition(), messageId.offset());
                            }
                            // Find offsets that are ready to be committed for every topic partition
                            final Map<TopicPartition, OffsetAndMetadata> nextCommitOffsets = new HashMap<>();
                            for (Map.Entry<TopicPartition, OffsetEntry> tpOffset : stored.entrySet()) {
                                final OffsetAndMetadata nextCommitOffset = tpOffset.getValue().findNextCommitOffset();
                                if (nextCommitOffset != null) {
                                    nextCommitOffsets.put(tpOffset.getKey(), nextCommitOffset);
                                }
                            }
                            // Commit offsets that are ready to be committed for every topic partition
                            if (!nextCommitOffsets.isEmpty()) {
                                kafkaConsumer.commitSync(nextCommitOffsets);
                                LOG.debug("Offsets successfully committed to Kafka [{}]", nextCommitOffsets);
                                // Instead of iterating again, it would be possible to store and update the state for each TopicPartition
                                // in the prior loop, but the multiple network calls should be more expensive than iterating twice over a small loop
                                for (Map.Entry<TopicPartition, OffsetEntry> tpOffset : stored.entrySet()) {
                                    final OffsetEntry offsetEntry = tpOffset.getValue();
                                    offsetEntry.commit(nextCommitOffsets.get(tpOffset.getKey()));
                                }
                            } else {
                                LOG.trace("No offsets to store. {}", this);
                            }
                        }

                        @Override
                        public void onError(Throwable t) {
                            LOG.error(t.getMessage(), t);
                            for (Kafka10CollectorMessageId messageId : messageIds) {
                                if (messageId.numFails() < maxRetries) {
                                    messageId.incrementNumFails();
                                    retryService.schedule(messageId);
                                } else { // limit to max number of retries
                                    LOG.debug("Reached maximum number of retries. Message [{}] being marked as acked.", messageId);
                                    stored.get(messageId.getTopicPartition()).add(messageId);
                                }
                            }
                        }
                    }
            );
        }
    }
    // ======== codes for storing end ========= //

    // ======== codes for polling ========= //
    private boolean poll() {
        final int maxUncommittedOffsets = this.kafka10CollectorConfig.getMaxUncommittedOffsets();
        return !(waitingToStore.size() > this.maxWaitingToStore) && numUncommittedOffsets < maxUncommittedOffsets;
    }

    private ConsumerRecords<String, String> pollKafkaBroker() {
        doSeekRetriableTopicPartitions();
        LOG.info("Polling from kafka ...");
        final ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(this.kafka10CollectorConfig.getPollTimeoutMs());
        final int numPolledRecords = consumerRecords.count();
        LOG.debug("Polled [{}] records from Kafka. [{}] uncommitted offsets across all topic partitions", numPolledRecords, numUncommittedOffsets);
        return consumerRecords;
    }

    private void addWaitingToStore(ConsumerRecords<String, String> consumerRecords) {
        consumerRecords.forEach(this::addWaitToStore);
    }

    private void addWaitToStore(ConsumerRecord<String, String> record) {
        final TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        final Kafka10CollectorMessageId msgId = new Kafka10CollectorMessageId(record);
        if (stored.containsKey(tp) && stored.get(tp).contains(msgId)) {   // has been stored
            LOG.trace("Tuple for record [{}] has already been stored. Skipping", record);
        } else if (!retryService.isScheduled(msgId) || retryService.isReady(msgId)) {   // not scheduled <=> never failed (i.e. never emitted) or ready to be retried
            waitingToStore.add(record);
            numUncommittedOffsets++;
            if (retryService.isReady(msgId)) { // has failed. Is it ready for retry ?
                retryService.remove(msgId);  // re-emitted hence remove from failed
            }
        }
    }

    private void doSeekRetriableTopicPartitions() {
        final Set<TopicPartition> retriableTopicPartitions = retryService.retriableTopicPartitions();

        for (TopicPartition rtp : retriableTopicPartitions) {
            final OffsetAndMetadata offsetAndMeta = stored.get(rtp).findNextCommitOffset();
            if (offsetAndMeta != null) {
                kafkaConsumer.seek(rtp, offsetAndMeta.offset() + 1);  // seek to the next offset that is ready to store in next store cycle
            } else {
                Collection<TopicPartition> parts = new ArrayList<>(1);
                parts.add(rtp);
                kafkaConsumer.seekToEnd(parts);    // Seek to last committed offset
            }
        }
    }
    // ======== codes for polling end ========= //

    // =========== Consumer Rebalance Listener - On the same thread as the caller =========== //
    private class Kafka10ConsumerRebalancedListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            LOG.info("Partitions revoked. [consumer-group={}, consumer={}, topic-partitions={}]",
                    kafka10CollectorConfig.getConsumerGroupId(), kafkaConsumer, partitions);
            if (!consumerAutoCommitMode && initialized) {
                initialized = false;
                storeAndCommitOffset();
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            LOG.info("Partitions reassignment. [consumer-group={}, consumer={}, topic-partitions={}]",
                    kafka10CollectorConfig.getConsumerGroupId(), kafkaConsumer, partitions);

            initialize(partitions);
        }

        private void initialize(Collection<TopicPartition> partitions) {
            if (!consumerAutoCommitMode) {
                stored.keySet().retainAll(partitions);   // remove from stored all partitions that are no longer assigned to this collector
            }

            retryService.retainAll(partitions);

            for (TopicPartition tp : partitions) {
                final OffsetAndMetadata committedOffset = kafkaConsumer.committed(tp);
                final long fetchOffset = doSeek(tp, committedOffset);
                setStored(tp, fetchOffset);
            }
            initialized = true;
            LOG.info("Initialization complete");
        }

        /**
         * sets the cursor to the location dictated by the first poll strategy and returns the fetch offset
         */
        private long doSeek(TopicPartition tp, OffsetAndMetadata committedOffset) {
            long fetchOffset;
            if (committedOffset != null) {             // offset was committed for this TopicPartition
                if (firstPollOffsetStrategy.equals(EARLIEST)) {
                    Collection<TopicPartition> parts = new ArrayList<>(1);
                    parts.add(tp);
                    kafkaConsumer.seekToBeginning(parts);
                    fetchOffset = kafkaConsumer.position(tp);
                } else if (firstPollOffsetStrategy.equals(LATEST)) {
                    Collection<TopicPartition> parts = new ArrayList<>(1);
                    parts.add(tp);
                    kafkaConsumer.seekToEnd(parts);
                    fetchOffset = kafkaConsumer.position(tp);
                } else {
                    // By default polling starts at the last committed offset. +1 to point fetch to the first uncommitted offset.
                    fetchOffset = committedOffset.offset() + 1;
                    kafkaConsumer.seek(tp, fetchOffset);
                }
            } else {    // no commits have ever been done, so start at the beginning or end depending on the strategy
                if (firstPollOffsetStrategy.equals(EARLIEST) || firstPollOffsetStrategy.equals(UNCOMMITTED_EARLIEST)) {
                    Collection<TopicPartition> parts = new ArrayList<>(1);
                    parts.add(tp);
                    kafkaConsumer.seekToBeginning(parts);
                } else if (firstPollOffsetStrategy.equals(LATEST) || firstPollOffsetStrategy.equals(UNCOMMITTED_LATEST)) {
                    Collection<TopicPartition> parts = new ArrayList<>(1);
                    parts.add(tp);
                    kafkaConsumer.seekToEnd(parts);
                }
                fetchOffset = kafkaConsumer.position(tp);
            }
            return fetchOffset;
        }
    }
    // =========== Consumer Rebalance Listener - On the same thread as the caller =========== //

    // ======= Offsets Commit Management ========== //
    private static final Comparator<Kafka10CollectorMessageId> OFFSET_COMPARATOR = new OffsetComparator();

    private static class OffsetComparator implements Comparator<Kafka10CollectorMessageId> {
        @Override
        public int compare(Kafka10CollectorMessageId m1, Kafka10CollectorMessageId m2) {
            return m1.offset() < m2.offset() ? -1 : m1.offset() == m2.offset() ? 0 : 1;
        }
    }

    /**
     * This class is not thread safe
     */
    private class OffsetEntry {
        private final TopicPartition tp;
        private final long initialFetchOffset;  /* First offset to be fetched. It is either set to the beginning, end, or to the first uncommitted offset.
                                                 * Initial value depends on offset strategy. See Kafka10ConsumerRebalanceListener */
        private long committedOffset;     // last offset committed to Kafka. Initially it is set to fetchOffset - 1
        private final NavigableSet<Kafka10CollectorMessageId> ackedMsgs = new TreeSet<>(OFFSET_COMPARATOR);     // stored messages sorted by ascending order of offset

        OffsetEntry(TopicPartition tp, long initialFetchOffset) {
            this.tp = tp;
            this.initialFetchOffset = initialFetchOffset;
            this.committedOffset = initialFetchOffset - 1;
            LOG.debug("Instantiated {}", this);
        }

        public void add(Kafka10CollectorMessageId msgId) {          // O(Log N)
            this.ackedMsgs.add(msgId);
        }

        /**
         * @return the next OffsetAndMetadata to store, or null if no offset is ready to store.
         */
        OffsetAndMetadata findNextCommitOffset() {
            boolean found = false;
            long currOffset;
            long nextCommitOffset = committedOffset;
            Kafka10CollectorMessageId nextCommitMsg = null;     // this is a convenience variable to make it faster to create OffsetAndMetadata

            for (Kafka10CollectorMessageId currAckedMsg : ackedMsgs) {  // complexity is that of a linear scan on a TreeMap
                if ((currOffset = currAckedMsg.offset()) == initialFetchOffset || currOffset == nextCommitOffset + 1) {            // found the next offset to store
                    found = true;
                    nextCommitMsg = currAckedMsg;
                    nextCommitOffset = currOffset;
                } else if (currAckedMsg.offset() > nextCommitOffset + 1) {    // offset found is not continuous to the offsets listed to go in the next store, so stop search
                    LOG.debug("topic-partition [{}] has non-continuous offset [{}]. It will be processed in a subsequent batch.", tp, currOffset);
                    break;
                } else {
                    LOG.debug("topic-partition [{}] has unexpected offset [{}].", tp, currOffset);
                    break;
                }
            }

            OffsetAndMetadata nextCommitOffsetAndMetadata = null;
            if (found) {
                nextCommitOffsetAndMetadata = new OffsetAndMetadata(nextCommitOffset, nextCommitMsg.getMetadata(Thread.currentThread()));
                LOG.debug("topic-partition [{}] has offsets [{}-{}] ready to be committed", tp, committedOffset + 1, nextCommitOffsetAndMetadata.offset());
            } else {
                LOG.debug("topic-partition [{}] has NO offsets ready to be committed", tp);
            }
            LOG.trace("{}", this);
            return nextCommitOffsetAndMetadata;
        }

        /**
         * Marks an offset has committed. This method has side effects - it sets the internal state in such a way that future
         * calls to {@link #findNextCommitOffset()} will return offsets greater than the offset specified, if any.
         *
         * @param committedOffset offset to be marked as committed
         */
        void commit(OffsetAndMetadata committedOffset) {
            long numCommittedOffsets = 0;
            if (committedOffset != null) {
                final long oldCommittedOffset = this.committedOffset;
                numCommittedOffsets = committedOffset.offset() - this.committedOffset;
                this.committedOffset = committedOffset.offset();
                for (Iterator<Kafka10CollectorMessageId> iterator = ackedMsgs.iterator(); iterator.hasNext(); ) {
                    if (iterator.next().offset() <= committedOffset.offset()) {
                        iterator.remove();
                    } else {
                        break;
                    }
                }
                numUncommittedOffsets -= numCommittedOffsets;
                LOG.debug("Committed offsets [{}-{} = {}] for topic-partition [{}]. [{}] uncommitted offsets across all topic partitions",
                        oldCommittedOffset + 1, this.committedOffset, numCommittedOffsets, tp, numUncommittedOffsets);
            } else {
                LOG.debug("Committed [{}] offsets for topic-partition [{}]. [{}] uncommitted offsets across all topic partitions",
                        numCommittedOffsets, tp, numUncommittedOffsets);
            }
        }

        boolean contains(Kafka10CollectorMessageId msgId) {
            return ackedMsgs.contains(msgId);
        }

        @Override
        public String toString() {
            return "OffsetEntry{" +
                    "topic-partition=" + tp +
                    ", fetchOffset=" + initialFetchOffset +
                    ", committedOffset=" + committedOffset +
                    ", ackedMsgs=" + ackedMsgs +
                    '}';
        }
        // ======= Offsets Commit Management ========== //
    }
}
