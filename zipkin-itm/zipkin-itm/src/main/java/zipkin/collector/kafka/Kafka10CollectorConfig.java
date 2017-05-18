package zipkin.collector.kafka;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * KafkaSpoutConfig defines the required configuration to connect a consumer to a consumer group, as well as the subscribing topics
 */
public final class Kafka10CollectorConfig implements Serializable {

    public static final long DEFAULT_POLL_TIMEOUT_MS = 500;                // 500ms
    public static final long DEFAULT_OFFSET_COMMIT_PERIOD_MS = 30_000;     // 30s
    public static final int DEFAULT_MAX_RETRIES = 3;                       // Retry forever
    public static final int DEFAULT_MAX_UNCOMMITTED_OFFSETS = 10_000_000;  // 10,000,000 records => 80MBs of memory footprint in the worst case

    // Kafka property names
    public interface Consumer {
        String GROUP_ID = "group.id";
        String BOOTSTRAP_SERVERS = "bootstrap.servers";
        String ENABLE_AUTO_COMMIT = "enable.auto.commit";
        String AUTO_COMMIT_INTERVAL_MS = "auto.commit.interval.ms";
        String KEY_DESERIALIZER = "key.deserializer";
        String VALUE_DESERIALIZER = "value.deserializer";
    }

    /**
     * The offset used by the Kafka spout in the first poll to Kafka broker. The choice of this parameter will
     * affect the number of consumer records returned in the first poll. By default this parameter is set to UNCOMMITTED_EARLIEST. <br/><br/>
     * The allowed values are EARLIEST, LATEST, UNCOMMITTED_EARLIEST, UNCOMMITTED_LATEST. <br/>
     * <ul>
     * <li>EARLIEST means that the kafka spout polls records starting in the first offset of the partition, regardless of previous commits</li>
     * <li>LATEST means that the kafka spout polls records with offsets greater than the last offset in the partition, regardless of previous commits</li>
     * <li>UNCOMMITTED_EARLIEST means that the kafka spout polls records from the last committed offset, if any.
     * If no offset has been committed, it behaves as EARLIEST.</li>
     * <li>UNCOMMITTED_LATEST means that the kafka spout polls records from the last committed offset, if any.
     * If no offset has been committed, it behaves as LATEST.</li>
     * </ul>
     */
    public enum FirstPollOffsetStrategy {
        EARLIEST,
        LATEST,
        UNCOMMITTED_EARLIEST,
        UNCOMMITTED_LATEST
    }

    // Kafka consumer configuration
    private final Map<String, Object> kafkaProps;
    private final Deserializer<String> keyDeserializer;
    private final Deserializer<String> valueDeserializer;
    private final long pollTimeoutMs;

    // Kafka collector configuration
    private final long offsetCommitPeriodMs;
    private final int maxRetries;
    private final int maxUncommittedOffsets;
    private final FirstPollOffsetStrategy firstPollOffsetStrategy;
    private final String[] topics;
    private final Kafka10CollectorRetryService retryService;

    private Kafka10CollectorConfig(Builder builder) {
        this.kafkaProps = setDefaultsAndGetKafkaProps(builder.kafkaProps);
        this.keyDeserializer = builder.keyDeserializer;
        this.valueDeserializer = builder.valueDeserializer;
        this.pollTimeoutMs = builder.pollTimeoutMs;
        this.offsetCommitPeriodMs = builder.offsetCommitPeriodMs;
        this.maxRetries = builder.maxRetries;
        this.firstPollOffsetStrategy = builder.firstPollOffsetStrategy;
        this.topics = builder.topics;
        this.maxUncommittedOffsets = builder.maxUncommittedOffsets;
        this.retryService = builder.retryService;
    }

    private Map<String, Object> setDefaultsAndGetKafkaProps(Map<String, Object> kafkaProps) {
        // set defaults for properties not specified
        if (!kafkaProps.containsKey(Consumer.ENABLE_AUTO_COMMIT)) {
            kafkaProps.put(Consumer.ENABLE_AUTO_COMMIT, "false");
        }
        return kafkaProps;
    }

    public static class Builder {
        private final Map<String, Object> kafkaProps;
        private Deserializer<String> keyDeserializer;
        private Deserializer<String> valueDeserializer;
        private long pollTimeoutMs = DEFAULT_POLL_TIMEOUT_MS;
        private long offsetCommitPeriodMs = DEFAULT_OFFSET_COMMIT_PERIOD_MS;
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private FirstPollOffsetStrategy firstPollOffsetStrategy = FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST;
        private String[] topics;
        private int maxUncommittedOffsets = DEFAULT_MAX_UNCOMMITTED_OFFSETS;
        private final Kafka10CollectorRetryService retryService;

        public Builder(Map<String, Object> kafkaProps,
                       String[] topics,
                       int maxRetries,
                       FirstPollOffsetStrategy firstPollOffsetStrategy) {
            this(kafkaProps,
                    topics,
                    new Kafka10CollectorRetryExponentialBackoff(
                            Kafka10CollectorRetryExponentialBackoff.TimeInterval.seconds(0),
                            Kafka10CollectorRetryExponentialBackoff.TimeInterval.milliSeconds(2),
                            maxRetries,
                            Kafka10CollectorRetryExponentialBackoff.TimeInterval.seconds(10)
                    ),
                    firstPollOffsetStrategy
            );
        }

        /***
         * KafkaSpoutConfig defines the required configuration to connect a consumer to a consumer group, as well as the subscribing topics
         * The optional configuration can be specified using the set methods of this builder
         * @param kafkaProps    properties defining consumer connection to Kafka broker as specified in @see <a href="http://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html">KafkaConsumer</a>
         * @param retryService  logic that manages the retrial of failed tuples
         */
        public Builder(Map<String, Object> kafkaProps,
                       String[] topics,
                       Kafka10CollectorRetryService retryService,
                       FirstPollOffsetStrategy firstPollOffsetStrategy) {
            if (kafkaProps == null || kafkaProps.isEmpty()) {
                throw new IllegalArgumentException("Properties defining consumer connection to Kafka broker are required: " + kafkaProps);
            }

            if (topics == null || topics.length < 1) {
                throw new IllegalArgumentException("topics are required. ");
            }

            if (retryService == null) {
                throw new IllegalArgumentException("Must specify at implementation of retry service");
            }

            if (firstPollOffsetStrategy == null) {
                throw new IllegalArgumentException("firstPollOffsetStrategy is required. values [ EARLIEST | LATEST | UNCOMMITTED_EARLIEST | UNCOMMITTED_LATEST ]");
            }

            this.topics = topics;
            this.kafkaProps = kafkaProps;
            this.retryService = retryService;
            this.firstPollOffsetStrategy = firstPollOffsetStrategy;
        }

        /**
         * Specifying this key deserializer overrides the property key.deserializer
         */
        public Builder setKeyDeserializer(Deserializer<String> keyDeserializer) {
            this.keyDeserializer = keyDeserializer;
            return this;
        }

        /**
         * Specifying this value deserializer overrides the property value.deserializer
         */
        public Builder setValueDeserializer(Deserializer<String> valueDeserializer) {
            this.valueDeserializer = valueDeserializer;
            return this;
        }

        /**
         * Specifies the time, in milliseconds, spent waiting in poll if data is not available. Default is 2s
         *
         * @param pollTimeoutMs time in ms
         */
        public Builder setPollTimeoutMs(long pollTimeoutMs) {
            this.pollTimeoutMs = pollTimeoutMs;
            return this;
        }

        /**
         * Specifies the period, in milliseconds, the offset commit task is periodically called. Default is 15s.
         *
         * @param offsetCommitPeriodMs time in ms
         */
        public Builder setOffsetCommitPeriodMs(long offsetCommitPeriodMs) {
            this.offsetCommitPeriodMs = offsetCommitPeriodMs;
            return this;
        }

        /**
         * Defines the max number of retrials in case of tuple failure. The default is to retry forever, which means that
         * no new records are committed until the previous polled records have been acked. This guarantees at once delivery of
         * all the previously polled records.
         * By specifying a finite value for maxRetries, the user decides to sacrifice guarantee of delivery for the previous
         * polled records in favor of processing more records.
         *
         * @param maxRetries max number of retrials
         */
        public Builder setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         * Defines the max number of polled offsets (records) that can be pending commit, before another poll can take place.
         * Once this limit is reached, no more offsets (records) can be polled until the next successful commit(s) sets the number
         * of pending offsets bellow the threshold. The default is {@link #DEFAULT_MAX_UNCOMMITTED_OFFSETS}.
         *
         * @param maxUncommittedOffsets max number of records that can be be pending commit
         */
        public Builder setMaxUncommittedOffsets(int maxUncommittedOffsets) {
            this.maxUncommittedOffsets = maxUncommittedOffsets;
            return this;
        }

        /**
         * Sets the offset used by the Kafka spout in the first poll to Kafka broker upon process start.
         * Please refer to to the documentation in {@link FirstPollOffsetStrategy}
         *
         * @param firstPollOffsetStrategy Offset used by Kafka spout first poll
         */
        public Builder setFirstPollOffsetStrategy(FirstPollOffsetStrategy firstPollOffsetStrategy) {
            this.firstPollOffsetStrategy = firstPollOffsetStrategy;
            return this;
        }

        public Kafka10CollectorConfig build() {
            return new Kafka10CollectorConfig(this);
        }
    }

    public Map<String, Object> getKafkaProps() {
        return kafkaProps;
    }

    public Deserializer<String> getKeyDeserializer() {
        return keyDeserializer;
    }

    public Deserializer<String> getValueDeserializer() {
        return valueDeserializer;
    }

    public long getPollTimeoutMs() {
        return pollTimeoutMs;
    }

    public long getOffsetsCommitPeriodMs() {
        return offsetCommitPeriodMs;
    }

    public boolean isConsumerAutoCommitMode() {
        return kafkaProps.get(Consumer.ENABLE_AUTO_COMMIT) == null     // default is true
                || Boolean.valueOf((String) kafkaProps.get(Consumer.ENABLE_AUTO_COMMIT));
    }

    public String getConsumerGroupId() {
        return (String) kafkaProps.get(Consumer.GROUP_ID);
    }

    public List<String> getSubscribedTopics() {
        List<String> topics = new ArrayList<>(this.topics.length);
        for (String topic : this.topics) {
            topics.add(topic);
        }
        return topics;
    }

    public int getMaxTupleRetries() {
        return maxRetries;
    }

    public FirstPollOffsetStrategy getFirstPollOffsetStrategy() {
        return firstPollOffsetStrategy;
    }

    public int getMaxUncommittedOffsets() {
        return maxUncommittedOffsets;
    }

    public Kafka10CollectorRetryService getRetryService() {
        return retryService;
    }

    @Override
    public String toString() {
        return "Kafka10CollectorConfig{" +
                "kafkaProps=" + kafkaProps +
                ", keyDeserializer=" + keyDeserializer +
                ", valueDeserializer=" + valueDeserializer +
                ", pollTimeoutMs=" + pollTimeoutMs +
                ", offsetCommitPeriodMs=" + offsetCommitPeriodMs +
                ", maxRetries=" + maxRetries +
                ", maxUncommittedOffsets=" + maxUncommittedOffsets +
                ", firstPollOffsetStrategy=" + firstPollOffsetStrategy +
                ", retryService=" + retryService +
                ", topics=" + getSubscribedTopics() +
                '}';
    }
}
