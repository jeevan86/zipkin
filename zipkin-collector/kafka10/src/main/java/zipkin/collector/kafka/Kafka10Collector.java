package zipkin.collector.kafka;

import zipkin.collector.CollectorComponent;
import zipkin.collector.CollectorMetrics;
import zipkin.collector.CollectorSampler;
import zipkin.storage.StorageComponent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static zipkin.collector.kafka.Kafka10CollectorConfig.FirstPollOffsetStrategy;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static zipkin.internal.Util.checkNotNull;

/**
 * Created by huangjian on 2017/5/10.
 */
public final class Kafka10Collector implements CollectorComponent {

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Configuration including defaults needed to consume spans from a Kafka topic.
     */
    public static final class Builder implements CollectorComponent.Builder {
        final Map<String, Object> properties = new HashMap<>();
        Kafka10CollectorSink.Builder delegate = Kafka10CollectorSink.builder(Kafka10Collector.class);
        CollectorMetrics metrics = CollectorMetrics.NOOP_METRICS;
        String[] topics = {"zipkin"};
        FirstPollOffsetStrategy firstPollOffsetStrategy;

        @Override
        public Builder storage(StorageComponent storage) {
            delegate.storage(storage);
            return this;
        }

        @Override
        public Builder sampler(CollectorSampler sampler) {
            delegate.sampler(sampler);
            return this;
        }

        @Override
        public Builder metrics(CollectorMetrics metrics) {
            this.metrics = checkNotNull(metrics, "metrics").forTransport("kafka");
            delegate.metrics(this.metrics);
            return this;
        }

        /**
         * Topic zipkin spans will be consumed from. Defaults to "zipkin"
         */
        public Builder topics(String topics) {
            if (null == topics || topics.length() < 1) {
                checkNotNull(null, "topic");
            }
            this.topics = topics.split(",");
            return this;
        }

        /**
         * The zookeeper connect string, ex. 127.0.0.1:2181/kafka. No default
         */
//        public Builder zookeeper(String zookeeper) {
//            properties.put("zookeeper.connect", checkNotNull(zookeeper, "zookeeper.connect"));
//            return this;
//        }

        /**
         * The bootstrapServers string, ex. zipkin01:9602,zipkin02:9602,zipkin03:9602
         **/
        public Builder bootstrapServers(String bootstrapServers) {
            properties.put("bootstrap.servers", checkNotNull(bootstrapServers, "bootstrap.servers"));
            return this;
        }

        /**
         * The consumer group this process is consuming on behalf of. Defaults to "zipkin"
         */
        public Builder groupId(String groupId) {
            properties.put(GROUP_ID_CONFIG, checkNotNull(groupId, "groupId"));
            return this;
        }

        /**
         * Count of threads/streams consuming the topic. Defaults to 1
         */
//        int streams = 1;
//        public Builder streams(int streams) {
//            this.streams = streams;
//            return this;
//        }

        /**
         * Maximum size of a message containing spans in bytes. Defaults to 1 MiB
         */
//        public Builder maxMessageSize(int bytes) {
//            properties.put("fetch.message.max.bytes", String.valueOf(bytes));
//            return this;
//        }

        /**
         * By default, a consumer will be built from properties derived from builder defaults,
         * as well "auto.offset.reset" -> "earliest". Any properties set here will override the
         * consumer config.
         * <p>
         * <p>For example: Only consume spans since you connected by setting the below.
         * <pre>{@code
         * Map<String, String> overrides = new LinkedHashMap<>();
         * overrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
         * builder.overrides(overrides);
         * }</pre>
         *
         * @see org.apache.kafka.clients.consumer.ConsumerConfig
         */
        public final Builder overrides(Map<String, String> overrides) {
            properties.putAll(checkNotNull(overrides, "overrides"));
            return this;
        }

        public final Builder firstPollOffsetStrategy(FirstPollOffsetStrategy firstPollOffsetStrategy) {
            this.firstPollOffsetStrategy = firstPollOffsetStrategy;
            return this;
        }

        @Override
        public Kafka10Collector build() {
            return new Kafka10Collector(this);
        }

        Builder() {
            // Settings below correspond to "Old Consumer Configs"
            // http://kafka.apache.org/documentation.html
            properties.put(GROUP_ID_CONFIG, "zipkin");
            properties.put("fetch.message.max.bytes", String.valueOf(1024 * 1024));
            // Same default as zipkin-scala, and keeps tests from hanging
            properties.put(AUTO_OFFSET_RESET_CONFIG, "smallest");
        }
    }

    private Kafka10CollectorConsumerStream streams;

    private Kafka10Collector(Builder builder) {
        Kafka10CollectorConfig.Builder configBuilder = new Kafka10CollectorConfig.Builder(
                builder.properties, builder.topics, 3, builder.firstPollOffsetStrategy);
        this.streams = new Kafka10CollectorConsumerStream(builder, configBuilder.build());
    }

    @Override
    public CollectorComponent start() {
        this.streams.get();
        return this;
    }

    @Override
    public CheckResult check() {
        return null;
    }

    @Override
    public void close() throws IOException {
        this.streams.close();
    }
}
