/**
 * Copyright 2015-2016 The OpenZipkin Authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.autoconfigure.collector.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin.collector.kafka.Kafka10Collector;
import zipkin.collector.kafka.Kafka10CollectorConfig;

import java.util.LinkedHashMap;
import java.util.Map;

import static zipkin.collector.kafka.Kafka10CollectorConfig.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST;

@ConfigurationProperties("zipkin.collector.kafka10")
public class Kafka10CollectorProperties {
    private String topics;
    private String bootstrapServers;
    private String groupId = "zipkin";
    private Kafka10CollectorConfig.FirstPollOffsetStrategy firstPollOffsetStrategy = UNCOMMITTED_EARLIEST;
    private int maxUncommittedOffsets = 1000;
    private Map<String, String> overrides = new LinkedHashMap<>();

    public String getTopics() {
        return topics;
    }

    public void setTopics(String topics) {
        this.topics = topics;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public Kafka10CollectorConfig.FirstPollOffsetStrategy getFirstPollOffsetStrategy() {
        return firstPollOffsetStrategy;
    }

    public void setFirstPollOffsetStrategy(Kafka10CollectorConfig.FirstPollOffsetStrategy firstPollOffsetStrategy) {
        this.firstPollOffsetStrategy = firstPollOffsetStrategy;
    }

    public int getMaxUncommittedOffsets() {
        return maxUncommittedOffsets;
    }

    public void setMaxUncommittedOffsets(int maxUncommittedOffsets) {
        this.maxUncommittedOffsets = maxUncommittedOffsets;
    }

    public Map<String, String> getOverrides() {
        return overrides;
    }

    public void setOverrides(Map<String, String> overrides) {
        this.overrides = overrides;
    }

    public Kafka10Collector.Builder toBuilder() {
        return Kafka10Collector.builder()
                .topics(topics)
                .bootstrapServers(bootstrapServers)
                .groupId(groupId)
                .firstPollOffsetStrategy(firstPollOffsetStrategy)
                .overrides(overrides);
    }
}
