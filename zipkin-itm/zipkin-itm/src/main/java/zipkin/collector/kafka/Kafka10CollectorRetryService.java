package zipkin.collector.kafka;

import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;

/**
 * Created by huangjian on 2017/5/10.
 */
public interface Kafka10CollectorRetryService extends Serializable {
    void schedule(Kafka10CollectorMessageId msgId);
    boolean remove(Kafka10CollectorMessageId msgId);
    boolean retainAll(Collection<TopicPartition> topicPartitions);
    Set<TopicPartition> retriableTopicPartitions();
    boolean isReady(Kafka10CollectorMessageId msgId);
    boolean isScheduled(Kafka10CollectorMessageId msgId);
}
