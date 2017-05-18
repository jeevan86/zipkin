package zipkin.collector.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;

/**
 * Created by huangjian on 2017/5/10.
 */
final class Kafka10CollectorMessageId implements Serializable {

    public static final long serialVersionUID = -1L;

    private transient TopicPartition topicPart;
    private transient long offset;
    private transient int numFails = 0;

    public Kafka10CollectorMessageId(ConsumerRecord consumerRecord) {
        this(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), consumerRecord.offset());
    }

    public Kafka10CollectorMessageId(TopicPartition topicPart, long offset) {
        this.topicPart = topicPart;
        this.offset = offset;
    }

    public int partition() {
        return topicPart.partition();
    }

    public String topic() {
        return topicPart.topic();
    }

    public long offset() {
        return offset;
    }

    public int numFails() {
        return numFails;
    }

    public void incrementNumFails() {
        ++numFails;
    }

    public TopicPartition getTopicPartition() {
        return topicPart;
    }

    public String getMetadata(Thread currThread) {
        return "{" +
                "topic-partition=" + topicPart +
                ", offset=" + offset +
                ", numFails=" + numFails +
                ", thread='" + currThread.getName() + "'" +
                '}';
    }

    @Override
    public String toString() {
        return "{" +
                "topic-partition=" + topicPart +
                ", offset=" + offset +
                ", numFails=" + numFails +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Kafka10CollectorMessageId messageId = (Kafka10CollectorMessageId) o;
        if (offset != messageId.offset) {
            return false;
        }
        return topicPart.equals(messageId.topicPart);
    }

    @Override
    public int hashCode() {
        int result = topicPart.hashCode();
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        return result;
    }
}
