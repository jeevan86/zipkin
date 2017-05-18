package zipkin.collector.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import zipkin.Span;

import java.io.Serializable;
import java.util.List;

/**
 * Created by huangjian on 2017/5/10.
 */
public interface Kafka10CollectorSpansBuilder extends Serializable {
    List<Span> buildSpans(ConsumerRecord<String, String> consumerRecord) throws Exception;
}
