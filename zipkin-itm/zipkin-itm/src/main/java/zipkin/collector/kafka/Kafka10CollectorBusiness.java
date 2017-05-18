package zipkin.collector.kafka;

import zipkin.Span;

import java.util.List;

/**
 * Created by huangjian on 2017/5/17.
 */
public interface Kafka10CollectorBusiness {
    void process(List<Span> spans);
}
