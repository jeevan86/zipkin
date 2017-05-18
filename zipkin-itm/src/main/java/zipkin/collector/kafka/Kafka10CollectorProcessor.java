package zipkin.collector.kafka;

import com.ffcs.itm.zipkin.busi.BusiTracesJoiner;
import com.ffcs.itm.zipkin.busi.storage.cache.ICacheUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin.Span;
import zipkin.collector.CollectorMetrics;
import zipkin.collector.CollectorSampler;
import zipkin.storage.Callback;
import zipkin.storage.StorageComponent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;
import static zipkin.internal.Util.checkNotNull;

/**
 * Created by huangjian on 2017/5/11.
 */
final class Kafka10CollectorProcessor {
    static Builder builder(Class<?> loggingClass) {
        return new Builder(LoggerFactory.getLogger(checkNotNull(loggingClass, "loggingClass").getName()));
    }

    static final class Builder {
        final Logger logger;
        StorageComponent storage = null;
        CollectorSampler sampler = CollectorSampler.ALWAYS_SAMPLE;
        CollectorMetrics metrics = CollectorMetrics.NOOP_METRICS;
        ICacheUtils cache;

        Builder(Logger logger) {
            this.logger = logger;
        }

        Builder storage(StorageComponent storage) {
            this.storage = checkNotNull(storage, "storage");
            return this;
        }

        Builder metrics(CollectorMetrics metrics) {
            this.metrics = checkNotNull(metrics, "metrics");
            return this;
        }

        Builder sampler(CollectorSampler sampler) {
            this.sampler = checkNotNull(sampler, "sampler");
            return this;
        }

        Builder cache(ICacheUtils cache) {
            this.cache = cache;
            return this;
        }

        Kafka10CollectorProcessor build() {
            return new Kafka10CollectorProcessor(this, new BusiTracesJoiner(cache));
        }
    }

    private final Logger logger;
    private final StorageComponent storage;
    private final CollectorSampler sampler;
    private final CollectorMetrics metrics;
    private final Kafka10CollectorBusiness business;

    Kafka10CollectorProcessor(Builder builder, Kafka10CollectorBusiness business) {
        this.logger = checkNotNull(builder.logger, "logger");
        this.storage = checkNotNull(builder.storage, "storage");
        this.sampler = builder.sampler == null ? CollectorSampler.ALWAYS_SAMPLE : builder.sampler;
        this.metrics = builder.metrics == null ? CollectorMetrics.NOOP_METRICS : builder.metrics;
        this.business = business;
    }

    void accept(List<Span> spans, Callback<Void> callback) {
        if (spans.isEmpty()) {
            callback.onSuccess(null);
            return;
        }
        metrics.incrementSpans(spans.size());

        List<Span> sampled = sample(spans);
        if (sampled.isEmpty()) {
            callback.onSuccess(null);
            return;
        }
        try {
            storage.asyncSpanConsumer().accept(sampled, storeSpansCallback(callback, sampled));
            callback.onSuccess(null);
        } catch (RuntimeException e) {
            callback.onError(errorStoringSpans(sampled, e));
        }

        try {
            business.process(sampled);
            callback.onSuccess(null);
        } catch (RuntimeException e) {
            callback.onError(errorStoringSpans(sampled, e));
        }
    }

    private List<Span> sample(List<Span> input) {
        List<Span> sampled = new ArrayList<>(input.size());
        for (Span s : input) {
            if (sampler.isSampled(s)) sampled.add(s);
        }
        int dropped = input.size() - sampled.size();
        if (dropped > 0) metrics.incrementSpansDropped(dropped);
        return sampled;
    }

    private Callback<Void> storeSpansCallback(
            final Callback<Void> acceptSpansCallback,
            final List<Span> spans) {
        return new Callback<Void>() {
            @Override
            public void onSuccess(Void value) {
                acceptSpansCallback.onSuccess(value);
            }

            @Override
            public void onError(Throwable t) {
                errorStoringSpans(spans, t);
            }

            @Override
            public String toString() {
                return appendSpanIds(spans, new StringBuilder("AcceptSpans(")).append(")").toString();
            }
        };
    }

    /**
     * When storing spans, an exception can be raised before or after the fact. This adds context of
     * span ids to give logs more relevance.
     */
    private RuntimeException errorStoringSpans(List<Span> spans, Throwable e) {
        metrics.incrementSpansDropped(spans.size());
        // The exception could be related to a span being huge. Instead of filling logs,
        // print trace id, span id pairs
        StringBuilder msg = appendSpanIds(spans, new StringBuilder("Cannot store spans "));
        return doError(msg.toString(), e);
    }

    private RuntimeException doError(String message, Throwable e) {
        if (e instanceof RuntimeException && e.getMessage() != null && e.getMessage()
                .startsWith("Malformed")) {
            logger.warn(e.getMessage(), e);
            return (RuntimeException) e;
        } else {
            message = format("%s due to %s(%s)", message, e.getClass().getSimpleName(),
                    e.getMessage() == null ? "" : e.getMessage());
            logger.warn(message, e);
            return new RuntimeException(message, e);
        }
    }

    private static StringBuilder appendSpanIds(List<Span> spans, StringBuilder message) {
        message.append("[");
        for (Iterator<Span> iterator = spans.iterator(); iterator.hasNext(); ) {
            message.append(iterator.next().idString());
            if (iterator.hasNext()) message.append(", ");
        }
        return message.append("]");
    }
}
