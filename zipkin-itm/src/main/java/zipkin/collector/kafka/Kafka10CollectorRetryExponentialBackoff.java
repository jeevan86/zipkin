package zipkin.collector.kafka;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by huangjian on 2017/5/10.
 */
final class Kafka10CollectorRetryExponentialBackoff implements Kafka10CollectorRetryService {
    private static final Logger LOG = LoggerFactory.getLogger(Kafka10CollectorRetryExponentialBackoff.class);
    private static final RetryEntryTimeStampComparator RETRY_ENTRY_TIME_STAMP_COMPARATOR = new RetryEntryTimeStampComparator();

    private TimeInterval initialDelay;
    private TimeInterval delayPeriod;
    private TimeInterval maxDelay;
    private int maxRetries;

    private Set<RetrySchedule> retrySchedules = new TreeSet<>(RETRY_ENTRY_TIME_STAMP_COMPARATOR);
    private Set<Kafka10CollectorMessageId> toRetryMsgs = new HashSet<>();      // Convenience data structure to speedup lookups

    /**
     * Comparator ordering by timestamp
     */
    private static class RetryEntryTimeStampComparator implements Serializable, Comparator<RetrySchedule> {
        @Override
        public int compare(RetrySchedule entry1, RetrySchedule entry2) {
            return Long.valueOf(entry1.nextRetryTimeNanos()).compareTo(entry2.nextRetryTimeNanos());
        }
    }

    private class RetrySchedule {
        private Kafka10CollectorMessageId msgId;
        private long nextRetryTimeNanos;

        public RetrySchedule(Kafka10CollectorMessageId msgId, long nextRetryTime) {
            this.msgId = msgId;
            this.nextRetryTimeNanos = nextRetryTime;
            LOG.debug("Created {}", this);
        }

        public void setNextRetryTime() {
            nextRetryTimeNanos = nextTime(msgId);
            LOG.debug("Updated {}", this);
        }

        boolean retry(long currentTimeNanos) {
            return nextRetryTimeNanos <= currentTimeNanos;
        }

        @Override
        public String toString() {
            return "RetrySchedule{" +
                    "msgId=" + msgId +
                    ", nextRetryTime=" + nextRetryTimeNanos +
                    '}';
        }

        Kafka10CollectorMessageId msgId() {
            return msgId;
        }

        long nextRetryTimeNanos() {
            return nextRetryTimeNanos;
        }
    }

    public static class TimeInterval implements Serializable {
        private long lengthNanos;
        private long length;
        private TimeUnit timeUnit;

        /**
         * @param length   length of the time interval in the units specified by {@link TimeUnit}
         * @param timeUnit unit used to specify a time interval on which to specify a time unit
         */
        TimeInterval(long length, TimeUnit timeUnit) {
            this.length = length;
            this.timeUnit = timeUnit;
            this.lengthNanos = timeUnit.toNanos(length);
        }

        static TimeInterval seconds(long length) {
            return new TimeInterval(length, TimeUnit.SECONDS);
        }

        static TimeInterval milliSeconds(long length) {
            return new TimeInterval(length, TimeUnit.MILLISECONDS);
        }

        public static TimeInterval microSeconds(long length) {
            return new TimeInterval(length, TimeUnit.MICROSECONDS);
        }

        public long lengthNanos() {
            return lengthNanos;
        }

        public long length() {
            return length;
        }

        public TimeUnit timeUnit() {
            return timeUnit;
        }

        @Override
        public String toString() {
            return "TimeInterval{" +
                    "length=" + length +
                    ", timeUnit=" + timeUnit +
                    '}';
        }
    }

    /**
     * The time stamp of the next retry is scheduled according to the exponential backoff formula ( geometric progression):
     * nextRetry = failCount == 1 ? currentTime + initialDelay : currentTime + delayPeriod^(failCount-1) where failCount = 1, 2, 3, ...
     * nextRetry = Min(nextRetry, currentTime + maxDelay)
     *
     * @param initialDelay initial delay of the first retry
     * @param delayPeriod  the time interval that is the ratio of the exponential backoff formula (geometric progression)
     * @param maxRetries   maximum number of times a tuple is retried before being acked and scheduled for commit
     * @param maxDelay     maximum amount of time waiting before retrying
     */
    Kafka10CollectorRetryExponentialBackoff(TimeInterval initialDelay, TimeInterval delayPeriod, int maxRetries, TimeInterval maxDelay) {
        this.initialDelay = initialDelay;
        this.delayPeriod = delayPeriod;
        this.maxRetries = maxRetries;
        this.maxDelay = maxDelay;
        LOG.debug("Instantiated {}", this);
    }

    @Override
    public Set<TopicPartition> retriableTopicPartitions() {
        final Set<TopicPartition> tps = new HashSet<>();
        final long currentTimeNanos = System.nanoTime();
        for (RetrySchedule retrySchedule : retrySchedules) {
            if (retrySchedule.retry(currentTimeNanos)) {
                final Kafka10CollectorMessageId msgId = retrySchedule.msgId;
                tps.add(new TopicPartition(msgId.topic(), msgId.partition()));
            } else {
                break;  // Stop searching as soon as passed current time
            }
        }
        LOG.debug("Topic partitions with entries ready to be retried [{}] ", tps);
        return tps;
    }

    @Override
    public boolean isReady(Kafka10CollectorMessageId msgId) {
        boolean retry = false;
        if (toRetryMsgs.contains(msgId)) {
            final long currentTimeNanos = System.nanoTime();
            for (RetrySchedule retrySchedule : retrySchedules) {
                if (retrySchedule.retry(currentTimeNanos)) {
                    if (retrySchedule.msgId.equals(msgId)) {
                        retry = true;
                        LOG.debug("Found entry to retry {}", retrySchedule);
                    }
                } else {
                    LOG.debug("Entry to retry not found {}", retrySchedule);
                    break;  // Stop searching as soon as passed current time
                }
            }
        }
        return retry;
    }

    @Override
    public boolean isScheduled(Kafka10CollectorMessageId msgId) {
        return toRetryMsgs.contains(msgId);
    }

    @Override
    public boolean remove(Kafka10CollectorMessageId msgId) {
        boolean removed = false;
        if (toRetryMsgs.contains(msgId)) {
            for (Iterator<RetrySchedule> iterator = retrySchedules.iterator(); iterator.hasNext(); ) {
                final RetrySchedule retrySchedule = iterator.next();
                if (retrySchedule.msgId().equals(msgId)) {
                    iterator.remove();
                    toRetryMsgs.remove(msgId);
                    removed = true;
                    break;
                }
            }
        }
        LOG.debug(removed ? "Removed {} " : "Not removed {}", msgId);
        LOG.trace("Current state {}", retrySchedules);
        return removed;
    }

    @Override
    public boolean retainAll(Collection<TopicPartition> topicPartitions) {
        boolean result = false;
        for (Iterator<RetrySchedule> rsIterator = retrySchedules.iterator(); rsIterator.hasNext(); ) {
            final RetrySchedule retrySchedule = rsIterator.next();
            final Kafka10CollectorMessageId msgId = retrySchedule.msgId;
            final TopicPartition tpRetry = new TopicPartition(msgId.topic(), msgId.partition());
            if (!topicPartitions.contains(tpRetry)) {
                rsIterator.remove();
                toRetryMsgs.remove(msgId);
                LOG.debug("Removed {}", retrySchedule);
                LOG.trace("Current state {}", retrySchedules);
                result = true;
            }
        }
        return result;
    }

    @Override
    public void schedule(Kafka10CollectorMessageId msgId) {
        if (msgId.numFails() > maxRetries) {
            LOG.debug("Not scheduling [{}] because reached maximum number of retries [{}].", msgId, maxRetries);
        } else {
            if (toRetryMsgs.contains(msgId)) {
                for (Iterator<RetrySchedule> iterator = retrySchedules.iterator(); iterator.hasNext(); ) {
                    final RetrySchedule retrySchedule = iterator.next();
                    if (retrySchedule.msgId().equals(msgId)) {
                        iterator.remove();
                        toRetryMsgs.remove(msgId);
                    }
                }
            }
            final RetrySchedule retrySchedule = new RetrySchedule(msgId, nextTime(msgId));
            retrySchedules.add(retrySchedule);
            toRetryMsgs.add(msgId);
            LOG.debug("Scheduled. {}", retrySchedule);
            LOG.trace("Current state {}", retrySchedules);
        }
    }

    // if value is greater than Long.MAX_VALUE it truncates to Long.MAX_VALUE
    private long nextTime(Kafka10CollectorMessageId msgId) {
        final long currentTimeNanos = System.nanoTime();
        final long nextTimeNanos = msgId.numFails() == 1                // numFails = 1, 2, 3, ...
                ? currentTimeNanos + initialDelay.lengthNanos
                : (currentTimeNanos + delayPeriod.timeUnit.toNanos((long) Math.pow(delayPeriod.length, msgId.numFails() - 1)));
        return Math.min(nextTimeNanos, currentTimeNanos + maxDelay.lengthNanos);
    }

    @Override
    public String toString() {
        return "KafkaSpoutRetryExponentialBackoff{" +
                "delay=" + initialDelay +
                ", ratio=" + delayPeriod +
                ", maxRetries=" + maxRetries +
                ", maxRetryDelay=" + maxDelay +
                '}';
    }
}

