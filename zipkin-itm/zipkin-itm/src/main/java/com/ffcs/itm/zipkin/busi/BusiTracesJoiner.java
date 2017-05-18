package com.ffcs.itm.zipkin.busi;

import com.ffcs.itm.zipkin.busi.storage.cache.ICacheUtils;
import com.ffcs.itm.zipkin.busi.data.E2eBusinessId;
import zipkin.BinaryAnnotation;
import zipkin.Span;
import zipkin.collector.kafka.Kafka10CollectorBusiness;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Created by huangjian on 2017/5/17.
 */
public class BusiTracesJoiner implements Kafka10CollectorBusiness {

    private transient ICacheUtils cache;

    public BusiTracesJoiner(ICacheUtils cache) {
        this.cache = cache;
    }

    @Override
    public void process(List<Span> spans) {
        Map<String, Set<String>> traceKeyBusiKeyMap = new HashMap<>();
        Map<String, Set<String>> busiKeyTraceKeyMap = new HashMap<>();
        for (Span span : spans) {
            Set<E2eBusinessId> busiJoinKey = new TreeSet<>();
            for (BinaryAnnotation banno :
                    span.binaryAnnotations) {
                if (banno.key.startsWith("busi.id")) {
                    busiJoinKey.add(
                            new E2eBusinessId(
                                    banno.key,
                                    new String(banno.value, StandardCharsets.UTF_8)
                            )
                    );
                }
            }
            StringBuilder sb = new StringBuilder();
            for (E2eBusinessId id : busiJoinKey) {
                sb.append(id).append(',');
            }
            String busiKey = sb.toString();
            String traceKey = span.traceIdHigh + "." + span.traceId;

            //添加到缓存-用于双向查找.
            cache.sadd(busiKey, traceKey);
            cache.sadd(traceKey, busiKey);
            //同时持久化一份到cassandra
            cassandra.sadd(busiKey, traceKey);
            cassandra.sadd(traceKey, busiKey);

            busiKeyTraceKeyMap.computeIfAbsent(busiKey, k -> new HashSet<>());
            busiKeyTraceKeyMap.get(busiKey).add(traceKey);
            traceKeyBusiKeyMap.computeIfAbsent(traceKey, k -> new HashSet<>());
            traceKeyBusiKeyMap.get(traceKey).add(busiKey);
        }
    }
}
