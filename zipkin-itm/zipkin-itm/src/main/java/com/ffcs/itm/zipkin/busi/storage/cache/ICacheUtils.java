package com.ffcs.itm.zipkin.busi.storage.cache;

import java.util.List;
import java.util.Set;

/**
 * Created by huangjian on 2017/3/25.
 */
public interface ICacheUtils {

    String set(String key, String value);

    String set(String key, String value, int expired);

    String get(String key);

    Long sadd(String key, String... values);

    Set<String> smembers(String key);

    Long scard(String key);

    /* �ڼ�Ⱥģʽ�¿��ܵ����쳣 */
    List<String> mget(String... keys);

    /* �ڼ�Ⱥģʽ�¿��ܵ����쳣 */
    String mset(String... keyValues);

    Long incrBy(String key, long val);

    Long incr(String key);

    Long del(String key);

    Long mdel(String... keys);
}
