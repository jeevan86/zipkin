package com.ffcs.itm.zipkin.busi.storage.cache.redis;

import com.ffcs.itm.zipkin.busi.context.ContextRedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by huangjian on 2017/3/20.
 */
public class RedisUtils {

    /* 获取单例对象 */
    public static IRedisUtils getInstance(ContextRedis context) {
        if (null == redis) {
            synchronized (lock) {
                if (null == redis) {
                    if (context.getMode() == ContextRedis.Mode.CLUSTER) {
                        //Jedis Cluster will attempt to discover cluster nodes automatically
                        redis = new RedisClusterUtils(
                                new JedisCluster(
                                        context.getCluster(),
                                        context.getPoolConfig()
                                )
                        );
                    } else {
                        redis = new RedisStandaloneUtils(
                                new JedisPool(
                                        context.getPoolConfig(),
                                        context.getHost(),
                                        context.getPort()
                                        /*, Protocol.DEFAULT_TIMEOUT */
                                        /*, contextRedis.REDIS_AUTH */
                                )
                        );
                    }
                }
            }
        }
        return redis;
    }

    /* 使用单例 */
    private static IRedisUtils redis;
    private static final Object lock = new Object();

    private RedisUtils() {
    }

    /* redis 单节点操作工具 */
    static class RedisStandaloneUtils implements IRedisUtils {

        private JedisPool jedisPool;

        private RedisStandaloneUtils(JedisPool jedisPool) {
            this.jedisPool = jedisPool;
        }

        @Override
        public String set(String key, String value) {
            Jedis jedis = jedisPool.getResource();
            String result = jedis.set(key, value);
            jedis.close();
            return result;
        }

        @Override
        public String set(String key, String value, int expired) {
            Jedis jedis = jedisPool.getResource();
            String result = jedis.set(key, value);
            jedis.expire(key, expired);
            jedis.close();
            return result;
        }

        @Override
        public String get(String key) {
            Jedis jedis = jedisPool.getResource();
            String result = jedis.get(key);
            jedis.close();
            return result;
        }

        @Override
        public Long sadd(String key, String... values) {
            Jedis jedis = jedisPool.getResource();
            Long result = jedis.sadd(key, values);
            jedis.close();
            return result;
        }

        @Override
        public Set<String> smembers(String key) {
            Jedis jedis = jedisPool.getResource();
            Set<String> result = jedis.smembers(key);
            jedis.close();
            return result;
        }

        @Override
        public Long scard(String key) {
            Jedis jedis = jedisPool.getResource();
            Long result = jedis.scard(key);
            jedis.close();
            return result;
        }

        @Override
        public List<String> mget(String... keys) {
            Jedis jedis = jedisPool.getResource();
            List<String> result = jedis.mget(keys);
            jedis.close();
            return result;
        }

        @Override
        public String mset(String... keyValues) {
            Jedis jedis = jedisPool.getResource();
            String result = jedis.mset(keyValues);
            jedis.close();
            return result;
        }

        @Override
        public Long incrBy(String key, long val) {
            Jedis jedis = jedisPool.getResource();
            Long result = jedis.incrBy(key, val);
            jedis.close();
            return result;
        }

        @Override
        public Long incr(String key) {
            Jedis jedis = jedisPool.getResource();
            Long result = jedis.incr(key);
            jedis.close();
            return result;
        }

        @Override
        public Long del(String key) {
            Jedis jedis = jedisPool.getResource();
            Long result = jedis.del(key);
            jedis.close();
            return result;
        }

        @Override
        public Long mdel(String... keys) {
            Jedis jedis = jedisPool.getResource();
            Long result = jedis.del(keys);
            jedis.close();
            return result;
        }
    }

    /* redis 集群操作工具 */
    static class RedisClusterUtils implements IRedisUtils {

        private JedisCluster jedisCluster;

        private RedisClusterUtils(JedisCluster jedisCluster) {
            this.jedisCluster = jedisCluster;
        }

        @Override
        public String set(String key, String value) {
            return jedisCluster.set(key, value);
        }

        @Override
        public String set(String key, String value, int expired) {
            String result = jedisCluster.set(key, value);
            jedisCluster.expire(key, expired);
            return result;
        }

        @Override
        public String get(String key) {
            return jedisCluster.get(key);
        }

        @Override
        public Long sadd(String key, String... values) {
            return jedisCluster.sadd(key, values);
        }

        @Override
        public Set<String> smembers(String key) {
            return jedisCluster.smembers(key);
        }

        @Override
        public Long scard(String key) {
            return jedisCluster.scard(key);
        }

        @Override
        public List<String> mget(String... keys) {
            /* 在集群模式下可能导致异常，转化为get */
            List<String> result = new ArrayList<>(keys.length);
            for (String key : keys) {
                result.add(jedisCluster.get(key));
            }
            return result;
        }

        @Override
        public String mset(String... keyValues) {
            /* 在集群模式下可能导致异常，转化为set*/
            for (int i = 0; i < keyValues.length - 1; i += 2) {
                String key = keyValues[i];
                String val = keyValues[i + 1];
                jedisCluster.set(key, val);
            }
            return "";
        }

        @Override
        public Long incrBy(String key, long val) {
            return jedisCluster.incrBy(key, val);
        }

        @Override
        public Long incr(String key) {
            return jedisCluster.incr(key);
        }

        @Override
        public Long del(String key) {
            return jedisCluster.del(key);
        }

        @Override
        public Long mdel(String... keys) {
            long result = 0l;
            for (String key : keys) {
                result += jedisCluster.del(key);
            }
            return result;
        }
    }
}
