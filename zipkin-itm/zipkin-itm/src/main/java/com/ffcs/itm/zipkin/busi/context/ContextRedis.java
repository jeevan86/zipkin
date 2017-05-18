package com.ffcs.itm.zipkin.busi.context;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by huangjian on 2017/3/23.
 */
public final class ContextRedis {

    private String auth = "redis"; // Redis用户密码

    private String host = "192.168.17.239"; // Redis单库配置
    public int port = 6379; // Redis单库配置

    private Mode mode = Mode.CLUSTER; // Redis模式配置
    private final Set<HostAndPort> cluster = new HashSet<>(); // Redis集群配置

    private GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();

    public String getAuth() {
        return auth;
    }

    public void setAuth(String auth) {
        this.auth = auth;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public Set<HostAndPort> getCluster() {
        return cluster;
    }

    public GenericObjectPoolConfig getPoolConfig() {
        return poolConfig;
    }

    public void setPoolConfig(GenericObjectPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
    }

    public enum Mode {
        CLUSTER, SINGLE_NODE;
    }
}
