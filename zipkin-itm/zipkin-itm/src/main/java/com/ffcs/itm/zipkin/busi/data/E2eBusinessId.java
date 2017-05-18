/*
 * 用于分解消息key.
 */
package com.ffcs.itm.zipkin.busi.data;

import java.io.Serializable;

/**
 * Created by huangjian on 2017/3/24.
 */
public class E2eBusinessId implements Serializable, Comparable<E2eBusinessId> {

    public static final long serialVersionUID = -1L;

    String key;       // binaryAnnotation.key (busi.id.XXXXX);
    String value;     // String value .

    public E2eBusinessId() {
    }

    public E2eBusinessId(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public int compareTo(E2eBusinessId o) {
        if (null == o) return -1;
        return key.compareTo(o.key);
    }

    @Override
    public String toString() {
        return key + ':' + value;
    }
}
