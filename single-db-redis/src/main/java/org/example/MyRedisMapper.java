package org.example;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;

import java.io.Serializable;
import java.util.Optional;

/**
 * @author guowb1
 * @description TODO
 * @date 2022/10/26 18:03
 */
public interface MyRedisMapper<T> extends Function, Serializable {

    MyRedisCommandDescription getCommandDescription(T var1);

    String getKeyFromData(T var1);

    String getValueFromData(T var1);

    default Optional<String> getAdditionalKey(T data) {
        return Optional.empty();
    }

    default Optional<Integer> getAdditionalTTL(T data) {
        return Optional.empty();
    }
}
