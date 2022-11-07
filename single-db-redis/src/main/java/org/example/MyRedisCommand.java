package org.example;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;

/**
 * @author guowb1
 * @description TODO
 * @date 2022/10/26 17:53
 */
public enum MyRedisCommand {

    LPUSH(RedisDataType.LIST),
    RPUSH(RedisDataType.LIST),
    SADD(RedisDataType.SET),
    SREM(RedisDataType.SET),
    SET(RedisDataType.STRING),
    SETEX(RedisDataType.STRING),
    PFADD(RedisDataType.HYPER_LOG_LOG),
    PUBLISH(RedisDataType.PUBSUB),
    ZADD(RedisDataType.SORTED_SET),
    ZINCRBY(RedisDataType.SORTED_SET),
    ZREM(RedisDataType.SORTED_SET),
    HSET(RedisDataType.HASH),
    HINCRBY(RedisDataType.HINCRBY),
    INCRBY(RedisDataType.STRING),
    INCRBY_EX(RedisDataType.STRING),
    DECRBY(RedisDataType.STRING),
    DESCRBY_EX(RedisDataType.STRING);
    ;

    private RedisDataType redisDataType;

    private MyRedisCommand(RedisDataType redisDataType) {
        this.redisDataType = redisDataType;
    }

    public RedisDataType getRedisDataType() {
        return this.redisDataType;
    }
}
