package org.example;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author guowb1
 * @description TODO
 * @date 2022/10/26 18:04
 */
public class MyRedisCommandDescription implements Serializable {
    private static final long serialVersionUID = 1L;
    private MyRedisCommand redisCommand;
    private String additionalKey;
    private Integer additionalTTL;

    public MyRedisCommandDescription(MyRedisCommand redisCommand, String additionalKey, Integer additionalTTL) {
        Objects.requireNonNull(redisCommand, "Redis command type can not be null");
        this.redisCommand = redisCommand;
        this.additionalKey = additionalKey;
        this.additionalTTL = additionalTTL;
        if ((redisCommand.getRedisDataType() == RedisDataType.HASH || redisCommand.getRedisDataType() == RedisDataType.SORTED_SET) && additionalKey == null) {
            throw new IllegalArgumentException("Hash and Sorted Set should have additional key");
        } else if (redisCommand.equals(MyRedisCommand.SETEX) && additionalTTL == null) {
            throw new IllegalArgumentException("SETEX command should have time to live (TTL)");
        } else if (redisCommand.equals(MyRedisCommand.INCRBY_EX) && additionalTTL == null) {
            throw new IllegalArgumentException("INCRBY_EX command should have time to live (TTL)");
        } else if (redisCommand.equals(MyRedisCommand.DESCRBY_EX) && additionalTTL == null) {
            throw new IllegalArgumentException("INCRBY_EX command should have time to live (TTL)");
        }
    }

    public MyRedisCommandDescription(MyRedisCommand redisCommand, String additionalKey) {
        this(redisCommand, additionalKey, (Integer)null);
    }

    public MyRedisCommandDescription(MyRedisCommand redisCommand, Integer additionalTTL) {
        this(redisCommand, (String)null, additionalTTL);
    }

    public MyRedisCommandDescription(MyRedisCommand redisCommand) {
        this(redisCommand, (String)null, (Integer)null);
    }

    public MyRedisCommand getCommand() {
        return this.redisCommand;
    }

    public String getAdditionalKey() {
        return this.additionalKey;
    }

    public Integer getAdditionalTTL() {
        return this.additionalTTL;
    }
}
