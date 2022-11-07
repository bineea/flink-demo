package org.example;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author guowb1
 * @description TODO
 * @date 2022/10/26 14:13
 */
public class HandleSetRedis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(5, TimeUnit.SECONDS)));

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(environment);

        tEnv.executeSql("CREATE TABLE sample_original (\n" +
                "id BIGINT,\n" +
                "remark VARCHAR(100),\n" +
                "create_time TIMESTAMP(3),\n" +
                "PRIMARY KEY (id) NOT ENFORCED\n" +
                //使用window需要声明 create_time 是事件时间属性，并且用 延迟 1 秒的策略来生成 watermark
                //"WATERMARK FOR create_time AS create_time - INTERVAL '1' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'hostname' = '152.136.155.204',\n" +
                "    'port' = '3316',\n" +
                "    'username' = 'username',\n" +
                "    'password' = 'password',\n" +
                "    'database-name' = 'sampledb',\n" +
                "    'table-name' = 'sample_original'\n" +
                ")");

        Table original = tEnv.from("sample_original");

        Table handleTable = original
                .select($("id").mod(3).as("num"), $("id"), $("remark"), $("create_time"));

        DataStream<Row> dataStream = tEnv.toChangelogStream(handleTable);
        dataStream.print();
        System.out.println("---------------------------------------------------");

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("152.136.155.204")
                .setPort(63791)
                .setPassword("password")
                .build();

        dataStream.addSink(new MyRedisSink<Row>(conf, new RedisDistinctMapper()));
        environment.execute();
    }


    public static class MyRedisSink<IN> extends RichSinkFunction<IN> {
        private static final Logger LOG = LoggerFactory.getLogger(MyRedisSink.class);
        private MyRedisMapper<IN> redisSinkMapper;
        private FlinkJedisConfigBase flinkJedisConfigBase;
        private JedisPool jedisPool;

        public MyRedisSink(FlinkJedisConfigBase flinkJedisConfigBase, MyRedisMapper<IN> redisSinkMapper) {
            Objects.requireNonNull(flinkJedisConfigBase, "Redis connection pool config should not be null");
            Objects.requireNonNull(redisSinkMapper, "Redis Mapper can not be null");
            this.flinkJedisConfigBase = flinkJedisConfigBase;
            this.redisSinkMapper = redisSinkMapper;
        }

        @Override
        public void invoke(IN input, SinkFunction.Context context) throws Exception {
            String key = this.redisSinkMapper.getKeyFromData(input);
            String value = this.redisSinkMapper.getValueFromData(input);
            MyRedisCommandDescription commandDescription = this.redisSinkMapper.getCommandDescription(input);
            MyRedisCommand redisCommand = commandDescription.getCommand();
            if (redisCommand == MyRedisCommand.SADD) {
                Jedis jedis = null;

                try {
                    jedis = this.jedisPool.getResource();
                    jedis.sadd(key, value);
                } catch (Exception var10) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Cannot send Redis message with command SADD to key {} error message {}", new Object[]{key, var10.getMessage()});
                    }
                    throw var10;
                } finally {
                    if (jedis != null) {
                        try {
                            jedis.close();
                        } catch (Exception var3) {
                            LOG.error("Failed to close (return) instance to pool", var3);
                        }
                    }
                }
            }
            if (redisCommand == MyRedisCommand.SREM) {
                Jedis jedis = null;

                try {
                    jedis = this.jedisPool.getResource();
                    jedis.srem(key, value);
                } catch (Exception var10) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Cannot send Redis message with command SREM to key {} error message {}", new Object[]{key, var10.getMessage()});
                    }
                    throw var10;
                } finally {
                    if (jedis != null) {
                        try {
                            jedis.close();
                        } catch (Exception var3) {
                            LOG.error("Failed to close (return) instance to pool", var3);
                        }
                    }
                }
            }
        }

        public void open(Configuration parameters) throws Exception {
            try {
                FlinkJedisPoolConfig flinkJedisPoolConfig = (FlinkJedisPoolConfig)flinkJedisConfigBase;
                Objects.requireNonNull(flinkJedisPoolConfig, "Redis pool config should not be Null");
                GenericObjectPoolConfig genericObjectPoolConfig = getGenericObjectPoolConfig(flinkJedisPoolConfig);
                this.jedisPool = new JedisPool(genericObjectPoolConfig,
                        flinkJedisPoolConfig.getHost(),
                        flinkJedisPoolConfig.getPort(),
                        flinkJedisPoolConfig.getConnectionTimeout(),
                        flinkJedisPoolConfig.getPassword(),
                        flinkJedisPoolConfig.getDatabase());
                this.jedisPool.getResource();
            } catch (Exception var3) {
                LOG.error("Redis has not been properly initialized: ", var3);
                throw var3;
            }
        }

        public void close() throws IOException {
            if (this.jedisPool != null) {
                this.jedisPool.close();
            }

        }

        public GenericObjectPoolConfig getGenericObjectPoolConfig(FlinkJedisConfigBase jedisConfig) {
            GenericObjectPoolConfig genericObjectPoolConfig = jedisConfig.getTestWhileIdle() ? new JedisPoolConfig() : new GenericObjectPoolConfig();
            ((GenericObjectPoolConfig)genericObjectPoolConfig).setMaxIdle(jedisConfig.getMaxIdle());
            ((GenericObjectPoolConfig)genericObjectPoolConfig).setMaxTotal(jedisConfig.getMaxTotal());
            ((GenericObjectPoolConfig)genericObjectPoolConfig).setMinIdle(jedisConfig.getMinIdle());
            ((GenericObjectPoolConfig)genericObjectPoolConfig).setTestOnBorrow(jedisConfig.getTestOnBorrow());
            ((GenericObjectPoolConfig)genericObjectPoolConfig).setTestOnReturn(jedisConfig.getTestOnReturn());
            return (GenericObjectPoolConfig)genericObjectPoolConfig;
        }
    }

    public static class RedisDistinctMapper implements MyRedisMapper<Row> {
        private static final Logger LOG = LoggerFactory.getLogger(RedisDistinctMapper.class);
        @Override
        public MyRedisCommandDescription getCommandDescription(Row data) {
            if (data.getKind() == RowKind.DELETE) {
                return new MyRedisCommandDescription(MyRedisCommand.SREM);
            } else {
                return new MyRedisCommandDescription(MyRedisCommand.SADD);
            }
        }

        @Override
        public String getKeyFromData(Row data) {
            return "TEST_FLINK_CDC_ID_SET_NAME_" + data.getField("num").toString();
        }

        @Override
        public String getValueFromData(Row data) {
            Long id = (Long) data.getField("id");
            System.out.println("id instanceof Long: " + (data.getField("id") instanceof Long) + ", id: " + id);
            LocalDateTime createTime = (LocalDateTime) data.getField("create_time");
            System.out.println("createTime instanceof LocalDateTime: " + (data.getField("create_time") instanceof LocalDateTime) + ", createTime: " + createTime);
            return data.getField("id").toString();
        }
    }
}
