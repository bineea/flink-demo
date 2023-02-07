package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.concurrent.TimeUnit;


/**
 * @author guowb1
 * @description TODO
 * @date 2022/10/14 11:37
 */
public class HandleSampleRedis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置checkpoint
        //environment.getCheckpointConfig().disableCheckpointing();
        environment.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        environment.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        environment.getCheckpointConfig().setCheckpointTimeout(180000L);
        environment.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(5, TimeUnit.SECONDS)));

        //设置时间类型为eventtime，并设置水印的生成时间为100ms
        environment.getConfig().setAutoWatermarkInterval(100L);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(environment);

        tEnv.executeSql("CREATE TABLE sample_original (\n" +
                "id BIGINT,\n" +
                "remark VARCHAR(100),\n" +
                "create_time TIMESTAMP(6),\n" +
                "PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'hostname' = '152.136.155.204',\n" +
                "    'port' = '3316',\n" +
                "    'username' = 'username',\n" +
                "    'password' = 'password',\n" +
                "    'database-name' = 'sampledb',\n" +
                "    'table-name' = 'sample_original'\n" +
                ")");

        Table transactions = tEnv.from("sample_original");

        DataStream<Row> dataStream = tEnv.toChangelogStream(transactions);
        dataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Row>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner((event, timestamp) -> (long) event.getField("id")));
        dataStream.print();

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("152.136.155.204")
                .setPort(63791)
                .setPassword("password")
                .build();

        SingleOutputStreamOperator<Row> filter = dataStream.filter(new FilterFunction<Row>() {
            @Override
            public boolean filter(Row value) throws Exception {
                return true;
            }
        });

        filter.addSink(new RedisSink<Row>(conf, new RedisSampleMapper()));

        environment.execute();
    }

    public static class RedisSampleMapper implements RedisMapper<Row> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "TEST_FLINK_CDC_HASH_NAME");
        }

        @Override
        public String getKeyFromData(Row data) {
            return data.getField("id").toString();
        }

        @Override
        public String getValueFromData(Row data) {
            return data.getField("remark").toString();
        }
    }
}