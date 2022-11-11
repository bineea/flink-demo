package org.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author guowb1
 * @description TODO
 * @date 2022/10/26 10:28
 */
public class HandleGroupRedis {

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

//        Table handleRes = original
//                .groupBy($("id"))
//                .select($("id").as("num"), $("id").count().as("quantity"));

        //https://github.com/ververica/flink-cdc-connectors/issues/904
        //The reason is Flink SQL window operator doesn't support changelog input before Flink 1.15, and flink cdc connector can do nothing now.
//        Table handleRes = original
//                .select($("id").mod(3).as("num"), $("id"), $("remark"), $("create_time"))
//                .window(Tumble.over(lit(1500).millis()).on($("create_time")).as("groupwindow"))
//                .groupBy($("groupwindow"), $("num"))
//                .select($("num"), $("id").count().as("quantity"));

        Table handleRes = original
                .select($("id").mod(3).as("num"), $("id"), $("remark"), $("create_time"))
                .groupBy($("num"))
                .select($("num"), $("id").count().as("quantity"));

        DataStream<Row> dataStream = tEnv.toChangelogStream(handleRes);
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

        filter.addSink(new RedisSink<Row>(conf, new RedisGroupMapper()));

        environment.execute();
    }

    public static class RedisGroupMapper implements RedisMapper<Row> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "TEST_FLINK_CDC_COUNT_HASH_NAME");
        }

        @Override
        public String getKeyFromData(Row data) {
            return data.getField("num").toString();
        }

        @Override
        public String getValueFromData(Row data) {
            return data.getField("quantity").toString();
        }
    }
}
