package org.example;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.auth.AuthScope;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.client.CredentialsProvider;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.RestClientBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author guowb1
 * @description TODO
 * @date 2022/10/28 19:34
 */
public class HandleSampleHttpES {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(5, TimeUnit.SECONDS)));

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(environment);

        tEnv.executeSql("CREATE TABLE sample_original (\n" +
                "id BIGINT,\n" +
                "remark VARCHAR(100),\n" +
                "create_time TIMESTAMP(3),\n" +
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

        Table sdmsDataTable = tEnv.from("sample_original");

        Table table = sdmsDataTable.select($("id").as("id"),
                $("id").as("dataNo"),
                $("remark").as("dataName"),
                $("create_time").as("dataTime")
        );

        DataStream<Row> dataStream = tEnv.toChangelogStream(table);
        dataStream.print();


        RestClientFactory restClientFactory = new RestClientFactory() {
            @Override
            public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY,
                        new UsernamePasswordCredentials("用户名", "密码"));
                restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        httpAsyncClientBuilder.disableAuthCaching();
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
            }
        };
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200));
        ElasticsearchSink.Builder<Row> sensorReadingBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new MyEsSinkFunction());
        sensorReadingBuilder.setRestClientFactory(restClientFactory);
        dataStream.addSink(sensorReadingBuilder.build());
        environment.execute();
    }

    // 实现自定义的ES写入操作
    public static class MyEsSinkFunction implements ElasticsearchSinkFunction<Row> {

        @Override
        public void process(Row element, RuntimeContext ctx, RequestIndexer indexer) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", element.getField("id"));
            map.put("dataNo", element.getField("dataNo"));
            map.put("dataName", element.getField("dataName"));
            //TODO 转换时间为long类型
            //map.put("dataTime", element.getField("dataTime"));
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sdms_data_index")
                    .id(element.getField("id").toString())
                    .source(map);

            // 用index发送请求
            indexer.add(indexRequest);
        }
    }


}
