package org.example;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.auth.AuthScope;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.client.CredentialsProvider;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.ssl.SSLContextBuilder;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.ssl.TrustStrategy;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.RestClientBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import javax.net.ssl.SSLContext;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author guowb1
 * @description TODO
 * @date 2022/10/27 19:47
 */
public class HandleSampleHttpsES {

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
        dataStream.addSink(getESSinkBuilder().build());

        environment.execute();
    }



    public static ElasticsearchSink.Builder<Row> getESSinkBuilder() {
        List<HttpHost> httpHosts = new ArrayList<>();
        HttpHost http = new HttpHost("10.122.147.60", 9200, "http");

        httpHosts.add(http);
        ElasticsearchSink.Builder<Row> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Row>() {
                    public IndexRequest createIndexRequest(Row element) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", element.getField("id"));
                        map.put("dataNo", element.getField("dataNo"));
                        map.put("dataName", element.getField("dataName"));
                        //TODO 转换时间为long类型
                        //map.put("dataTime", element.getField("dataTime"));
                        return Requests.indexRequest()
                                .index("sdms_data_index")
                                .id(element.getField("id").toString())
                                .source(map);
                    }

                    //TODO
//                    public UpdateRequest createUpdateRequest(Row element) {
//                        return Requests.clusterUpdateSettingsRequest()
//                                .index("sdms_data_index")
//                                .source(element);
//                    }
//
//                    public DeleteRequest createDeleteRequest(Row element) {
//                        return Requests.deleteRequest()
//                                .index("sdms_data_index")
//                                .source(element);
//                    }


                    @Override
                    public void process(Row element, RuntimeContext ctx, RequestIndexer indexer) {
                        switch (element.getKind()) {
                            case INSERT:
                                indexer.add(createIndexRequest(element));
                                break;
                            case UPDATE_AFTER:
                                indexer.add(createIndexRequest(element));
                                break;
                            default:
                                indexer.add(createIndexRequest(element));
                                break;
                        }
                    }
                }
        );
        //设置用户名密码
        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> {
                    restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("username","password"));

                            TrustStrategy acceptingTrustStrategy = (X509Certificate[] chain, String authType) -> true;
                            SSLContext sslContext = null;
                            try {
                                sslContext = new SSLContextBuilder()
                                        //服务端证书配置
                                        //.loadKeyMaterial(new ClassPathResource("certs/http.p12").getURL(), "a0vWVpZmGFssF5H_wBoZ".toCharArray(), "a0vWVpZmGFssF5H_wBoZ".toCharArray())
                                        //.loadTrustMaterial(new ClassPathResource("certs/http.p12").getURL(), "a0vWVpZmGFssF5H_wBoZ".toCharArray())
                                        //忽略服务端证书校验
                                        .loadTrustMaterial(null, acceptingTrustStrategy)
                                        .build();
                            } catch (NoSuchAlgorithmException e) {
                                throw new RuntimeException(e);
                            } catch (KeyManagementException e) {
                                throw new RuntimeException(e);
                            } catch (KeyStoreException e) {
                                throw new RuntimeException(e);
                            }
                            SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(sslContext, NoopHostnameVerifier.INSTANCE);

                            httpAsyncClientBuilder.setSSLStrategy(sessionStrategy);
                            httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                            return httpAsyncClientBuilder;
                        }
                    });
                }
        );
        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);
        return esSinkBuilder;

    }

}
