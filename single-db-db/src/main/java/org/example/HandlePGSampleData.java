package org.example;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.TimeUnit;

/**
 * @author guowb1
 * @description TODO
 * @date 2022/10/28 19:59
 */
public class HandlePGSampleData {

    public static void main(String[] args) {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(5, TimeUnit.SECONDS)));

        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql("CREATE TABLE sample_original (\n" +
                "id BIGINT NOT NULL,\n" +
                // flink sql不支持default默认值
                "remark VARCHAR(100) NULL,\n" +
                "create_time TIMESTAMP(6) NULL,\n" +
                "PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'postgres-cdc',\n" +
                "    'hostname' = '152.136.155.204',\n" +
                "    'port' = '5432',\n" +
                "    'username' = 'username',\n" +
                "    'password' = 'password',\n" +
                "    'database-name' = 'postgres',\n" +
                // schema-name支持正则表达式，注意：符合正则表达式的表需要1：已发布；2：配置REPLICA
                "    'schema-name' = 'sample(_other){0,1}',\n" +
                // table-name支持正则表达式，注意：符合正则表达式的表需要1：已发布；2：配置REPLICA
                "    'table-name' = 'sample_original_[1-3]{1}',\n" +
                "    'decoding.plugin.name' = 'pgoutput',\n" +
                "    'debezium.slot.name' = 'sample_original_to_db_slot'" +
                ")");

        tEnv.executeSql("CREATE TABLE sample_new (\n" +
                "id BIGINT,\n" +
                "remark VARCHAR(100),\n" +
                "create_time TIMESTAMP(6),\n" +
                "PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:postgresql://152.136.155.204:5432/postgres?currentSchema=sample',\n" +
                "    'table-name' = 'sample_new',\n" +
                "    'driver' = 'org.postgresql.Driver',\n" +
                "    'username' = 'username',\n" +
                "    'password' = 'password',\n" +
                "    'scan.fetch-size' = '200'\n" +
                ")");

        //TableResult tableResult = tEnv.executeSql("insert into sample_new select id, remark, create_time from sample_original");

        Table transactions = tEnv.from("sample_original");
        TableResult tableResult = transactions.executeInsert("sample_new");


        tableResult.print();
    }
}
