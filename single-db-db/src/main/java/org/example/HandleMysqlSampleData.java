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
 * @date 2022/10/13 14:59
 */
public class HandleMysqlSampleData {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(5, TimeUnit.SECONDS)));

        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

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

        tEnv.executeSql("CREATE TABLE sample_new (\n" +
                "id BIGINT,\n" +
                "remark VARCHAR(100),\n" +
                "create_time TIMESTAMP(6),\n" +
                "PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://152.136.155.204:3316/sampledb',\n" +
                "    'table-name' = 'sample_new',\n" +
                "    'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
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
