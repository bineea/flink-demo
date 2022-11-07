package org.example;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @author guowb1
 * @description TODO
 * @date 2022/10/27 21:29
 */
public class HandleSampleExecuteSql2ES {

    public static void main(String[] args) {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(5, TimeUnit.SECONDS)));

        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

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

        tEnv.executeSql("CREATE TABLE sdms_data_index (\n" +
                "id bigint NOT NULL,\n" +
                "dataNo decimal(18,2) NOT NULL,\n" +
                "dataName string NOT NULL,\n" +
                "dataTime timestamp(3) NOT NULL,\n" +
                "PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'elasticsearch-7',\n" +
                "    'hosts' = 'http://10.122.147.60:9200',\n" +
                "    'index' = 'sdms_data_index',\n" +
                "    'username' = 'username',\n" +
                "    'password' = 'password'\n" +
                ")");

        tEnv.executeSql(" INSERT INTO sdms_data_index\n" +
                " SELECT d.id as id, d.id as dataNo, d.remark as dataName, d.create_time as dataTime\n" +
                " FROM sample_original AS d");
    }
}
