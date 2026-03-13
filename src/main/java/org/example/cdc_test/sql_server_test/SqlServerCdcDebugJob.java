package org.example.cdc_test.sql_server_test;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.Duration;
import java.util.Arrays;

/**
 * 这是一个详细打印 CDC binlog 方式同步表数据的 job
 * 能清楚地知道数据到底是什么样子
 */
public class SqlServerCdcDebugJob {

    public static void main(String[] args) throws Exception {

        // ========== 1 Flink Env ==========
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Duration.ofSeconds(30)));

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance().inStreamingMode().build()
        );

        // ========== 2 SQLServer CDC Source ==========
        tEnv.executeSql(
                "CREATE TABLE sqlserver_users ("
                        + " id INT,"
                        + " name STRING,"
                        + " age INT,"
                        + " create_time TIMESTAMP(3),"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector'='sqlserver-cdc',"
                        + " 'hostname'='192.168.250.46',"
                        + " 'port'='1433',"
                        + " 'username'='sa',"
                        + " 'password'='Test@123456',"
                        + " 'database-name'='test_db',"
                        + " 'table-name'='dbo.users',"
                        + " 'scan.startup.mode'='initial',"
                        // 这里使用
                        + " 'scan.incremental.snapshot.enabled'='true'"
                        + ")"
        );

        // ========== 3 Table -> Stream ==========
        Table table = tEnv.sqlQuery("SELECT * FROM sqlserver_users");
        ResolvedSchema schema = table.getResolvedSchema();

        String[] fieldNames = schema.getColumnNames().toArray(new String[0]);

        System.out.println("===== TABLE SCHEMA =====");
        Arrays.stream(fieldNames).forEach(f -> System.out.println("COLUMN : " + f));

        DataStream<Row> stream = tEnv.toChangelogStream(table);

        // ========== 4 CDC Debug Print ==========
        DataStream<Row> debugStream = stream.map(row -> {

            RowKind kind = row.getKind();

            String op;
            switch (kind) {
                case INSERT:
                    op = "INSERT";
                    break;
                case UPDATE_BEFORE:
                    op = "UPDATE_BEFORE";
                    break;
                case UPDATE_AFTER:
                    op = "UPDATE_AFTER";
                    break;
                case DELETE:
                    op = "DELETE";
                    break;
                default:
                    op = "UNKNOWN";
            }

            StringBuilder sb = new StringBuilder();
            sb.append("\n===== CDC EVENT =====\n");
            sb.append("OP : ").append(op).append("\n");

            for (int i = 0; i < row.getArity(); i++) {
                sb.append(fieldNames[i]).append(" = ").append(row.getField(i)).append("\n");
            }

            sb.append("=====================\n");

            System.out.println(sb);

            return row;
        });

        // ========== 5 Sink (仅打印) ==========
        debugStream.addSink(new RichSinkFunction<Row>() {
        });

        env.execute("SQLServer CDC Debug Job");
    }
}