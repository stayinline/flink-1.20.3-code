package org.example.sql_server_test;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;
import java.time.Duration;

public class SqlServerToClickHouseJob {

    public static void main(String[] args) throws Exception {

        // ------------------------------------------------------------
        // 1 Flink Environment
        // ------------------------------------------------------------
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        // checkpoint
//        env.enableCheckpointing(300000);
        env.enableCheckpointing(1000);

        env.getCheckpointConfig().setCheckpointingMode(
                CheckpointingMode.EXACTLY_ONCE
        );

        env.getCheckpointConfig().setCheckpointTimeout(10 * 60 * 1000);

        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60 * 1000);

        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

//        // state backend
//        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        // restart strategy
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        5,
                        Duration.ofSeconds(30)
                )
        );

        EnvironmentSettings settings = 
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .build();

        StreamTableEnvironment tEnv = 
                StreamTableEnvironment.create(env, settings);

        // ------------------------------------------------------------
        // 2 SQLServer CDC Source
        // ------------------------------------------------------------

        tEnv.executeSql(
                "CREATE TABLE sqlserver_users (" +
                        "id INT," +
                        "name STRING," +
                        "age INT," +
                        "create_time TIMESTAMP(3)," +
                        "PRIMARY KEY (id) NOT ENFORCED" +
                        ") WITH (" +

                        "'connector' = 'sqlserver-cdc'," +

                        "'hostname' = '192.168.250.46'," +
                        "'port' = '1433'," +
                        "'username' = 'sa'," +
                        "'password' = 'Test@123456'," +

                        "'database-name' = 'test_db'," +
                        "'table-name' = 'dbo.users'," +

                        "'scan.startup.mode' = 'initial'," +
                        "'scan.incremental.snapshot.enabled' = 'true'," +
                        "'scan.incremental.snapshot.chunk.size' = '8096'" +

                        ")"
        );

        // ------------------------------------------------------------
        // 3 Convert to DataStream
        // ------------------------------------------------------------

        Table sqlserverTable = tEnv.sqlQuery("SELECT * FROM sqlserver_users");
        DataStream<Row> changelog = tEnv.toChangelogStream(sqlserverTable);

        // 只保留最终值
        DataStream<Row> stream = changelog
                //这里如果想要全部类型的binlog,就不要过滤，然后在clickhouse 使用replaceMergingTree的表引擎，根据主键去重
//                .filter(row ->
//                        row.getKind() == RowKind.INSERT ||
//                        row.getKind() == RowKind.UPDATE_AFTER
//                )
                .map(x -> {
                    System.out.println("读取到SQL Server数据: " + x);
                    return x;
                });
        // ------------------------------------------------------------
        // 4 ClickHouse Sink
        // ------------------------------------------------------------

        stream.addSink(new ClickHouseSink())
                .setParallelism(1);

        env.execute("SQL Server To ClickHouse Job");

    }

    public static class ClickHouseSink extends RichSinkFunction<Row> {

        private transient Connection connection;
        private transient PreparedStatement ps;

        private int batchSize = 0;
        private static final int BATCH_SIZE = 1;

        @Override
        public void open(Configuration parameters) throws Exception {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");

            String url = "jdbc:clickhouse://192.168.1.124:8123/testdb";
            Properties properties = new Properties();
            properties.setProperty("user", "default");
            properties.setProperty("password", "65e84be3");

            connection = DriverManager.getConnection(url, properties);
            connection.setAutoCommit(false);

            String sql = "INSERT INTO testdb.users (id, name, age, create_time) VALUES (?, ?, ?, ?)";
            ps = connection.prepareStatement(sql);
        }

        @Override
        public void invoke(Row value, Context context) throws Exception {

            ps.setInt(1, (Integer) value.getField(0));
            ps.setString(2, (String) value.getField(1));
            ps.setInt(3, (Integer) value.getField(2));

            java.time.LocalDateTime time = (java.time.LocalDateTime) value.getField(3);
            ps.setTimestamp(4, java.sql.Timestamp.valueOf(time));

            ps.addBatch();
            batchSize++;

            if (batchSize >= BATCH_SIZE) {
                ps.executeBatch();
                connection.commit();
                batchSize = 0;
            }
        }

        @Override
        public void close() throws Exception {
            if (ps != null) {
                ps.executeBatch();
                connection.commit();
                ps.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }
}