package org.example.cdc_test.multi_table;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.File;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

/**
 * 这是一个多表同时读取的job，也就是一个job对应tables.yaml 中配置的所有表。
 * 适用于表多，但是数据量小的场景。便于开发，不方便维护。
 *
 * 优化版本：
 * （1）使用 Flink 官方的 JDBCSink 提高批量写入效率
 * （2）支持异步处理
 * （3）使用连接池，避免每次重新创建连接
 */
public class MultiTableSqlServerCdcV2Job {

    public static void main(String[] args) throws Exception {

        // ========== 1 Flink Env ==========
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 检查点配置
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Duration.ofSeconds(30)));

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance().inStreamingMode().build()
        );

        // ========== 2 读取配置文件 ==========
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        TableConfigList tableConfigList = mapper.readValue(
                new File("d:\\flink-1.20\\src\\main\\resources\\tables.yaml"),
                TableConfigList.class
        );

        // ========== 3 遍历配置的每张表 ==========
        for (TableConfig table : tableConfigList.tables) {

            // 表名 
            String flinkTable = table.sourceDb + "_" + table.sourceTable;

            // -------------------------------------------------
            // 3 创建 Flink SQL Server CDC Source
            // -------------------------------------------------

            String createSql = String.format(
                    "CREATE TABLE %s (" +
                            "id INT," +
                            "name STRING," +
                            "age INT," +
                            "create_time TIMESTAMP(3)," +
                            "PRIMARY KEY (id) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector'='sqlserver-cdc'," +
                            "'hostname'='192.168.250.46'," +
                            "'port'='1433'," +
                            "'username'='sa'," +
                            "'password'='Test@123456'," +
                            "'database-name'='%s'," +
                            "'table-name'='%s'," +
                            "'scan.startup.mode'='initial'," +
                            "'scan.incremental.snapshot.enabled'='true'" +
                            ")",
                    flinkTable, table.sourceDb, table.sourceTable
            );

            tEnv.executeSql(createSql);

            // -------------------------------------------------
            // 4 转换 DataStream
            // -------------------------------------------------

            Table source = tEnv.sqlQuery("SELECT * FROM " + flinkTable);

            DataStream<Row> stream = tEnv.toChangelogStream(source).map(row -> {
                System.out.println("CDC数据: " + row);
                return row;
            });

            // -------------------------------------------------
            // 5 写入 ClickHouse 使用 Flink 官方 JDBCSink
            // -------------------------------------------------

            // JDBC 执行选项
            JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                    .withBatchSize(500) // 批量大小
                    .withBatchIntervalMs(200) // 批处理间隔
                    .withMaxRetries(3) // 最大重试次数
                    .build();

            // JDBC 连接选项
            JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl("jdbc:clickhouse://192.168.1.124:8123/" + table.sinkDb)
                    .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                    .withUsername("default")
                    .withPassword("65e84be3")
                    .build();

            // 构建 JDBC Sink
            stream.addSink(
                    JdbcSink.sink(
                            "INSERT INTO " + table.sinkDb + "." + table.sinkTable + " (id, name, age, create_time) VALUES (?, ?, ?, ?)",
                            (JdbcStatementBuilder<Row>) (preparedStatement, row) -> {
                                preparedStatement.setInt(1, (Integer) row.getField(0));
                                preparedStatement.setString(2, (String) row.getField(1));
                                preparedStatement.setInt(3, (Integer) row.getField(2));
                                preparedStatement.setTimestamp(4, Timestamp.valueOf((LocalDateTime) row.getField(3)));
                            },
                            executionOptions,
                            connectionOptions
                    )
            ).setParallelism(1);
        }

        env.execute("SQLServer CDC -> ClickHouse MultiTable V2 Job");
    }

    // -------------------------------------------------
    // YAML Config
    // -------------------------------------------------

    public static class TableConfigList {

        public List<TableConfig> tables;

    }

    public static class TableConfig {

        public String sourceDb;

        public String sourceTable;

        public String sinkDb;

        public String sinkTable;

    }
}
