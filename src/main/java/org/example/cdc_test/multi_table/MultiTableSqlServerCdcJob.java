package org.example.cdc_test.multi_table;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
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

import java.io.File;
import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * 这是一个多表同时读取的job，也就是一个job对应tables.yaml 中配置的所有表。
 * 适用于表多，但是数据量小的场景。便于开发，不方便维护。
 * <p>
 * 这个job没有手动调试运行。只可当做逻辑参考。
 * <p>
 * 这个job的缺点是使用了一个自定义的 ClickHouseSink ，有以下缺点：
 * （1）批量写入效率低。对比 Flink 官方的 JDBCSink 而言
 * （2）没有异步处理
 * （3）每一个数据库连接，每一次都得重新创建，很耗时
 */
public class MultiTableSqlServerCdcJob {

    public static void main(String[] args) throws Exception {

        // -------------------------------------------------
        // 1 Flink 环境
        // -------------------------------------------------

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        env.enableCheckpointing(5000);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Duration.ofSeconds(30)));

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        // -------------------------------------------------
        // 2 读取 YAML 配置
        // -------------------------------------------------

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        TableConfigList config = mapper.readValue(new File("tables.yaml"), TableConfigList.class);

        // -------------------------------------------------
        // 3 为每张表创建 CDC Source
        // -------------------------------------------------

        for (TableConfig table : config.tables) {

            String flinkTable = table.sourceTable.replace(".", "_");

            String createSql = String.format(

                    "CREATE TABLE %s (" + "id INT," +
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
                            "'scan.incremental.snapshot.enabled'='true'"
                            + ")",

                    flinkTable, table.sourceDb, table.sourceTable);

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
            // 5 写入 ClickHouse
            // -------------------------------------------------

            stream.addSink(new ClickHouseSink(table.sinkDb, table.sinkTable)).setParallelism(1);
        }

        env.execute("SQLServer CDC -> ClickHouse MultiTable Job");
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

    // -------------------------------------------------
    // 生产级 ClickHouse Sink
    // -------------------------------------------------

    public static class ClickHouseSink extends RichSinkFunction<Row> {

        private static final int BATCH_SIZE = 500;
        private final String db;
        private final String table;
        private transient Connection conn;
        private transient PreparedStatement ps;
        private List<String> columns;
        private int batchSize = 0;

        public ClickHouseSink(String db, String table) {
            this.db = db;
            this.table = table;
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
            String url = "jdbc:clickhouse://192.168.1.124:8123/" + db;
            Properties props = new Properties();
            props.setProperty("user", "default");
            props.setProperty("password", "65e84be3");
            conn = DriverManager.getConnection(url, props);
            conn.setAutoCommit(false);

            // 获取表结构
            columns = new ArrayList<>();
            ResultSet rs = conn.getMetaData().getColumns(null, db, table, null);
            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME"));
            }
            if (columns.isEmpty()) {
                throw new RuntimeException("ClickHouse表不存在: " + db + "." + table);
            }
            String columnList = String.join(",", columns);
            String placeholders = String.join(",", Collections.nCopies(columns.size(), "?"));
            String sql = String.format("INSERT INTO %s.%s (%s) VALUES (%s)", db, table, columnList, placeholders);
            ps = conn.prepareStatement(sql);
        }

        @Override
        public void invoke(Row value, Context context) throws Exception {
            if (value.getArity() != columns.size()) {
                throw new RuntimeException("字段数量不匹配");
            }
            for (int i = 0; i < value.getArity(); i++) {
                Object field = value.getField(i);
                if (field == null) {
                    ps.setObject(i + 1, null);
                } else if (field instanceof java.time.LocalDateTime) {
                    ps.setTimestamp(i + 1, Timestamp.valueOf((java.time.LocalDateTime) field));
                } else {
                    ps.setObject(i + 1, field);
                }
            }
            ps.addBatch();
            batchSize++;
            if (batchSize >= BATCH_SIZE) {
                ps.executeBatch();
                conn.commit();
                batchSize = 0;
            }
        }

        @Override
        public void close() throws Exception {
            if (ps != null) {
                ps.executeBatch();
                conn.commit();
                ps.close();
            }

            if (conn != null) {
                conn.close();
            }
        }
    }
}