package org.example.cdc_test.multi_table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 这是一个根据配置文件，自动自动建表，自动适配的逻辑
 */
public class EnterpriseCdcJob {

    public static void main(String[] args) throws Exception {

        // ========== 1 Flink Env ==========
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(10000);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance().inStreamingMode().build()
        );

        // ========== 2 SQLServer CDC Source ==========
        tEnv.executeSql(
                "CREATE TABLE sqlserver_source (" +
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
                        "'database-name'='test_db'," +
                        "'table-name'='dbo.*'," +
                        "'scan.startup.mode'='initial'," +
                        "'scan.incremental.snapshot.enabled'='true'" +
                        ")"
        );

        // ========== 3 Convert to DataStream ==========
        Table sourceTable = tEnv.sqlQuery("SELECT * FROM sqlserver_source");
        DataStream<Row> stream = tEnv.toChangelogStream(sourceTable);

        // ========== 4 Process and Sink ==========
        stream.map(row -> {
                    // Extract table name from the row (this is a simplification)
                    // In a real scenario, you might need to parse the CDC event differently
                    CdcRecord record = new CdcRecord(
                            "users", // This should be dynamically extracted
                            new HashMap<String, Object>() {
                                {
                                    put("id", row.getField(0));
                                    put("name", row.getField(1));
                                    put("age", row.getField(2));
                                    put("create_time", row.getField(3));
                                }
                            }
                    );
                    return record;
                }).filter(Objects::nonNull)
                .addSink(new DynamicClickHouseSink());

        env.execute("Enterprise SQLServer CDC Job");
    }

    // =============================
    // CDC 数据结构
    // =============================

    static class CdcRecord {
        String table;
        Map<String, Object> data;

        CdcRecord(String table, Map<String, Object> data) {
            this.table = table;
            this.data = data;
        }
    }

    // =============================
    // Dynamic ClickHouse Sink
    // =============================

    static class DynamicClickHouseSink
            extends org.apache.flink.streaming.api.functions.sink.RichSinkFunction<CdcRecord> {

        static final int BATCH = 500;
        Connection conn;
        Map<String, PreparedStatement> psCache;
        Map<String, Set<String>> schemaCache;
        int batchSize = 0;

        @Override
        public void open(Configuration parameters) throws Exception {

            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");

            conn = DriverManager.getConnection(
                    "jdbc:clickhouse://192.168.1.124:8123/testdb",
                    "default",
                    "65e84be3"
            );

            psCache = new LinkedHashMap<>(100, 0.75f, true) {
                protected boolean removeEldestEntry(Map.Entry eldest) {
                    return size() > 100;
                }
            };

            schemaCache = new HashMap<>();
        }

        @Override
        public void invoke(CdcRecord r, Context ctx) throws Exception {

            ensureTable(r);

            PreparedStatement ps = psCache.get(r.table);

            if (ps == null) {

                List<String> cols = new ArrayList<>(r.data.keySet());

                String colStr = String.join(",", cols);
                String placeholders =
                        cols.stream().map(c -> "?").collect(Collectors.joining(","));

                String sql = String.format(
                        "INSERT INTO %s (%s) VALUES (%s)",
                        r.table, colStr, placeholders
                );

                ps = conn.prepareStatement(sql);
                psCache.put(r.table, ps);
            }

            int i = 1;
            for (Object v : r.data.values())
                ps.setObject(i++, v);

            ps.addBatch();
            batchSize++;

            if (batchSize >= BATCH) {
                flush();
            }
        }

        void flush() throws Exception {
            for (PreparedStatement ps : psCache.values())
                ps.executeBatch();
            batchSize = 0;
        }

        // =============================
        // 自动建表 + Schema Evolution
        // =============================

        void ensureTable(CdcRecord r) throws Exception {

            Set<String> known = schemaCache.get(r.table);

            if (known == null) {

                known = new HashSet<>(r.data.keySet());

                String columns =
                        r.data.keySet().stream()
                                .map(c -> c + " String")
                                .collect(Collectors.joining(","));

                String ddl = String.format(
                        "CREATE TABLE IF NOT EXISTS %s (%s) ENGINE=ReplacingMergeTree ORDER BY tuple()",
                        r.table, columns
                );

                conn.createStatement().execute(ddl);

                schemaCache.put(r.table, known);
                return;
            }

            for (String col : r.data.keySet()) {

                if (!known.contains(col)) {

                    String ddl =
                            String.format(
                                    "ALTER TABLE %s ADD COLUMN %s String",
                                    r.table, col
                            );

                    conn.createStatement().execute(ddl);
                    known.add(col);
                }
            }
        }

        @Override
        public void close() throws Exception {
            flush();
            conn.close();
        }
    }
}