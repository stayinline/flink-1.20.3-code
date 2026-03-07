package org.example.paimon_test;

import org.apache.flink.configuration.Configuration;
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
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Properties;

public class PaimonToClickHouseJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10000);

        EnvironmentSettings settings =
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .build();

        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, settings);

        // Paimon Catalog
        tEnv.executeSql(
                "CREATE CATALOG IF NOT EXISTS paimon WITH (" +
                        "'type'='paimon'," +
                        "'warehouse'='file:///d:/paimon_warehouse'" +
                        ")"
        );

        tEnv.executeSql("USE CATALOG paimon");

        // full + incremental
        Table paimonTable = tEnv.sqlQuery(
                "SELECT * FROM plant_health_paimon_v2 " +
                        "/*+ OPTIONS('scan.mode'='latest-full') */"
        );

        // changelog stream
        DataStream<Row> changelog =
                tEnv.toChangelogStream(paimonTable);

        // 只保留最终值
        DataStream<Row> stream =
                changelog
                        .filter(row ->
                                row.getKind() == RowKind.INSERT ||
                                        row.getKind() == RowKind.UPDATE_AFTER
                        )
                        .map(x -> {
                            System.out.println("读取到Paimon数据: " + x);
                            return x;
                        });

        stream.addSink(new ClickHouseSink())
                .setParallelism(4);

        env.execute("Paimon To ClickHouse Job");

    }

    public static class ClickHouseSink extends RichSinkFunction<Row> {

        private transient Connection connection;
        private transient PreparedStatement ps;

        private int batchSize = 0;

        private static final int BATCH_SIZE = 1;

        @Override
        public void open(Configuration parameters) throws Exception {

            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");

            String url =
                    "jdbc:clickhouse://192.168.1.124:8123/default";

            Properties properties = new Properties();
            properties.setProperty("user", "default");
            properties.setProperty("password", "65e84be3");

            connection = DriverManager.getConnection(url, properties);

            connection.setAutoCommit(false);

            String sql =
                    "INSERT INTO default.dwd_paimon_greenhouse_plant_health_detail (" +
                            "greenhouse_id, ts, crop_type, growth_stage, plant_height_cm, leaf_count," +
                            "canopy_coverage, chlorophyll_index, wilting_score, fruit_count, disease_risk," +
                            "pest_risk, soil_moisture, soil_ec, soil_ph, temperature, humidity" +
                            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            ps = connection.prepareStatement(sql);

        }

        @Override
        public void invoke(Row value, Context context) throws Exception {

            ps.setString(1, (String) value.getField(0));

            Object tsObj = value.getField(1);

            if (tsObj instanceof Timestamp) {
                ps.setTimestamp(2, (Timestamp) tsObj);
            } else if (tsObj instanceof LocalDateTime) {
                ps.setTimestamp(2, Timestamp.valueOf((LocalDateTime) tsObj));
            }

            ps.setString(3, (String) value.getField(2));
            ps.setString(4, (String) value.getField(3));
            ps.setDouble(5, (Double) value.getField(4));
            ps.setInt(6, (Integer) value.getField(5));
            ps.setDouble(7, (Double) value.getField(6));
            ps.setDouble(8, (Double) value.getField(7));
            ps.setDouble(9, (Double) value.getField(8));
            ps.setInt(10, (Integer) value.getField(9));
            ps.setString(11, (String) value.getField(10));
            ps.setString(12, (String) value.getField(11));
            ps.setDouble(13, (Double) value.getField(12));
            ps.setDouble(14, (Double) value.getField(13));
            ps.setDouble(15, (Double) value.getField(14));
            ps.setDouble(16, (Double) value.getField(15));
            ps.setDouble(17, (Double) value.getField(16));

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