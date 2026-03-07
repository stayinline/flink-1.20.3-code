package org.example.paimon_test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PlantHealthFlinkJob {

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

        // -----------------------------
        // Kafka Source
        // -----------------------------
        tEnv.executeSql(
                "CREATE TABLE kafka_source (" +
                        "greenhouse_id STRING," +
                        "ts TIMESTAMP(3)," +
                        "crop_type STRING," +
                        "growth_stage STRING," +
                        "plant_height_cm DOUBLE," +
                        "leaf_count INT," +
                        "canopy_coverage DOUBLE," +
                        "chlorophyll_index DOUBLE," +
                        "wilting_score DOUBLE," +
                        "fruit_count INT," +
                        "disease_risk STRING," +
                        "pest_risk STRING," +
                        "soil_moisture DOUBLE," +
                        "soil_ec DOUBLE," +
                        "soil_ph DOUBLE," +
                        "temperature DOUBLE," +
                        "humidity DOUBLE" +
                        ") WITH (" +
                        "'connector' = 'kafka'," +
                        "'topic' = 'dwd_plant_health_detail'," +
                        "'properties.bootstrap.servers' = '192.168.1.124:9092'," +
                        "'format' = 'json'," +
                        "'properties.group.id' = 'flink-paimon-test',"+
                        "'scan.startup.mode'='earliest-offset'" +
                        ")"
        );

        // -----------------------------
        // Paimon Catalog
        // -----------------------------
        tEnv.executeSql(
                "CREATE CATALOG IF NOT EXISTS  paimon WITH (" +
                        "'type'='paimon'," +
                        "'warehouse'='file:///d:/paimon_warehouse'" +
                        ")"
        );

        tEnv.executeSql("USE CATALOG paimon");

        // -----------------------------
        // Paimon Table
        // -----------------------------
        tEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS  plant_health_paimon (" +
                        "greenhouse_id STRING," +
                        "ts TIMESTAMP(3)," +
                        "crop_type STRING," +
                        "growth_stage STRING," +
                        "plant_height_cm DOUBLE," +
                        "leaf_count INT," +
                        "canopy_coverage DOUBLE," +
                        "chlorophyll_index DOUBLE," +
                        "wilting_score DOUBLE," +
                        "fruit_count INT," +
                        "disease_risk STRING," +
                        "pest_risk STRING," +
                        "soil_moisture DOUBLE," +
                        "soil_ec DOUBLE," +
                        "soil_ph DOUBLE," +
                        "temperature DOUBLE," +
                        "humidity DOUBLE," +
                        "PRIMARY KEY (greenhouse_id) NOT ENFORCED" +
                        ")"
        );
//
//        // -----------------------------
//        // ClickHouse Sink
//        // -----------------------------
//        tEnv.executeSql(
//                "CREATE TABLE IF NOT EXISTS  clickhouse_sink (" +
//                        "greenhouse_id STRING," +
//                        "ts TIMESTAMP(3)," +
//                        "crop_type STRING," +
//                        "growth_stage STRING," +
//                        "plant_height_cm DOUBLE," +
//                        "leaf_count INT," +
//                        "canopy_coverage DOUBLE," +
//                        "chlorophyll_index DOUBLE," +
//                        "wilting_score DOUBLE," +
//                        "fruit_count INT," +
//                        "disease_risk STRING," +
//                        "pest_risk STRING," +
//                        "soil_moisture DOUBLE," +
//                        "soil_ec DOUBLE," +
//                        "soil_ph DOUBLE," +
//                        "temperature DOUBLE," +
//                        "humidity DOUBLE" +
//                        ") WITH (" +
//                        "'connector'='jdbc'," +
//                        "'url'='jdbc:clickhouse://192.168.1.124:8123/default'," +
//                        "'table-name'='dwd_plant_health_detail'," +
//                        "'username'='default'," +
//                        "'password'=''," +
//                        "'sink.buffer-flush.max-rows'='1000'," +
//                        "'sink.buffer-flush.interval'='2s'" +
//                        ")"
//        );
        // 写入 paimon
        tEnv.executeSql(
                "INSERT INTO plant_health_paimon " +
                        "SELECT * FROM default_catalog.default_database.kafka_source"
        );

        // 查询 paimon
        Table result = tEnv.sqlQuery(
                "SELECT * FROM plant_health_paimon"
        );
        System.out.println("paimon 数据查询");
        tEnv.toChangelogStream(result).print();

        env.execute();


        tEnv.toDataStream(result).print();
//
//        // -----------------------------
//        // 写入 ClickHouse
//        // -----------------------------
//        tEnv.executeSql(
//                "INSERT INTO clickhouse_sink " +
//                        "SELECT * FROM default_catalog.default_database.kafka_source"
//        );
    }
}