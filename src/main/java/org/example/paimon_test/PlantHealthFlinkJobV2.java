package org.example.paimon_test;


import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;


/**
 * 大数据量优化版本
 */
public class PlantHealthFlinkJobV2 {

    public static void main(String[] args) throws Exception {

        // ------------------------------------------------------------
        // 1 Flink Environment
        // ------------------------------------------------------------
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(8);

        // checkpoint
        env.enableCheckpointing(300000); // 5 min
//        env.enableCheckpointing(1000);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60 * 1000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60 * 1000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        // state backend
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        // restart strategy
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        5,
                        Duration.ofSeconds(30)
                )
        );

//        String warehouse = "file:///D:/paimon_warehouse";
        String warehouse = "file:///root/flink/flink-1.20.3/paimon_warehouse";

        EnvironmentSettings settings =
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .build();

        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, settings);

        // ------------------------------------------------------------
        // 2 Kafka Source
        // ------------------------------------------------------------
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
                        "humidity DOUBLE," +

                        "WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +

                        ") WITH (" +
                        "'connector'='kafka'," +
                        "'topic'='dwd_plant_health_detail'," +
                        "'properties.bootstrap.servers'='192.168.1.124:9092'," +
                        "'properties.group.id'='flink-paimon-rocksDB'," +
                        "'scan.startup.mode'='earliest-offset'," +
                        "'format'='json'," +
                        "'json.ignore-parse-errors'='true'," +
                        "'json.fail-on-missing-field'='false'" +
                        ")"
        );

        // ------------------------------------------------------------
        // 3 Paimon Catalog
        // ------------------------------------------------------------
        tEnv.executeSql(
                "CREATE CATALOG paimon WITH (" +
                        "'type'='paimon'," +
                        "'warehouse'='" + warehouse + "'" +
                        ")"
        );

        tEnv.executeSql("USE CATALOG paimon");

        // ------------------------------------------------------------
        // 4 Paimon Table (Production Config)
        // ------------------------------------------------------------
        tEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS plant_health_paimon_v5_rocksDB (" +
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

                        "dt STRING," +

                        "PRIMARY KEY (greenhouse_id, ts) NOT ENFORCED" +
                        ")" +
                        "PARTITIONED BY (dt)" +

                        " WITH (" +

                        // bucket
                        "'bucket'='64'," +

                        // changelog
                        "'changelog-producer'='input'," +

                        // compaction
                        "'compaction.max.file-num'='100'," +
                        "'compaction.size-ratio'='1.5'," +

                        // snapshot
                        "'snapshot.time-retained'='7 d'," +

                        // manifest
                        "'manifest.target-file-size'='8MB'" +

                        ")"
        );


//        tEnv.executeSql(
//                "SELECT * FROM default_catalog.default_database.kafka_source"
//        ).print();
        // ------------------------------------------------------------
        // 5 Kafka -> Paimon
        // ------------------------------------------------------------
        TableResult result = tEnv.executeSql(
                "INSERT INTO plant_health_paimon_v5_rocksDB " +
                        "SELECT *, DATE_FORMAT(ts,'yyyy-MM-dd') " +
                        "FROM default_catalog.default_database.kafka_source"
        );

        result.getJobClient().get().getJobExecutionResult().get();

    }
}
