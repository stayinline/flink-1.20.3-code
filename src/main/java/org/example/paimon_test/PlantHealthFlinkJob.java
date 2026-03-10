package org.example.paimon_test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class PlantHealthFlinkJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
//        String filePath = "'file:///d:/paimon_warehouse'";
        String filePath = "file:///root/flink/flink-1.20.3/paimon_warehouse";

        EnvironmentSettings settings =
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .build();

        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, settings);

        // Kafka Source
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
                        "'connector'='kafka'," +
                        "'topic'='dwd_plant_health_detail'," +
                        "'properties.bootstrap.servers'='192.168.1.124:9092'," +
                        "'properties.group.id'='flink-paimon-test-v4'," +
                        "'json.ignore-parse-errors'='true'," +
                        "'scan.startup.mode'='earliest-offset'," +
                        "'format'='json'" +
                        ")"
        );

        // Paimon Catalog
        tEnv.executeSql(
                "CREATE CATALOG paimon WITH (" +
                        "'type'='paimon'," +
                        "'warehouse'='" + filePath + "'" +
                        ")"
        );

        tEnv.executeSql("USE CATALOG paimon");

        // Paimon Table
        tEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS plant_health_paimon_v4 (" +
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
                        "PRIMARY KEY (greenhouse_id,ts) NOT ENFORCED" +
                        ")" +
                        "WITH (" +
                        "'bucket'='4'," +
                        "'changelog-producer'='input'" +
                        ")"
        );

//        tEnv.executeSql(
//                "SELECT * FROM default_catalog.default_database.kafka_source"
//        ).print();

        // Kafka -> Paimon
        tEnv.executeSql(
                "INSERT INTO plant_health_paimon_v4 " +
                        "SELECT * FROM default_catalog.default_database.kafka_source"
        );
    }
}