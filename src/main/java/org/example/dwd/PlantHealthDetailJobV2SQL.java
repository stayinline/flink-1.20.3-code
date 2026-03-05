package org.example.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PlantHealthDetailJobV2SQL {

    public static void main(String[] args) throws Exception {

        // 执行环境
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints");

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, settings);

        // =====================================================
        // Kafka 源 1
        // =====================================================

        tEnv.executeSql(
                "CREATE TABLE plant_vision_source ("
                        + "greenhouse_id STRING,"
                        + "`timestamp` BIGINT,"
                        + "plant_basic ROW<"
                        + "  crop_type STRING,"
                        + "  growth_stage STRING,"
                        + "  plant_height_cm DOUBLE,"
                        + "  leaf_count INT,"
                        + "  canopy_coverage DOUBLE"
                        + ">,"
                        + "plant_health ROW<"
                        + "  chlorophyll_index DOUBLE,"
                        + "  wilting_score DOUBLE,"
                        + "  disease_risk STRING,"
                        + "  pest_risk STRING"
                        + ">,"
                        + "fruit_info ROW<fruit_count INT>,"
                        + "ts AS TO_TIMESTAMP_LTZ(`timestamp`, 3),"
                        + "WATERMARK FOR ts AS ts - INTERVAL '15' SECOND"
                        + ") WITH ("
                        + "'connector' = 'kafka',"
                        + "'topic' = 'smart_agriculture_plant_vision',"
                        + "'properties.bootstrap.servers' = '192.168.1.124:9092',"
                        + "'properties.group.id' = 'plant-vision-sql-consumer',"
                        + "'format' = 'json',"
                        + "'json.ignore-parse-errors' = 'true',"
                        + "'json.fail-on-missing-field' = 'false',"
                        + "'scan.startup.mode' = 'latest-offset'"
                        + ")");

        // =====================================================
        // Soil
        // =====================================================

        tEnv.executeSql(
                "CREATE TABLE soil_sensor_source ("
                        + "greenhouse_id STRING,"
                        + "`timestamp` BIGINT,"
                        + "metrics ROW<"
                        + "  soil_moisture DOUBLE,"
                        + "  soil_ec DOUBLE,"
                        + "  soil_ph DOUBLE"
                        + ">,"
                        + "ts AS TO_TIMESTAMP_LTZ(`timestamp`, 3),"
                        + "WATERMARK FOR ts AS ts - INTERVAL '15' SECOND"
                        + ") WITH ("
                        + "'connector' = 'kafka',"
                        + "'topic' = 'smart_agriculture_soil_sensor',"
                        + "'properties.bootstrap.servers' = '192.168.1.124:9092',"
                        + "'properties.group.id' = 'soil-sensor-sql-consumer',"
                        + "'format' = 'json',"
                        + "'json.ignore-parse-errors' = 'true',"
                        + "'json.fail-on-missing-field' = 'false',"
                        + "'scan.startup.mode' = 'latest-offset'"
                        + ")");

        // =====================================================
        // Env
        // =====================================================

        tEnv.executeSql(
                "CREATE TABLE env_sensor_source ("
                        + "greenhouse_id STRING,"
                        + "`timestamp` BIGINT,"
                        + "metrics ROW<"
                        + "  temperature DOUBLE,"
                        + "  humidity DOUBLE"
                        + ">,"
                        + "ts AS TO_TIMESTAMP_LTZ(`timestamp`, 3),"
                        + "WATERMARK FOR ts AS ts - INTERVAL '15' SECOND"
                        + ") WITH ("
                        + "'connector' = 'kafka',"
                        + "'topic' = 'smart_agriculture_env_sensor',"
                        + "'properties.bootstrap.servers' = '192.168.1.124:9092',"
                        + "'properties.group.id' = 'env-sensor-sql-consumer',"
                        + "'format' = 'json',"
                        + "'json.ignore-parse-errors' = 'true',"
                        + "'json.fail-on-missing-field' = 'false',"
                        + "'scan.startup.mode' = 'latest-offset'"
                        + ")");

        // =====================================================
        // ClickHouse Sink
        // =====================================================

        tEnv.executeSql(
                "CREATE TABLE clickhouse_sink ("
                        + "greenhouse_id STRING,"
                        + "ts TIMESTAMP(3),"
                        + "crop_type STRING,"
                        + "growth_stage STRING,"
                        + "plant_height_cm DOUBLE,"
                        + "leaf_count INT,"
                        + "canopy_coverage DOUBLE,"
                        + "chlorophyll_index DOUBLE,"
                        + "wilting_score DOUBLE,"
                        + "fruit_count INT,"
                        + "disease_risk STRING,"
                        + "pest_risk STRING,"
                        + "soil_moisture DOUBLE,"
                        + "soil_ec DOUBLE,"
                        + "soil_ph DOUBLE,"
                        + "temperature DOUBLE,"
                        + "humidity DOUBLE"
                        + ") WITH ("
                        + "'connector' = 'jdbc',"
                        + "'url' = 'jdbc:clickhouse://192.168.1.124:9000/default',"
                        + "'table-name' = 'dwd_greenhouse_plant_health_detail',"
                        + "'username' = 'default',"
                        + "'password' = '65e84be3',"
//                        + "'driver' = 'com.clickhouse.jdbc.ClickHouseDriver',"
                        + "'sink.buffer-flush.max-rows' = '1000',"
                        + "'sink.buffer-flush.interval' = '2s',"
                        + "'sink.max-retries' = '3'"
                        + ")");

        // =====================================================
        // Interval Join
        // =====================================================

        tEnv.executeSql(
                "INSERT INTO clickhouse_sink "
                        + "SELECT pv.greenhouse_id,"
                        + " pv.ts,"
                        + " pv.plant_basic.crop_type,"
                        + " pv.plant_basic.growth_stage,"
                        + " pv.plant_basic.plant_height_cm,"
                        + " pv.plant_basic.leaf_count,"
                        + " pv.plant_basic.canopy_coverage,"
                        + " pv.plant_health.chlorophyll_index,"
                        + " pv.plant_health.wilting_score,"
                        + " pv.fruit_info.fruit_count,"
                        + " pv.plant_health.disease_risk,"
                        + " pv.plant_health.pest_risk,"
                        + " ss.metrics.soil_moisture,"
                        + " ss.metrics.soil_ec,"
                        + " ss.metrics.soil_ph,"
                        + " es.metrics.temperature,"
                        + " es.metrics.humidity "
                        + "FROM plant_vision_source pv "
                        + "LEFT JOIN soil_sensor_source ss "
                        + "ON pv.greenhouse_id = ss.greenhouse_id "
                        + "AND ss.ts BETWEEN pv.ts - INTERVAL '15' SECOND AND pv.ts + INTERVAL '5' SECOND "
                        + "LEFT JOIN env_sensor_source es "
                        + "ON pv.greenhouse_id = es.greenhouse_id "
                        + "AND es.ts BETWEEN pv.ts - INTERVAL '15' SECOND AND pv.ts + INTERVAL '5' SECOND"
        );
    }
}