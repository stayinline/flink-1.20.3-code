package org.example.clickhouse_test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ClickHouse 向量化写入 Demo Job
 *
 * 流程：
 *  1. 读取 Kafka topic: dwd_plant_health_detail
 *  2. 解析 JSON -> PlantHealthRecord POJO
 *  3. 将业务字段组合成自然语言描述文本
 *  4. 调用 Ollama nomic-embed-text 模型生成 768 维语义向量
 *  5. 通过 ClickHouse HTTP API (JSONEachRow) 批量写入，包含 Array(Float32) 向量列
 *     并配置 usearch ANN 索引，支持余弦相似度向量检索
 *
 * ============================================================
 * 前置操作：在 ClickHouse 中执行以下 DDL 建表
 * ============================================================
 *
 * CREATE TABLE IF NOT EXISTS dwd_plant_health_detail_vector
 * (
 *     greenhouse_id     String,
 *     ts                DateTime,
 *     crop_type         String,
 *     growth_stage      String,
 *     plant_height_cm   Float64,
 *     leaf_count        Int32,
 *     canopy_coverage   Float64,
 *     chlorophyll_index Float64,
 *     wilting_score     Float64,
 *     fruit_count       Int32,
 *     disease_risk      String,
 *     pest_risk         String,
 *     soil_moisture     Float64,
 *     soil_ec           Float64,
 *     soil_ph           Float64,
 *     temperature       Float64,
 *     humidity          Float64,
 *     text_desc         String,
 *     feature_vector    Array(Float32),
 *     -- 第三个参数是向量维度数，必须与写入的 feature_vector 长度严格一致
 *     INDEX vec_idx feature_vector
 *     TYPE vector_similarity('hnsw', 'L2Distance', 11)
 *     GRANULARITY 1
 * )
 * ENGINE = MergeTree()
 * ORDER BY (greenhouse_id, ts);
 *
 * ============================================================
 * 向量查询示例
 * ============================================================
 *
 * -- 找出与目标向量最相似的 10 条记录（L2 欧式距离，值越小越相似）
 * SELECT
 *     greenhouse_id, ts, crop_type, growth_stage, disease_risk,
 *     L2Distance(feature_vector, [...目标向量...]) AS dist
 * FROM dwd_plant_health_detail_vector
 * ORDER BY dist ASC
 * LIMIT 10;
 *
 * -- 找出与某条记录最相似的记录（自连接）
 * WITH target AS (
 *     SELECT feature_vector FROM dwd_plant_health_detail_vector
 *     WHERE greenhouse_id = 'gh_01' LIMIT 1
 * )
 * SELECT v.greenhouse_id, v.ts, v.crop_type,
 *        L2Distance(v.feature_vector, t.feature_vector) AS dist
 * FROM dwd_plant_health_detail_vector v, target t
 * ORDER BY dist ASC
 * LIMIT 10;
 */
public class ClickhouseVectorDemoJob {

    // ================================================================
    // 配置常量
    // ================================================================

    static final String KAFKA_BROKERS  = "192.168.1.124:9092";
    static final String KAFKA_TOPIC    = "dwd_plant_health_detail";
    static final String KAFKA_GROUP_ID = "flink-ck-vector-consumer";

    /** ClickHouse HTTP API 地址（8123 是 HTTP 端口，用于 JSONEachRow 写入） */
    static final String CK_HTTP_URL    = "http://192.168.1.124:8123";
    static final String CK_USER        = "default";
    static final String CK_PASSWORD    = "65e84be3";
    static final String CK_TABLE       = "dwd_plant_health_detail_vector";

    public static void main(String[] args) throws Exception {

        // ========== 1. Flink 环境 ==========
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(10_000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Duration.ofSeconds(15)));

        // ========== 2. Kafka Source ==========
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(KAFKA_TOPIC)
                .setGroupId(KAFKA_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka-" + KAFKA_TOPIC);

        // ========== 3. 解析 JSON + 调用 Ollama 生成向量 ==========
        DataStream<PlantHealthRecord> records = rawStream
                .map(new ParseAndEmbedFunction())
                .name("ParseAndEmbed(Ollama)")
                // 过滤掉解析失败或 embedding 失败的记录
                .filter(r -> r != null && r.featureVector != null && r.featureVector.length > 0);

        // ========== 4. 写入 ClickHouse（含向量列） ==========
        // 使用 ClickHouse HTTP API + JSONEachRow 格式写入，可靠支持 Array(Float32)
        records
                .addSink(new ClickHouseVectorSink())
                .name("ClickHouseVectorSink")
                .setParallelism(1); // Sink 单并行，便于批量合并

        env.execute("ClickHouse Vector Demo - " + KAFKA_TOPIC);
    }

    // ================================================================
    // POJO
    // ================================================================

    static class PlantHealthRecord implements Serializable {
        private static final long serialVersionUID = 1L;

        String  greenhouseId;
        String  ts;
        String  cropType;
        String  growthStage;
        double  plantHeightCm;
        int     leafCount;
        double  canopyCoverage;
        double  chlorophyllIndex;
        double  wiltingScore;
        int     fruitCount;
        String  diseaseRisk;
        String  pestRisk;
        double  soilMoisture;
        double  soilEc;
        double  soilPh;
        double  temperature;
        double  humidity;
        /** 自然语言描述，作为 Ollama 的 embedding 输入 */
        String  textDesc;
        /** nomic-embed-text 768 维向量 */
        float[] featureVector;
    }

    // ================================================================
    // Step 1：解析 JSON + 构建数值特征向量
    // ================================================================

    static class ParseAndEmbedFunction extends RichMapFunction<String, PlantHealthRecord> {

        private static final Logger LOG = LoggerFactory.getLogger(ParseAndEmbedFunction.class);
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper();
            LOG.info("ParseAndEmbedFunction 初始化完成");
        }

        @Override
        public PlantHealthRecord map(String json) {
            try {
                return parse(json);
            } catch (Exception e) {
                LOG.warn("处理记录失败，已跳过。原因: {}", e.getMessage());
                return null;
            }
        }

        private PlantHealthRecord parse(String json) throws Exception {
            Map<String, Object> m = mapper.readValue(json, Map.class);

            PlantHealthRecord r = new PlantHealthRecord();
            r.greenhouseId     = str(m, "greenhouse_id");
            // ClickHouse DateTime 格式为 "YYYY-MM-DD HH:MM:SS"，截断 Kafka 中的毫秒部分
            // 例如 "2026-03-05 11:34:23.387" -> "2026-03-05 11:34:23"
            String rawTs = str(m, "ts");
            r.ts = rawTs.contains(".") ? rawTs.substring(0, rawTs.indexOf('.')) : rawTs;
            r.cropType         = str(m, "crop_type");
            r.growthStage      = str(m, "growth_stage");
            r.plantHeightCm    = dbl(m, "plant_height_cm");
            r.leafCount        = intVal(m, "leaf_count");
            r.canopyCoverage   = dbl(m, "canopy_coverage");
            r.chlorophyllIndex = dbl(m, "chlorophyll_index");
            r.wiltingScore     = dbl(m, "wilting_score");
            r.fruitCount       = intVal(m, "fruit_count");
            r.diseaseRisk      = str(m, "disease_risk");
            r.pestRisk         = str(m, "pest_risk");
            r.soilMoisture     = dbl(m, "soil_moisture");
            r.soilEc           = dbl(m, "soil_ec");
            r.soilPh           = dbl(m, "soil_ph");
            r.temperature      = dbl(m, "temperature");
            r.humidity         = dbl(m, "humidity");

            r.textDesc      = buildTextDesc(r);
            r.featureVector = buildNumericVector(r);

            LOG.debug("向量构建完成: greenhouse={}, vector_dim={}", r.greenhouseId, r.featureVector.length);
            return r;
        }

        /**
         * 构建自然语言描述，存入 text_desc 列，方便后续排查和全文检索。
         */
        private String buildTextDesc(PlantHealthRecord r) {
            return String.format(
                    "温室编号: %s。作物类型: %s，当前生长阶段: %s。" +
                    "植株株高 %.1f 厘米，叶片数量 %d 片，冠层覆盖率 %.0f%%。" +
                    "叶绿素指数 %.1f，萎蔫评分 %.2f，果实数量 %d 个。" +
                    "病害风险等级: %s，虫害风险等级: %s。" +
                    "土壤湿度 %.1f%%，土壤 EC 值 %.1f μS/cm，土壤 pH %.2f。" +
                    "环境温度 %.1f°C，空气相对湿度 %.1f%%。",
                    r.greenhouseId, r.cropType, r.growthStage,
                    r.plantHeightCm, r.leafCount, r.canopyCoverage * 100,
                    r.chlorophyllIndex, r.wiltingScore, r.fruitCount,
                    r.diseaseRisk, r.pestRisk,
                    r.soilMoisture, r.soilEc, r.soilPh,
                    r.temperature, r.humidity);
        }

        /**
         * 从 11 个数值指标构建归一化特征向量（11 维 Float32）。
         * 各字段除以领域最大值，映射到 [0, 1]，消除量纲差异（soil_ec 原值 ~800，
         * 若不归一化会主导 L2Distance 计算结果，导致其他维度失效）。
         *
         * 维度顺序：
         *  [0] plant_height_cm / 200
         *  [1] leaf_count      / 50
         *  [2] canopy_coverage       (已在 0~1)
         *  [3] chlorophyll_index / 100
         *  [4] wilting_score         (已在 0~1)
         *  [5] fruit_count    / 50
         *  [6] soil_moisture  / 100
         *  [7] soil_ec        / 2000
         *  [8] soil_ph        / 14
         *  [9] temperature    / 50
         * [10] humidity       / 100
         */
        private float[] buildNumericVector(PlantHealthRecord r) {
            return new float[]{
                (float) (r.plantHeightCm    / 200.0),
                (float) (r.leafCount        / 50.0),
                (float)  r.canopyCoverage,
                (float) (r.chlorophyllIndex / 100.0),
                (float)  r.wiltingScore,
                (float) (r.fruitCount       / 50.0),
                (float) (r.soilMoisture     / 100.0),
                (float) (r.soilEc           / 2000.0),
                (float) (r.soilPh           / 14.0),
                (float) (r.temperature      / 50.0),
                (float) (r.humidity         / 100.0)
            };
        }

        // ---- 安全取值工具方法 ----
        private String str(Map<String, Object> m, String key) {
            Object v = m.get(key);
            return v != null ? v.toString() : "";
        }
        private double dbl(Map<String, Object> m, String key) {
            Object v = m.get(key);
            return v instanceof Number ? ((Number) v).doubleValue() : 0.0;
        }
        private int intVal(Map<String, Object> m, String key) {
            Object v = m.get(key);
            return v instanceof Number ? ((Number) v).intValue() : 0;
        }
    }

    // ================================================================
    // Step 2：写入 ClickHouse
    // 使用 HTTP API + JSONEachRow 格式，可靠支持 Array(Float32) 向量列
    // ================================================================

    static class ClickHouseVectorSink extends RichSinkFunction<PlantHealthRecord> {

        private static final Logger LOG          = LoggerFactory.getLogger(ClickHouseVectorSink.class);
        private static final int    BATCH_SIZE   = 1;//debug时，设置成1，正常是100
        private static final long   FLUSH_PERIOD = 3_000L; // ms

        /** ClickHouse HTTP INSERT 地址 */
        private static final String INSERT_URL =
                CK_HTTP_URL + "/?query=" +
                java.net.URLEncoder.encode(
                        "INSERT INTO " + CK_TABLE + " FORMAT JSONEachRow",
                        java.nio.charset.StandardCharsets.UTF_8) +
                "&user=" + CK_USER +
                "&password=" + CK_PASSWORD;

        private transient ObjectMapper     mapper;
        private transient HttpClient       httpClient;
        private transient List<String>     buffer;
        private transient long             lastFlushMs;

        @Override
        public void open(Configuration parameters) {
            mapper      = new ObjectMapper();
            httpClient  = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();
            buffer      = new ArrayList<>(BATCH_SIZE);
            lastFlushMs = System.currentTimeMillis();
            LOG.info("ClickHouseVectorSink 初始化完成，目标: {}/{}", CK_HTTP_URL, CK_TABLE);
        }

        @Override
        public void invoke(PlantHealthRecord r, Context ctx) throws Exception {
            buffer.add(toJsonRow(r));

            long now = System.currentTimeMillis();
            if (buffer.size() >= BATCH_SIZE || (now - lastFlushMs) >= FLUSH_PERIOD) {
                flush();
            }
        }

        /**
         * 将记录序列化为 ClickHouse JSONEachRow 格式的一行 JSON。
         * feature_vector 字段序列化为 JSON Array，ClickHouse 可自动转换为 Array(Float32)。
         */
        private String toJsonRow(PlantHealthRecord r) throws Exception {
            // 手动构建 Map 以控制字段顺序和 float[] 的序列化
            java.util.LinkedHashMap<String, Object> row = new java.util.LinkedHashMap<>();
            row.put("greenhouse_id",     r.greenhouseId);
            row.put("ts",                r.ts);
            row.put("crop_type",         r.cropType);
            row.put("growth_stage",      r.growthStage);
            row.put("plant_height_cm",   r.plantHeightCm);
            row.put("leaf_count",        r.leafCount);
            row.put("canopy_coverage",   r.canopyCoverage);
            row.put("chlorophyll_index", r.chlorophyllIndex);
            row.put("wilting_score",     r.wiltingScore);
            row.put("fruit_count",       r.fruitCount);
            row.put("disease_risk",      r.diseaseRisk);
            row.put("pest_risk",         r.pestRisk);
            row.put("soil_moisture",     r.soilMoisture);
            row.put("soil_ec",           r.soilEc);
            row.put("soil_ph",           r.soilPh);
            row.put("temperature",       r.temperature);
            row.put("humidity",          r.humidity);
            row.put("text_desc",         r.textDesc);
            // float[] 会被 Jackson 正确序列化为 JSON 数组 [f1,f2,...]
            row.put("feature_vector",    toFloatList(r.featureVector));
            return mapper.writeValueAsString(row);
        }

        /** float[] → List<Float>，使 Jackson 序列化为标准 JSON Array */
        private List<Float> toFloatList(float[] arr) {
            List<Float> list = new ArrayList<>(arr.length);
            for (float v : arr) list.add(v);
            return list;
        }

        /**
         * 批量提交到 ClickHouse HTTP API。
         * 请求体为多行 JSON（每行一条记录），ClickHouse 用 JSONEachRow 格式解析。
         */
        private void flush() throws Exception {
            if (buffer.isEmpty()) return;

            // 多行 JSON，换行符分隔
            int count = buffer.size();
            String body = String.join("\n", buffer);
            buffer.clear();
            lastFlushMs = System.currentTimeMillis();

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(INSERT_URL))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(30))
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                throw new RuntimeException(
                        "ClickHouse 写入失败 " + resp.statusCode() + ": " + resp.body());
            }
            LOG.debug("批量写入 {} 条向量记录到 ClickHouse 成功", count);
        }

        @Override
        public void close() throws Exception {
            try {
                flush();
            } catch (Exception e) {
                LOG.warn("关闭时 flush 失败", e);
            }
        }
    }
}
