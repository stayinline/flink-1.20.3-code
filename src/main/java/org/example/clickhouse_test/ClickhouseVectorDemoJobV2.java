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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * ClickHouse 向量化写入 V2 Job
 *
 * 对比 V1 的新增点：
 *  1. 衍生指标计算：health_score / yield_estimate / issue_type
 *  2. 双向量列：
 *       feature_vector  (11维，L2Distance)  — 归一化数值特征，找"指标绝对值相近"的记录
 *       semantic_vector (768维，cosineDistance) — Ollama nomic-embed-text 语义向量，找"业务场景相似"的记录
 *  3. 目标表：dwd_plant_health_detail_vector_v2
 *
 * ============================================================
 * 衍生字段计算逻辑
 * ============================================================
 *
 * health_score (0~1，越大越健康)：
 *   = chlorophyll_index/100 * 0.4
 *   + (1 - wilting_score)   * 0.3
 *   + disease_factor        * 0.2   (LOW=1.0, MEDIUM=0.5, HIGH=0.0)
 *   + pest_factor           * 0.1   (LOW=1.0, MEDIUM=0.5, HIGH=0.0)
 *
 * yield_estimate (相对产量估算)：
 *   = fruit_count * canopy_coverage * (1 - wilting_score) * (chlorophyll_index / 50)
 *
 * issue_type (优先级由高到低)：
 *   病害 → 虫害 → 缺水 → 高温 → 低温 → pH异常 → 正常
 *
 * ============================================================
 * 双向量的区别与用途
 * ============================================================
 *
 * feature_vector（11维，L2Distance）：
 *   由 11 个归一化数值指标直接构成。衡量绝对数值差异，两条记录各指标值越接近
 *   L2 距离越小。适合查询：「找出与当前温室环境参数最接近的历史记录」
 *
 * semantic_vector（768维，cosineDistance）：
 *   由 Ollama nomic-embed-text 对 embed_text 编码生成。
 *   embed_text 结构类似 title + tags + content 三段联合 embed：
 *     title  = 作物类型 + 生长阶段（最核心的分类标识符）
 *     tags   = 问题类型 + 病/虫害风险标签（关键语义标签）
 *     content= 关键指标的自然语言描述
 *   捕获"业务语义相似性"，例如同为「高温缺水的番茄开花期」就会向量相近，
 *   即使数值细节不同。适合查询：「找出业务场景最相似的历史案例」
 *
 * ⚠ 注意：semantic_vector 为 768 维，需要重建 ClickHouse 表，
 *   将 idx_semantic_vector 索引的维度参数从 11 改为 768：
 *   TYPE vector_similarity('hnsw', 'cosineDistance', 768)
 *
 * ============================================================
 * 向量查询示例
 * ============================================================
 *
 * -- 基于 feature_vector 查找绝对环境最相似的 10 条记录
 * SELECT greenhouse_id, ts, crop_type, health_score, issue_type,
 *        L2Distance(feature_vector, [...]) AS dist
 * FROM dwd_plant_health_detail_vector_v2
 * ORDER BY dist ASC LIMIT 10;
 *
 * -- 基于 semantic_vector 查找生长模式最相似的记录（忽略量级）
 * WITH target AS (
 *     SELECT semantic_vector FROM dwd_plant_health_detail_vector_v2
 *     WHERE greenhouse_id = 'gh_01' ORDER BY ts DESC LIMIT 1
 * )
 * SELECT v.greenhouse_id, v.ts, v.issue_type, v.health_score,
 *        cosineDistance(v.semantic_vector, t.semantic_vector) AS dist
 * FROM dwd_plant_health_detail_vector_v2 v, target t
 * ORDER BY dist ASC LIMIT 10;
 */
public class ClickhouseVectorDemoJobV2 {

    // ================================================================
    // 配置常量
    // ================================================================

    static final String KAFKA_BROKERS  = "192.168.1.124:9092";
    static final String KAFKA_TOPIC    = "dwd_plant_health_detail";
    static final String KAFKA_GROUP_ID = "flink-ck-vector-v2-early-consumer";

    static final String CK_HTTP_URL    = "http://192.168.1.124:8123";
    static final String CK_USER        = "default";
    static final String CK_PASSWORD    = "65e84be3";
    static final String CK_TABLE       = "dwd_plant_health_detail_vector_v2";

    /** Ollama embedding API，nomic-embed-text 输出 768 维语义向量 */
//    static final String OLLAMA_URL     = "http://192.168.250.46:11434/api/embeddings";
    static final String OLLAMA_URL     = "http://127.0.0.1:11434/api/embeddings";
    static final String OLLAMA_MODEL   = "nomic-embed-text";

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
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka-" + KAFKA_TOPIC);

        // ========== 3. 解析 + 衍生指标计算 + 双向量构建 ==========
        DataStream<PlantHealthRecordV2> records = rawStream
                .map(new ParseAndVectorizeFunction())
                .name("ParseAndVectorize")
                .filter(r -> r != null && r.featureVector != null && r.featureVector.length > 0);

        // ========== 4. 写入 ClickHouse ==========
        records
                .addSink(new ClickHouseVectorSinkV2())
                .name("ClickHouseVectorSinkV2")
                .setParallelism(1);

        env.execute("ClickHouse Vector V2 - " + KAFKA_TOPIC);
    }

    // ================================================================
    // POJO
    // ================================================================

    static class PlantHealthRecordV2 implements Serializable {
        private static final long serialVersionUID = 1L;

        // ---- 基础字段（来自 Kafka） ----
        String greenhouseId;
        String ts;
        String cropType;
        String growthStage;
        double plantHeightCm;
        int    leafCount;
        double canopyCoverage;
        double chlorophyllIndex;
        double wiltingScore;
        int    fruitCount;
        String diseaseRisk;
        String pestRisk;
        double soilMoisture;
        double soilEc;
        double soilPh;
        double temperature;
        double humidity;

        // ---- 衍生字段（实时计算） ----
        /** 综合健康评分，0~1，越大越健康 */
        float  healthScore;
        /** 相对产量估算（与 fruit_count、冠层覆盖率、萎蔫程度正相关） */
        float  yieldEstimate;
        /** 主要问题类型：病害/虫害/缺水/高温/低温/pH异常/正常 */
        String issueType;

        // ---- 语义层 ----
        String  textDesc;
        /** 11维归一化数值向量，适配 L2Distance 绝对距离检索 */
        float[] featureVector;
        /** Ollama nomic-embed-text 生成的 768 维语义向量，适配 cosineDistance 业务语义检索 */
        float[] semanticVector;
    }

    // ================================================================
    // Step 1：解析 JSON + 计算衍生指标 + 构建双向量
    // ================================================================

    static class ParseAndVectorizeFunction extends RichMapFunction<String, PlantHealthRecordV2> {

        private static final Logger LOG = LoggerFactory.getLogger(ParseAndVectorizeFunction.class);
        private transient ObjectMapper mapper;
        private transient HttpClient   httpClient;

        @Override
        public void open(Configuration parameters) {
            mapper     = new ObjectMapper();
            httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();
            LOG.info("ParseAndVectorizeFunction 初始化完成，Ollama: {}", OLLAMA_URL);
        }

        @Override
        public PlantHealthRecordV2 map(String json) {
            try {
                return parse(json);
            } catch (Exception e) {
                LOG.warn("处理记录失败，已跳过。原因: {}", e.getMessage());
                return null;
            }
        }

        private PlantHealthRecordV2 parse(String json) throws Exception {
            Map<String, Object> m = mapper.readValue(json, Map.class);

            PlantHealthRecordV2 r = new PlantHealthRecordV2();

            // ---- 基础字段解析 ----
            r.greenhouseId     = str(m, "greenhouse_id");
            String rawTs = str(m, "ts");
            // ClickHouse DateTime 格式 "YYYY-MM-DD HH:MM:SS"，截断毫秒
            r.ts               = rawTs.contains(".") ? rawTs.substring(0, rawTs.indexOf('.')) : rawTs;
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

            // ---- 衍生字段计算 ----
            r.healthScore    = computeHealthScore(r);
            r.yieldEstimate  = computeYieldEstimate(r);
            r.issueType      = classifyIssue(r);

            // ---- 语义层 ----
            r.textDesc       = buildTextDesc(r);
            r.featureVector  = buildNumericVector(r);
            // 调用 Ollama 对 embed_text（title+tags+content 联合）生成 768 维语义向量
            r.semanticVector = callOllamaEmbedding(buildEmbedText(r));

            LOG.debug("记录处理完成: greenhouse={}, health={}, issue={}",
                    r.greenhouseId, r.healthScore, r.issueType);
            return r;
        }

        // ----------------------------------------------------------------
        // 衍生字段计算
        // ----------------------------------------------------------------

        /**
         * 综合健康评分（0~1）：
         *   叶绿素占 40%（光合能力）
         *   不萎蔫程度占 30%（水分状态）
         *   病害因子占 20%（LOW=1.0, MEDIUM=0.5, HIGH=0.0）
         *   虫害因子占 10%
         */
        private float computeHealthScore(PlantHealthRecordV2 r) {
            double chlorophyllScore = r.chlorophyllIndex / 100.0;
            double wiltingScore     = 1.0 - r.wiltingScore;
            double diseaseFactor    = riskFactor(r.diseaseRisk);
            double pestFactor       = riskFactor(r.pestRisk);
            double score = chlorophyllScore * 0.4
                         + wiltingScore     * 0.3
                         + diseaseFactor    * 0.2
                         + pestFactor       * 0.1;
            return (float) Math.max(0.0, Math.min(1.0, score));
        }

        private double riskFactor(String risk) {
            if ("HIGH".equalsIgnoreCase(risk))   return 0.0;
            if ("MEDIUM".equalsIgnoreCase(risk)) return 0.5;
            return 1.0; // LOW or unknown
        }

        /**
         * 相对产量估算：
         *   果实数量 × 冠层覆盖率 × (1 - 萎蔫) × (叶绿素/50)
         *   值越大代表预估产量越高
         */
        private float computeYieldEstimate(PlantHealthRecordV2 r) {
            double estimate = r.fruitCount
                    * r.canopyCoverage
                    * (1.0 - r.wiltingScore)
                    * (r.chlorophyllIndex / 50.0);
            return (float) Math.max(0.0, estimate);
        }

        /**
         * 主要问题分类（优先级从高到低）：
         *   病害 > 虫害 > 缺水 > 高温 > 低温 > pH异常 > 正常
         */
        private String classifyIssue(PlantHealthRecordV2 r) {
            if ("HIGH".equalsIgnoreCase(r.diseaseRisk))               return "病害";
            if ("HIGH".equalsIgnoreCase(r.pestRisk))                  return "虫害";
            if (r.wiltingScore > 0.5 || r.soilMoisture < 25.0)       return "缺水";
            if (r.temperature > 35.0)                                 return "高温";
            if (r.temperature < 10.0)                                 return "低温";
            if (r.soilPh < 5.5 || r.soilPh > 7.5)                   return "pH异常";
            return "正常";
        }

        // ----------------------------------------------------------------
        // 向量构建
        // ----------------------------------------------------------------

        private String buildTextDesc(PlantHealthRecordV2 r) {
            return String.format(
                    "温室编号: %s。作物类型: %s，当前生长阶段: %s。" +
                    "植株株高 %.1f 厘米，叶片数量 %d 片，冠层覆盖率 %.0f%%。" +
                    "叶绿素指数 %.1f，萎蔫评分 %.2f，果实数量 %d 个。" +
                    "病害风险: %s，虫害风险: %s。" +
                    "土壤湿度 %.1f%%，EC值 %.1f，pH %.2f。" +
                    "温度 %.1f°C，湿度 %.1f%%。" +
                    "综合健康评分: %.2f，问题类型: %s。",
                    r.greenhouseId, r.cropType, r.growthStage,
                    r.plantHeightCm, r.leafCount, r.canopyCoverage * 100,
                    r.chlorophyllIndex, r.wiltingScore, r.fruitCount,
                    r.diseaseRisk, r.pestRisk,
                    r.soilMoisture, r.soilEc, r.soilPh,
                    r.temperature, r.humidity,
                    r.healthScore, r.issueType);
        }

        /**
         * 11维归一化数值特征向量（与 V1 完全一致），适配 L2Distance。
         * 各维度除以领域最大值，映射到 [0,1]，消除量纲差异。
         *
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
        private float[] buildNumericVector(PlantHealthRecordV2 r) {
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

        /**
         * 构建专用于 Ollama embedding 的语义文本，参考 Python 三段联合 embed 模式：
         *   title   = 作物类型 + 生长阶段（最核心的分类标识符，放在最前面权重最高）
         *   tags    = 问题类型 + 病/虫害风险（关键语义标签，驱动模型理解业务场景）
         *   content = 关键指标的自然语言描述（细节背景，丰富语义）
         *
         * 例如输出：
         *   "Tomato FLOWERING 病害 HIGH LOW
         *    健康评分0.62 叶绿素43.1 萎蔫0.11 株高58.4cm 果实4个
         *    土壤pH5.95 湿度36.5% 温度23.4°C 空气湿度72.3%"
         */
        private String buildEmbedText(PlantHealthRecordV2 r) {
            // title：最核心的分类标识
            String title = r.cropType + " " + r.growthStage;

            // tags：业务语义标签（issue_type 是最重要的语义锚点）
            String tags = r.issueType
                    + " " + r.diseaseRisk
                    + " " + r.pestRisk;

            // content：关键数值指标的自然语言描述
            String content = String.format(
                    "健康评分%.2f 叶绿素%.1f 萎蔫%.2f 株高%.1fcm 果实%d个 " +
                    "土壤pH%.2f 湿度%.1f%% 温度%.1f°C 空气湿度%.1f%%",
                    r.healthScore, r.chlorophyllIndex, r.wiltingScore,
                    r.plantHeightCm, r.fruitCount,
                    r.soilPh, r.soilMoisture, r.temperature, r.humidity);

            return title + " " + tags + " " + content;
        }

        /**
         * 调用 Ollama nomic-embed-text API 获取 768 维语义向量。
         *
         * 请求：POST http://192.168.1.131:11434/api/embeddings
         *       {"model":"nomic-embed-text","prompt":"<embed_text>"}
         * 响应：{"embedding":[f1, f2, ..., f768]}
         */
        @SuppressWarnings("unchecked")
        private float[] callOllamaEmbedding(String embedText) throws Exception {
            String body = mapper.writeValueAsString(Map.of(
                    "model",  OLLAMA_MODEL,
                    "prompt", embedText));

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(OLLAMA_URL))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(30))
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                throw new RuntimeException("Ollama 返回错误 " + resp.statusCode() + ": " + resp.body());
            }

            Map<String, Object> respMap = mapper.readValue(resp.body(), Map.class);
            List<Number> embedding      = (List<Number>) respMap.get("embedding");

            float[] vector = new float[embedding.size()];
            for (int i = 0; i < embedding.size(); i++) {
                vector[i] = embedding.get(i).floatValue();
            }
            LOG.debug("Ollama embedding 完成，维度={}", vector.length);
            return vector;
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
    // Step 2：写入 ClickHouse（HTTP API + JSONEachRow）
    // ================================================================

    static class ClickHouseVectorSinkV2 extends RichSinkFunction<PlantHealthRecordV2> {

        private static final Logger LOG          = LoggerFactory.getLogger(ClickHouseVectorSinkV2.class);
        private static final int    BATCH_SIZE   = 1;
        private static final long   FLUSH_PERIOD = 3_000L;

        private static final String INSERT_URL =
                CK_HTTP_URL + "/?query=" +
                java.net.URLEncoder.encode(
                        "INSERT INTO " + CK_TABLE + " FORMAT JSONEachRow",
                        java.nio.charset.StandardCharsets.UTF_8) +
                "&user=" + CK_USER +
                "&password=" + CK_PASSWORD;

        private transient ObjectMapper  mapper;
        private transient HttpClient    httpClient;
        private transient List<String>  buffer;
        private transient long          lastFlushMs;

        @Override
        public void open(Configuration parameters) {
            mapper      = new ObjectMapper();
            httpClient  = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();
            buffer      = new ArrayList<>(BATCH_SIZE);
            lastFlushMs = System.currentTimeMillis();
            LOG.info("ClickHouseVectorSinkV2 初始化完成，目标: {}/{}", CK_HTTP_URL, CK_TABLE);
        }

        @Override
        public void invoke(PlantHealthRecordV2 r, Context ctx) throws Exception {
            buffer.add(toJsonRow(r));
            long now = System.currentTimeMillis();
            if (buffer.size() >= BATCH_SIZE || (now - lastFlushMs) >= FLUSH_PERIOD) {
                flush();
            }
        }

        /**
         * 序列化为 JSONEachRow 格式。
         * 新增字段：health_score / yield_estimate / issue_type / semantic_vector。
         * 空字段（health_score=0, yield_estimate=0, issue_type=""）也会写入，
         * ClickHouse 会以默认值处理。
         */
        private String toJsonRow(PlantHealthRecordV2 r) throws Exception {
            LinkedHashMap<String, Object> row = new LinkedHashMap<>();

            // 基础字段
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

            // 衍生字段
            row.put("health_score",      r.healthScore);
            row.put("yield_estimate",    r.yieldEstimate);
            row.put("issue_type",        r.issueType);

            // 语义层
            row.put("text_desc",         r.textDesc);
            row.put("feature_vector",    toFloatList(r.featureVector));
            row.put("semantic_vector",   toFloatList(r.semanticVector));

            return mapper.writeValueAsString(row);
        }

        private List<Float> toFloatList(float[] arr) {
            List<Float> list = new ArrayList<>(arr.length);
            for (float v : arr) {
                list.add(v);
            }
            return list;
        }

        private void flush() throws Exception {
            if (buffer.isEmpty()) return;

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
            LOG.debug("批量写入 {} 条向量记录成功", count);
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
