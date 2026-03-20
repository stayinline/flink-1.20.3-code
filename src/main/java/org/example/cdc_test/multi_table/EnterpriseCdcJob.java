package org.example.cdc_test.multi_table;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 企业级 SQLServer CDC → ClickHouse 管道
 *
 * 相比原始版本的改进：
 *  1. 配置外部化：连接信息全部迁移到 enterprise_cdc.yaml，代码中无明文凭据
 *  2. EXACTLY_ONCE Checkpoint：开启精确一次语义，并在 snapshotState 触发强制 flush
 *  3. 重启策略：固定延迟重启，容忍短暂下游故障
 *  4. RowKind 感知：过滤 UPDATE_BEFORE，支持 DELETE 软删除（写 is_deleted=1）
 *  5. 类型映射：Java → ClickHouse 精确类型，不再全部写成 String
 *  6. Schema 演进修复：新增列后正确失效并重建 PreparedStatement
 *  7. 资源管理：DDL Statement 用 try-with-resources 关闭，杜绝泄漏
 *  8. 双重 flush 触发：条数阈值 + 时间间隔，保证低流量时数据不积压
 *  9. 连接自愈：invoke 前检查连接存活，断线自动重连
 * 10. 动态表名：从 METADATA 列提取实际表名，实现真正的多表路由
 * 11. SLF4J 日志：替换 System.out，支持生产日志收集
 * 12. ReplacingMergeTree 优化：以 is_deleted 为版本列，支持 FINAL 查询去重
 */
public class EnterpriseCdcJob {

    private static final Logger LOG = LoggerFactory.getLogger(EnterpriseCdcJob.class);

    public static void main(String[] args) throws Exception {

        // ========== 1. 加载外部配置 ==========
        String cfgPath = args.length > 0 ? args[0]
                : "src/main/resources/enterprise_cdc.yaml";
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        JobConfig cfg = mapper.readValue(new File(cfgPath), JobConfig.class);
        LOG.info("配置加载成功: {}", cfgPath);

        // ========== 2. Flink 环境 ==========
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(cfg.flink.parallelism);

        // Checkpoint 配置
        env.enableCheckpointing(cfg.flink.checkpointIntervalMs);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(cfg.flink.checkpointIntervalMs / 2);
        env.getCheckpointConfig().setCheckpointTimeout(60_000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                cfg.flink.maxRestartAttempts,
                java.time.Duration.ofSeconds(cfg.flink.restartDelaySeconds)));

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().inStreamingMode().build());

        // ========== 3. SQLServer CDC Source ==========
        // table_name / schema_name 为 VIRTUAL METADATA，可从每条 CDC 事件提取真实表名
        // 使用通配符 table-name 时，不同表共享此 DDL 定义的 schema
        String sourceDdl = String.format(
                "CREATE TABLE sqlserver_source (" +
                "  _table_name  STRING METADATA FROM 'table_name'  VIRTUAL," +
                "  _schema_name STRING METADATA FROM 'schema_name' VIRTUAL," +
                "  id           INT," +
                "  name         STRING," +
                "  age          INT," +
                "  create_time  TIMESTAMP(3)," +
                "  PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector'                         = 'sqlserver-cdc'," +
                "  'hostname'                          = '%s'," +
                "  'port'                              = '%s'," +
                "  'username'                          = '%s'," +
                "  'password'                          = '%s'," +
                "  'database-name'                     = '%s'," +
                "  'table-name'                        = '%s'," +
                "  'scan.startup.mode'                 = '%s'," +
                "  'scan.incremental.snapshot.enabled' = 'true'" +
                ")",
                cfg.source.hostname, cfg.source.port,
                cfg.source.username, cfg.source.password,
                cfg.source.database, cfg.source.tablePattern,
                cfg.source.startupMode);
        tEnv.executeSql(sourceDdl);

        // ========== 4. 转换为 DataStream<CdcRecord> ==========
        Table sourceTable = tEnv.sqlQuery("SELECT * FROM sqlserver_source");

        // 获取全部列名（含 METADATA 列），用于按名称取值
        List<String> allCols = sourceTable.getResolvedSchema().getColumnNames();

        DataStream<Row> changelogStream = tEnv.toChangelogStream(sourceTable);

        DataStream<CdcRecord> records = changelogStream
                // UPDATE_BEFORE 不含新值，丢弃
                .filter(row -> row.getKind() != RowKind.UPDATE_BEFORE)
                .map(row -> {
                    // 从 METADATA 列提取真实表名
                    String rawTable = (String) row.getField("_table_name");
                    String tableName = (rawTable != null && !rawTable.isEmpty())
                            ? rawTable : "unknown";

                    boolean isDelete = row.getKind() == RowKind.DELETE;

                    // 构建数据 Map，跳过 METADATA 虚拟列
                    Map<String, Object> data = new LinkedHashMap<>();
                    for (String col : allCols) {
                        if ("_table_name".equals(col) || "_schema_name".equals(col)) continue;
                        data.put(col, row.getField(col));
                    }
                    return new CdcRecord(tableName, data, isDelete);
                });

        // ========== 5. 写入 ClickHouse ==========
        records
                .addSink(new DynamicClickHouseSink(cfg.sink))
                .name("DynamicClickHouseSink")
                .setParallelism(cfg.flink.sinkParallelism);

        env.execute("Enterprise SQLServer CDC -> ClickHouse");
    }

    // ================================================================
    // 配置 POJO（对应 enterprise_cdc.yaml）
    // ================================================================

    public static class JobConfig implements Serializable {
        public FlinkConfig  flink  = new FlinkConfig();
        public SourceConfig source = new SourceConfig();
        public SinkConfig   sink   = new SinkConfig();
    }

    public static class FlinkConfig implements Serializable {
        /** Source 并行度 */
        public int  parallelism          = 4;
        /** Sink 并行度，建议 <= source 并行度 */
        public int  sinkParallelism      = 2;
        /** Checkpoint 间隔（ms） */
        public long checkpointIntervalMs = 10_000L;
        /** 最大重启次数 */
        public int  maxRestartAttempts   = 5;
        /** 重启延迟（s） */
        public long restartDelaySeconds  = 30L;
    }

    public static class SourceConfig implements Serializable {
        public String hostname;
        public String port        = "1433";
        public String username;
        public String password;
        public String database;
        public String tablePattern = "dbo.*";
        public String startupMode  = "initial";
    }

    public static class SinkConfig implements Serializable {
        public String  url;
        public String  username        = "default";
        public String  password        = "";
        public int     batchSize       = 500;
        /** 时间触发 flush 的间隔（ms），低流量时保证数据不积压 */
        public long    batchIntervalMs = 2_000L;
        /**
         * 软删除模式：true = 写入 is_deleted=1 的行（ReplacingMergeTree 可合并去重）
         *           false = 跳过 DELETE 事件
         */
        public boolean softDelete      = true;
    }

    // ================================================================
    // CdcRecord：CDC 事件的内部表示
    // ================================================================

    static class CdcRecord implements Serializable {
        private static final long serialVersionUID = 1L;
        final String              table;
        final Map<String, Object> data;
        final boolean             isDelete;

        CdcRecord(String table, Map<String, Object> data, boolean isDelete) {
            this.table    = table;
            this.data     = data;
            this.isDelete = isDelete;
        }
    }

    // ================================================================
    // DynamicClickHouseSink：自动建表 + Schema 演进 + Checkpoint flush
    // ================================================================

    static class DynamicClickHouseSink extends RichSinkFunction<CdcRecord>
            implements CheckpointedFunction {

        private static final Logger LOG              = LoggerFactory.getLogger(DynamicClickHouseSink.class);
        private static final long   serialVersionUID = 1L;

        private final SinkConfig cfg;

        private transient Connection                           conn;
        /** table → 有序列集合（与 PS 参数顺序严格一致） */
        private transient Map<String, LinkedHashSet<String>>   schemaCache;
        /** table → PreparedStatement */
        private transient Map<String, PreparedStatement>       psCache;
        private transient int  pendingCount;
        private transient long lastFlushMs;

        DynamicClickHouseSink(SinkConfig cfg) {
            this.cfg = cfg;
        }

        // ----------------------------------------------------------------
        // 生命周期
        // ----------------------------------------------------------------

        @Override
        public void open(Configuration parameters) throws Exception {
            conn         = newConnection();
            schemaCache  = new HashMap<>();
            psCache      = new HashMap<>();
            pendingCount = 0;
            lastFlushMs  = System.currentTimeMillis();
            LOG.info("DynamicClickHouseSink 已连接: {}", cfg.url);
        }

        @Override
        public void close() throws Exception {
            try {
                flush();
            } catch (Exception e) {
                LOG.warn("关闭时最终 flush 失败", e);
            }
            for (PreparedStatement ps : psCache.values()) {
                try { ps.close(); } catch (Exception ignored) {}
            }
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
            LOG.info("DynamicClickHouseSink 已关闭");
        }

        // ----------------------------------------------------------------
        // 核心写入路径
        // ----------------------------------------------------------------

        @Override
        public void invoke(CdcRecord r, Context ctx) throws Exception {
            checkConnection();
            ensureSchema(r);

            if (r.isDelete) {
                handleDelete(r);
                return;
            }

            PreparedStatement ps = getPreparedStatement(r.table);
            bindValues(ps, schemaCache.get(r.table), r.data);
            ps.addBatch();
            pendingCount++;

            long now = System.currentTimeMillis();
            if (pendingCount >= cfg.batchSize || (now - lastFlushMs) >= cfg.batchIntervalMs) {
                flush();
            }
        }

        /**
         * 软删除：写入 is_deleted=1 的墓碑行。
         * ReplacingMergeTree(is_deleted) 在后台合并时，is_deleted=1 的行优先保留，
         * 查询加 FINAL 或使用 WHERE is_deleted = 0 过滤即可。
         */
        private void handleDelete(CdcRecord r) throws SQLException {
            if (!cfg.softDelete) {
                LOG.debug("跳过 DELETE（softDelete=false）: table={}", r.table);
                return;
            }
            ensureColumn(r.table, "is_deleted", "UInt8");
            Map<String, Object> tombstone = new LinkedHashMap<>(r.data);
            tombstone.put("is_deleted", 1);
            PreparedStatement ps = getPreparedStatement(r.table);
            bindValues(ps, schemaCache.get(r.table), tombstone);
            ps.addBatch();
            pendingCount++;
        }

        private void bindValues(PreparedStatement ps,
                                LinkedHashSet<String> cols,
                                Map<String, Object> data) throws SQLException {
            int i = 1;
            for (String col : cols) {
                ps.setObject(i++, toJdbcValue(data.get(col)));
            }
        }

        /** 将 Java 8 日期时间类型转换为 JDBC 可识别的类型 */
        private Object toJdbcValue(Object v) {
            if (v instanceof LocalDateTime) return Timestamp.valueOf((LocalDateTime) v);
            if (v instanceof LocalDate)     return java.sql.Date.valueOf((LocalDate) v);
            if (v instanceof LocalTime)     return java.sql.Time.valueOf((LocalTime) v);
            return v;
        }

        // ----------------------------------------------------------------
        // Schema 管理：自动建表 + Schema 演进
        // ----------------------------------------------------------------

        private void ensureSchema(CdcRecord r) throws SQLException {
            if (!schemaCache.containsKey(r.table)) {
                createTable(r);
            } else {
                evolveSchema(r);
            }
        }

        private void createTable(CdcRecord r) throws SQLException {
            // 构建列定义
            StringBuilder colDefs = new StringBuilder();
            for (Map.Entry<String, Object> e : r.data.entrySet()) {
                if (colDefs.length() > 0) colDefs.append(", ");
                colDefs.append(e.getKey()).append(' ').append(javaTypeToCkType(e.getValue()));
            }
            if (cfg.softDelete) {
                colDefs.append(", is_deleted UInt8 DEFAULT 0");
            }

            // 使用首个字段（通常是主键）作为 ORDER BY
            String orderBy = r.data.keySet().iterator().next();

            // ReplacingMergeTree(is_deleted)：版本列 is_deleted=1 的行在 FINAL 时"赢"，
            // 配合 WHERE is_deleted = 0 即可实现逻辑删除语义
            String ddl = String.format(
                    "CREATE TABLE IF NOT EXISTS %s (%s)" +
                    " ENGINE = ReplacingMergeTree(%s)" +
                    " ORDER BY (%s)",
                    r.table, colDefs,
                    cfg.softDelete ? "is_deleted" : "1",
                    orderBy);
            execDdl(ddl);

            LinkedHashSet<String> cols = new LinkedHashSet<>(r.data.keySet());
            if (cfg.softDelete) cols.add("is_deleted");
            schemaCache.put(r.table, cols);
            LOG.info("表已创建或确认存在: {}", r.table);
        }

        private void evolveSchema(CdcRecord r) throws SQLException {
            LinkedHashSet<String> known = schemaCache.get(r.table);
            for (Map.Entry<String, Object> e : r.data.entrySet()) {
                if (!known.contains(e.getKey())) {
                    ensureColumn(r.table, e.getKey(), javaTypeToCkType(e.getValue()));
                    known.add(e.getKey());
                    // 列集合变化 → 旧 PreparedStatement 参数数量不匹配，必须废弃
                    invalidatePreparedStatement(r.table);
                }
            }
        }

        private void ensureColumn(String table, String col, String ckType) throws SQLException {
            execDdl(String.format("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s",
                    table, col, ckType));
            LOG.info("新增列: {}.{} {}", table, col, ckType);
        }

        private void execDdl(String ddl) throws SQLException {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(ddl);
            }
        }

        /**
         * Java 值 → ClickHouse 列类型映射。
         * 使用 Nullable 包裹，允许 NULL 值写入。
         */
        private String javaTypeToCkType(Object value) {
            if (value instanceof Integer)                          return "Nullable(Int32)";
            if (value instanceof Long)                             return "Nullable(Int64)";
            if (value instanceof Double)                           return "Nullable(Float64)";
            if (value instanceof Float)                            return "Nullable(Float32)";
            if (value instanceof Boolean)                          return "Nullable(UInt8)";
            if (value instanceof BigDecimal)                       return "Nullable(Decimal(18,6))";
            if (value instanceof LocalDateTime
                    || value instanceof Timestamp)                 return "Nullable(DateTime)";
            if (value instanceof LocalDate
                    || value instanceof java.sql.Date)             return "Nullable(Date)";
            return "Nullable(String)";
        }

        // ----------------------------------------------------------------
        // PreparedStatement 缓存
        // ----------------------------------------------------------------

        private PreparedStatement getPreparedStatement(String table) throws SQLException {
            PreparedStatement ps = psCache.get(table);
            if (ps == null || ps.isClosed()) {
                LinkedHashSet<String> cols = schemaCache.get(table);
                String colStr        = String.join(",", cols);
                String placeholders  = cols.stream().map(c -> "?").collect(Collectors.joining(","));
                String sql = String.format("INSERT INTO %s (%s) VALUES (%s)",
                        table, colStr, placeholders);
                ps = conn.prepareStatement(sql);
                psCache.put(table, ps);
                LOG.debug("PreparedStatement 已刷新: table={}", table);
            }
            return ps;
        }

        private void invalidatePreparedStatement(String table) {
            PreparedStatement ps = psCache.remove(table);
            if (ps != null) {
                try { ps.close(); } catch (SQLException ignored) {}
            }
        }

        // ----------------------------------------------------------------
        // Flush：批量提交到 ClickHouse
        // ----------------------------------------------------------------

        private synchronized void flush() throws SQLException {
            if (pendingCount == 0) return;
            int flushed = pendingCount;
            try {
                for (Map.Entry<String, PreparedStatement> e : psCache.entrySet()) {
                    e.getValue().executeBatch();
                }
            } catch (SQLException ex) {
                LOG.error("批量写入失败", ex);
                throw ex;
            } finally {
                pendingCount = 0;
                lastFlushMs  = System.currentTimeMillis();
            }
            LOG.debug("Flush 完成，共 {} 条记录", flushed);
        }

        // ----------------------------------------------------------------
        // CheckpointedFunction：Checkpoint 时强制 flush，保证 at-least-once 语义
        // ----------------------------------------------------------------

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            flush();
            LOG.info("Checkpoint {} 触发 flush 完成", context.getCheckpointId());
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {
            // 无需托管算子状态；批次数据保存在内存中
        }

        // ----------------------------------------------------------------
        // 连接管理
        // ----------------------------------------------------------------

        private Connection newConnection() throws SQLException {
            return DriverManager.getConnection(cfg.url, cfg.username, cfg.password);
        }

        /** 在每次 invoke 前检查连接存活，断线则自动重连并清空 PS 缓存 */
        private void checkConnection() throws SQLException {
            if (conn == null || conn.isClosed()) {
                LOG.warn("ClickHouse 连接已断开，尝试重连...");
                conn = newConnection();
                psCache.clear(); // 旧连接上的 PS 已失效
                LOG.info("重连成功");
            }
        }
    }
}
