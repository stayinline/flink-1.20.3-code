package org.example.weaviate_test;

/**
 * # ================================================================
 * # 配置类
 * # ================================================================
 * @dataclass
 * class WeaviateConfig:
 *     host: str = "http://192.168.1.131:18080"
 *     grpc_port: int = 50051
 *     collection_name: str = "KnowledgeArticle"
 *     embed_model: str = "nomic-embed-text"
 *
 * kafka 数据（这是一个智慧农业的dwd_plant_health_detail数据）
 * {
 *     "greenhouse_id": "gh_01",
 *     "ts": "2026-03-05 11:34:23.387",
 *     "crop_type": "Tomato",
 *     "growth_stage": "FLOWERING",
 *     "plant_height_cm": 58.4,
 *     "leaf_count": 14,
 *     "canopy_coverage": 0.74,
 *     "chlorophyll_index": 43.1,
 *     "wilting_score": 0.11,
 *     "fruit_count": 4,
 *     "disease_risk": "LOW",
 *     "pest_risk": "LOW",
 *     "soil_moisture": 36.47,
 *     "soil_ec": 808.14,
 *     "soil_ph": 5.95,
 *     "temperature": 24.11,
 *     "humidity": 51.58
 * }
 *     这个job的主要作用是读取kafka中的数据，然后把每一条数据转换成向量，也就是需要包含这条数据的业务含义。
 */
public class DataToVectorJob {


}
