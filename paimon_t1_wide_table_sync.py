# -*- coding: utf-8 -*-
import os
import shutil
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. 环境准备 (生产环境需移除清理逻辑)
warehouse_path = os.path.abspath("./paimon_warehouse")
# 请将此处的路径替换为您本地或集群中 Paimon Spark JAR 包的实际绝对路径
paimon_spark_jar = "/path/to/your/paimon-spark-3.5-1.3.1.jar"

if os.path.exists(warehouse_path): shutil.rmtree(warehouse_path)

spark = SparkSession.builder \
    .appName("Paimon_T1_Incremental_Pipeline") \
    .config("spark.jars", paimon_spark_jar) \
    .config("spark.sql.extensions", "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions") \
    .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog") \
    .config("spark.sql.catalog.paimon.warehouse", warehouse_path) \
    .getOrCreate()

# ======================================================================
# 第一步：定义表结构 (DDL)
# ======================================================================
def create_tables():
    spark.sql("CREATE DATABASE IF NOT EXISTS paimon.default")
    
    # 配置 changelog-producer = none
    # 此场景下仅依赖快照生成的新文件进行增量读取，降低双写开销
    spark.sql("""
    CREATE TABLE IF NOT EXISTS paimon.default.ods_table_a (
        serial_no BIGINT, policy_no STRING, amount DECIMAL(18,2), c_fk_id BIGINT
    ) TBLPROPERTIES ('primary-key' = 'serial_no', 'bucket' = '4', 'changelog-producer' = 'none')
    """)
    
    spark.sql("""
    CREATE TABLE IF NOT EXISTS paimon.default.dim_policy (
        c_serial_no BIGINT, v_product_code STRING, amend_count INT
    ) TBLPROPERTIES ('primary-key' = 'c_serial_no', 'bucket' = '2', 'changelog-producer' = 'none')
    """)
    
    # 关系映射表，使用复合主键实现自动去重
    spark.sql("""
    CREATE TABLE IF NOT EXISTS paimon.default.map_policy_to_group (
        c_serial_no BIGINT, serial_no BIGINT
    ) TBLPROPERTIES ('primary-key' = 'c_serial_no, serial_no', 'bucket' = '4')
    """)
    
    # 宽表，使用 partial-update 引擎实现局部列更新
    spark.sql("""
    CREATE TABLE IF NOT EXISTS paimon.default.dwd_wide_fact (
        serial_no BIGINT, policy_no STRING, amount DECIMAL(18,2), 
        v_product_code STRING, amend_count INT
    ) TBLPROPERTIES ('primary-key' = 'serial_no', 'merge-engine' = 'partial-update', 'bucket' = '4')
    """)

# ======================================================================
# 第二步：增量游标管理 (基于 Paimon Tag)
# ======================================================================
def get_latest_snapshot(table_name):
    """获取表当前的最新快照 ID"""
    df = spark.sql(f"SELECT snapshot_id FROM paimon.default.`{table_name}$snapshots` ORDER BY snapshot_id DESC LIMIT 1")
    return df.collect()[0][0] if not df.isEmpty() else None

def get_last_processed_tag_snapshot(table_name, job_prefix):
    """获取指定任务前一次成功执行并记录的 Tag 快照 ID"""
    try:
        df = spark.sql(f"""
            SELECT snapshot_id FROM paimon.default.`{table_name}$tags` 
            WHERE tag_name LIKE '{job_prefix}_%' 
            ORDER BY snapshot_id DESC LIMIT 1
        """)
        return df.collect()[0][0] if not df.isEmpty() else None
    except Exception:
        return None

def mark_job_success_with_tag(table_name, job_prefix, snapshot_id):
    """任务成功执行后创建 Tag，保存消费进度并防止历史快照被自动清理"""
    tag_name = f"{job_prefix}_{int(time.time())}"
    spark.sql(f"CALL paimon.sys.create_tag(table => 'default.{table_name}', tag => '{tag_name}', snapshot => {snapshot_id})")
    print(f"  [Tag 管理] 已为 {table_name} 创建 Tag: {tag_name} (快照 ID: {snapshot_id})")

def cleanup_old_tags(table_name, job_prefix, keep_num=2):
    """保留指定数量的 Tag，清理过期 Tag 以释放存储空间"""
    try:
        tags_df = spark.sql(f"""
            SELECT tag_name 
            FROM paimon.default.`{table_name}$tags` 
            WHERE tag_name LIKE '{job_prefix}_%' 
            ORDER BY snapshot_id DESC
        """).collect()
        
        if len(tags_df) > keep_num:
            tags_to_delete = [row['tag_name'] for row in tags_df[keep_num:]]
            for tag in tags_to_delete:
                spark.sql(f"CALL paimon.sys.delete_tag(table => 'default.{table_name}', tag => '{tag}')")
                print(f"  [Tag 管理] 已清理过期 Tag: {tag}")
    except Exception:
        pass

def prepare_incremental_read(table_name, job_prefix):
    """确定增量读取区间，返回对应的 DataFrame 和最新快照 ID"""
    last_snap = get_last_processed_tag_snapshot(table_name, job_prefix)
    latest_snap = get_latest_snapshot(table_name)
    
    if latest_snap is None: 
        return None, None
        
    if last_snap is None:
        print(f"  [增量读取] {table_name}: 未发现历史 Tag，执行全量同步...")
        return spark.table(f"paimon.default.{table_name}"), latest_snap
    elif last_snap == latest_snap:
        return None, latest_snap
    else:
        print(f"  [增量读取] {table_name}: 读取区间 Tag({last_snap}) -> 快照({latest_snap}]")
        df = spark.read.format("paimon") \
            .option("incremental-between", f"{last_snap},{latest_snap}") \
            .table(f"paimon.default.{table_name}")
        return df, latest_snap

# ======================================================================
# 第三步：批处理主逻辑
# ======================================================================
def run_t1_daily_job():
    print(u"\n>>> T+1 批处理任务启动")

    # --- 处理事实表 (ODS_A) 增量 ---
    inc_a, latest_snap_a = prepare_incremental_read("ods_table_a", "fact_sync")
    if inc_a is not None and not inc_a.isEmpty():
        print(f"  [ODS_A] 读取到增量数据: {inc_a.count()} 条，开始处理...")
        dim_df = spark.table("paimon.default.dim_policy")
        
        inc_a.alias("a").join(dim_df.alias("c"), F.col("a.c_fk_id") == F.col("c.c_serial_no"), "left") \
            .select("a.serial_no", "a.policy_no", "a.amount", "c.v_product_code", "c.amend_count") \
            .write.insertInto("paimon.default.dwd_wide_fact")
            
        inc_a.select(F.col("c_fk_id").alias("c_serial_no"), F.col("serial_no")) \
            .write.insertInto("paimon.default.map_policy_to_group")
        
        # 确保数据成功写入下游后再打 Tag
        mark_job_success_with_tag("ods_table_a", "fact_sync", latest_snap_a)
        cleanup_old_tags("ods_table_a", "fact_sync", keep_num=2)
    else:
        print("  [ODS_A] 无新增数据，跳过处理。")

    # --- 处理维度表 (ODS_C) 增量 ---
    inc_c, latest_snap_c = prepare_incremental_read("dim_policy", "dim_sync")
    if inc_c is not None and not inc_c.isEmpty():
        print(f"  [ODS_C] 读取到维度变更: {inc_c.count()} 条，开始处理...")
        map_df = spark.table("paimon.default.map_policy_to_group")
        
        inc_c.alias("c").join(map_df.alias("m"), F.col("c.c_serial_no") == F.col("m.c_serial_no")) \
            .select("m.serial_no", "c.v_product_code", "c.amend_count") \
            .withColumn("policy_no", F.lit(None).cast("string")) \
            .withColumn("amount", F.lit(None).cast("decimal(18,2)")) \
            .select("serial_no", "policy_no", "amount", "v_product_code", "amend_count") \
            .write.insertInto("paimon.default.dwd_wide_fact")
            
        mark_job_success_with_tag("dim_policy", "dim_sync", latest_snap_c)
        cleanup_old_tags("dim_policy", "dim_sync", keep_num=2)
    else:
        print("  [ODS_C] 无维度变更，跳过处理。")

    # --- DWD 表维护 ---
    print(u"\n  [表维护] 开始执行 DWD 宽表 Full Compaction...")
    spark.sql("CALL paimon.sys.compact(table => 'default.dwd_wide_fact')")
    print(u"  [表维护] Full Compaction 执行完成。")

    print(u">>> 批处理任务结束。")

# ======================================================================
# 运行示例
# ======================================================================
if __name__ == "__main__":
    create_tables()

    print(u"\n=====================================================================")
    print(u"                      [DAY 1] 初始化与全量同步                       ")
    print(u"=====================================================================")
    spark.sql("INSERT INTO paimon.default.dim_policy VALUES (1, 'PROD_OLD', 0)")
    spark.sql("INSERT INTO paimon.default.ods_table_a VALUES (101, 'POL_001', 1000.0, 1), (102, 'POL_001', 2000.0, 1)")
    run_t1_daily_job()

    print(u"\n=====================================================================")
    print(u"                      [DAY 2] 增量同步测试 (新增数据)                ")
    print(u"=====================================================================")
    spark.sql("INSERT INTO paimon.default.ods_table_a VALUES (103, 'POL_001', 3000.0, 1), (104, 'POL_004', 4000.0, 2)")
    spark.sql("INSERT INTO paimon.default.dim_policy VALUES (2, 'PROD_OLD_2', 2)")
    run_t1_daily_job()

    print(u"\n=====================================================================")
    print(u"                      [DAY 3] 增量同步测试 (数据更新与 Tag 清理)     ")
    print(u"=====================================================================")
    # 模拟已有数据更新
    spark.sql("INSERT INTO paimon.default.ods_table_a VALUES (101, 'POL_001', 8888.0, 1)")
    spark.sql("INSERT INTO paimon.default.dim_policy VALUES (1, 'PROD_V2_UPDATED', 1)")
    run_t1_daily_job()

    print(u"\n[验证] ODS_A 表 Tag 记录 (预期仅保留最新 2 个 Tag):")
    spark.sql("SELECT tag_name, snapshot_id FROM paimon.default.`ods_table_a$tags` ORDER BY snapshot_id").show(truncate=False)

    print(u"\n[验证] DWD 宽表最终状态:")
    spark.table("paimon.default.dwd_wide_fact").sort("serial_no").show()

    spark.stop()
