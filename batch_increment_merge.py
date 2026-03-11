# -*- coding: utf-8 -*-
import os
import shutil
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. 无条件清理环境
warehouse_path = os.path.abspath("./paimon_warehouse")
tracker_file = os.path.abspath("./snapshot_tracker.json")
paimon_spark_jar = "。/paimon-spark-3.5-1.3.1.jar"

if os.path.exists(warehouse_path): shutil.rmtree(warehouse_path)
if os.path.exists(tracker_file): os.remove(tracker_file)

spark = SparkSession.builder \
    .appName("Paimon_Input_Producer_Fix") \
    .config("spark.jars", paimon_spark_jar) \
    .config("spark.sql.extensions", "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions") \
    .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog") \
    .config("spark.sql.catalog.paimon.warehouse", warehouse_path) \
    .getOrCreate()

# ======================================================================
# 第一步：DDL (核心神级改动：changelog-producer = input)
# ======================================================================
def create_tables():
    spark.sql("CREATE DATABASE IF NOT EXISTS paimon.default")
    
    # 核心改动：使用 input，完全抛弃底层的 -U/+U 比对，只记录上游写入的动作！
    spark.sql("""
    CREATE TABLE IF NOT EXISTS paimon.default.ods_table_a (
        serial_no BIGINT, policy_no STRING, amount DECIMAL(18,2), c_fk_id BIGINT
    ) TBLPROPERTIES ('primary-key' = 'serial_no', 'bucket' = '4', 'changelog-producer' = 'input')
    """)
    
    spark.sql("""
    CREATE TABLE IF NOT EXISTS paimon.default.dim_policy (
        c_serial_no BIGINT, v_product_code STRING, amend_count INT
    ) TBLPROPERTIES ('primary-key' = 'c_serial_no', 'bucket' = '2', 'changelog-producer' = 'input')
    """)
    
    spark.sql("""
    CREATE TABLE IF NOT EXISTS paimon.default.map_policy_to_group (
        c_serial_no BIGINT, serial_no BIGINT
    ) TBLPROPERTIES ('primary-key' = 'c_serial_no, serial_no', 'bucket' = '4')
    """)
    
    spark.sql("""
    CREATE TABLE IF NOT EXISTS paimon.default.dwd_wide_fact (
        serial_no BIGINT, policy_no STRING, amount DECIMAL(18,2), 
        v_product_code STRING, amend_count INT
    ) TBLPROPERTIES ('primary-key' = 'serial_no', 'merge-engine' = 'partial-update', 'bucket' = '4')
    """)

# ======================================================================
# 第二步：纯粹的快照追踪读取 (无需再做任何“提纯”)
# ======================================================================
def get_tracker():
    if not os.path.exists(tracker_file): return {}
    with open(tracker_file, 'r') as f: return json.load(f)

def save_tracker(tracker):
    with open(tracker_file, 'w') as f: json.dump(tracker, f)

def get_latest_snapshot(table_name):
    df = spark.sql(f"SELECT snapshot_id FROM paimon.default.`{table_name}$snapshots` ORDER BY snapshot_id DESC LIMIT 1")
    return df.collect()[0][0] if not df.isEmpty() else None

def read_incremental(table_name, task_key):
    """由于配置了 'input'，读出来的数据绝对干净，全是 +I，不会有 -U"""
    tracker = get_tracker()
    last_snap = tracker.get(task_key)
    latest_snap = get_latest_snapshot(table_name)
    
    if latest_snap is None: return None
    if last_snap is None:
        tracker[task_key] = latest_snap
        save_tracker(tracker)
        return spark.table(f"paimon.default.{table_name}")
    elif last_snap == latest_snap:
        return None 
    else:
        df = spark.read.format("paimon") \
            .option("incremental-between", f"{last_snap},{latest_snap}") \
            .table(f"paimon.default.{table_name}")
        tracker[task_key] = latest_snap
        save_tracker(tracker)
        return df

# ======================================================================
# 第三步：极致简化的每日跑批逻辑
# ======================================================================
def run_t1_daily_job():
    print(u"\n>>> T+1 每日批处理增量任务启动...")
    # 注意：使用 input 后，连强制的 CALL compact 都不需要了！速度起飞！

    # 处理 A 表
    inc_a = read_incremental("ods_table_a", "fact_sync")
    if inc_a is not None and not inc_a.isEmpty():
        print(f"  ✅ [ODS_A 事实增量] 解析到纯净追加日志: {inc_a.count()} 条")
        dim_df = spark.table("paimon.default.dim_policy")
        
        inc_a.alias("a").join(dim_df.alias("c"), F.col("a.c_fk_id") == F.col("c.c_serial_no"), "left") \
            .select("a.serial_no", "a.policy_no", "a.amount", "c.v_product_code", "c.amend_count") \
            .write.insertInto("paimon.default.dwd_wide_fact")
            
        inc_a.select(F.col("c_fk_id").alias("c_serial_no"), F.col("serial_no")) \
            .write.insertInto("paimon.default.map_policy_to_group")
    else:
        print("  ✅ [ODS_A 事实增量] 今日无新增数据，自动跳过。")

    # 处理 C 表
    inc_c = read_incremental("dim_policy", "dim_sync")
    if inc_c is not None and not inc_c.isEmpty():
        print(f"  ✅ [ODS_C 维度增量] 解析到纯净维度变更: {inc_c.count()} 条")
        map_df = spark.table("paimon.default.map_policy_to_group")
        
        inc_c.alias("c").join(map_df.alias("m"), F.col("c.c_serial_no") == F.col("m.c_serial_no")) \
            .select("m.serial_no", "c.v_product_code", "c.amend_count") \
            .withColumn("policy_no", F.lit(None).cast("string")) \
            .withColumn("amount", F.lit(None).cast("decimal(18,2)")) \
            .select("serial_no", "policy_no", "amount", "v_product_code", "amend_count") \
            .write.insertInto("paimon.default.dwd_wide_fact")
    else:
        print("  ✅ [ODS_C 维度增量] 今日无新增维度变更，自动跳过。")

    print(u">>> 本次任务完美结束。")

# ======================================================================
# 测试剧本
# ======================================================================
if __name__ == "__main__":
    create_tables()

    print(u"\n=====================================================================")
    print(u"                      [DAY 1] 宽表全量初始化                         ")
    print(u"=====================================================================")
    spark.sql("INSERT INTO paimon.default.dim_policy VALUES (1, 'PROD_OLD', 0)")
    spark.sql("INSERT INTO paimon.default.ods_table_a VALUES (101, 'POL_001', 1000.0, 1), (102, 'POL_001', 2000.0, 1)")
    run_t1_daily_job()

    print(u"\n=====================================================================")
    print(u"                 [DAY 2] 严格增量：只新增 103 和 104                 ")
    print(u"=====================================================================")
    spark.sql("INSERT INTO paimon.default.ods_table_a VALUES (103, 'POL_001', 3000.0, 1), (104, 'POL_004', 4000.0, 2)")
    spark.sql("INSERT INTO paimon.default.dim_policy VALUES (2, 'PROD_OLD_2', 2)")
    run_t1_daily_job()

    print(u"\n=====================================================================")
    print(u"                 [DAY 4] 高能预警：更新(UPDATE)测试                  ")
    print(u"=====================================================================")
    # 事实表更新：把 101 的 amount 从 1000 更新为 8888.0
    spark.sql("INSERT INTO paimon.default.ods_table_a VALUES (101, 'POL_001', 8888.0, 1)")
    
    # 维度表更新：把 1 的 product_code 改为 'PROD_V2_UPDATED'，修改次数变 1
    spark.sql("INSERT INTO paimon.default.dim_policy VALUES (1, 'PROD_V2_UPDATED', 1)")
    
    run_t1_daily_job()

    print(u"\n[终极验证] Day 4 宽表 (DWD) 最新状态：")
    print(u"预期 1：101 的保额为 8888，且产品编码被级联更新为 PROD_V2_UPDATED")
    print(u"预期 2：102、103 的产品编码同样被级联更新为 PROD_V2_UPDATED")
    print(u"预期 3：104 不受影响，产品编码还是 PROD_OLD_2")
    spark.table("paimon.default.dwd_wide_fact").sort("serial_no").show()

    spark.stop()
