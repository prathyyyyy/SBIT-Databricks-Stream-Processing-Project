# Databricks notebook source
# MAGIC %run "./02.setup"

# COMMAND ----------

# MAGIC %run "./03.history-loader"

# COMMAND ----------

# MAGIC %run "./04.bronze_layer"

# COMMAND ----------

# MAGIC %run "./05.silver_layer"

# COMMAND ----------

# MAGIC %run "./06.gold_layer"

# COMMAND ----------

dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
dbutils.widgets.text("RunType", "once", "Set once to run as a batch")
dbutils.widgets.text("ProcessingTime", "5 seconds", "Set the microbatch interval")

# COMMAND ----------

env = dbutils.widgets.get("Environment")
once = True if dbutils.widgets.get("RunType")=="once" else False
processing_time = dbutils.widgets.get("ProcessingTime")
if once:
    print(f"Starting sbit in batch mode.")
else:
    print(f"Starting sbit in stream mode with {processing_time} microbatch.")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# COMMAND ----------

SH = setupHelper(env)
HL = historyLoader(env)

# COMMAND ----------

setup_required = spark.sql(f"SHOW DATABASES IN {SH.catalog}").filter(f"databaseName == '{SH.db_name}'").count() != 1
if setup_required:
    SH.setup()
    SH.validate()
    HL.load_history()
    HL.validate()
else:
    spark.sql(f"USE {SH.catalog}.{SH.db_name}")

# COMMAND ----------

bz = bronzeLayer(env)
sl = silverLayer(env)
gl = goldLayer(env)

# COMMAND ----------

bz.consume(once, processing_time)

sl.upsert(once, processing_time)

gl.upsert(once, processing_time)
