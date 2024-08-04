# Databricks notebook source
# MAGIC %run "./02.setup"

# COMMAND ----------

dbutils.widgets.text("Environment", "sbit_dev", "Set the current environment/catalog name")
env = dbutils.widgets.get("Environment")

# COMMAND ----------

sh = setupHelper("sbit_dev")
sh.cleanup()

# COMMAND ----------

dbutils.notebook.run("./07-run", 600, {"Environment": env, "RunType": "once"})

# COMMAND ----------

HL = HistoryLoader(env)
SH.validate()
HL.validate()

# COMMAND ----------


PR =Producer()
PR.produce(1)
PR.validate(1)
dbutils.notebook.run("./07-run", 600, {"Environment": env, "RunType": "once"})

# COMMAND ----------



BZ = Bronze(env)
SL = Silver(env)
GL = Gold(env)
BZ.validate(1)
SL.validate(1)
GL.validate(1)

# COMMAND ----------

PR.produce(2)
PR.validate(2)
dbutils.notebook.run("./07-run", 600, {"Environment": env, "RunType": "once"})

# COMMAND ----------

BZ.validate(2)
SL.validate(2)
GL.validate(2)
SH.cleanup()
