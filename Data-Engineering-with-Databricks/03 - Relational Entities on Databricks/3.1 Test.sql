-- Databricks notebook source
-- MAGIC %fs
-- MAGIC mounts

-- COMMAND ----------

-- MAGIC %fs ls /mnt/rgstorageellipse/folder

-- COMMAND ----------

create database if not exists learning

-- COMMAND ----------

show databases

-- COMMAND ----------

use learning

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val df = spark.read.format("csv").option("header","true").option("path","/mnt/rgstorageellipse/folder/Department.csv").load

-- COMMAND ----------

use learning

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC df.write.mode("overwrite").saveAsTable("department")

-- COMMAND ----------

describe detail department

-- COMMAND ----------

DESCRIBE TABLE EXTENDED department;

-- COMMAND ----------

drop table department

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tbl_path = f"dbfs:/user/hive/warehouse/learning.db"
-- MAGIC files = dbutils.fs.ls(tbl_path)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/

-- COMMAND ----------

create database datalakedatabase
location '/mnt/rgstorageellipse/dbdatalake'

-- COMMAND ----------

-- MAGIC %fs ls /mnt/input/

-- COMMAND ----------

use datalakedatabase

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC df.write.mode("overwrite").saveAsTable("department")

-- COMMAND ----------

select * from C.department

-- COMMAND ----------

drop table department

-- COMMAND ----------

drop database datalakedatabase
