# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "7d5fc9f3-f896-4a2d-b319-ffde40c58a00",
       "fs.azure.account.oauth2.client.secret": "316RjWCGpT.yNukgQb*]QpB9GBvatld+",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/8414509f-6652-4da8-9f27-32e2fe3c3111/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

dbutils.fs.mount(
source = "abfss://files@hxdatastorage.dfs.core.windows.net/input-data",
mount_point = "/mnt/input-data",
extra_configs = configs)

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder \
	.appName("test_script - 1")\
	.enableHiveSupport()\
	.getOrCreate()

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, spark_partition_id, lit, when, col
from pyspark.sql.functions import *

## Read HVAC file(s)
usage_dico = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/input-data/data.csv")
#usage_dico = spark.read.csv('wasbs://files@hxdatastorage.dfs.core.windows.net/input-data/dico.csv', header = True, inferSchema = True)
usage_data = spark.read.csv('wasbs://adfgetstarted@hxdatastorageblob.blob.core.windows.net/test_export_1/usages_data.csv', header = True, inferSchema = True)
usage_data = usage_data.select(col('USAGE_ID').alias('USAGE_DATA_ID'), col('DATETIME'), col('VALUE'))
entities = spark.read.csv('wasbs://adfgetstarted@hxdatastorageblob.blob.core.windows.net/test_export_1/entity.csv', header = True, inferSchema = True)

usage_dico = usage_dico.withColumn('TAG', when((col('DATA_TYPE') == 'INDEX') & (col('DATA_SUB_TYPE') == 'value'), 'consumption').when((lower((usage_dico.EQUIPMENT_NAME)).contains('élec')), 'electricity').when((lower((usage_dico.EQUIPMENT_NAME)).contains('eau') & (col('DATA_TYPE') == 'INDEX') & (col('DATA_SUB_TYPE') == 'delta')), 'water').when((col('DATA_TYPE') == 'TEMPERATURE'), 'temperature').when((col('EQUIPMENT_NAME').contains('gaz') & (col('DATA_TYPE') == 'INDEX') & (col('DATA_SUB_TYPE') == 'delta')), 'gas'))


# COMMAND ----------

import os.path
import IPython
from pyspark.sql import SQLContext
display(dbutils.fs.ls("/mnt/input-data"))

# COMMAND ----------

usage_dico = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/input-data/dico.csv")

# COMMAND ----------

usage_data = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/input-data/data.csv")


# COMMAND ----------

usage_data = usage_data.select(col('DEVICE_ID').alias('USAGE_DATA_ID'), col('DATE'), col('VALUE'))

# COMMAND ----------

usage_data.write.csv('')