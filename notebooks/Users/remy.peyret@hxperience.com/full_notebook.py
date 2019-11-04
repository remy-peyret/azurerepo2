# Databricks notebook source
dbutils.library.installPyPI("cloudant")
dbutils.library.installPyPI("pandas")
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Setup Cloudant
from cloudant.client import Cloudant
from cloudant.result import Result, ResultByKey
import json
import pandas as pd

production_user = "c3281b90-6b66-49fe-98f4-22006fcce0c1-bluemix"
production_pass = "30a519d9437c7c08da33af79f8f5acc6826d53301e243e508c0cabaa0b2be199"
production_server = "https://c3281b90-6b66-49fe-98f4-22006fcce0c1-bluemix.cloudantnosqldb.appdomain.cloud"

db_name = ['usages-processed-data-id']

# COMMAND ----------

def get_cloudant_client(user, cloudant_password, server_url):
    return Cloudant(user, cloudant_password, url=server_url, connect=True, auto_renew=True)

def get_all_database_docs(cloudant_client, database, include_docs = 'True' ):
  end_point = '{0}/{1}'.format(cloudant_client.server_url, database + '/_all_docs')
  params = {'include_docs': include_docs}
  db_dump = cloudant_client.r_session.get(end_point, params=params)
  return db_dump.json()

def get_cloudant_docs(tcv_id, view, database, verbose=True):
    cloudant_req = database.get_view_result('_design/views', view, key=tcv_id)
    returned_docs = cloudant_req.all()
    if verbose:
        print("Number of docs returned by cloudant for the tcv {} : {}".format(tcv_id, len(returned_docs)))
    return returned_docs
  
def get_all_database_docs_streaming(cloudant_client, database, include_docs = 'True' ):
    end_point = '{0}/{1}'.format(cloudant_client.server_url, str(database) + '/_all_docs')
    params = {'include_docs': include_docs}
    db_stream = cloudant_client.r_session.get(end_point, params=params, stream = True)
    if db_stream is None :
        db_stream.encoding = 'utf-8'
    return db_stream.iter_lines()

def dump_cloudant_database(dumpPath, cloudant_client, database, include_docs = 'True', streaming = False):
    if streaming:
        db_stream = get_all_database_docs_streaming(cloudant_client, database, include_docs)
    else :
        db_dump = get_all_database_docs(cloudant_client, database, include_docs)
        dump_file_path = open(dumpPath, 'w')
        dump_file_path.write(json.dumps(db_dump))
        dump_file_path.close()

def json_doc(tcv_id, dt=None, data={}, metadata={}):
    """
    This function Formats the document for cloudant injection
    It gets a dictionary containing the data, source (EUID) and datetime as inputs
    It returns a cloudant compatible dictionary
    """
    return dict(
        _id=dt.strftime('%Y%m%d%H%M%S') + '_' + tcv_id,
        datetime=dt.isoformat(),
        timestamp=dt.timestamp(),
        source=tcv_id,
        data=data,
        metadata=metadata
    )

# COMMAND ----------

production_cloudant = get_cloudant_client(production_user, production_pass, production_server)

# COMMAND ----------

# DBTITLE 1,Mount postgres copy
configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "7d5fc9f3-f896-4a2d-b319-ffde40c58a00",
       "fs.azure.account.oauth2.client.secret": "316RjWCGpT.yNukgQb*]QpB9GBvatld+",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/8414509f-6652-4da8-9f27-32e2fe3c3111/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

dbutils.fs.mount(
source = "abfss://postgresqlcopy@hxdatastorage.dfs.core.windows.net/tables",
mount_point = "/mnt/postgres-tables",
extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,Mount Sink BI-models storage point
configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "7d5fc9f3-f896-4a2d-b319-ffde40c58a00",
       "fs.azure.account.oauth2.client.secret": "316RjWCGpT.yNukgQb*]QpB9GBvatld+",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/8414509f-6652-4da8-9f27-32e2fe3c3111/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

dbutils.fs.mount(
source = "abfss://bi-models@hxdatastorage.dfs.core.windows.net/",
mount_point = "/mnt/bi-models",
extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,Setup Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder \
	.appName("test_script - 1")\
	.enableHiveSupport()\
	.getOrCreate()

# COMMAND ----------

# DBTITLE 1,Create tenant directories
tenants = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-tables/public.tenancy_tenant.txt")
for r in tenants.select('name').collect():
  dirname = '/mnt/bi-models/' + str(r.name)
  dbutils.fs.mkdirs(dirname)

# COMMAND ----------

# DBTITLE 1,Create spark dataframes
all_tables = dbutils.fs.ls('/mnt/postgres-tables/')
for t in all_tables:
  if 'public.core' in t.name:
    print(t)

# COMMAND ----------

device = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-tables/public.core_device.txt")
entity = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-tables/public.core_entity.txt")
entitytype = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-tables/public.core_entitytype.txt")
equipment = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-tables/public.core_equipment.txt")
equipmenttemplate = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-tables/public.core_equipmenttemplate.txt")
fluidtype = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-tables/public.core_fluidtype.txt")
functionalgroup = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-tables/public.core_functionalgroup.txt")
location = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-tables/public.core_location.txt")
tag = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-tables/public.core_tag.txt")
transceiver = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-tables/public.core_transceiver.txt")
transceivertype = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-tables/public.core_transceivertype.txt")
unit = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-tables/public.core_unit.txt")
usage = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-tables/public.core_usage.txt")
usagetype = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-tables/public.core_usagetype.txt")
kpi = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-tables/public.kpi_kpi.txt")

# COMMAND ----------

usage.columns

# COMMAND ----------

entitytype.columns

# COMMAND ----------

# DBTITLE 1,make usage table
usage_table = usage.drop('uid', 'last_data_id', 'last_clocked_data_id', 'custom_id').join(device.select('id', 'tc_input', 'description', 'transceiver_id', 'data_type', 'sampling_frequency', 'ph_threshold', 'pl_threshold').withColumnRenamed('description', 'device_description'), usage.device_id == device.id, how='left').drop(device.id)
usage_table = usage_table.join(equipment.select('id', 'name', 'location_id', 'template_id', 'entity_id', 'extra').withColumnRenamed('name', 'equipment_name').withColumnRenamed('extra', 'equipment_extra'), usage_table.equipment_id == equipment.id, how='left').drop(equipment.id).drop(usage_table.equipment_id)
usage_table = usage_table.join(equipmenttemplate.select('id', 'name').withColumnRenamed('name', 'equipment_name'), usage_table.template_id == equipmenttemplate.id, how='left').drop(equipmenttemplate.id).drop(usage_table.template_id)
usage_table = usage_table.join(functionalgroup.select('id', 'designation').withColumnRenamed('designation', 'functional_group_designation'), usage_table.functional_group_id == functionalgroup.id, how='left').drop(functionalgroup.id).drop(usage_table.functional_group_id)
usage_table = usage_table.join(usagetype.select('id', 'name', 'data_type', 'data_sub_type', 'description').withColumnRenamed('name', 'usage_type_name').withColumnRenamed('description', 'usage_type_descr'), usage_table.usage_type_id == usagetype.id, how='left').drop(usagetype.id).drop(usage_table.usage_type_id)
usage_table = usage_table.join(fluidtype.select('id', 'designation').withColumnRenamed('designation', 'fluid_type_designation'), usage_table.fluid_type_id == fluidtype.id, how='left').drop(fluidtype.id).drop(usage_table.fluid_type_id)
usage_table = usage_table.join(transceiver.select('id', 'transmitting_frequency', 'local_status', 'remote_status', 'devid', 'network', 'is_active', 'timezone', 'tc_type_id'), usage_table.transceiver_id == transceiver.id, how='left').drop(transceiver.id).drop(usage_table.transceiver_id)
usage_table = usage_table.join(transceivertype.select('id', 'name', 'brand', 'data_types').withColumnRenamed('name', 'tc_type_name').withColumnRenamed('brand', 'tc_brand'), usage_table.tc_type_id == transceivertype.id, how='left').drop(transceivertype.id).drop(usage_table.tc_type_id)

# COMMAND ----------

usage_table.columns

# COMMAND ----------

entity = entity.join(entitytype.select('id', 'name').withColumnRenamed('name', 'entity_type'), entity.entity_type_id == entitytype.id).drop(entity.entity_type_id).drop(entitytype.id)

# COMMAND ----------

entity.select('depth').distinct().collect()

# COMMAND ----------

entity.write.partitionBy('depth').save('/mnt/bi_models/entity', format='csv')

# COMMAND ----------

dbutils.fs.ls('/mnt/bi_models/entity/depth=1')

# COMMAND ----------

tenants.show()
for r in tenants.select('name', 'id').collect():
  print(r.name, r.id)
  ent_tmp = entity.filter(entity.tenant_id == r.id)
  print(ent_tmp.count())
  ent_tmp.write.partitionBy('depth').save('/mnt/bi-models/' + r.name + '/entity', format='csv', mode="overwrite")
  
  usage_tmp = usage_table.filter(usage_table.tenant_id == r.id)
  ent_tmp.write.save('/mnt/bi-models/' + r.name + '/entity', format='csv', mode="overwrite")

# COMMAND ----------

entity.filter(entity.tenant_id=='2').show()

# COMMAND ----------

dbutils.fs.ls('/mnt/')

# COMMAND ----------

dbutils.fs.ls('/mnt/bi-models/demo/entity')

# COMMAND ----------

dbutils.fs.unmount("/mnt/postgres-tables")

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