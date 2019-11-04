# Databricks notebook source
dbutils.library.installPyPI("cloudant")
dbutils.library.installPyPI("pandas")
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Setup Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder \
	.appName("test_script - 1")\
	.enableHiveSupport()\
	.getOrCreate()

# COMMAND ----------

# DBTITLE 1,Setup Cloudant Production
from cloudant.client import Cloudant
from cloudant.result import Result, ResultByKey
import json
import pandas as pd
from pyspark.sql.functions import *

production_user = "423209ee-de09-45c1-ba70-c65fa5b8933b-bluemix"
production_pass = "3abbc64e7d0dc917c0faa13d7e4ff107019e74cb8253acc69b463d9074542b00"
production_server = "https://423209ee-de09-45c1-ba70-c65fa5b8933b-bluemix.cloudantnosqldb.appdomain.cloud"

db_name = ['usages-processed-data']

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
configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
       "dfs.adls.oauth2.client.id": "7d5fc9f3-f896-4a2d-b319-ffde40c58a00",
       "dfs.adls.oauth2.credential": "316RjWCGpT.yNukgQb*]QpB9GBvatld+",
       "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/8414509f-6652-4da8-9f27-32e2fe3c3111/oauth2/token"}

#dbutils.fs.mount(
#  source = "adl://hxdatalakestorage.azuredatalakestore.net/postgresqlcopy/",
#  mount_point = "/mnt/postgres-DLGen1",
#  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls('/mnt/postgres-DLGen1/')

# COMMAND ----------

# DBTITLE 1,Mount Sink BI-models storage point
# dbutils.fs.mount(
# source = "adl://hxdatalakestorage.azuredatalakestore.net/bi-models/",
# mount_point = "/mnt/bi-models-DLGen1",
# extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,Create tenant directories
tenants = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1/public.tenancy_tenant.txt")
for r in tenants.select('name').collect():
  dirname = '/mnt/bi-models-DLGen1/' + str(r.name)
  dbutils.fs.mkdirs(dirname)

# COMMAND ----------

# DBTITLE 1,Create spark dataframes
all_tables = dbutils.fs.ls('/mnt/postgres-DLGen1/')
for t in all_tables:
  if 'public.core' in t.name:
    print(t)

# COMMAND ----------

# DBTITLE 1,Read files from PostgreSQL models
device = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1/public.core_device.txt")
entity = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1/public.core_entity.txt")
entitytype = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1/public.core_entitytype.txt")
equipment = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1/public.core_equipment.txt")
equipmenttemplate = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1/public.core_equipmenttemplate.txt")
fluidtype = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1/public.core_fluidtype.txt")
functionalgroup = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1/public.core_functionalgroup.txt")
location = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1/public.core_location.txt")
tag = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1/public.core_tag.txt")
transceiver = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1/public.core_transceiver.txt")
transceivertype = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1/public.core_transceivertype.txt")
unit = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1/public.core_unit.txt")
usage = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1/public.core_usage.txt")
usagetype = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1/public.core_usagetype.txt")
kpi = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1/public.kpi_kpi.txt")

# COMMAND ----------

# DBTITLE 1,Usage tables
usage_table = usage.drop('uid', 'last_data_id', 'last_clocked_data_id', 'custom_id').join(device.select('id', 'tc_input', 'description', 'transceiver_id', 'data_type', 'sampling_frequency', 'ph_threshold', 'pl_threshold', 'unit_id').withColumnRenamed('description', 'device_description'), usage.device_id == device.id, how='left').drop(device.id)
usage_table = usage_table.join(unit.select('id', 'name').withColumnRenamed('name', 'unit_name'), usage_table.unit_id == unit.id, how='left').drop(unit.id).drop(usage_table.unit_id)
usage_table = usage_table.join(equipment.select('id', 'name', 'location_id', 'template_id', 'entity_id', 'extra').withColumnRenamed('name', 'equipment_name').withColumnRenamed('extra', 'equipment_extra'), usage_table.equipment_id == equipment.id, how='left').drop(equipment.id).drop(usage_table.equipment_id)
usage_table = usage_table.join(equipmenttemplate.select('id', 'name').withColumnRenamed('name', 'equipment_template_name'), usage_table.template_id == equipmenttemplate.id, how='left').drop(equipmenttemplate.id).drop(usage_table.template_id)
usage_table = usage_table.join(functionalgroup.select('id', 'designation').withColumnRenamed('designation', 'functional_group_designation'), usage_table.functional_group_id == functionalgroup.id, how='left').drop(functionalgroup.id).drop(usage_table.functional_group_id)
usage_table = usage_table.join(usagetype.select('id', 'name', 'data_sub_type', 'description').withColumnRenamed('name', 'usage_type_name').withColumnRenamed('description', 'usage_type_descr'), usage_table.usage_type_id == usagetype.id, how='left').drop(usagetype.id).drop(usage_table.usage_type_id)
usage_table = usage_table.join(fluidtype.select('id', 'designation').withColumnRenamed('designation', 'fluid_type_designation'), usage_table.fluid_type_id == fluidtype.id, how='left').drop(fluidtype.id).drop(usage_table.fluid_type_id)
usage_table = usage_table.join(transceiver.select('id', 'transmitting_frequency', 'local_status', 'remote_status', 'devid', 'network', 'is_active', 'timezone', 'tc_type_id'), usage_table.transceiver_id == transceiver.id, how='left').drop(transceiver.id).drop(usage_table.transceiver_id)
usage_table = usage_table.join(transceivertype.select('id', 'name', 'brand', 'data_types').withColumnRenamed('name', 'tc_type_name').withColumnRenamed('brand', 'tc_brand'), usage_table.tc_type_id == transceivertype.id, how='left').drop(transceivertype.id).drop(usage_table.tc_type_id)

# COMMAND ----------

unit.columns

# COMMAND ----------

tag.show()

# COMMAND ----------

conso_usage_table = usage_table.filter(usage_table.data_type == 'INDEX').filter(usage_table.data_sub_type == 'delta')
temp_ext_usage_table = usage_table.filter(usage_table.data_type == 'TEMPERATURE').filter(lower(usage_table.equipment_name).like('%ext%') | lower(usage_table.equipment_template_name).like('%ext%'))

# COMMAND ----------

print("{0} usages total".format(usage_table.count()))
print("{0} consumption usages".format(conso_usage_table.count()))
print("{0} ext. temperature usages".format(temp_ext_usage_table.count()))

# COMMAND ----------

usagetype.filter(usagetype.tenant_id==159).show()
print(usagetype.filter(usagetype.tenant_id==159).select(usagetype.name).collect())

# COMMAND ----------

# DBTITLE 1,Entity table 
entity = entity.join(entitytype.select('id', 'name').withColumnRenamed('name', 'entity_type'), entity.entity_type_id == entitytype.id, how='left').drop(entity.entity_type_id).drop(entitytype.id).drop(entity.slug).drop(entity.depth).drop(entity.path).drop(entity.custom_id).drop(entity.entity_dashboard_id).drop(entity.numchild).drop(entity.created_at)
print("{0} entities counted throughtout all the tenants".format(entity.count()))

# COMMAND ----------

i = 0
ent = entity.withColumnRenamed('parent_id', 'parent_id_0')
ent_full = ent
while ent_full.count()-ent_full.filter(ent_full['parent_id_'+str(i)].isNull()).count() != 0:
  ent_parent = ent_full.select(ent_full.id, ent_full.name, ent_full['parent_id_0']).withColumnRenamed('name', 'parent_name_'+str(i)).withColumnRenamed('id', 'parent_id').withColumnRenamed('parent_id_0', 'parent_id_'+str(i+1))
  ent_full = ent_full.join(ent_parent, ent_full['parent_id_'+str(i)] == ent_parent['parent_id'], how='left').drop('parent_id')
  if i > 0:
    ent_full = ent_full.drop('parent_id_' + str(i))
  i+=1
entity = ent_full.drop('parent_id_0').drop('parent_id_'+str(i))

# COMMAND ----------

entity.show(10)

# COMMAND ----------

# DBTITLE 1,Data table
usg_processed = production_cloudant['usages-processed-data']
all_db_docs = usg_processed.all_docs(include_docs=True)

# COMMAND ----------

all_data_dict_list = [{'cloudant_id': d['doc']['s'], 'datetime': d['doc']['dt'], 'value': d['doc']['v']} for d in all_db_docs['rows'] if 's' in d['doc']]

# COMMAND ----------

myJson = sc.parallelize(all_data_dict_list)
all_data_df = sqlContext.read.json(myJson)

# COMMAND ----------

# DBTITLE 1,Save tables per tenant
tenants.show()
for r in tenants.select('name', 'id').collect():
  print("-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-")
  print(r.name, r.id)
  ent_tmp = entity.filter(entity.tenant_id == r.id)
  print("{0} entities".format(ent_tmp.count()))
  ent_tmp.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/' + r.name + '/entity')
  
  usage_tmp = usage_table.filter(usage_table.tenant_id == r.id)
  print("{0} usages".format(usage_tmp.count()))
  usage_tmp.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/' + r.name + '/usage')
  
  usage_temp_ext_tmp = temp_ext_usage_table.filter(temp_ext_usage_table.tenant_id == r.id)
  print("{0} usages temperature exterieure".format(usage_temp_ext_tmp.count()))
  usage_temp_ext_tmp.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/' + r.name + '/usage_temp_ext')
  
  usage_conso_tmp = conso_usage_table.filter(conso_usage_table.tenant_id == r.id)
  print("{0} usages consommation".format(usage_conso_tmp.count()))
  usage_conso_tmp.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/' + r.name + '/usage_conso')
  
  data_tmp = all_data_df[all_data_df.cloudant_id.isin([int(row['cloudant_id']) for row in usage_tmp.select('cloudant_id').collect()])]
  print("{0} data points".format(data_tmp.count()))
  data_tmp.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/' + r.name + '/data')
  
  data_conso_tmp = all_data_df[all_data_df.cloudant_id.isin([int(row['cloudant_id']) for row in usage_conso_tmp.select('cloudant_id').collect()])]
  print("{0} consumption data points".format(data_conso_tmp.count()))
  data_conso_tmp.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/' + r.name + '/data_conso')
  
  data_temp_ext_tmp = all_data_df[all_data_df.cloudant_id.isin([int(row['cloudant_id']) for row in usage_temp_ext_tmp.select('cloudant_id').collect()])]
  print("{0} ext. temperature data points".format(data_temp_ext_tmp.count()))
  data_temp_ext_tmp.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/' + r.name + '/data_temp_ext')

  loc_tmp = location.filter(location.tenant_id == r.id)
  print("{0} locations".format(loc_tmp.count()))
  loc_tmp.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/' + r.name + '/loc')

# COMMAND ----------

