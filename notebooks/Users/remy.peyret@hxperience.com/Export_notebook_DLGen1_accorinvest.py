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

# DBTITLE 1,Setup Cloudant Logista
from cloudant.client import Cloudant
from cloudant.result import Result, ResultByKey
import json
import pandas as pd
from pyspark.sql.functions import *

accorinvest_user = "1e36de8b-cf2b-47c0-a263-d14e6b419d2b-bluemix"
accorinvest_pass = "47f6045258639b77494eb911667efb769c9aa152be8e42e8dcf52e7d80f3876f"
accorinvest_server = "https://1e36de8b-cf2b-47c0-a263-d14e6b419d2b-bluemix.cloudant.com"

db_name = ['usages-processed-data']

# COMMAND ----------

# DBTITLE 1,Setup Cloudant Logista
from cloudant.client import Cloudant
from cloudant.result import Result, ResultByKey
import json
import pandas as pd
from pyspark.sql.functions import *

accorinvest_user = "8d04777a-5bcc-4c99-bddf-8f08c63cebf8-bluemix"
accorinvest_pass = "ec1fac69f95d825fcf926bf63286ef5aafa870f7520ba529a3e07a4c794f50e1"
accorinvest_server = "https://8d04777a-5bcc-4c99-bddf-8f08c63cebf8-bluemix.cloudantnosqldb.appdomain.cloud"

db_name = ['usages-processed-data']

# COMMAND ----------



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

accorinvest_cloudant = get_cloudant_client(accorinvest_user, accorinvest_pass, accorinvest_server)

# COMMAND ----------

# DBTITLE 1,Mount postgres copy
configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
       "dfs.adls.oauth2.client.id": "7d5fc9f3-f896-4a2d-b319-ffde40c58a00",
       "dfs.adls.oauth2.credential": "316RjWCGpT.yNukgQb*]QpB9GBvatld+",
       "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/8414509f-6652-4da8-9f27-32e2fe3c3111/oauth2/token"}

#dbutils.fs.mount(
#  source = "adl://hxdatalakestorage.azuredatalakestore.net/postgresqlcopy_accorinvest/",
#  mount_point = "/mnt/postgres-DLGen1-accorinvest",
#  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls('/mnt/postgres-DLGen1-accorinvest/')

# COMMAND ----------

# DBTITLE 1,Mount Sink BI-models storage point
dbutils.fs.mount(
source = "adl://hxdatalakestorage.azuredatalakestore.net/accor_export/",
mount_point = "/mnt/accor_export-DLGen1",
extra_configs = configs)

# COMMAND ----------

dbutils.fs.mkdirs('/mnt/bi-models-DLGen1/AccorInvest_space')

# COMMAND ----------

# DBTITLE 1,Create spark dataframes
all_tables = dbutils.fs.ls('/mnt/postgres-DLGen1-accorinvest/')
for t in all_tables:
  if 'public.core' in t.name:
    print(t)

# COMMAND ----------

# DBTITLE 1,Read files from PostgreSQL models
device = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1-accorinvest/public.core_device.txt")
entity = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1-accorinvest/public.core_entity.txt")
entitytype = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1-accorinvest/public.core_entitytype.txt")
equipment = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1-accorinvest/public.core_equipment.txt")
equipmenttemplate = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1-accorinvest/public.core_equipmenttemplate.txt")
fluidtype = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1-accorinvest/public.core_fluidtype.txt")
functionalgroup = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1-accorinvest/public.core_functionalgroup.txt")
location = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1-accorinvest/public.core_location.txt")
tag = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1-accorinvest/public.core_tag.txt")
transceiver = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1-accorinvest/public.core_transceiver.txt")
transceivertype = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1-accorinvest/public.core_transceivertype.txt")
unit = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1-accorinvest/public.core_unit.txt")
usage = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1-accorinvest/public.core_usage.txt")
usagetype = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1-accorinvest/public.core_usagetype.txt")
kpi = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/postgres-DLGen1-accorinvest/public.kpi_kpi.txt")

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

hotels = entity.filter(entity.entity_type=='hotel').select('id', 'name').alias('hotels')
entity = entity.alias('entity')
hotels_2 = entity.join(hotels, col('entity.parent_id') == col('hotels.id'), how='left').select('entity.id', 'hotels.name').alias('hotels_2')
hotels_3 = entity.join(hotels_2, col('entity.parent_id') == col('hotels_2.id'), how='left').select('entity.id', 'hotels_2.name')

# COMMAND ----------

entity.join(hotels, entity.select(regexp_extract('parent_name_0', '(H\d+ .*)',1).alias('hotel')), how='left')
#entity.select(regexp_extract('parent_name_1', '(H\d+ .*)', 1).alias('hotel1')).collect()
#entity.select(regexp_extract('parent_name_2', '(H\d+ .*)', 1).alias('hotel2')).collect()

# COMMAND ----------

hotels_3.show(100)

# COMMAND ----------

hotels_2.count()

# COMMAND ----------

hotels_2.count()-hotels_2.filter(hotels_2.name.isNull()).count()

# COMMAND ----------

entity.filter(entity.id==42).collect()

# COMMAND ----------

usage_table = usage_table.join(entity, usage_table.entity_id == entity.id, how='left').drop(usage_table.entity_id).drop(entity.id)


# COMMAND ----------

entity.show(10)

# COMMAND ----------

# DBTITLE 1,Data table
usg_processed = accorinvest_cloudant['usages-raw-data']
all_db_docs = usg_processed.get_view_result("_design/views", "source_date")#all_docs(include_docs=True)

# COMMAND ----------

from pyspark.sql.types import *
usg_processed = accorinvest_cloudant['usages-raw-data']
doc_count = usg_processed.doc_count()
i = 0
step = 3000000
all_data_df = spark.createDataFrame(sc.emptyRDD(), schema=StructType([StructField("cloudant_id", StringType(), True), StructField("datetime", StringType(), True), StructField("value", StringType(), True)]))
while (i - 1) * step <= doc_count:
  print(i)
  all_db_docs = usg_processed.get_view_result("_design/views", "source_date", skip = i * step, limit = step)#.all_docs(include_docs = True, skip = i * step, limit = step)
  all_data_dict_list = [{'cloudant_id': d['doc']['s'], 'datetime': d['doc']['dt'], 'value': d['doc']['v']} for d in all_db_docs['rows'] if 's' in d['doc']]
  print(len(all_data_dict_list))
  myJson = sc.parallelize(all_data_dict_list)
  sqlContext.read.json(myJson).repartition(1).write.save(path='/mnt/accor_export-DLGen1/data.csv', format='csv', mode='append')
  i += 1

# COMMAND ----------

all_db_docs = usg_processed.get_view_result("_design/views", "source_timestamp", raw_result=True)

# COMMAND ----------

len(all_db_docs['rows'])

# COMMAND ----------

docs = list(all_db_docs)

# COMMAND ----------

all_db_docs['rows']

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
  ent_tmp.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/AccorInvest_space/' + r.name + '/entity')
  
  usage_tmp = usage_table.filter(usage_table.tenant_id == r.id)
  print("{0} usages".format(usage_tmp.count()))
  usage_tmp.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/AccorInvest_space/' + r.name + '/usage')
  
  usage_temp_ext_tmp = temp_ext_usage_table.filter(temp_ext_usage_table.tenant_id == r.id)
  print("{0} usages temperature exterieure".format(usage_temp_ext_tmp.count()))
  usage_temp_ext_tmp.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/AccorInvest_space/' + r.name + '/usage_temp_ext')
  
  usage_conso_tmp = conso_usage_table.filter(conso_usage_table.tenant_id == r.id)
  print("{0} usages consommation".format(usage_conso_tmp.count()))
  usage_conso_tmp.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/AccorInvest_space/' + r.name + '/usage_conso')
  
  data_tmp = all_data_df[all_data_df.cloudant_id.isin([int(row['cloudant_id']) for row in usage_tmp.select('cloudant_id').collect()])]
  print("{0} data points".format(data_tmp.count()))
  data_tmp.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/AccorInvest_space/' + r.name + '/data')
  
  data_conso_tmp = all_data_df[all_data_df.cloudant_id.isin([int(row['cloudant_id']) for row in usage_conso_tmp.select('cloudant_id').collect()])]
  print("{0} consumption data points".format(data_conso_tmp.count()))
  data_conso_tmp.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/AccorInvest_space/' + r.name + '/data_conso')
  
  data_temp_ext_tmp = all_data_df[all_data_df.cloudant_id.isin([int(row['cloudant_id']) for row in usage_temp_ext_tmp.select('cloudant_id').collect()])]
  print("{0} ext. temperature data points".format(data_temp_ext_tmp.count()))
  data_temp_ext_tmp.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/AccorInvest_space/' + r.name + '/data_temp_ext')

  loc_tmp = location.filter(location.tenant_id == r.id)
  print("{0} locations".format(loc_tmp.count()))
  loc_tmp.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/AccorInvest_space/' + r.name + '/loc')