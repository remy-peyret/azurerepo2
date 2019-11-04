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

# DBTITLE 1,Setup Cloudant AccorInvest
from cloudant.client import Cloudant
from cloudant.result import Result, ResultByKey
import json
import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.types import *

accorinvest_user = "1e36de8b-cf2b-47c0-a263-d14e6b419d2b-bluemix"
accorinvest_pass = "47f6045258639b77494eb911667efb769c9aa152be8e42e8dcf52e7d80f3876f"
accorinvest_server = "https://1e36de8b-cf2b-47c0-a263-d14e6b419d2b-bluemix.cloudant.com"

db_name = 'usages-processed-data'

# COMMAND ----------

def get_cloudant_client(user, cloudant_password, server_url):
    return Cloudant(user, cloudant_password, url=server_url, connect=True, auto_renew=True)

# COMMAND ----------

# -*- coding: utf-8 -*-

import pytz
import logging
from queue import Queue
from threading import Thread
from datetime import datetime
from urllib.parse import urlparse

#from django.conf import settings

DATE_COMPONENTS = ['year', 'month', 'day', 'hour', 'minute', 'second']

def get_utc_date(date):
    """
    From a given date (timezone aware or not), this function will return the
    datetime in UTC timezone.
    :return: datetime
    """
    if has_utc_offset(date):
        return pytz.UTC.normalize(date)
    else:
        return pytz.UTC.localize(date)
      
      
def get_database_from_type(db_type, cloudant_client=None):
    """
    Return database object for a given db_type
    :param db_type: available database types are stored in
    `commons.const.database.DB_TYPE_LIST`
    """
    if cloudant_client is None:
        from messengers import cloudantc
        cloudant_client = cloudantc
    if cloudant_client is not None:
        return cloudant_client.get_database(settings.SERVICES['cloudant'][db_type])


def get_view(db_name, view_name):
    """
    Return view object for a given db_type and view_name
    :param db_type: available database types are stored in
    `commons.const.database.DB_TYPE_LIST`
    :param view_name: name of view to return.
    """
    from messengers import cloudantc
    db = cloudantc.get_database(db_name)
    return db.get_view(view_name)


def get_datetime_from_key(view_key):
    """
    Extract UTC date from cloudant view key.
    :return: datetime of None
    """

    if len(view_key) < 2:
        return None

    date_kwargs = dict()
    view_key.extend([1] * (4 - len(view_key)))
    for i, name in enumerate(DATE_COMPONENTS):
        if i < len(view_key) - 1:
            date_kwargs[name] = view_key[i + 1]
        else:
            break
    return get_utc_date(datetime(**date_kwargs))

  
def get_datetime_from_timestamp_key(view_key):
    """
    Extract UTC date from cloudant view key.
    :return: datetime of None
    """
    return datetime.fromtimestamp(view_key[-1], pytz.UTC)


def cloudant_client_from_url(url):
    from .client import Cloudant
    cant = urlparse(url)
    usrn, pw, host = cant.username, cant.password, cant.scheme + '://' + cant.netloc
    cloudant_client = Cloudant(usrn, pw, host)
    cloudant_client.connect()
    return cloudant_client

  

class DatabaseChangesMigrator:
    """
    A class to transfer and modify ALL DOCUMENTS from one database to another.
    This class can be used for copy only. Or it can be overriden to handle data
    copy with transformations.
    This class will use :
    - One thread to download data
    - One thread to transform data (can be overriden to have multiple threads)
    - At least one thread to insert data (can be overriden to have multiple threads)
    """
    nb_inserting_threads = 1
    nb_transforming_threads = 1
    logger = logging.getLogger("database-migrator")

    def __init__(self, from_db, to_db, chunk_size):
        """
        :param from_db: The database used to get changes data.
        :param to_db: The database used to insert data.
        :param chunk_size: size of chunk when downloading data.
        """
        self.to_db = to_db
        self.from_db = from_db
        self.doc_count = self.from_db.doc_count()
        self.chunk_size = chunk_size

        self.transform_queue = Queue()
        self.insert_queue = Queue()
        self.delete_queue = Queue()
        
        self.start_sequence=''
        self.end_sequence=''

        self.get_data_thread = Thread(target=self.downloading_worker)
        self.transforming_threads = [
            Thread(target=self.transforming_worker)
            for _ in range(self.nb_transforming_threads)
        ]
        self.inserting_threads = [
            Thread(target=self.inserting_worker)
            for _ in range(self.nb_inserting_threads)
        ]

    def start(self, start_sequence=None):
        """
        Start downloading, transforming and inserting.
        """
        self.start_sequence = start_sequence or self.start_sequence
        self.get_data_thread.start()
        for thread in self.transforming_threads + self.inserting_threads:
            thread.start()

    def join(self):
        """
        Wait for end of downloading, transforming and inserting.
        """
        self.get_data_thread.join()
        for thread in self.transforming_threads + self.inserting_threads:
            thread.join()

    def transform_docs(self, docs):
        """
        Default function to transform data (simply copy docs)
        This function must be overriden in case of data transformation.
        :param docs: list of doc to transform
        :return: list of transformed docs
        """
        docs = list(docs)
        out_docs = []
        out_del_docs = []
        
        for doc in docs:
            if 's' in doc.keys():
              if doc.get('_deleted', False):
                out_del_docs.append({'id': doc['_id'], 'deleted': True})
              else:
                out_docs.append({'cloudant_id': doc['s'], 'value': float(doc['v']), 'datetime': doc['dt'], 'id': doc['_id']})
        return spark.createDataFrame(out_docs, schema=schema), spark.createDataFrame(out_del_docs, schema=StructType([StructField("id", StringType(), True), StructField("deleted", BooleanType(), True)]))

    def downloading_worker(self):
        """
        Download data from database using `.all_docs()` method
        Download data by chunk of size : `self.chunk_size`
        Put downloaded chunks of data in `self.transform_queue`
        """
        chunk_index = 0
        while self.start_sequence is not None:
            self.logger.info("[{0}] Downloading data (start_seq:{1})".format(chunk_index, self.start_sequence))
            kwargs = dict(limit=self.chunk_size, include_docs=True, since=self.start_sequence)
            rows = list(self.from_db.changes(**kwargs))#.get('rows', [])
            rows = list(filter(None, rows))
            self.start_sequence = None if len(rows) < self.chunk_size else rows[-1]['seq']
            self.transform_queue.put((chunk_index, map(lambda row: row['doc'], rows)))
            chunk_index += 1
        self.end_sequence = rows[-1]['seq']
        for _ in range(self.nb_transforming_threads):
            self.transform_queue.put(None)

    def transforming_worker(self):
        """
        Worker to transform data from `transform_queue` and
        put it into `insert_queue`
        """
        while True:
            item = self.transform_queue.get()
            if item is None:
                self.transform_queue.task_done()
                break
            chunk_index, docs = item
            self.logger.info("[{0}] Transforming data".format(chunk_index))
            new_docs, del_docs = self.transform_docs(docs)
            self.insert_queue.put((chunk_index, new_docs, del_docs))
            self.transform_queue.task_done()
        for _ in range(self.nb_inserting_threads):
            self.insert_queue.put(None)

    def inserting_worker(self):
        """
        Worker to insert data from `insert_queue` in the new database.
        """
        while True:
            item = self.insert_queue.get()
            if item is None:
                self.insert_queue.task_done()
                break
            chunk_index, new_docs, del_docs = item
            self.logger.info("[{0}] Inserting data.".format(chunk_index))
            print("docs to del: {0}".format(del_docs.count()))
            print("docs to add: {0}".format(new_docs.count()))
            if del_docs.count() > 0:
              rows_to_del = self.to_db.filter(col('id').isin(list(del_docs.select('id').collect())))
              self.to_db = self.to_db.join(del_docs, self.to_db['id'] == del_docs['id'], how='left').drop(del_docs.id)
              self.to_db = self.to_db.where(col('deleted').isNotNull()).drop(del_docs.deleted)
            if new_docs.count() > 0:
              new_docs = new_docs.withColumnRenamed('value', 'changed_value').withColumnRenamed('id', 'changed_id').withColumnRenamed('datetime', 'changed_datetime').withColumnRenamed('cloudant_id', 'changed_cloudant_id')
              df_joined = self.to_db.join(new_docs, self.to_db['id'] == new_docs['changed_id'], how='full')
              self.to_db = df_joined.withColumn("updated_value", when(col("changed_value").isNull(), col('value')).otherwise(col('changed_value'))).drop(df_joined.value).drop(df_joined.changed_value).withColumnRenamed('updated_value', 'value')
              self.to_db = self.to_db.withColumn("updated_id", when(col("changed_id").isNull(), col('id')).otherwise(col('changed_id'))).drop(df_joined.id).drop(self.to_db.changed_id).withColumnRenamed('updated_id', 'id')
              self.to_db = self.to_db.withColumn("updated_cloudant_id", when(col("changed_cloudant_id").isNull(), col('cloudant_id')).otherwise(col('changed_cloudant_id'))).drop(df_joined.cloudant_id).drop(self.to_db.changed_cloudant_id).withColumnRenamed('updated_cloudant_id', 'cloudant_id')
              self.to_db = self.to_db.withColumn("updated_datetime", when(col("changed_datetime").isNull(), col('datetime')).otherwise(col('changed_datetime'))).drop(df_joined.datetime).drop(self.to_db.changed_datetime).withColumnRenamed('updated_datetime', 'datetime')
            self.insert_queue.task_done()

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

dbutils.fs.ls('/mnt/')

# COMMAND ----------

# DBTITLE 1,Mount Sink BI-models storage point
# dbutils.fs.mount(
# source = "adl://hxdatalakestorage.azuredatalakestore.net/bi-models/",
# mount_point = "/mnt/bi-models-DLGen1",
# extra_configs = configs)

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

usage_conso_table = usage_table.filter(usage_table.data_type == 'INDEX').filter(usage_table.data_sub_type == 'delta')
usage_temp_ext_table = usage_table.filter(usage_table.equipment_name =="Données météo").filter(usage_table.usage_type_name == 'HDD')
open_usage_table = usage_table.filter(usage_table.equipment_template_name.like("OPD_%"))
#usage_temp_ext_table = usage_table.filter(usage_table.data_type == 'TEMPERATURE').filter(lower(usage_table.equipment_name).like('%ext%') | lower(usage_table.equipment_template_name).like('%ext%'))

# COMMAND ----------

print("{0} usages total".format(usage_table.count()))
print("{0} consumption usages".format(usage_conso_table.count()))
print("{0} ext. temperature usages".format(usage_temp_ext_table.count()))
print("{0} OPEN usages".format(open_usage_table.count()))

# COMMAND ----------

# DBTITLE 1,Entity table 
entity = entity.join(entitytype.select('id', 'name').withColumnRenamed('name', 'entity_type'), entity.entity_type_id == entitytype.id, how='left').drop(entity.entity_type_id).drop(entitytype.id).drop(entity.slug).drop(entity.depth).drop(entity.path).drop(entity.custom_id).drop(entity.entity_dashboard_id).drop(entity.numchild).drop(entity.created_at)
print("{0} entities counted throughtout all the tenants".format(entity.count()))

# COMMAND ----------

i = 0
ent = entity.withColumnRenamed('parent_id', 'parent_id_0')
ent_full = ent
while ent_full.count() - ent_full.filter(ent_full['parent_id_'+str(i)].isNull()).count() != 0:
  ent_parent = ent_full.select(ent_full.id, ent_full.name, ent_full['parent_id_0']).withColumnRenamed('name', 'parent_name_'+str(i)).withColumnRenamed('id', 'parent_id').withColumnRenamed('parent_id_0', 'parent_id_'+str(i+1))
  ent_full = ent_full.join(ent_parent, ent_full['parent_id_'+str(i)] == ent_parent['parent_id'], how='left').drop('parent_id')
  if i > 0:
    ent_full = ent_full.drop('parent_id_' + str(i))
  i+=1
entity = ent_full.drop('parent_id_0').drop('parent_id_'+str(i))

# COMMAND ----------

entity.show(10)

# COMMAND ----------

hotel = entity.select('parent_name_0', 'parent_name_1').distinct()

# COMMAND ----------

# DBTITLE 1,Data table
usg_processed = accorinvest_cloudant[db_name]

# COMMAND ----------

last_seq = dbutils.fs.head('/mnt/bi-models-DLGen1/AccorInvest_space/last_seq.txt').split(' : ')[1]

# COMMAND ----------

schema = StructType([StructField("cloudant_id", StringType(), True), StructField("value", DoubleType(), True), StructField("datetime", StringType(), True), StructField("id", StringType(), True)])
df = spark.read.csv('/mnt/bi-models-DLGen1/AccorInvest_space/data', header=True, sep='\t', schema=schema)
CHUNK_SIZE = 100000
migrator = DatabaseChangesMigrator(accorinvest_cloudant['usages-processed-data'], df, CHUNK_SIZE)
migrator.start(start_sequence = last_seq)
migrator.join()
now = datetime.now()
str2write = now.strftime("%d/%m/%y %H.%M.%s") + ' : ' + last_seq
dbutils.fs.put('/mnt/bi-models-DLGen1/AccorInvest_space/last_seq.txt', str2write, True)

# COMMAND ----------

# DBTITLE 1,Save tables per tenant
all_data_df = migrator.to_db

print("-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-")
#print("{0} data points".format(all_data_df.count()))
all_data_df.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/AccorInvest_space/data')
all_data_df = spark.read.format('csv').options(header='true', inferschema='true', delimiter='\t').load("/mnt/bi-models-DLGen1/AccorInvest_space/data")

# COMMAND ----------

print("{0} entities".format(entity.count()))
entity.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/AccorInvest_space/entity')

# COMMAND ----------

print("{0} usages".format(usage_table.count()))
usage_table.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/AccorInvest_space/usage')

# COMMAND ----------

print("{0} usages temperature exterieure".format(usage_temp_ext_table.count()))
usage_temp_ext_table.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/AccorInvest_space/usage_temp_ext')

# COMMAND ----------

print("{0} usages consommation".format(usage_conso_table.count()))
usage_conso_table.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/AccorInvest_space/usage_conso')

# COMMAND ----------

print("{0} usages OPEN".format(open_usage_table.count()))
open_usage_table.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/AccorInvest_space/usage_open')

# COMMAND ----------

usage_conso_ids = usage_conso_table.select("cloudant_id").withColumnRenamed('cloudant_id', 'usage_cloudant_id')
data_conso = usage_conso_ids.join(all_data_df, usage_conso_ids['usage_cloudant_id']==all_data_df['cloudant_id'], how='left')
data_conso = data_conso.drop('usage_cloudant_id')#all_data_df[all_data_df.cloudant_id.isin([int(row['cloudant_id']) for row in usage_conso_tmp.select('cloudant_id').collect()])]
#print("{0} consumption data points".format(data_conso.count()))
data_conso.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/AccorInvest_space/data_conso')

# COMMAND ----------

usage_open_ids = open_usage_table.select("cloudant_id").withColumnRenamed('cloudant_id', 'usage_cloudant_id')
data_open = usage_open_ids.join(all_data_df, usage_open_ids['usage_cloudant_id']==all_data_df['cloudant_id'], how='left')
data_open = data_open.drop('usage_cloudant_id')#all_data_df[all_data_df.cloudant_id.isin([int(row['cloudant_id']) for row in usage_conso_tmp.select('cloudant_id').collect()])]
#print("{0} open data points".format(data_open.count()))
data_open.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/AccorInvest_space/data_open')

# COMMAND ----------

usage_temp_ext_ids = usage_temp_ext_table.select("cloudant_id").withColumnRenamed('cloudant_id', 'usage_cloudant_id')
data_temp_ext = usage_temp_ext_ids.join(all_data_df, usage_temp_ext_ids['usage_cloudant_id']==all_data_df['cloudant_id'], how='left')
data_temp_ext = data_temp_ext.drop('usage_cloudant_id') #data_temp_ext_tmp = all_data_df[all_data_df.cloudant_id.isin([int(row['cloudant_id']) for row in usage_temp_ext_tmp.select('cloudant_id').collect()])]
#print("{0} ext. temperature data points".format(data_temp_ext.count()))
data_temp_ext.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/AccorInvest_space/data_temp_ext')

# COMMAND ----------

print("{0} locations".format(location.count()))
location.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/AccorInvest_space/loc')

# COMMAND ----------

print("{0} hotels".format(hotel.count()))
hotel.write.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").mode('overwrite').csv('/mnt/bi-models-DLGen1/AccorInvest_space/hotel')