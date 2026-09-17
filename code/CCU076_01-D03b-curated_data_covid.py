# Databricks notebook source
# MAGIC %md # CCU076_01_D03c_curated_data_covid
# MAGIC
# MAGIC
# MAGIC **Description** This notebook creates the covid phenotypes table of CCU076_01. This notebook creates COVID variables during follow-up
# MAGIC
# MAGIC **Author(s)** Tom Bolton, Yueying Li
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on CCU004_03_D03c_curated_data_covid (Tom Bolton, Fionna Chalmers, Anna Stevenson)
# MAGIC - **Modification**
# MAGIC - Add pillar 2 dataset: Cell 8 Line 4, Cell 10, Cell 12, Cell 15, Cell 37
# MAGIC - Add another check that tabulates the overlap between the different data sources for individuals: Cell 45
# MAGIC
# MAGIC **Data Input**
# MAGIC - **`sgss`, `gdppr`, `hes_apc`, `hes_cc`, `sus`, `chess`, `pillar2`**
# MAGIC - **`codelist_covid`, `ccu076_01_cur_deaths_sing`**
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu076_01_cur_covid`** : Covid phenotypes for each person

# COMMAND ----------

spark.sql('CLEAR CACHE')
spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')

# COMMAND ----------

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import databricks.koalas as ks
import pandas as pd
import pyspark.pandas as ps
import numpy as np

import re
import io
import datetime

import matplotlib
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns

print("Matplotlib version: ", matplotlib.__version__)
print("Seaborn version: ", sns.__version__)
_datetimenow = datetime.datetime.now() # .strftime("%Y%m%d")
print(f"_datetimenow:  {_datetimenow}")

# COMMAND ----------

# MAGIC %run "/Workspace/Shared/CCU076_01/Spencers_common_functions"

# COMMAND ----------

# MAGIC %md # 0. Parameters

# COMMAND ----------

# MAGIC %run "/Workspace/Shared/CCU076_01/CCU076_01-D01-parameters"

# COMMAND ----------

# # widgets
# dbutils.widgets.removeAll()
# dbutils.widgets.text('1 project', proj)
# dbutils.widgets.text('2 cohort', cohort)
# dbutils.widgets.text('3 pipeline production date', pipeline_production_date)

# COMMAND ----------

start_date = study_start_date
#start_date = covid_start_date #ccu004-03
end_date   = study_end_date
# n_rand_part_id = 10  # number of random partition ids to use
print(start_date, end_date) # , n_rand_part_id)

# COMMAND ----------

# MAGIC %md # 1. Data

# COMMAND ----------

codelist = spark.table(path_out_codelist_covid)

sgss     = extract_batch_from_archive(parameters_df_datasets, 'sgss')
gdppr    = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
hes_apc  = extract_batch_from_archive(parameters_df_datasets, 'hes_apc')
hes_cc   = extract_batch_from_archive(parameters_df_datasets, 'hes_cc')
sus      = extract_batch_from_archive(parameters_df_datasets, 'sus')
chess    = extract_batch_from_archive(parameters_df_datasets, 'chess')
deaths   = spark.table(path_cur_deaths_sing)

pillar2  = extract_batch_from_archive(parameters_df_datasets, 'pillar2')

# COMMAND ----------

# MAGIC %md # 2. Prepare

# COMMAND ----------

# sgss
_sgss = sgss\
  .select(['PERSON_ID_DEID', 'Reporting_Lab_ID', 'Specimen_Date'])\
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')\
  .withColumnRenamed('Specimen_Date', 'DATE')\
  .where((f.col('DATE') >= start_date) & (f.col('DATE') <= end_date))\
  .dropDuplicates()

# gdppr
# omitted: 'LSOA'
_gdppr = gdppr\
  .select(['NHS_NUMBER_DEID', 'DATE', 'CODE'])\
  .withColumnRenamed('NHS_NUMBER_DEID', 'PERSON_ID')\
  .where((f.col('DATE') >= start_date) & (f.col('DATE') <= end_date))\
  .dropDuplicates()

# hes_apc
# omitted: 'DISMETH', 'DISDEST', 'DISDATE', 'SUSRECID'
_hes_apc = hes_apc\
  .select(['PERSON_ID_DEID', 'EPISTART', 'DIAG_4_01', 'DIAG_4_CONCAT', 'OPERTN_4_CONCAT', 'SUSRECID'])\
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')\
  .withColumnRenamed('EPISTART', 'DATE')\
  .where(f.col('DIAG_4_CONCAT').rlike('U07(1|2)'))\
  .where((f.col('DATE') >= start_date) & (f.col('DATE') <= end_date))\
  .dropDuplicates()

# hes_cc
_hes_cc = hes_cc\
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')\
  .withColumnRenamed('CCSTARTDATE', 'DATE')\
  .withColumn('DATE', f.to_date(f.substring('DATE', 0, 8), 'yyyyMMdd'))\
  .where((f.col('DATE') >= start_date) & (f.col('DATE') <= end_date))\
  .dropDuplicates()

# sus
_sus = sus\
  .select(['NHS_NUMBER_DEID'
    , 'EPISODE_START_DATE'
    , 'PRIMARY_PROCEDURE_DATE'
    , 'SECONDARY_PROCEDURE_DATE_1'
    , 'DISCHARGE_DESTINATION_HOSPITAL_PROVIDER_SPELL'
    , 'DISCHARGE_METHOD_HOSPITAL_PROVIDER_SPELL'
    , 'END_DATE_HOSPITAL_PROVIDER_SPELL'           
    ]\
    + [col for col in sus.columns if re.match('.*(DIAGNOSIS|PROCEDURE)_CODE.*', col)]
  )\
  .withColumnRenamed('NHS_NUMBER_DEID', 'PERSON_ID')\
  .withColumnRenamed('EPISODE_START_DATE', 'DATE')\
  .withColumn('DIAG_CONCAT', f.concat_ws(',', *[col for col in sus.columns if re.match('.*DIAGNOSIS_CODE.*', col)]))\
  .withColumn('PROCEDURE_CONCAT', f.concat_ws(',', *[col for col in sus.columns if re.match('.*PROCEDURE_CODE.*', col)]))\
  .where((f.col('DATE') >= start_date) & (f.col('DATE') <= end_date))\
  .where(\
    ((f.col('END_DATE_HOSPITAL_PROVIDER_SPELL') >= start_date) | (f.col('END_DATE_HOSPITAL_PROVIDER_SPELL').isNull()))\
    & ((f.col('END_DATE_HOSPITAL_PROVIDER_SPELL') <= end_date) | (f.col('END_DATE_HOSPITAL_PROVIDER_SPELL').isNull()))\
  )\
  .where(f.col('DATE').isNotNull())\
  .dropDuplicates()

# chess
_chess = chess\
  .select(['PERSON_ID_DEID', 'Typeofspecimen', 'Covid19', 'AdmittedToICU', 'Highflownasaloxygen', 'NoninvasiveMechanicalventilation', 'Invasivemechanicalventilation', 'RespiratorySupportECMO', 'DateAdmittedICU', 'HospitalAdmissionDate', 'InfectionSwabDate'])\
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')\
  .withColumnRenamed('InfectionSwabDate', 'DATE')\
  .where(f.col('Covid19') == 'Yes')\
  .where(\
    ((f.col('DATE') >= start_date) | (f.col('DATE').isNull()))\
    & ((f.col('DATE') <= end_date) | (f.col('DATE').isNull()))\
  )\
  .where(\
    ((f.col('HospitalAdmissionDate') >= start_date) | (f.col('HospitalAdmissionDate').isNull()))\
    & ((f.col('HospitalAdmissionDate') <= end_date) | (f.col('HospitalAdmissionDate').isNull()))\
  )\
  .where(\
    ((f.col('DateAdmittedICU') >= start_date) | (f.col('DateAdmittedICU').isNull()))\
    & ((f.col('DateAdmittedICU') <= end_date) | (f.col('DateAdmittedICU').isNull()))\
  )\
  .dropDuplicates()
  
# deaths
_deaths = deaths\
  .where((f.col('REG_DATE_OF_DEATH') >= start_date) & (f.col('REG_DATE_OF_DEATH') <= end_date))

# pillar2 - reduce, filter, add random partition column
_pillar2 = (
  pillar2
  .select(f.col('Person_ID_DEID').alias('PERSON_ID'), 'TestStartDate', 'TestResult')
  .withColumn('DATE', f.to_date(f.substring(f.col('TestStartDate'), 1, 10), 'yyyy-MM-dd'))
  .drop('TestStartDate')
  .where((f.col('DATE') >= start_date) & (f.col('DATE') <= end_date))
  .dropDuplicates()
  # .withColumn('rand_part_id', (f.rand() * n_rand_part_id).cast("int"))
)

# check
# tmpt = tab(_pillar2, 'rand_part_id'); print()

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Covid positive

# COMMAND ----------

# sgss
# note: all records are included as every record is a "positive test"
# -- TODO: wranglers please clarify whether LAB ID 840 is still the best means of identifying pillar 1 vs 2
# -- CASE WHEN REPORTING_LAB_ID = '840' THEN "pillar_2" ELSE "pillar_1" END as description,
#   .withColumn('description', f.when(f.col('Reporting_Lab_ID') == '840', 'pillar_2').otherwise('pillar_1'))\
_sgss_pos = _sgss\
  .withColumn('covid_phenotype', f.lit('01_Covid_positive_test'))\
  .withColumn('clinical_code', f.lit(''))\
  .withColumn('description', f.lit(''))\
  .withColumn('covid_status', f.lit(''))\
  .withColumn('code', f.lit(''))\
  .withColumn('source', f.lit('sgss'))\
  .select('PERSON_ID', 'DATE', 'covid_phenotype', 'clinical_code', 'description', 'covid_status', 'code', 'source')

# gdppr
# note: need to inspect and identify which are only suspected NOT confirmed!
_codelist_gdppr = codelist\
  .where(f.col('name') == 'covid19')\
  .select(['code', 'term'])

_gdppr_pos = _gdppr\
  .select(['PERSON_ID', 'DATE', 'CODE'])\
  .join(f.broadcast(_codelist_gdppr), on='code', how='inner')\
  .withColumn('covid_phenotype', f.lit('01_GP_covid_diagnosis'))\
  .withColumnRenamed('CODE', 'clinical_code')\
  .withColumnRenamed('term', 'description')\
  .withColumn('covid_status', f.lit(''))\
  .withColumn('code', f.lit('SNOMED'))\
  .withColumn('source', f.lit('gdppr'))

# note: consider replacing join with where is in and map term  

# COMMAND ----------

checks_on = False
from itertools import chain

# pillar2 lookup
pillar2_lookup = """
TestResult,Description,Status
Positive,,Positive
Void,,Unknown
SCT:1322821000000105,Severe acute respiratory syndrome coronavirus 2 antigen detection result unknown (finding),Unknown
SCT:1322781000000102,Severe acute respiratory syndrome coronavirus 2 antigen detection result positive (finding),Positive
Negative,,Negative
SCT:1321691000000102,Severe acute respiratory syndrome coronavirus 2 ribonucleic acid detection result unknown (finding),Unknown
SCT:1240581000000104,Severe acute respiratory syndrome coronavirus 2 detected (finding),Positive
SCT:1322791000000100,Severe acute respiratory syndrome coronavirus 2 antigen detection result negative (finding),Negative
SCT:1240591000000102,Severe acute respiratory syndrome coronavirus 2 not detected (finding),Negative
"""
# pillar2_lookup = spark.createDataFrame(pd.DataFrame(pd.read_csv(io.StringIO(pillar2_lookup))).astype(str))
# display(pillar2_lookup)


p2_csv = pd.read_csv(io.StringIO(pillar2_lookup))
print(p2_csv.to_string(),'\n')
dic_p2 = {}
for index, row in p2_csv.iterrows():
  key = str(row['TestResult'])
  values = {col: row[col] for col in ['Description', 'Status']}
  dic_p2[key] = values
print(dic_p2)

# ------------------------------------------------------------------------------------------
# map Description and Status
# ------------------------------------------------------------------------------------------
dic_p2_desc = {k: v['Description'] for k, v in dic_p2.items()}
dic_p2_stat = {k: v['Status'] for k, v in dic_p2.items()}
map_p2_desc = f.create_map(*[f.lit(x) for x in chain(*dic_p2_desc.items())])
map_p2_stat = f.create_map(*[f.lit(x) for x in chain(*dic_p2_stat.items())])
_pillar2_status = (
  _pillar2
  .withColumn('Description', map_p2_desc[_pillar2['TestResult']])
  .withColumn('Status', map_p2_stat[_pillar2['TestResult']])
)

# check
if checks_on:
  tmpt = tab(_pillar2_status, 'TestResult', 'Description', var2_wide=0); print()
  tmpt = tab(_pillar2_status, 'TestResult', 'Status', var2_wide=0); print()


# filter, tidy
_pillar2_pos = (
  _pillar2_status
  .where(f.col('Status') == 'Positive')
  .drop('status', 'rand_part_id')
  .withColumn('covid_phenotype', f.lit('01_Covid_positive_test_pillar2'))\
  .withColumnRenamed('TestResult', 'clinical_code')\
  .withColumnRenamed('Description', 'description')\
  .withColumn('covid_status', f.lit(''))\
  .withColumn('code', f.lit('SNOMED'))\
  .withColumn('source', f.lit('pillar2'))  
)

# COMMAND ----------

# MAGIC %md # 4. Covid admission

# COMMAND ----------

# ------------------------------------------------------------------------------
# hes_apc
# ------------------------------------------------------------------------------
# any
_hes_apc_adm_any = _hes_apc\
  .where(f.col('DIAG_4_CONCAT').rlike('U07(1|2)'))\
  .withColumn('covid_phenotype', f.lit('02_Covid_admission_any_position'))\
  .withColumn('clinical_code',\
    f.when(f.col('DIAG_4_CONCAT').rlike('U071'), 'U071')\
    .when(f.col('DIAG_4_CONCAT').rlike('U072'), 'U072')\
  )\
  .withColumn('description',\
    f.when(f.col('DIAG_4_CONCAT').rlike('U071'), 'Confirmed_COVID19')\
    .when(f.col('DIAG_4_CONCAT').rlike('U072'), 'Suspected_COVID19')\
  )\
  .withColumn('covid_status',\
    f.when(f.col('DIAG_4_CONCAT').rlike('U071'), 'confirmed')\
    .when(f.col('DIAG_4_CONCAT').rlike('U072'), 'suspected')\
  )\
  .withColumn('code', f.lit('ICD10'))\
  .withColumn('source', f.lit('hes_apc'))\
  .select('PERSON_ID', 'DATE', 'covid_phenotype', 'clinical_code', 'description', 'covid_status', 'code', 'source')

# pri
_hes_apc_adm_pri = _hes_apc\
  .where(f.col('DIAG_4_01').rlike('U07(1|2)'))\
  .withColumn('covid_phenotype', f.lit('02_Covid_admission_primary_position'))\
  .withColumn('clinical_code',\
    f.when(f.col('DIAG_4_01').rlike('U071'), 'U071')\
    .when(f.col('DIAG_4_01').rlike('U072'), 'U072')\
  )\
  .withColumn('description',\
    f.when(f.col('DIAG_4_01').rlike('U071'), 'Confirmed_COVID19')\
    .when(f.col('DIAG_4_01').rlike('U072'), 'Suspected_COVID19')\
  )\
  .withColumn('covid_status',\
    f.when(f.col('DIAG_4_01').rlike('U071'), 'confirmed')\
    .when(f.col('DIAG_4_01').rlike('U072'), 'suspected')\
  )\
  .withColumn('code', f.lit('ICD10'))\
  .withColumn('source', f.lit('hes_apc'))\
  .select('PERSON_ID', 'DATE', 'covid_phenotype', 'clinical_code', 'description', 'covid_status', 'code', 'source')


# ------------------------------------------------------------------------------
# sus
# ------------------------------------------------------------------------------
# any
_sus_adm_any = _sus\
  .where(f.col('DIAG_CONCAT').rlike('U07(1|2)'))\
  .withColumn('covid_phenotype', f.lit('02_Covid_admission_any_position'))\
  .withColumn('clinical_code',\
    f.when(f.col('DIAG_CONCAT').rlike('U071'), 'U071')\
    .when(f.col('DIAG_CONCAT').rlike('U072'), 'U072')\
  )\
  .withColumn('description',\
    f.when(f.col('DIAG_CONCAT').rlike('U071'), 'Confirmed_COVID19')\
    .when(f.col('DIAG_CONCAT').rlike('U072'), 'Suspected_COVID19')\
  )\
  .withColumn('covid_status',\
    f.when(f.col('DIAG_CONCAT').rlike('U071'), 'confirmed')\
    .when(f.col('DIAG_CONCAT').rlike('U072'), 'suspected')\
  )\
  .withColumn('code', f.lit('ICD10'))\
  .withColumn('source', f.lit('sus'))\
  .select('PERSON_ID', 'DATE', 'covid_phenotype', 'clinical_code', 'description', 'covid_status', 'code', 'source')

# pri
_sus_adm_pri = _sus\
  .where(f.col('PRIMARY_DIAGNOSIS_CODE').rlike('U07(1|2)'))\
  .withColumn('covid_phenotype', f.lit('02_Covid_admission_primary_position'))\
  .withColumn('clinical_code',\
    f.when(f.col('PRIMARY_DIAGNOSIS_CODE').rlike('U071'), 'U071')\
    .when(f.col('PRIMARY_DIAGNOSIS_CODE').rlike('U072'), 'U072')\
  )\
  .withColumn('description',\
    f.when(f.col('PRIMARY_DIAGNOSIS_CODE').rlike('U071'), 'Confirmed_COVID19')\
    .when(f.col('PRIMARY_DIAGNOSIS_CODE').rlike('U072'), 'Suspected_COVID19')\
  )\
  .withColumn('covid_status',\
    f.when(f.col('PRIMARY_DIAGNOSIS_CODE').rlike('U071'), 'confirmed')\
    .when(f.col('PRIMARY_DIAGNOSIS_CODE').rlike('U072'), 'suspected')\
  )\
  .withColumn('code', f.lit('ICD10'))\
  .withColumn('source', f.lit('sus'))\
  .select('PERSON_ID', 'DATE', 'covid_phenotype', 'clinical_code', 'description', 'covid_status', 'code', 'source')


# ------------------------------------------------------------------------------
# chess
# ------------------------------------------------------------------------------
_chess_adm = _chess\
  .select(['PERSON_ID', f.col('HospitalAdmissionDate').alias('DATE')])\
  .withColumn('covid_phenotype', f.lit('02_Covid_admission_any_position'))\
  .withColumn('clinical_code', f.lit(''))\
  .withColumn('description', f.lit('HospitalAdmissionDate IS NOT null'))\
  .withColumn('covid_status', f.lit('confirmed'))\
  .withColumn('code', f.lit(''))\
  .withColumn('source', f.lit('chess'))

# COMMAND ----------

# MAGIC %md # 5. Covid critical care

# COMMAND ----------

# MAGIC %md ## 5.1 ICU

# COMMAND ----------

# display(_hes_cc)

# COMMAND ----------

# ------------------------------------------------------------------------------
# chess
# ------------------------------------------------------------------------------
_chess_icu = _chess\
  .where(f.col('DateAdmittedICU').isNotNull())\
  .select(['PERSON_ID', 'DateAdmittedICU'])\
  .withColumnRenamed('DateAdmittedICU', 'DATE')\
  .withColumn('covid_phenotype', f.lit('03_ICU_admission'))\
  .withColumn('clinical_code', f.lit(''))\
  .withColumn('description', f.lit('DateAdmittedICU IS NOT null'))\
  .withColumn('covid_status', f.lit('confirmed'))\
  .withColumn('code', f.lit(''))\
  .withColumn('source', f.lit('chess'))


# ------------------------------------------------------------------------------
# hes_cc     #ccu004-03
# ------------------------------------------------------------------------------
_hes_icu = _hes_cc\
  .where(f.col('DATE').isNotNull())\
  .select(['PERSON_ID', 'DATE', 'SUSRECID'])\
  .withColumn('covid_phenotype', f.lit('03_ICU_admission'))\
  .withColumn('clinical_code', f.lit(''))\
  .withColumn('description', f.lit('id is in hes_cc table'))\
  .withColumn('covid_status', f.lit('confirmed'))\
  .withColumn('code', f.lit(''))\
  .withColumn('source', f.lit('HES CC'))

_hes_apc2 = _hes_apc\
  .withColumnRenamed('PERSON_ID', 'PERSON_ID2')\
  .withColumnRenamed('DATE', 'DATE2')\

_hes_icu = _hes_icu\
  .where(f.col('BESTMATCH')==1)\
  .join(_hes_apc2, on='SUSRECID', how='inner')\
  .dropDuplicates()

columns_to_drop = ['PERSON_ID2', 'DATE2'] 
_hes_icu = _hes_icu.drop(*columns_to_drop)

# #HES_CC
# #ID is in HES_CC AND has U071 or U072 from HES_APC 
# spark.sql(f"""
# CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}cc_covid as
# SELECT apc.person_id_deid, cc.date,
# '03_ICU_admission' as covid_phenotype,
# "" as clinical_code,
# "id is in hes_cc table" as description,
# "confirmed" as covid_status,
# "" as code,
# 'HES CC' as source, cc.date_is, BRESSUPDAYS, ARESSUPDAYS
# FROM {collab_database_name}.{project_prefix}{temp_hes_apc} as apc
# INNER JOIN {collab_database_name}.{project_prefix}{temp_hes_cc} AS cc
# ON cc.SUSRECID = apc.SUSRECID
# WHERE cc.BESTMATCH = 1
# AND (DIAG_4_CONCAT LIKE '%U071%' OR DIAG_4_CONCAT LIKE '%U072%') """)





# COMMAND ----------

# display(_hes_icu)

# COMMAND ----------

# display(_chess_icu)

# COMMAND ----------

_hes_icu = _hes_icu\
  .select(['PERSON_ID', 'DATE', 'covid_phenotype', 'clinical_code', 'description', 'covid_status', 'code', 'source'])

# COMMAND ----------

# TBC

# COMMAND ----------

# MAGIC %md ## 5.2 NIV

# COMMAND ----------

# TBC

# COMMAND ----------

# MAGIC %md ## 5.3 IMV

# COMMAND ----------

# TBC

# COMMAND ----------

# MAGIC %md ## 5.4 ECMO

# COMMAND ----------

# TBC

# COMMAND ----------

# MAGIC %md # 6. Covid death

# COMMAND ----------

# TBC

# COMMAND ----------

# MAGIC %md # 7. Covid severity

# COMMAND ----------

# TBC

# COMMAND ----------

# MAGIC %md # 8. Combine

# COMMAND ----------

tmp = (
  _sgss_pos
  .unionByName(_gdppr_pos)
  .unionByName(_pillar2_pos)
  .unionByName(_sus_adm_any)
  .unionByName(_sus_adm_pri)
  .unionByName(_hes_apc_adm_any)
  .unionByName(_hes_apc_adm_pri)
  .unionByName(_chess_adm)
  .unionByName(_chess_icu)
  .unionByName(_hes_icu)
)

# COMMAND ----------

# MAGIC %md # 9. Save

# COMMAND ----------

save_table(df=tmp, out_name=f'{proj}_cur_covid', save_previous=False)

# partition
tmp = spark.table(f'{dsa}.{proj}_cur_covid')    
tmp.write.partitionBy('source', 'covid_phenotype').mode("overwrite").option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_cur_covid')

# COMMAND ----------

# MAGIC %md # 10. Check

# COMMAND ----------

# read
tmp = spark.table(f'{dsa}.{proj}_cur_covid')  

# COMMAND ----------

display(tmp)

# COMMAND ----------

# check combined
count_var(tmp, 'PERSON_ID'); print()
tmpt = tab(tmp, 'covid_phenotype', 'source', var2_unstyled=1); print()
# tmp1 = tmp.withColumn('source_pheno', f.concat_ws('_', f.col('source'), f.col('covid_phenotype')))
tmpt = tabstat(tmp, 'DATE', byvar=['source', 'covid_phenotype'], date=1); print()

# COMMAND ----------

# check individual
tmpt = tab(tmp, 'source', 'covid_phenotype', var2_wide=0)

# checks
for index, row in tmpt.iterrows():
  val_source = str(row['source'])
  val_covid_phenotype = str(row['covid_phenotype'])
  print(index, val_source, val_covid_phenotype)
  tmpc = (
      tmp
      .where(f.col('source') == val_source)
      .where(f.col('covid_phenotype') == val_covid_phenotype)
  )
  count_var(tmpc, 'PERSON_ID'); print()


# COMMAND ----------

tmp1 = (
  tmp
  .select('PERSON_ID', 'source')  
  .dropDuplicates()
  .withColumn('all1', f.lit(1))
  .groupBy('PERSON_ID')
  .pivot('source')
  .agg(f.first(f.col('all1')))  
)
cols_list = [_ for _ in tmp1.columns if _ not in ['PERSON_ID']]
print(cols_list)
tmp1 = (
  tmp1
  .na.fill(0, subset=cols_list)
  .withColumn('concat', f.concat(*cols_list))
)
tmpt = tab(tmp1, 'concat'); print()

# COMMAND ----------

tmpt1 = tmpt[~tmpt.index.isin(['Total'])].copy()
tmpt1 = tmpt1.sort_values(by='PCT', ascending=False)
tmpt1['PCT_cumsum'] = round(tmpt1['PCT'].cumsum(),2)
print(cols_list)
display(tmpt1)