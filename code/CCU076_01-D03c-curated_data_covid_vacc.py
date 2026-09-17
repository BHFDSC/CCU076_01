# Databricks notebook source
# MAGIC %md # CCU076_01-D03c-curated_data_covid_vacc
# MAGIC
# MAGIC **Description** This notebook produces a curated and streamlined version of the vaccine_status table. 
# MAGIC
# MAGIC **Author(s)** Thomas Bolton (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Date code last updated** 2024-08-19
# MAGIC
# MAGIC **Data input** 
# MAGIC * []([url](url))`.vaccine_status__archive` (where `archived_on` == YYYY-MM-DD)
# MAGIC * `codelist_vacc_procedure.csv`
# MAGIC * `codelist_vacc_product.csv`
# MAGIC
# MAGIC **Data output** 
# MAGIC * `cur_covid_vacc`
# MAGIC
# MAGIC **Runtime**
# MAGIC
# MAGIC Note: Runtime tests were performed on a quiet cluster.
# MAGIC
# MAGIC * ~18 mins if not running any checks (i.e., all [`run_level_[123x]_checks`] set to False)
# MAGIC * ~20 mins if only running level 1 checks (i.e., only `run_level_1_checks` set to True, others set to False)
# MAGIC * ~35 mins if running all level checks (i.e., all set to True)
# MAGIC
# MAGIC
# MAGIC
# MAGIC **Notes**
# MAGIC * **HDS team to create a monthly updated curated asset for this table**
# MAGIC
# MAGIC * The table is in long format with multiple rows for each individual representing vaccination doses. 
# MAGIC
# MAGIC * Lookup tables are used to map procedure and product codes to descriptions and categorisations.
# MAGIC
# MAGIC * Quality assurance exclusions and flags have been applied that are consistent with the SAIL Research Ready Data Asset for COVID-19 Vaccination Data (RRDA_CVVD). To avoid duplication, please refer to the dictionary of quality assurance rules below and the summary table of descriptions and exclusion/flag status.
# MAGIC
# MAGIC * After quality assurance exclusions the table is uniquely determined by PERSON_ID and DATE
# MAGIC
# MAGIC * A pragmatic approach for exluding records failing quality assurance rules has been adopted. For example, keeping the earliest record for duplicates on PERSON_ID and PROCEDURE (recognising and accepting that this may not be optimal for all procedures and individuals). A more involved and complex algorithmic approach could be used to identify the most appropriate record with reference to other records for the individual. However, the latter approach is more time consuming and may still not provide a satisfactory result for the relatively small number of duplicate records identified. 
# MAGIC
# MAGIC
# MAGIC **Final streamlined table structure**
# MAGIC
# MAGIC |column_name | column_description |
# MAGIC |----------------|--------------------|
# MAGIC |PERSON_ID | Person identifier |
# MAGIC |DATE | Date of vaccination |
# MAGIC |PROCEDURE_CAT | Vaccination procedure (i.e., first dose ['1'], second dose ['2'], booster dose ['B']) corresponding to SNOMED coded VACCINATION_PROCEDURE_CODE |
# MAGIC |PRODUCT_CAT | Vaccination product (i.e., 'AstraZeneca', 'Pfizer', 'Moderna', ...) corresponding to SNOMED coded VACCINE_PRODUCT_CODE |

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import databricks.koalas as ks
import pandas as pd
import numpy as np

import re
import io
import datetime

import matplotlib
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns

from itertools import chain

print("Matplotlib version: ", matplotlib.__version__)
print("Seaborn version: ", sns.__version__)
_datetimenow = datetime.datetime.now() # .strftime("%Y%m%d")
print(f"_datetimenow:  {_datetimenow}")

# COMMAND ----------

# DBTITLE 1,Functions - Common
# MAGIC %run "/Shared/SHDS/common/functions"

# COMMAND ----------

# DBTITLE 1,Functions - Notebook
# simple functions
def count_var_sp(df, var, var_out, i, qa, desc):
  df_out = (
    df
    .select(var)
    .groupBy()
    .agg(
      f.count(f.lit(1)).alias('n')
      , f.count(f.col(var)).alias(f'n_{var_out}')
      , f.countDistinct(f.col(var)).alias(f'n_{var_out}_distinct')
    )
    .withColumn('i', f.lit(i))
    .withColumn('qa', f.lit(qa))
    .withColumn('desc', f.lit(desc))
    .select('i', 'qa', 'desc', 'n', 'n_id', 'n_id_distinct')
  )    
  return df_out

def flow_diff(df):
  r = df.copy()
  for col in ['n', 'n_id', 'n_id_distinct']:
      r[col] = df[col].astype(int)
      diff_col = f'{col}_diff'
      r[diff_col] = (r[col] - r[col].shift(1)).fillna(0).astype(int)
  return r

def flow_format(df):
  r = df.copy()
  for col in ['n', 'n_id', 'n_id_distinct', 'n_diff', 'n_id_diff', 'n_id_distinct_diff']:
    r[col] = df[col].map('{:,.0f}'.format).astype(str)
  return r # .set_index('indx')

# COMMAND ----------

# MAGIC %md # Parameters

# COMMAND ----------

# MAGIC %run "./CCU076_01-D01-parameters"

# COMMAND ----------

path_project = re.search('(.*)\/',dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()).group(1)

run_level_1_checks = True 
run_level_2_checks = False
run_level_3_checks = False
run_level_x_checks = False

archived_on = parameters_df_datasets.loc[parameters_df_datasets['dataset'] == 'vacc', 'archived_on'].values[0]

# COMMAND ----------

# MAGIC %md # Data

# COMMAND ----------

# vaccine_status
vacc = extract_batch_from_archive(parameters_df_datasets, 'vacc')

# lookup tables (providing descriptions and categorisations of SNOMED codes)
procedure_csv = pd.read_csv('/Workspace/Shared/CCU076_01/Codelists/codelist_vacc_procedure.csv', dtype=str)
product_csv = pd.read_csv('/Workspace/Shared/CCU076_01/Codelists//codelist_vacc_product.csv', dtype=str).fillna('').astype(str)

# COMMAND ----------

# MAGIC %md # Checks

# COMMAND ----------

# MAGIC %md ## Displays

# COMMAND ----------

if run_level_1_checks:
  display(vacc.orderBy('PERSON_ID_DEID', 'DATE_AND_TIME'))
  display(procedure_csv.sort_values('PROCEDURE_CODE'))
  display(product_csv.sort_values('PRODUCT_CODE'))

# COMMAND ----------

# MAGIC %md ## Lookup tables

# COMMAND ----------

# mandatory check:

# check that the lookup tables include all of the codes that are present in the vaccine_status table
# merge function also checks that the (PROCEDURE|PRODUCT)_CODE is unique 

print('check PROCEDURE')
tmp_proc = (
  vacc
  .groupBy('VACCINATION_PROCEDURE_CODE')
  .agg(
    f.count(f.lit(1)).alias('n')
    , f.count(f.col('PERSON_ID_DEID')).alias('n_id') 
    , f.countDistinct(f.col('PERSON_ID_DEID')).alias('n_id_distinct') 
  )
  .withColumnRenamed('VACCINATION_PROCEDURE_CODE', 'PROCEDURE_CODE')
  .toPandas()
)
tmp_proc_1 = pd.merge(tmp_proc, procedure_csv, on='PROCEDURE_CODE', how='left', validate='1:1', indicator=True)
display(tmp_proc_1.sort_values('PROCEDURE_CAT'))
assert (tmp_proc_1['_merge'] == 'both').all(), "  Not all values in the _merge column are 'both' - please add PROCEDURE_CODE (_DESC, _CAT) to the PROCEDURE.csv lookup table\n"


print('check PRODUCT')
tmp_prod = (
  vacc
  .groupBy('VACCINE_PRODUCT_CODE')
  .agg(
    f.count(f.lit(1)).alias('n')
    , f.count(f.col('PERSON_ID_DEID')).alias('n_id')
    , f.countDistinct(f.col('PERSON_ID_DEID')).alias('n_id_distinct')
  )
  .withColumnRenamed('VACCINE_PRODUCT_CODE', 'PRODUCT_CODE')
  .toPandas()
)
tmp_prod_1 = pd.merge(tmp_prod, product_csv, on='PRODUCT_CODE', how='left', validate='1:1', indicator=True)
display(tmp_prod_1.sort_values(by=['n', 'PRODUCT_CODE'], ascending=[False, True]))
assert (tmp_prod_1['_merge'] == 'both').all(), "  Not all values in the _merge column are 'both' - please add PRODUCT_CODE (_DESC, _CAT) to the PRODUCT.csv lookup table\n"

# check number of unknown procedure
tmp_proc_unk_n = tmp_proc_1[tmp_proc_1['PROCEDURE_CAT'] == 'Unknown'][['n']].sum()['n']
assert tmp_proc_unk_n <= 50, 'The number of records with Unknown PROCEDURE_CAT is >50, consider whether it is still appropriate to assume that these relate to Boosters'

# TODO add code that provides user with the description of the SNOMED code, which can be added to the csv. Meaning that the users only needs to add the _CAT
# These csv files should be stored centrally and updated by our team when the vaccination table becomes a curated asset

# COMMAND ----------

# MAGIC %md ## Exploratory (optional, time-consuming)

# COMMAND ----------

# Note 1
# Advised by NHS England Data Wrangler Team to exclude records where VACCINATION_SITUATION_CODE == '1324751000000103' (/ not null)
# checked that VACCINATION_SITUATION_CODE only takes known values in checks below before excluding as part of quality assurance

# Note 2
# NOT_GIVEN==FALSE for all, but REASON_NOT_GIVEN_CODE exists for a small number
# Advised by NHS England Data Wrangler Team to exclude records where REASON_NOT_GIVEN_CODE is not null
# checked that REASON_NOT_GIVEN_CODE only takes known values in checks below before excluding as part of quality assurance

# Note 3
# Advised by NHS England Data Wrangler Team to use DATE as primary source and to use RECORDED_DATE where DATE is missing or out of range (before 20201208 - start of vaccination programme)

# Note 4
# Advised by NHS England Data Wrangler Team to use assume that the small number of Unknown PROCEDURE_CAT relate to booster doses as these appeared after dose 1 and 2 waves

# potentially helpful checks to run if working with a new monthly batch, but time consuming, hence highest level of checks (run_level_x_checks)
if run_level_x_checks:

  print('---------------------------------------------------------------------------------')
  print('Shape')
  print('---------------------------------------------------------------------------------')
  count_var(vacc, 'PERSON_ID_DEID'); print()
  print(len(vacc.columns)); print()
  print(pd.DataFrame({f'_cols': vacc.columns}).to_string()); print()


  print('---------------------------------------------------------------------------------')
  print('Number of rows per PERSON_ID_DEID')
  print('---------------------------------------------------------------------------------')
  win_rownum = Window.partitionBy('PERSON_ID_DEID').orderBy('DATE_AND_TIME')
  win_rownummax = Window.partitionBy('PERSON_ID_DEID')
  tmp = (
    vacc
    .withColumn('rownum', f.row_number().over(win_rownum))
    .withColumn('rownummax', f.count(f.lit(1)).over(win_rownummax))
  )      
  tmpt = tab(tmp.where(f.col('rownum') == 1), 'rownummax'); print()


  print('--------------------------------------------------------------------------------')
  print('check format of DATE_AND_TIME before below substring and reformat')
  print('--------------------------------------------------------------------------------')
  tmp = (
    vacc
    .withColumn('DATE_AND_TIME_check', 
                f.when(f.col('DATE_AND_TIME').rlike(r'^\d{8}T\d{6}00$'), f.lit(1)) #yyyyMMTddHHmmss00
                .when(f.col('DATE_AND_TIME').isNull(), f.lit(2))
                .otherwise(0)))
  tmpt = tab(tmp, 'DATE_AND_TIME_check'); print()
  assert tmp.select('DATE_AND_TIME_check').where(f.col('DATE_AND_TIME_check') == 0).count() == 0


  print('---------------------------------------------------------------------------------')
  print('Number of rows per PERSON_ID_DEID, DATE')
  print('---------------------------------------------------------------------------------')
  tmp = (
    vacc
    .withColumn('DATE', f.to_date(f.substring(f.col('DATE_AND_TIME'), 1, 8), 'yyyyMMdd'))
  )

  # check
  count_varlist(tmp, ['PERSON_ID_DEID', 'DATE']); print()

  win_rownum = Window.partitionBy('PERSON_ID_DEID', 'DATE').orderBy('DATE_AND_TIME')
  win_rownummax = Window.partitionBy('PERSON_ID_DEID', 'DATE')
  tmp = (
    vacc
    .withColumn('DATE', f.to_date(f.substring(f.col('DATE_AND_TIME'), 1, 8), 'yyyyMMdd'))
    .withColumn('rownum', f.row_number().over(win_rownum))
    .withColumn('rownummax', f.count(f.lit(1)).over(win_rownummax))
  )

  # check
  tmpt = tab(tmp.where(f.col('rownum') == 1), 'rownummax'); print()
          
      
  print('---------------------------------------------------------------------------------')
  print('UNIQUE_ID row number')
  print('---------------------------------------------------------------------------------')
  # check 
  count_var(vacc, 'UNIQUE_ID'); print()       

  win_rownum = Window.partitionBy('UNIQUE_ID').orderBy('PERSON_ID_DEID')
  win_rownummax = Window.partitionBy('UNIQUE_ID')

  tmp = (
    vacc
    .withColumn('rownum', f.row_number().over(win_rownum))
    .withColumn('rownummax', f.count(f.lit(1)).over(win_rownummax))
  )

  # check
  tmpt = tab(tmp.where(f.col('rownum') == 1), 'rownummax'); print()
  # => UNIQUE_ID is not unique


  print('--------------------------------------------------------------------------------')
  print('check single value data fields')
  print('--------------------------------------------------------------------------------')
  tmpt = tab(vacc, 'PRIMARY_SOURCE'); print()
  tmpt = tab(vacc, 'NOT_GIVEN'); print()
  tmpt = tab(vacc, 'TRACE_VERIFIED'); print()
  #assert vacc.select('PRIMARY_SOURCE').distinct().count() == 1
  assert vacc.select('NOT_GIVEN').distinct().count() == 1
  assert vacc.select('TRACE_VERIFIED').distinct().count() == 1
  # => no need to keep these columns with a single value


  print('--------------------------------------------------------------------------------')
  print('check DOSE_SEQUENCE')
  print('--------------------------------------------------------------------------------')
  tmpt = tab(vacc, 'DOSE_SEQUENCE'); print()
  tmp = (
    vacc
    .withColumn('DOSE_SEQUENCE_check', 
                f.when(f.col('DOSE_SEQUENCE').isin([1,2]), f.lit(1))
                .when(f.col('DOSE_SEQUENCE').isNull(), f.lit(2))
                .otherwise(0)
              )
  ) 
  tmpt = tab(tmp, 'DOSE_SEQUENCE', 'DOSE_SEQUENCE_check'); print()
  assert tmp.select('DOSE_SEQUENCE_check').where(f.col('DOSE_SEQUENCE_check') == 0).count() == 0

  # check cross-tabulation with DOSE_SEQUENCE
  tmp = (
    vacc
    .groupBy('VACCINATION_PROCEDURE_CODE', 'DOSE_SEQUENCE')
    .agg(f.count(f.lit(1)).alias('n'))  
    .withColumnRenamed('VACCINATION_PROCEDURE_CODE', 'PROCEDURE_CODE')
    .toPandas()    
  )
  tmp = pd.merge(tmp, procedure_csv, on='PROCEDURE_CODE', how='left', validate='m:1', indicator=True)
  display(tmp.sort_values(['DOSE_SEQUENCE', 'PROCEDURE_CAT']))


  print('--------------------------------------------------------------------------------')
  print('check VACCINATION_SITUATION_CODE')
  print('--------------------------------------------------------------------------------')
  # 1324741000000101 'first dose declined' 
  # 1324751000000103 'second dose declined' - 'Severe acute respiratory syndrome coronavirus 2 vaccination second dose declined (situation)'
  tmpt = tab(vacc, 'VACCINATION_SITUATION_CODE'); print()

  # check that VACCINATION_SITUATION_CODE only takes known values 
  tmpf = (
    vacc
    .withColumn('VACCINATION_SITUATION_CODE_check', 
                f.when(f.col('VACCINATION_SITUATION_CODE').isin(['1324751000000103']), f.lit(1))
                .when(f.col('VACCINATION_SITUATION_CODE').isNull(), f.lit(2))
                .otherwise(0)
              )  
  )
  tmpt = tab(tmpf, 'VACCINATION_SITUATION_CODE', 'VACCINATION_SITUATION_CODE_check'); print()
  assert tmpf.where(f.col('VACCINATION_SITUATION_CODE_check') == 0).count() == 0


  print('--------------------------------------------------------------------------------')
  print('check REASON_NOT_GIVEN_CODE')
  print('--------------------------------------------------------------------------------')
  # 1324721000000108 'Severe acute respiratory syndrome coronavirus 2 vaccination dose declined (situation)'
  # 1324731000000105 'Severe acute respiratory syndrome coronavirus 2 immunisation course not indicated (situation)'
  # 1324761000000100 'Severe acute respiratory syndrome coronavirus 2 immunisation course contraindicated (situation)'
  # 1324861000000109 'Severe acute respiratory syndrome coronavirus 2 immunisation course abandoned (situation)'
  # 213257006 'Generally unwell (finding)'
  # 310376006 'Immunization consent not given (finding)'
  tmpt = tab(vacc, 'REASON_NOT_GIVEN_CODE'); print()

  # check that REASON_NOT_GIVEN_CODE only takes known values 
  tmpf = (
    vacc
    .withColumn('REASON_NOT_GIVEN_CODE_check', 
                f.when(f.col('REASON_NOT_GIVEN_CODE').isin([
                  '1324721000000108'
                , '1324731000000105'
                , '1324761000000100'
                , '1324861000000109'
                , '213257006'
                , '310376006']), f.lit(1))
              .when(f.col('REASON_NOT_GIVEN_CODE').isNull(), f.lit(2))
              .otherwise(0)
              )
  )
  tmpt = tab(tmpf, 'REASON_NOT_GIVEN_CODE', 'REASON_NOT_GIVEN_CODE_check'); print()
  assert tmpf.where(f.col('REASON_NOT_GIVEN_CODE_check') == 0).count() == 0


  print('--------------------------------------------------------------------------------')
  print('check cross-tabulations')
  print('--------------------------------------------------------------------------------')
  tmpt = tab(vacc, 'REASON_NOT_GIVEN_CODE', 'VACCINATION_SITUATION_CODE'); print()
  tmpt = tab(vacc, 'DOSE_SEQUENCE', 'VACCINATION_SITUATION_CODE'); print()
  tmpt = tab(vacc, 'VACCINATION_PROCEDURE_CODE', 'VACCINATION_SITUATION_CODE'); print()


  print('--------------------------------------------------------------------------------')
  print('check NHS_NUMBER_STATUS_INDICATOR_CODE')
  print('--------------------------------------------------------------------------------')
  tmpt = tab(vacc, 'NHS_NUMBER_STATUS_INDICATOR_CODE'); print()


  print('---------------------------------------------------------------------------------')
  print('compare DATE (derived from DATE_AND_TIME) and RECORDED_DATE')
  print('---------------------------------------------------------------------------------')
  # DATE_AND_TIME	- The date and time on which the vaccination intervention was carried out or was meant to be administered
  # RECORDED_DATE	- The date that the vaccination administered (procedure) or not administered (situation) was recorded in the source system
  tmp = (
    vacc
    .withColumn('DATE', f.to_date(f.substring(f.col('DATE_AND_TIME'), 1, 8), 'yyyyMMdd'))
    .withColumn('RECORDED_DATE', f.to_date(f.col('RECORDED_DATE'), 'yyyyMMdd'))  
    .withColumn('DATE_notNull', f.when(f.col('DATE').isNotNull(), 1).otherwise(0))
    .withColumn('RECORDED_DATE_notNull', f.when(f.col('RECORDED_DATE').isNotNull(), 1).otherwise(0))
    .withColumn('DATE_lt_20201208', f.when(f.col('DATE') < f.to_date(f.lit('2020-12-08')), 1).otherwise(0))
    .withColumn('RECORDED_DATE_lt_20201208', f.when(f.col('RECORDED_DATE') < f.to_date(f.lit('2020-12-08')), 1).otherwise(0))
    .withColumn('diff', f.datediff(f.col('DATE'), f.col('RECORDED_DATE')))
  )

  # check
  tmpt = tabstat(tmp, 'DATE', date=1); print()
  tmpt = tabstat(tmp, 'RECORDED_DATE', date=1); print()
  tmpt = tab(tmp, 'DATE_notNull', 'RECORDED_DATE_notNull'); print()
  tmpt = tab(tmp, 'DATE_lt_20201208', 'RECORDED_DATE_lt_20201208'); print()
  tmpt = tab(tmp.where(f.col('DATE_lt_20201208') != f.col('RECORDED_DATE_lt_20201208')), 'DATE_notNull', 'RECORDED_DATE_notNull'); print()
  tmpt = tabstat(tmp, 'diff'); print()
  tmpt = tab(tmp, 'diff'); print()

  # Summary
  # DATE provides 19k values when RECORDED_DATE is null
  # DATE contains 11k out of range dates when RECORDED_DATE is within range
  # DATE and RECORDED_DATE have 93% same day agreement, 98% within 1 week
  # DATE is almost always before RECORDED_DATE (suggesting that RECORDED_DATE was the date the vaccination was later recorded in the system)

# COMMAND ----------

# MAGIC %md # Prepare

# COMMAND ----------

# MAGIC %md ## Select and reformat columns

# COMMAND ----------

# select and rename columns 
# reformat date and time columns
# create date
vacc_1 = (
  vacc
  .select(
    f.col('PERSON_ID_DEID').alias('PERSON_ID')
    , 'DATE_AND_TIME'
    , 'RECORDED_DATE'
    , 'DOSE_SEQUENCE'
    , f.col('VACCINATION_PROCEDURE_CODE').alias('PROCEDURE_CODE')
    , f.col('VACCINE_PRODUCT_CODE').alias('PRODUCT_CODE')
    , 'REASON_NOT_GIVEN_CODE'
    , f.col('VACCINATION_SITUATION_CODE').alias('SITUATION_CODE')
  )
  .withColumn('DATE_AND_TIME_formatted', f.to_timestamp(f.substring(f.col('DATE_AND_TIME'), 1, 17), "yyyyMMdd'T'HHmmssSS"))
  .withColumn('DATE_tmp', f.to_date(f.substring(f.col('DATE_AND_TIME'), 1, 8), 'yyyyMMdd'))
  .withColumn('TIME', f.substring(f.col('DATE_AND_TIME'), 10, 15))
  .withColumn('RECORDED_DATE', f.to_date(f.col('RECORDED_DATE'), 'yyyyMMdd'))  
  .withColumn('DATE_flag', 
    f.when(f.col('DATE_tmp').isNull(), f.lit(1))
    .when((f.col('DATE_tmp') < f.to_date(f.lit('2020-12-08'))) & (f.col('RECORDED_DATE') >= f.to_date(f.lit('2020-12-08'))), f.lit(2))
    .otherwise(f.lit(3))
  )  
  .withColumn('DATE', 
    f.when(f.col('DATE_tmp').isNull(), f.col('RECORDED_DATE'))
    .when((f.col('DATE_tmp') < f.to_date(f.lit('2020-12-08'))) & (f.col('RECORDED_DATE') >= f.to_date(f.lit('2020-12-08'))), f.col('RECORDED_DATE'))
    .otherwise(f.col('DATE_tmp'))
  )    
)

# check
if run_level_2_checks:
  tmpt = tab(vacc_1, 'DATE_flag'); print()
  tmpt = tabstat(vacc_1, 'DATE', date=1); print()
  count_var(vacc_1, 'PERSON_ID'); print()
  count_varlist(vacc_1, ['PERSON_ID', 'DATE']); print()
  
# tidy
vacc_1 = (
  vacc_1
  .select('PERSON_ID', 'DATE', 'TIME', 'PROCEDURE_CODE', 'PRODUCT_CODE', 'SITUATION_CODE', 'REASON_NOT_GIVEN_CODE')
)

# temp save
vacc_1 = temp_save(df=vacc_1, out_name=f'{proj}_tmp_vacc_1'); print()

# check
if run_level_1_checks:
  display(vacc_1.orderBy('PERSON_ID', 'DATE', 'TIME'))

# COMMAND ----------

# MAGIC %md ## Map lookup tables

# COMMAND ----------

# ------------------------------------------------------------------------------------------
# map PROCEDURE
# ------------------------------------------------------------------------------------------
# dictionary for procedure_csv
procedure_dict = {}
for index, row in procedure_csv.iterrows():
  key = str(row['PROCEDURE_CODE'])
  values = {col: row[col] for col in ['PROCEDURE_DESC', 'PROCEDURE_CAT']}
  procedure_dict[key] = values
# print(procedure_dict,'\n') 

procedure_dict_cat = {k: v['PROCEDURE_CAT'] for k, v in procedure_dict.items()}
procedure_dict_desc = {k: v['PROCEDURE_DESC'] for k, v in procedure_dict.items()}
procedure_map_cat = f.create_map(*[f.lit(x) for x in chain(*procedure_dict_cat.items())])
procedure_map_desc = f.create_map(*[f.lit(x) for x in chain(*procedure_dict_desc.items())])
vacc_1 = (
  vacc_1
  .withColumn('PROCEDURE_CAT', procedure_map_cat[vacc_1['PROCEDURE_CODE']])
  .withColumn('PROCEDURE_DESC', procedure_map_desc[vacc_1['PROCEDURE_CODE']])
)

# check
if run_level_3_checks:
  tmp = (
    vacc_1
    .groupBy('PROCEDURE_CODE', 'PROCEDURE_DESC', 'PROCEDURE_CAT')
    .agg(
      f.count(f.lit(1)).alias('n')
      , f.count(f.col('PERSON_ID')).alias('n_id')
      , f.countDistinct(f.col('PERSON_ID')).alias('n_id_distinct')
    )
  )
  display(tmp.orderBy('PROCEDURE_CAT'))


# ------------------------------------------------------------------------------------------
# map PRODUCT
# ------------------------------------------------------------------------------------------
# dictionary for product_csv
product_dict = {}
for index, row in product_csv.iterrows():
  key = str(row['PRODUCT_CODE'])
  values = {col: row[col] for col in ['PRODUCT_DESC', 'PRODUCT_CAT']}
  product_dict[key] = values
# print(product_dict,'\n')

product_dict_cat = {k: v['PRODUCT_CAT'] for k, v in product_dict.items()}
product_dict_desc = {k: v['PRODUCT_DESC'] for k, v in product_dict.items()}
product_map_cat = f.create_map(*[f.lit(x) for x in chain(*product_dict_cat.items())])
product_map_desc = f.create_map(*[f.lit(x) for x in chain(*product_dict_desc.items())])
vacc_1 = (
  vacc_1
  .withColumn('PRODUCT_CAT', product_map_cat[vacc_1['PRODUCT_CODE']])
  .withColumn('PRODUCT_DESC', product_map_desc[vacc_1['PRODUCT_CODE']])
)

# check
if run_level_3_checks:
  tmp = (
    vacc_1
    .groupBy('PRODUCT_CODE', 'PRODUCT_DESC', 'PRODUCT_CAT')
    .agg(
      f.count(f.lit(1)).alias('n')
      , f.count(f.col('PERSON_ID')).alias('n_id')
      , f.countDistinct(f.col('PERSON_ID')).alias('n_id_distinct')
    )
  )
  display(tmp.orderBy(f.desc('n'), 'PRODUCT_CODE'))


# check
if run_level_2_checks:
  display(vacc_1.orderBy('PERSON_ID', 'DATE', 'TIME'))  

# COMMAND ----------

# MAGIC %md ## Set small number of Unknown PROCEDURE_CAT to Booster

# COMMAND ----------

# check
if run_level_3_checks:
  tmpt = tab(vacc_1, 'PROCEDURE_CAT'); print()

# set the small number of Unknown PROCEDURE_CAT to B following advice from the NHS England Data Wrangler Team (see notes above)
vacc_1 = (
  vacc_1
  .withColumn('PROCEDURE_CAT', f.when(f.col('PROCEDURE_CAT') == 'Unknown', 'B').otherwise(f.col('PROCEDURE_CAT')))
)

# check
if run_level_3_checks:
  tmpt = tab(vacc_1, 'PROCEDURE_CAT'); print()

# COMMAND ----------

# MAGIC %md # Quality assurance

# COMMAND ----------

# Similar to SAIL RRDA_CVVD (Research Ready Data Asset for COVID-19 Vaccination Data) we perform quality assurance (validation rules).
# The following quality assurance rules are used with an exclusion boolean variable to indicate whether records failing the particular quality assurance rule should be excluded or flagged

qa_dict = {
  "qa_1": {
    "desc": "Null PERSON_ID or null DATE",
    "cols": {
      "qa_1": f.when((f.col('PERSON_ID').isNull()) | (f.col('DATE').isNull()), f.lit(1)).otherwise(f.lit(0))
    },
    "exc": True,
    "ord": 1
  },

  "qa_2": {
    "desc": "Not null SITUATION_CODE or not null REASON_NOT_GIVEN_CODE",
    "cols": {
      "qa_2": f.when((f.col('SITUATION_CODE').isNotNull()) | (f.col('REASON_NOT_GIVEN_CODE').isNotNull()), f.lit(1)).otherwise(f.lit(0))
    },
    "exc": True,
    "ord": 2
  },

  "qa_3": {
    "desc": "Duplicates on PERSON_ID, DATE, PROCEDURE_CAT, PRODUCT_CAT",
    "cols": {
      "tmp_qa_3_row_number": f.row_number().over(Window.partitionBy('PERSON_ID', 'DATE', 'PROCEDURE_CAT', 'PRODUCT_CAT').orderBy('TIME')),
      "qa_3": f.when(f.col('tmp_qa_3_row_number') > 1, f.lit(1)).otherwise(f.lit(0))
    },
    "exc": True,
    "ord": 3
  },

  "qa_4": {
    "desc": "Records on or after archived_on date (SAIL_QA_3)",
    "cols": {
      "qa_4": f.when(f.col('DATE') >= f.to_date(f.lit(archived_on)), f.lit(1)).otherwise(f.lit(0))
    },
    "exc": True,
    "ord": 4
  },  

  "qa_5": {
    "desc": "Records before 20201208 - start of vaccination programme (SAIL_QA_4)",
    "cols": {
      "qa_5": f.when(f.col('DATE') < f.to_date(f.lit('2020-12-08')), f.lit(1)).otherwise(f.lit(0))
    },
    "exc": True,
    "ord": 5
  },   

  "qa_6": {
    "desc": "Duplicate PERSON_ID and DATE (SAIL_QA_1)",
    "cols": {
      "tmp_qa_6_row_number": f.row_number().over(Window.partitionBy('PERSON_ID', 'DATE').orderBy(f.desc('PROCEDURE_CAT'), f.desc('TIME'))), 
      "qa_6": f.when(f.col('tmp_qa_6_row_number') > 1, f.lit(1)).otherwise(f.lit(0))
    },
    "exc": True,
    "ord": 6
  },   

  "qa_7": {
    "desc": "Multiple dose 1 and 2 PROCEDURE (SAIL_QA_2)",
    "cols": {
      "tmp_qa_7_row_number": f.row_number().over( Window.partitionBy('PERSON_ID', 'PROCEDURE_CAT').orderBy('DATE')),
      "tmp_qa_7_row_number_edit": f.when(f.col('PROCEDURE_CAT').isin('1', '2'), f.col('tmp_qa_7_row_number')).otherwise(f.lit(1)),
      "qa_7": f.when(f.col('tmp_qa_7_row_number_edit') > 1, f.lit(1)).otherwise(f.lit(0))
    },
    "exc": True,
    "ord": 7
  },     

  "qa_8": {
    "desc": "Interval between doses less than 20 days (SAIL_QA_6)",
    "cols": {
      "tmp_qa_8_date_diff": f.datediff(f.col('DATE'), f.lag(f.col('DATE'), 1).over(Window.partitionBy('PERSON_ID').orderBy('DATE'))),
      "qa_8": f.when(f.col('tmp_qa_8_date_diff') < 20, f.lit(1)).otherwise(f.lit(0)),
      "tmp_qa_8_row_number": f.row_number().over(Window.partitionBy('PERSON_ID').orderBy('DATE')),
      "tmp_qa_8_sum": f.sum(f.col('qa_8')).over(Window.partitionBy('PERSON_ID')),
    },
    "exc": True,
    "ord": 8
  },    
  "qa_9": {
    "desc": "DATE is before the specific vaccine PRODUCT start date (SAIL_QA_5)",
    "cols": {
      "tmp_qa_9_pf": f.when((f.trim(f.col('PRODUCT_CAT')) == 'Pfizer')        & (f.col('DATE') < f.to_date(f.lit('2020-12-08'))), 1).otherwise(0),
      "tmp_qa_9_az": f.when((f.trim(f.col('PRODUCT_CAT')) == 'AstraZeneca')   & (f.col('DATE') < f.to_date(f.lit('2021-01-04'))), 1).otherwise(0),
      "tmp_qa_9_mo": f.when((f.trim(f.col('PRODUCT_CAT')) == 'Moderna')       & (f.col('DATE') < f.to_date(f.lit('2021-01-04'))), 1).otherwise(0),
      "tmp_qa_9_ja": f.when((f.trim(f.col('PRODUCT_CAT')) == 'Janssen-Cilag') & (f.col('DATE') < f.to_date(f.lit('2021-03-01'))), 1).otherwise(0),
      "tmp_qa_9_pc": f.when((f.trim(f.col('PRODUCT_CAT')) == 'Pfizer child')  & (f.col('DATE') < f.to_date(f.lit('2022-01-20'))), 1).otherwise(0),
      "qa_9": f.greatest(*['tmp_qa_9_pf', 'tmp_qa_9_az', 'tmp_qa_9_mo', 'tmp_qa_9_ja', 'tmp_qa_9_pc'])
    },
    "exc": False,
    "ord": 9
  },    

  "qa_10": {
    "desc": "dose_sequence is NOT sequential (e.g., second dose before first dose or missing dose) (SAIL_QA_7)",
    "cols": {
      "tmp_qa_10_booster_cumsum_add2": f.sum(f.when(f.col('PROCEDURE_CAT') == 'B', f.lit(1)).otherwise(f.lit(0))).over(Window.partitionBy('PERSON_ID').orderBy('DATE').rowsBetween(Window.unboundedPreceding, Window.currentRow)) + f.lit(2),
      "tmp_qa_10_dose_sequence": f.when(f.col('PROCEDURE_CAT').isin(['1','2']), f.col('PROCEDURE_CAT').cast(t.IntegerType())).when(f.col('PROCEDURE_CAT') == 'B', f.col('tmp_qa_10_booster_cumsum_add2')).otherwise(999999),
      "tmp_qa_10_row_number": f.row_number().over(Window.partitionBy('PERSON_ID').orderBy('DATE')),
      "qa_10": f.lit(1) - udf_null_safe_equality('tmp_qa_10_dose_sequence', 'tmp_qa_10_row_number').cast(t.IntegerType())
    },
    "exc": False,
    "ord": 10
  },  

  "qa_11": {
    "desc": "Mixed vaccine PRODUCT before 20210507 (SAIL_QA_8)",
    "cols": {
      "tmp_qa_11_PRODUCT_CAT_diff": f.when(f.col('PRODUCT_CAT') != f.lag(f.col('PRODUCT_CAT'), 1).over(Window.partitionBy('PERSON_ID').orderBy('DATE')), f.lit(1)).otherwise(f.lit(0)),
      "qa_11": f.when((f.col('tmp_qa_11_PRODUCT_CAT_diff') == 1) & (f.col('DATE') < f.to_date(f.lit('2021-05-07'))), 1).otherwise(0)
    },
    "exc": False,
    "ord": 11
  },  

  "qa_12": {
    "desc": "Second dose or booster dose records before 20201229 (21 days after the earliest first dose date)",
    "cols": {
      "qa_12": f.when((f.col('PROCEDURE_CAT').isin(['2', 'B'])) & (f.col('DATE') < f.to_date(f.lit('2020-12-29'))), f.lit(1)).otherwise(f.lit(0))
    },
    "exc": False,
    "ord": 12
  },   

  "qa_13": {
    "desc": "Booster dose records before 20201229 + 90 days (90 days after the earliest second dose date)",
    "cols": {
      "qa_13": f.when((f.col('PROCEDURE_CAT').isin(['B'])) & (f.col('DATE') < f.date_add(f.to_date(f.lit('2020-12-29')), 90)), f.lit(1)).otherwise(f.lit(0))
    },
    "exc": False,
    "ord": 13
  },      

  "qa_14": {
    "desc": "Interval between booster doses less than 90 days",
    "cols": {
      "tmp_qa_14_booster_interval_flag": f.when((f.col('PROCEDURE_CAT').isin(['B'])) & (f.lag(f.col('PROCEDURE_CAT'), 1).over(Window.partitionBy('PERSON_ID').orderBy('DATE')).isin(['B'])), f.lit(1)),
      "tmp_qa_14_date_diff": f.when(f.col('tmp_qa_14_booster_interval_flag') == 1, f.datediff(f.col('DATE'), f.lag(f.col('DATE'), 1).over(Window.partitionBy('PERSON_ID').orderBy('DATE')))),
      "qa_14": f.when(f.col('tmp_qa_14_date_diff') < 90, f.lit(1)).otherwise(f.lit(0)),
      "tmp_qa_14_row_number": f.row_number().over(Window.partitionBy('PERSON_ID').orderBy('DATE')),
      "tmp_qa_14_sum": f.sum(f.col('qa_14')).over(Window.partitionBy('PERSON_ID')),
    },
    "exc": False,
    "ord": 14
  },    
}

# Pootential TODOs
# implement ordering using ord below
# qa_3 - include the data lag, so rather than checking archived_on = 2024-03-27 we might check archived_on minus 1 month = 2024-02-27 
# qa_6 - consider a mono_id for stability and a flag to indicate where a product is being chosen at random for the same person/date/time/procedure around (~2k)
# qa_8/14 - consider an alternative (more conservative) iterative approach. For example, say an individual has 3 vaccinations with inter-vaccination intervals of 14 and 14 (e.g., 20210301, 20210315, 20210329). Then, currently the below would exclude vaccinations 2 and 3. An iterative approach would only drop vaccination 2, with the interval between vaccination 1 and 3 being 28 days. However, the below shows that there are only ~100 individuals with more than one record where the inter-vaccination interval < 20 days, and of these only ~25 would have benefited from the iterative approach, therefore coding this more involved approach is low priority. But could argue to exclude both records or the individual in the example as it could be the first record that has an incorrect. Minor issue. 
# qa_xx: Age at DATE is before specific age range start date, but need to consider shielding and comorbidities 
# qa_xx: PRODUCT is not appropriate for age range (e.g., Pfizer only for children) 
# qa_xx: max number of doses per individual

# check
display(pd.DataFrame(qa_dict).T[['desc', 'exc', 'ord']].reset_index())

# COMMAND ----------

# MAGIC %md ## Exclusions and flags

# COMMAND ----------

flow_save_name = f'{proj}_tmp_vacc_flow'
vacc_save_name = f'{proj}_tmp_vacc_2'

vacc_2 = vacc_1

# initial flow table 
print(f'initialise and save flow table ({datetime.datetime.now()})')
flow = count_var_sp(df=vacc_2, var='PERSON_ID', var_out='id', i=0, qa='None', desc='Original')
flow = temp_save(df=flow, out_name=flow_save_name, quietly=True)

# loop through quality assurance dictionary
qa_flags = []
for i, qa_x in enumerate(qa_dict):
  desc = qa_dict[qa_x]['desc']
  print(f'\n{qa_x} ({datetime.datetime.now()})')  
  print(f'  desc: {desc}')

  # loop through cols to create
  cols = []
  for col in qa_dict[qa_x]['cols']:
    print('  create col:', col)
    cols = cols + [col]
    vacc_2 = vacc_2.withColumn(col, qa_dict[qa_x]['cols'][col])

  # qa_8 specific
  if((qa_x == 'qa_8') & (run_level_1_checks == True)):
    # check number of individuals with multiple records (i.e., >=2) that are <20 days apart (see notes above)
    tmpt = tab(vacc_2.where(f.col('tmp_qa_8_row_number') == 1), 'tmp_qa_8_sum'); print()

  # apply exclusion if specified and drop columns created
  if(qa_dict[qa_x]['exc'] == True):
    print('  apply exclusion')
    vacc_2 = vacc_2.where(f.col(qa_x) == 0).drop(*cols)

    print(f'  calculate and save flow table ({datetime.datetime.now()})')
    flow_tmp = count_var_sp(df=vacc_2, var='PERSON_ID', var_out='id', i=i+1, qa=qa_x, desc=f'{desc}')
    flow = flow.unionByName(flow_tmp)       
    flow = temp_save(df=flow, out_name=flow_save_name, quietly=True)

    print(f'  save vacc table ({datetime.datetime.now()})')
    vacc_2 = temp_save(df=vacc_2, out_name=vacc_save_name, quietly=True)

  elif(qa_dict[qa_x]['exc'] == False):
    qa_flags = qa_flags + [qa_x]

print(f'\nsave vacc table ({datetime.datetime.now()})')
vacc_2 = temp_save(df=vacc_2, out_name=vacc_save_name, quietly=True)
print(f'\nEND ({datetime.datetime.now()})')

print('\n', qa_flags)

# COMMAND ----------

# MAGIC %md ## Flow table

# COMMAND ----------

flow_p = flow_diff(flow.orderBy('i').toPandas())
print(flow_format(flow_p).to_string())

# COMMAND ----------

# MAGIC %md ## Tidy

# COMMAND ----------

vlist_1 = ['PERSON_ID', 'DATE', 'PROCEDURE_CAT', 'PRODUCT_CAT']
vlist_2 = vlist_1 + [f.col('tmp_qa_10_dose_sequence').alias('dose_sequence'), f.col('tmp_qa_10_row_number').alias('row_number'), 'TIME', 'PROCEDURE_CODE', 'PROCEDURE_DESC', 'PRODUCT_CODE', 'PRODUCT_DESC'] + qa_flags

vacc_3 = vacc_2.select(vlist_2)

# COMMAND ----------

# MAGIC %md # Checks

# COMMAND ----------

# MAGIC %md ## Displays

# COMMAND ----------

if run_level_1_checks:
  display(vacc_3.select(vlist_1).orderBy('PERSON_ID', 'DATE'))
  display(vacc_3.orderBy('PERSON_ID', 'DATE'))

if run_level_2_checks:
  count_var(vacc_3, 'PERSON_ID'); print()
  count_varlist(vacc_3, ['PERSON_ID', 'DATE']); print()
  tmpt = tab(vacc_3, 'PROCEDURE_CAT'); print()
  tmpt = tab(vacc_3, 'PRODUCT_CAT'); print()
  tmpt = tab(vacc_3, 'PRODUCT_CAT', 'PROCEDURE_CAT'); print()

# COMMAND ----------

# MAGIC %md ## Quality assurance flags

# COMMAND ----------

# check qa flags

if run_level_2_checks:
  # summarise by PERSON_ID
  tmp = (
    vacc_3
    .select(['PERSON_ID', 'dose_sequence'] + qa_flags)    
    .groupBy('PERSON_ID')
    .agg(
      f.count(f.lit(1)).alias('n_rows')
      , f.max(f.col('dose_sequence')).alias('dose_sequence_max')
      , *[f.max(col).alias(f'{col}_max') for col in qa_flags]
    )
    .withColumn('qa_concat', f.concat(*[f.col(f'{col}_max') for col in qa_flags]))
  )

  # check
  tmpt = tab(tmp, 'n_rows'); print()
  tmpt = tab(tmp, 'dose_sequence_max'); print()
  tmpt = tab(tmp, 'n_rows', 'dose_sequence_max'); print()
  for qa_x in qa_flags:
    print(qa_x, qa_dict[qa_x]['desc']); print()
    tmpt = tab(tmp, f'{qa_x}_max'); print()
  print(qa_flags); print()
  tmpt = tab(tmp, 'qa_concat'); print()

# COMMAND ----------

# MAGIC %md # Save

# COMMAND ----------

# save
save_table(df=vacc_3, out_name=f'{proj}_cur_covid_vacc', save_previous=True)
print(f'{datetime.datetime.now()}')

# COMMAND ----------

# MAGIC %md # Drop temporary tables

# COMMAND ----------

spark.sql(f"DROP TABLE {dsa}.{proj}_tmp_vacc_1")
spark.sql(f"DROP TABLE {dsa}.{proj}_tmp_vacc_2")