# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # CCU076_01-D01-parameters
# MAGIC
# MAGIC **Project** CCU076_01
# MAGIC
# MAGIC **Description** This notebook defines a set of parameters, which is loaded in each notebook in the data curation pipeline, so that helper functions and parameters are consistently available.
# MAGIC
# MAGIC **Authors** Alexia Sampri, Yueying Li, Isabel Walter
# MAGIC
# MAGIC **Reviewers** ⚠ Alexia Sampri
# MAGIC
# MAGIC **Acknowledgements** Based on CCU004_03-D01-parameters (Tom Bolton, Fionna Chalmers, Anna Stevenson)
# MAGIC
# MAGIC **Notes** This pipeline has an initial production date of 2024-03-12 (`pipeline_production_date` == `2024-03-12`) and the `archived_on` dates used for each dataset correspond to the latest (most recent) batch of data before this date. CCU076_01 used a pipeline_production_date of 2024-03-12. Should the pipeline and all the notebooks that follow need to be updated and rerun, then this notebook should be rerun directly (before being called by subsequent notebooks) with `pipeline_production_date` updated and `run_all_toggle` switched to True. After this notebook has been rerun the `run_all_toggle` should be reset to False to prevent subsequent notebooks that call this notebook from having to rerun the 'archived_on' section. Rerunning this notebook with the updated `pipeline_production_date` will ensure that the `archived_on` dates used for each dataset are updated with these dates being saved for reference in the collabortion database.
# MAGIC
# MAGIC **Data Output** 
# MAGIC - **`ccu076_01_parameters_df_datasets`**: table of `archived_on` dates for each dataset that can be used consistently throughout the pipeline 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show databases

# COMMAND ----------

# MAGIC %md
# MAGIC # 0. Setup

# COMMAND ----------

run_all_toggle = False # ccccccswitch to True or False when needed. Read description above.

# COMMAND ----------

spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Libraries

# COMMAND ----------

import pyspark.sql.functions as f
import pandas as pd
import pyspark.pandas as ps
import re
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Common Functions

# COMMAND ----------

# MAGIC %run "/Shared/SHDS/common/functions"

# COMMAND ----------

# %run "/Workspace/Shared/CCU076_01/Spencers_common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Custom Functions

# COMMAND ----------

# DBTITLE 1,save_table
def save_table(df, out_name:str, save_previous=True, data_base:str=f''):
  
  # assert that df is a dataframe
  #assert isinstance(df, f.DataFrame), 'df must be of type dataframe' #isinstance(df, pd.DataFrame) | 
  # if a pandas df then convert to spark
  #if(isinstance(df, pd.DataFrame)):
    #df = (spark.createDataFrame(df))

  # 20251009 - ADDED BLOCK BELOW
  try:
    # Spark Connect
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
  except ImportError:
    ConnectDataFrame = ()
  from pyspark.sql import DataFrame as SparkDataFrame
  assert isinstance(df, (SparkDataFrame, ConnectDataFrame)), (
      "df must be a Spark DataFrame (classic or connect)"
  )

  # if a pandas df then convert to spark
  #if(isinstance(df, pd.DataFrame)):
    #df = (spark.createDataFrame(df))
  

  # save name check
  if(any(char.isupper() for char in out_name)): 
    print(f'Warning: {out_name} converted to lowercase for saving')
    out_name = out_name.lower()
    print('out_name: ' + out_name)
    print('')
  
  # df name
  df_name = [x for x in globals() if globals()[x] is df][0]
  
  # ------------------------------------------------------------------------------------------------
  # save previous version for comparison purposes
  if(save_previous):
    tmpt = (
      spark.sql(f"""SHOW TABLES FROM {data_base}""")
      .select('tableName')
      .where(f.col('tableName') == out_name)
      .collect()
    )
    if(len(tmpt)>0):
      # save with production date appended
      _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
      out_name_pre = f'{out_name}_pre{_datetimenow}'.lower()
      print(f'saving (previous version):')
      print(f'  {out_name}')
      print(f'  as')
      print(f'  {out_name_pre}')
      spark.table(f'{data_base}.{out_name}').write.mode('overwrite').saveAsTable(f'{data_base}.{out_name_pre}')
      #spark.sql(f'ALTER TABLE {data_base}.{out_name_pre} OWNER TO {data_base}')
      print('saved')
      print('') 
    else:
      print(f'Warning: no previous version of {out_name} found')
      print('')
  # ------------------------------------------------------------------------------------------------  
  
  # save new version
  print(f'saving:')
  print(f'  {df_name}')
  print(f'  as')
  print(f'  {out_name}')
  df.write.mode('overwrite').option("overwriteSchema", "True").saveAsTable(f'{data_base}.{out_name}')
  #spark.sql(f'ALTER TABLE {data_base}.{out_name} OWNER TO {data_base}')
  print('saved')

# COMMAND ----------

# DBTITLE 1,Temp
# outName = f'{proj}_fc_save_table_TESTING'
# test = (spark.createDataFrame(tmp_df_datasets))
# #save_table(df=tmp_df_datasets, out_name=outName, save_previous=True)
# save_table(df=test, out_name=outName, save_previous=True)

# COMMAND ----------

# DBTITLE 1,temp_save
# temp_save lives in SHDS common functions but since migration _dbc has not been updated to new collab db dsa - will use this temp_save function here temporarily

#def temp_save(df, out_name:str, _dbc:str=f''):
  
#  spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')
  
#  dfname = [x for x in globals() if globals()[x] is df]
#  if(len(dfname) == 1): dfname = dfname[0]
#  else: dfname = "unknown dfname (not in globals())"      
  
#  print(f'saving {dfname} to {_dbc}.{out_name}')
#  if(out_name != out_name.lower()):
#    out_name_old = out_name
#    out_name == out_name.lower()
#    print(f'  out_name changed to lower case (from {out_name_old} to {out_name})')
    
  # save  
#  df.write.mode('overwrite').saveAsTable(f'{_dbc}.{out_name}')
#  #spark.sql(f'ALTER TABLE {_dbc}.{out_name} OWNER TO {_dbc}')
#  print(f'  saved')
  
  # repoint
#  spark.sql(f'REFRESH TABLE {_dbc}.{out_name}')
#  return spark.table(f'{_dbc}.{out_name}')

# COMMAND ----------

# DBTITLE 1,extract_batch_from_archive
# Updated function that compares the number of rows expected (the number that were found when running the parameters notebook in full) against the number of rows observed (the number that were found when extracting the data from the archive in a subsequent notebook). This would alert us to the number of rows being changed in the archive tables, which the data wranglers control.

# function to extract the batch corresponding to the pre-defined archived_on date - will be used in subsequent notebooks

from pyspark.sql import DataFrame
def extract_batch_from_archive(_df_datasets: DataFrame, _dataset: str):
  
  # get row from df_archive_tables corresponding to the specified dataset
  _row = _df_datasets[_df_datasets['dataset'] == _dataset]
  
  # check one row only
  assert _row.shape[0] != 0, f"dataset = {_dataset} not found in _df_datasets (datasets = {_df_datasets['dataset'].tolist()})"
  assert _row.shape[0] == 1, f"dataset = {_dataset} has >1 row in _df_datasets"
  
  # create path and extract archived on
  _row = _row.iloc[0]
  _path = _row['database'] + '.' + _row['table']  
  _archived_on = _row['archived_on']
  _n_rows_expected = _row['n']  
  print(_path + ' (archived_on = ' + _archived_on + ', n_rows_expected = ' + _n_rows_expected + ')')
  
  # check path exists # commented out for runtime
#   _tmp_exists = spark.sql(f"SHOW TABLES FROM {_row['database']}")\
#     .where(f.col('tableName') == _row['table'])\
#     .count()
#   assert _tmp_exists == 1, f"path = {_path} not found"

  # extract batch
  _tmp = spark.table(_path)\
    .where(f.col('archived_on') == _archived_on)  
  
  # check number of records returned
  _n_rows_observed = _tmp.count()
  print(f'  n_rows_observed = {_n_rows_observed:,}')
  assert _n_rows_observed > 0, f"_n_rows_observed == 0"
  assert f'{_n_rows_observed:,}' == _n_rows_expected, f"_n_rows_observed != _n_rows_expected ({_n_rows_observed:,} != {_n_rows_expected})"

  # return dataframe
  return _tmp

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Paths and Variables

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Set Project Specific Variables

# COMMAND ----------

# Please set and check the variables below

# -----------------------------------------------------------------------------
# Databases
# -----------------------------------------------------------------------------
db = ''
dbc = f'{db}_collab'
dsa = f''

# -----------------------------------------------------------------------------
# Project
# -----------------------------------------------------------------------------
proj = 'ccu076_01'


# -----------------------------------------------------------------------------
# Dates
# -----------------------------------------------------------------------------
study_start_date = '2020-01-01'
covid_start_date = '2020-01-01' 
study_end_date   = '2022-12-31'
cohort = 'c01'


# -----------------------------------------------------------------------------
# Pipeline production date
# -----------------------------------------------------------------------------
# date at which pipeline was created and archived_on dates for datasets have been selected based on

pipeline_production_date = '2024-03-12' 

# -----------------------------------------------------------------------------
# Datasets
# -----------------------------------------------------------------------------
# data frame of datasets
datasets = [
  # -----------------------------------------------------------------------------
  # Datasets requested by the project
  # -----------------------------------------------------------------------------  
    ['gdppr',         dbc, f'gdppr_{db}_archive',             'NHS_NUMBER_DEID',                'DATE']  
  , ['hes_apc',       dbc, f'hes_apc_all_years_archive',      'PERSON_ID_DEID',                 'EPISTART']
  , ['deaths',        dbc, f'deaths_{db}_archive',            'DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'REG_DATE_OF_DEATH']
  , ['pmeds',         dbc, f'primary_care_meds_{db}_archive', 'Person_ID_DEID',                 'ProcessingPeriodDate']           
  , ['sgss',          dbc, f'sgss_{db}_archive',              'PERSON_ID_DEID',                 'Specimen_Date']
  , ['sus',           dbc, f'sus_{db}_archive',               'NHS_NUMBER_DEID',                'EPISODE_START_DATE']  
  , ['vacc',          dbc, f'vaccine_status_{db}_archive',    'PERSON_ID_DEID',                 'RECORDED_DATE']  
  # -----------------------------------------------------------------------------
  # Additonal datasets needed for the data curation pipeline for this project
  # -----------------------------------------------------------------------------
  , ['hes_ae',        dbc, f'hes_ae_all_years_archive',       'PERSON_ID_DEID',                 'ARRIVALDATE']
  , ['hes_op',        dbc, f'hes_op_all_years_archive',       'PERSON_ID_DEID',                 'APPTDATE']
  , ['hes_cc',        dbc, f'hes_cc_all_years_archive',       'PERSON_ID_DEID',                 'CCSTARTDATE'] 
  , ['chess',         dbc, f'chess_{db}_archive',             'PERSON_ID_DEID',                 'InfectionSwabDate']
  , ['pillar2',       dbc, f'covid_antigen_testing_pillar2_{db}_archive',         'Person_ID_DEID',                 'TestStartDate']]


  # -----------------------------------------------------------------------------
  # Datasets not required for this project
  # -----------------------------------------------------------------------------  
 
# , ['icnarc',        dbc, f'icnarc_{db}_archive',            'NHS_NUMBER_DEID',               'Date_of_admission_to_your_unit'] 
#   , ['ssnap',         dbc, f'ssnap_{db}_archive',             'PERSON_ID_DEID',                'S1ONSETDATETIME'] 
#   , ['minap',         dbc, f'minap_{db}_archive',             'NHS_NUMBER_DEID',               'ARRIVAL_AT_HOSPITAL'] 
#   , ['nhfa',          dbc, f'nhfa_{db}_archive',              '1_03_NHS_NUMBER_DEID',          '2_00_DATE_OF_VISIT'] 
#   , ['nvra',          dbc, f'nvra_{db}_archive',              'NHS_NUMBER_DEID',               'DATE'] 


tmp_df_datasets =pd.DataFrame(datasets, columns=['dataset', 'database', 'table', 'id', 'date']).reset_index()

if(run_all_toggle):
  print('tmp_df_datasets:\n', tmp_df_datasets.to_string())

# tmp_df_datasets =spark.createDataFrame(tmp_df_datasets)
# tmp_df_datasets.cache().count()
tmp_df_datasets.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Datasets Archived States

# COMMAND ----------

type(tmp_df_datasets)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2.1 Create

# COMMAND ----------

# DBTITLE 1,All archived states
# for each dataset in tmp_df_datasets, 
#   tabulate all archived_on dates (for information)
#   find the latest (most recent) archived_on date before the pipeline_production_date
#   create a table containing a row with the latest archived_on date and count of the number of records for each dataset
  
# this will not run each time the Parameters notebook is run in annother notebook - will only run if the toggle is switched to True
if(run_all_toggle):

  latest_archived_on = []
  for index, row in tmp_df_datasets.iterrows():
    # initial  
    dataset = row['dataset']
    path = row['database'] + '.' + row['table']
    print(index, dataset, path); print()

    # point to table
    tmpd = spark.table(path)

    # tabulate all archived_on dates
    # tmpt = tab(tmpd, 'archived_on')
    
    tmpa = (
      tmpd
      .select('archived_on')
      .distinct()
      .orderBy(f.desc('archived_on'))
      .limit(1)
      .withColumn('dataset', f.lit(dataset))
      .select('dataset', 'archived_on')      
      .toPandas()      
    )
    tmpaa = tmpa.iloc[0]['archived_on']
    print(tmpaa)

    # get count
    tmpn = tmpd.where(f.col('archived_on') == tmpaa).count()
    tmpa['n'] = tmpn
    print(tmpa.to_string())

    # append results
    if(index == 0): latest_archived_on = tmpa
    else: latest_archived_on = pd.concat([latest_archived_on, tmpa]) # latest_archived_on.unionByName(tmpa1)

  # check
  print('Latest (most recent) archived_on date before pipeline_production_date')
  print(latest_archived_on.to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2.2 Check

# COMMAND ----------

# DBTITLE 1,Latest archived states
# this will not run each time the Parameters notebook is run in another notebook - will only run if the toggle is switched to True
if(run_all_toggle):
  # check
  display(latest_archived_on)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2.3 Prepare

# COMMAND ----------

# prepare the tables to be saved

# this will not run each time the Parameters notebook is run in annother notebook - will only run if the toggle is switched to True
if(run_all_toggle):
  
  # merge the datasets dataframe with the latest_archived_on
  tmp_df_datasets_sp = spark.createDataFrame(tmp_df_datasets) 
  latest_archived_on_sp = spark.createDataFrame(latest_archived_on) 
  parameters_df_datasets = merge(tmp_df_datasets_sp, latest_archived_on_sp, ['dataset'], validate='1:1', assert_results=['both'], indicator=0).orderBy('index'); print()
  
  # check  
  print(parameters_df_datasets.toPandas().to_string())


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2.4 Save

# COMMAND ----------

# save the parameters_df_datasets table
# which we can simply import below when not running all and calling this notebook in subsequent notebooks

# this will not run each time the Parameters notebook is run in annother notebook - will only run if the toggle is switched to True
if(run_all_toggle):
  parameters_df_datasets = (
    parameters_df_datasets
    .withColumn('n', f.format_number('n', 0))
  )
  save_table(df=parameters_df_datasets, out_name=f'{proj}_parameters_df_datasets', save_previous=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2.5 Import

# COMMAND ----------

# import the parameters_df_datasets table 
# convert to a Pandas dataframe and transform archived_on to a string (to conform to the input that the extract_batch_from_archive function is expecting)

spark.sql(f'REFRESH TABLE {dsa}.{proj}_parameters_df_datasets')
parameters_df_datasets = (
  spark.table(f'{dsa}.{proj}_parameters_df_datasets')
  .orderBy('index')
  .toPandas()
)
parameters_df_datasets['archived_on'] = parameters_df_datasets['archived_on'].astype(str)

# COMMAND ----------

display(parameters_df_datasets)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Curated Data Paths

# COMMAND ----------

# -----------------------------------------------------------------------------
# These are paths to data tables curated in subsequent notebooks that may be
# needed in subsequent notebooks from which they were curated
# -----------------------------------------------------------------------------

# note: the below is largely listed in order of appearance within the pipeline:  

# reference tables
path_ref_bhf_phenotypes  = 'bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127'
path_ref_geog            = 'dss_corporate.ons_chd_geo_listings'
path_ref_imd             = 'dss_corporate.english_indices_of_dep_v02'
path_ref_gp_refset       = 'dss_corporate.gpdata_snomed_refset_full'
path_ref_gdppr_refset    = 'dss_corporate.gdppr_cluster_refset'
path_ref_icd10           = 'dss_corporate.icd10_group_chapter_v01'
path_ref_opcs4           = 'dss_corporate.opcs_codes_v02'

# curated tables
path_cur_hes_apc_long      = f'{dsa}.{proj}_cur_hes_apc_all_years_archive_long'
path_cur_hes_apc_oper_long = f'{dsa}.{proj}_cur_hes_apc_all_years_archive_oper_long'
path_cur_deaths_long       = f'{dsa}.{proj}_cur_deaths_{db}_archive_long'
path_cur_deaths_sing       = f'{dsa}.{proj}_cur_deaths_{db}_archive_sing'
path_cur_lsoa_region       = f'{dsa}.hds_cur_lsoa_region_lookup'
path_cur_lsoa_imd          = f'{dsa}.hds_cur_lsoa_2011_imd_lookup'
path_cur_lsoa_multisource  = f'{dsa}.hds_curated_assets__lsoa_multisource_2024_07_23'
path_cur_covid             = f'{dsa}.{proj}_cur_covid' 
path_cur_vacc             = f'{dsa}.{proj}_cur_covid_vacc' 

# skinny table
hds_curated_assets_demographic = f'{dsa}.hds_curated_assets_demographics_20240425' 

# temp tables
path_tmp_quality_assurance_hx_1st_wide  = f'{dsa}.{proj}_tmp_quality_assurance_hx_1st_wide'
path_tmp_quality_assurance_hx_1st       = f'{dsa}.{proj}_tmp_quality_assurance_hx_1st'
path_tmp_quality_assurance_qax          = f'{dsa}.{proj}_tmp_quality_assurance_qax'
path_tmp_quality_assurance              = f'{dsa}.{proj}_tmp_quality_assurance'
path_tmp_inc_exc_cohort                 = f'{dsa}.{proj}_tmp_inc_exc_cohort'
path_tmp_inc_exc_flow                   = f'{dsa}.{proj}_tmp_inc_exc_flow'

path_tmp_env                              = f'{dsa}.{proj}_tmp_env'

# out tables
path_out_codelist_quality_assurance      = f'{dsa}.{proj}_out_codelist_quality_assurance'
path_out_codelist_covid                  = f'{dsa}.{proj}_out_codelist_covid'
path_out_codelist_covariates             = f'{dsa}.{proj}_out_codelist_covariates'
path_out_codelist_outcomes               = f'{dsa}.{proj}_out_codelist_outcomes'
path_out_cohort                          = f'{dsa}.{proj}_out_cohort'
path_out_covariates                   = f'{dsa}.{proj}_out_covariates'
path_out_exposures_covid                = f'{dsa}.{proj}_out_exposures_covid'
path_out_exposures_covid_with_washout   = f'{dsa}.{proj}_out_exposures_covid_with_washout'
path_out_exposures_vacc                 = f'{dsa}.{proj}_out_exposures_vacc'
path_out_outcomes                       = f'{dsa}.{proj}_out_outcomes'
path_out_outcomes_all_with_washout      = f'{dsa}.{proj}_out_outcomes_all_with_washout'
path_out_lsoa_dynamic                   = f'{dsa}.{proj}_out_lsoa_dynamic'
path_out_outcomes_wide                  = f'{dsa}.{proj}_out_outcomes_wide'
path_out_time_series_mi_fatal           = f'{dsa}.{proj}_out_time_series_mi_fatal'
path_out_time_series_mi_nonfatal        = f'{dsa}.{proj}_out_time_series_mi_nonfatal'
path_out_time_series_stroke_fatal       = f'{dsa}.{proj}_out_time_series_stroke_fatal'
path_out_time_series_stroke_nonfatal    = f'{dsa}.{proj}_out_time_series_stroke_nonfatal'
path_out_time_series_ae_fatal           = f'{dsa}.{proj}_out_time_series_ae_fatal'
path_out_time_series_ae_nonfatal        = f'{dsa}.{proj}_out_time_series_ae_nonfatal'
path_out_time_series_ve_fatal           = f'{dsa}.{proj}_out_time_series_ve_fatal'
path_out_time_series_ve_nonfatal        = f'{dsa}.{proj}_out_time_series_ve_nonfatal'

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Summary

# COMMAND ----------

# print variables, archived_on table, and paths for reference in subsequent notebooks
print(f"Project:")
print("  {0:<22}".format("proj") + " = " + f"{proj}")
print("  {0:<22}".format("cohort") + " = " + f"{cohort}")
print(f"")
print(f"Study dates:")
print("  {0:<22}".format("study_start_date") + " = " + f"{study_start_date}")
print("  {0:<22}".format("study_end_date") + " = " + f"{study_end_date}")
print(f"")
print(f"Databases:")
print("  {0:<22}".format("db") + " = " + f"{db}")
print("  {0:<22}".format("dbc") + " = " + f"{dbc}")
print(f"")
print(f"Pipeline production date:")
print(
    "  {0:<22}".format("pipeline_production_date")
    + " = "
    + f"{pipeline_production_date}"
)
print(f"")
print(f"parameters_df_datasets:")
print(
    parameters_df_datasets[
        ["dataset", "database", "table", "archived_on", "n"]
    ].to_string()
)
print(f"")
print(f"Paths:")
# with pd.option_context('display.max_rows', None, 'display.max_columns', None):
#  print(df_paths_raw_data[['dataset', 'database', 'table']])
# print(f'')
# print(f'  df_archive')
# print(df_archive[['dataset', 'database', 'table', 'productionDate']].to_string())
tmp = vars().copy()
for var in list(tmp.keys()):
    if re.match("^path_.*$", var):
        print("  {0:<22}".format(var) + " = " + tmp[var])


# print(f'composite_events:')
# for i, c in enumerate(composite_events):
# print('  ', i, c, '=', composite_events[c])
# print(f'')
# print(f'Out dates:')
# with pd.option_context('display.max_rows', None, 'display.max_columns', None):
#  print(df_paths_raw_data[['dataset', 'database', 'table']])
# print(f'')
# print(f'  df_out')
# print(df_out[['dataset', 'out_date']].to_string())
# print(f'')