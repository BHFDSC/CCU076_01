# Databricks notebook source
# MAGIC %md # CCU076_01_-D11b-time_series_censored_at_death
# MAGIC
# MAGIC Exactly same as previous notebook, but censoring at the actual death date.
# MAGIC
# MAGIC **Description** This notebook creates a time series dataset per outcome of interest.
# MAGIC 1) Datasets of cases are created per outcome of interest. Fatal and non-fatal cases datasets are created, based on who died at the outcome event or within 7 days after. 
# MAGIC 2) A time series is created per case for their respective follow-up time.
# MAGIC 3) The time series is expanded with an LSOA time series using the dynamic LSOA table and a backward and forward filling method.
# MAGIC 4) The time series is expanded with a daily indicator of whether the outcome occurred yes/no. 
# MAGIC 5) The time series is expanded with the COVID-19 exposure indicator, that is indicated yes on the diagnoses dates. 
# MAGIC 6) The time series is expanded with the relevant environmental variables using the LSOA time series to link the correct data.
# MAGIC
# MAGIC **Authors** Isabel Walter, Tom Bolton, Fionna Chalmers
# MAGIC
# MAGIC **Data Output**  
# MAGIC **`out_time_series_ve_fatal`** : time series with venous event cases including fatal venous events.  
# MAGIC **`out_time_series_ve_nonfatal`** : time series with venous event cases excluding fatal venous events.  
# MAGIC **`out_time_series_ae_fatal`** : time series with arterial event cases including fatal arterial events.  
# MAGIC **`out_time_series_ae_nonfatal`** : time series with arterial event cases excluding fatal arterial events.   
# MAGIC **`out_time_series_mi_fatal`** : time series with myocardial infarction cases including fatal myocardial infarction.  
# MAGIC **`out_time_series_mi_nonfatal`** : time series with myocardial infarction cases excluding fatal myocardial infarction.    
# MAGIC **`out_time_series_stroke_fatal`** : time series with ischaemic stroke cases including fatal ischaemic stroke.  
# MAGIC **`out_time_series_stroke_nonfatal`** : time series with ischaemic stroke cases excluding fatal ischaemic stroke.  

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

# MAGIC %md # 0. Setup

# COMMAND ----------

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

#import databricks.koalas as ks
import pandas as pd
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

# DBTITLE 1,Common functions
# MAGIC %run "/Shared/SHDS/common/functions"

# COMMAND ----------

# MAGIC %md # 1. Parameters

# COMMAND ----------

# MAGIC %run "./CCU076_01-D01-parameters"

# COMMAND ----------

# MAGIC %md # 2. Data sources

# COMMAND ----------

cohort = spark.table(path_out_cohort)
exposures_covid = spark.table(path_out_exposures_covid_with_washout)
outcomes   = spark.table(path_out_outcomes_all_with_washout)
lsoa_dynamic = spark.table(path_out_lsoa_dynamic)
env = spark.table(path_tmp_env) #now including 2019,2020,2021,2022

# COMMAND ----------

# MAGIC %md # 3. Data curation

# COMMAND ----------

# DBTITLE 1,Remove non-coding LSOA from lsoa_dynamic
lsoa_dynamic = lsoa_dynamic.filter(f.col("lsoa") != "E99999999")

# COMMAND ----------

# DBTITLE 1,Merge COVID and outcomes data
outcome_covid = (exposures_covid
                 .filter(f.col("covid_phenotype") == "02_Covid_admission_any_position")
                 .withColumn('name', f.lit('covid'))
                 .drop('code','covid_phenotype','covid_status', 'description')
                 .withColumnRenamed('clinical_code','CODE')
                 )

outcomes = (outcomes
            .drop('sourcen'))

outcomes = outcomes.unionByName(outcome_covid)


# COMMAND ----------

# DBTITLE 1,Add DOB and DOD to outcomes dataset
# Add date of birth and date of death to outcomes dataset and make sure only people in cohort are in outcomes dataset
dob_dod = cohort.select('PERSON_ID','date_of_birth', 'date_of_death')

outcomes_extended = outcomes.join(dob_dod, on='PERSON_ID', how='inner')

# COMMAND ----------

display(outcomes_extended)

# COMMAND ----------

# DBTITLE 1,Make cases dataset
# Make cases datasets

mi_fatal, mi_nonfatal, stroke_fatal, stroke_nonfatal, at_fatal, at_nonfatal = (
  outcomes_extended.where(f.col('name') == 'MI').drop('death_flag'),
  outcomes_extended.where(f.col('name') == 'MI').where(f.col('death_flag').isNull()).drop('death_flag'),
  outcomes_extended.where(f.col('name') == 'stroke_IS').drop('death_flag'),
  outcomes_extended.where(f.col('name') == 'stroke_IS').where(f.col('death_flag').isNull()).drop('death_flag'),
  outcomes_extended.where(f.col('name') == 'AT').drop('death_flag'),
  outcomes_extended.where(f.col('name') == 'AT').where(f.col('death_flag').isNull()).drop('death_flag')
  )
  
arterial_events_fatal, arterial_events_nonfatal, venous_events_fatal, venous_events_nonfatal = (
  outcomes_extended.where(f.col('name') == 'arterial_event').drop('death_flag'),
  outcomes_extended.where(f.col('name') == 'arterial_event').where(f.col('death_flag').isNull()).drop('death_flag'),
  outcomes_extended.where(f.col('name') == 'venous_event').drop('death_flag'),
  outcomes_extended.where(f.col('name') == 'venous_event').where(f.col('death_flag').isNull()).drop('death_flag')
  )

covid_fatal, covid_nonfatal = (
  outcomes_extended.where(f.col('name') == 'covid').drop('death_flag'),
  outcomes_extended.where(f.col('name') == 'covid').where(f.col('death_flag').isNull()).drop('death_flag')
  )

# COMMAND ----------

# MAGIC %md # 4. Create time series datasets

# COMMAND ----------

# DBTITLE 1,Time series function
def make_time_series(df1, df2, df3, df4, study_start, study_end):
    """
    Objective:
    1. Makes time series
    2. Create LSOA column
    3. Create outcome indicator
    4. Create COVID exposure indicator
    5. Create age column
    6. Create day of week, week of year and year column
    7. Create environmental variable columns
    
    Args:
    df1 (DataFrame): Input DataFrame with cases (all events in study period).
    df2 (DataFrame): Input DataFrame with dynamic LSOA data from study period and up to two years prior.
    df3 (DataFrame): Input DataFrame with COVID-19 exposure dates (long format) with each diagnosis per individual.
    df4 (DataFrame): Input DataFrame with daily environmental data per LSOA for the study period.
    study_start: string of study start date in yyyy-MM-dd.
    study_end: string of study end date in yyyy-MM-dd.
    
    Returns:
    DataFrame: time series across study period with time varying variables.
    """
    # Create column with date sequence follow up time per individual
    tmp1 = (
        df1
        .select('PERSON_ID', 'date_of_death').distinct() 
        .withColumn('study_start_date', f.to_date(f.lit(study_start), 'yyyy-MM-dd'))
        .withColumn('fu_start_date', f.date_sub(f.trunc(f.col('study_start_date'), 'MM'), 21))
        .withColumn('study_end_date', f.to_date(f.lit(study_end), 'yyyy-MM-dd'))
        .withColumn('end_date', f.when(f.col('date_of_death').isNull(), f.col('study_end_date'))
        .otherwise(f.least(f.col('study_end_date'), f.col('date_of_death'))))
        .withColumn('date_month_seq', f.expr('sequence(fu_start_date, end_date, interval 1 day)'))
        .drop('date_of_death', 'study_end_date'))
    
    # Create time series dataset
    tmp2 = (
        tmp1
        .withColumn('time_series_date', f.explode(f.col('date_month_seq')))
        .drop('date_month_seq'))

    # Prepare dynamic lSOA dataset
    df2 = (
        df2
        .withColumnRenamed('PERSON_ID', 'PERSON_ID_lsoa'))
        #.withColumn('record_date', f.when(f.col('record_date') < study_start_date, f.to_date(f.lit(study_start_date), 'yyyy-MM-dd')).otherwise(f.col('record_date'))))
    
    # Join time series dataset with the LSOA dataset to bring LSOA registration dates into the time series dataset.
    tmp3 = (tmp2
            .join(df2.select('PERSON_ID_lsoa', 'lsoa', 'record_date'), (tmp2.PERSON_ID == df2.PERSON_ID_lsoa) & (tmp2.time_series_date == df2.record_date), how='left')
            .drop('record_date', 'PERSON_ID_lsoa'))
    
    # Mid way fill LSOA
    win_bfill = Window.partitionBy('PERSON_ID').orderBy('time_series_date').rowsBetween(0, Window.unboundedFollowing)
    win_ffill = Window.partitionBy('PERSON_ID').orderBy('time_series_date').rowsBetween(Window.unboundedPreceding, Window.currentRow)
    win = Window.partitionBy('PERSON_ID').orderBy('time_series_date')
    win_cumsum = Window.partitionBy('PERSON_ID').orderBy('time_series_date').rowsBetween(Window.unboundedPreceding, Window.currentRow)
    win_rownum = Window.partitionBy('PERSON_ID', 'null_grp').orderBy('time_series_date')
    win_rownum_max = Window.partitionBy('PERSON_ID', 'null_grp')
    tmp4 = (
            tmp3
            .withColumn('null', f.when(f.col('lsoa').isNull(), 1).otherwise(0))
            .withColumn('null_lag1', f.lag(f.col('null'), 1).over(win))
            .na.fill(0, subset=['null_lag1'])
            .withColumn('null_grp_1st',f.when((f.col('null') == 1) & (f.col('null_lag1') == 0), 1).otherwise(0))
            .withColumn('null_grp', f.sum(f.col('null_grp_1st')).over(win_cumsum))
            .withColumn('null_grp', f.when(f.col('null') == 0, None).otherwise(f.col('null_grp')))
            .withColumn('null_grp_rownum', f.row_number().over(win_rownum))
            .withColumn('null_grp_rownum_max', f.max(f.col('null_grp_rownum')).over(win_rownum_max))
            .withColumn('null_grp_fill', f.when(f.col('null_grp_rownum') <= f.col('null_grp_rownum_max')/2, f.lit('ffill')).otherwise(f.lit('bfill')))
            .withColumn('null_grp_fill', f.when(f.col('null') == 0, None).otherwise(f.col('null_grp_fill')))
            .drop('null', 'null_lag1', 'null_grp_1st', 'null_grp', 'null_grp_rownum', 'null_grp_rownum_max')
            .withColumn('lsoa_code_bfill', f.first(f.col('lsoa'), ignorenulls=True).over(win_bfill))
            .withColumn('lsoa_code_ffill',  f.last(f.col('lsoa'), ignorenulls=True).over(win_ffill))
            .withColumn('lsoa_code_fill_midpt', f.when(f.col('null_grp_fill') == 'bfill', f.col('lsoa_code_bfill')).otherwise(f.col('lsoa_code_ffill')))
            .withColumn('lsoa_code_fill_midpt', f.coalesce('lsoa_code_fill_midpt', 'lsoa_code_bfill', 'lsoa_code_ffill'))
            .withColumn('lsoa', f.col('lsoa_code_fill_midpt'))
            .drop('null_grp_fill', 'lsoa_code_bfill', 'lsoa_code_ffill', 'lsoa_code_fill_midpt')
            .orderBy('PERSON_ID', 'time_series_date'))
    
    # Window to get the oldest record per person    
    window = Window.partitionBy("PERSON_ID_lsoa").orderBy("record_date")

    # Select oldest LSOA per person
    oldest_lsoa_df = df2.select("PERSON_ID_lsoa", "record_date", "lsoa") \
            .withColumn("rn", f.row_number().over(window)) \
            .filter("rn = 1") \
            .drop("record_date", "rn") \
            .withColumnRenamed("PERSON_ID_lsoa", "PERSON_ID") \
            .withColumnRenamed("lsoa", "oldest_lsoa")
    
    # Fill missing tmp4 with oldest LSOA info (there are missing lsoa values for those with an lsoa record_date after time series end date, so after death)
    tmp4 = tmp4.join(oldest_lsoa_df, on="PERSON_ID", how="left") \
            .withColumn("lsoa", f.when(f.col("lsoa").isNull(), f.col("oldest_lsoa")).otherwise(f.col("lsoa"))) \
            .drop("oldest_lsoa")

    # Select date and person ID column from outcome dataset
    df_out = (df1
           .withColumnRenamed('PERSON_ID', 'PERSON_ID_out')
           .withColumnRenamed('DATE', 'out_date')
           .select('PERSON_ID_out', 'out_date'))
    
    # Join outcome dataset into time series dataset
    tmp5 = (tmp4
            .join(df_out, (tmp4.PERSON_ID == df_out.PERSON_ID_out) &
            (tmp4.time_series_date == df_out.out_date), how='left')
            .drop('PERSON_ID_out'))
    
    # Create outcome indicator column
    tmp6 = (tmp5
            .withColumn('outcome_ind', f.when((f.col('out_date').isNotNull()) & (f.col('time_series_date') == f.col('out_date')), 1).otherwise(0))
            .drop('out_date'))

    # Create COVID exposure indicator column (1 if diagnosed on respective time series date, 0 otherwise)

    # Join time series dataset with the covid exposure dataset to bring diagnosis dates into the time series dataset.
    df3 =  (df3
            .withColumnRenamed('PERSON_ID', 'PERSON_ID_exp')
            .withColumnRenamed('DATE', 'covid_date')
            .dropna(subset=['covid_date']) # restrict to covid cases
            .select('PERSON_ID_exp', 'covid_date')) 
    
    tmp7 = (tmp6
            .join(df3, 
            (tmp6.PERSON_ID == df3.PERSON_ID_exp) &
            (tmp6.time_series_date == df3.covid_date), 
            how='left')
            .drop('PERSON_ID_exp'))
    
    # Add a column to indicate a COVID19 diagnosis in the time series on diagnosis date and 0 otherwise
    tmp8 = (tmp7
            .withColumn('COVID19_ind', f.when((f.col('covid_date').isNotNull()) & (f.col('time_series_date') == f.col('covid_date')), 1).otherwise(0))
            .drop('covid_date'))

    # Add age column
    df1 = df1.select('PERSON_ID', 'date_of_birth').distinct()

    tmp9 = (tmp8
            .join(df1, on='PERSON_ID', how='left')
            .withColumn('age', f.round(f.datediff(f.col('time_series_date'), f.col('date_of_birth')) / 365.25, 2))
            .drop('date_of_birth'))

    # Add day of week and week of year column
    tmp10 = (tmp9
            .withColumn('dayofweek', f.dayofweek(f.col('time_series_date')))
            .withColumn('weekofyear', f.weekofyear(f.col('time_series_date')))
            .withColumn('year', f.year(f.col('time_series_date'))))
    

    # Add environmental variables
    tmp11 = (tmp10
            .join(df4, 
            (tmp10.lsoa == df4.LSOA11CD) &
            (tmp10.time_series_date == df4.date), 
            how='left')
            .drop('LSOA11CD', 'date', 'yr')
            .orderBy(f.col('PERSON_ID').asc(), f.col('time_series_date').asc()))
    
    return tmp11

# COMMAND ----------

# DBTITLE 1,Make time series
time_series_mi_fatal = make_time_series(mi_fatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_mi_nonfatal = make_time_series(mi_nonfatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_stroke_fatal = make_time_series(stroke_fatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_stroke_nonfatal = make_time_series(stroke_nonfatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_at_fatal = make_time_series(at_fatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_at_nonfatal = make_time_series(at_nonfatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_ae_fatal = make_time_series(arterial_events_fatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_ae_nonfatal = make_time_series(arterial_events_nonfatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_ve_fatal = make_time_series(venous_events_fatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_ve_nonfatal = make_time_series(venous_events_nonfatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_covid_fatal = make_time_series(covid_fatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_covid_nonfatal = make_time_series(covid_nonfatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)


# COMMAND ----------

# DBTITLE 1,Save
# List of (variable_name_string, DataFrame_variable)
time_series_datasets = [
    ("time_series_mi_fatal", time_series_mi_fatal),
    ("time_series_mi_nonfatal", time_series_mi_nonfatal),
    ("time_series_stroke_fatal", time_series_stroke_fatal),
    ("time_series_stroke_nonfatal", time_series_stroke_nonfatal),
    ("time_series_at_fatal", time_series_at_fatal),
    ("time_series_at_nonfatal", time_series_at_nonfatal),
    ("time_series_ae_fatal", time_series_ae_fatal),
    ("time_series_ae_nonfatal", time_series_ae_nonfatal),
    ("time_series_ve_fatal", time_series_ve_fatal),
    ("time_series_ve_nonfatal", time_series_ve_nonfatal),
    ("time_series_covid_fatal", time_series_covid_fatal),
    ("time_series_covid_nonfatal", time_series_covid_nonfatal)
]

# Save with correct variable name string
for varname, df in time_series_datasets:
    save_table(df=df, out_name=f"{proj}_tmp_cens_{varname}", save_previous=False)


# COMMAND ----------

# DBTITLE 1,Re-load
time_series_mi_fatal = spark.table(f"{dsa}.{proj}_tmp_cens_time_series_mi_fatal")
time_series_mi_nonfatal = spark.table(f"{dsa}.{proj}_tmp_cens_time_series_mi_nonfatal")
time_series_stroke_fatal = spark.table(f"{dsa}.{proj}_tmp_cens_time_series_stroke_fatal")
time_series_stroke_nonfatal = spark.table(f"{dsa}.{proj}_tmp_cens_time_series_stroke_nonfatal")
time_series_at_fatal = spark.table(f"{dsa}.{proj}_tmp_cens_time_series_at_fatal")
time_series_at_nonfatal = spark.table(f"{dsa}.{proj}_tmp_cens_time_series_at_nonfatal")
time_series_ae_fatal = spark.table(f"{dsa}.{proj}_tmp_cens_time_series_ae_fatal")
time_series_ae_nonfatal = spark.table(f"{dsa}.{proj}_tmp_cens_time_series_ae_nonfatal")
time_series_ve_fatal = spark.table(f"{dsa}.{proj}_tmp_cens_time_series_ve_fatal")
time_series_ve_nonfatal = spark.table(f"{dsa}.{proj}_tmp_cens_time_series_ve_nonfatal")
time_series_covid_fatal = spark.table(f"{dsa}.{proj}_tmp_cens_time_series_covid_fatal")
time_series_covid_nonfatal = spark.table(f"{dsa}.{proj}_tmp_cens_time_series_covid_nonfatal")


# COMMAND ----------

display(time_series_mi_fatal)

# COMMAND ----------

# MAGIC %md # 5. Split up time series for analysis

# COMMAND ----------

import math

def split_time_series(df, max_individuals=50000):
    
    # Get distinct PERSON_IDs
    distinct_person_ids = df.select('PERSON_ID').distinct()
    
    # Count the total number of individuals
    total_individuals = distinct_person_ids.count()
    
    # Determine the number of splits needed
    num_splits = math.ceil(total_individuals / max_individuals)
    
    # Split into chunks
    split_dataframes = [
        df.join(_, on='PERSON_ID', how='inner')
        for _ in distinct_person_ids.randomSplit([1.0]*num_splits)
    ]

    return split_dataframes


# COMMAND ----------

split_df_mi_fatal = split_time_series(time_series_mi_fatal)
split_df_mi_nonfatal = split_time_series(time_series_mi_nonfatal)
split_df_stroke_fatal = split_time_series(time_series_stroke_fatal)
split_df_stroke_nonfatal = split_time_series(time_series_stroke_nonfatal)
split_df_at_fatal = split_time_series(time_series_at_fatal)
split_df_at_nonfatal = split_time_series(time_series_at_nonfatal)
split_df_ae_fatal = split_time_series(time_series_ae_fatal)
split_df_ae_nonfatal = split_time_series(time_series_ae_nonfatal)
split_df_ve_fatal = split_time_series(time_series_ve_fatal)
split_df_ve_nonfatal = split_time_series(time_series_ve_nonfatal)
split_df_covid_fatal = split_time_series(time_series_covid_fatal)
split_df_covid_nonfatal = split_time_series(time_series_covid_nonfatal)


# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Save time series

# COMMAND ----------

from mythoslib import save_table

def save_time_series(split_dfs, name):
  for i, split_df in enumerate(split_dfs):
    save_table(df=split_df, out_name=f'{proj}_out_time_series_cens_{name}_{i+1}', save_previous=False)

# COMMAND ----------

save_time_series(split_df_mi_fatal, 'mi_fatal')
save_time_series(split_df_mi_nonfatal, 'mi_nonfatal')
save_time_series(split_df_stroke_fatal, 'stroke_fatal')
save_time_series(split_df_stroke_nonfatal, 'stroke_nonfatal')
save_time_series(split_df_at_fatal, 'at_fatal')
save_time_series(split_df_at_nonfatal, 'at_nonfatal')
save_time_series(split_df_ae_fatal, 'ae_fatal')
save_time_series(split_df_ae_nonfatal, 'ae_nonfatal')
save_time_series(split_df_ve_fatal, 've_fatal')
save_time_series(split_df_ve_nonfatal, 've_nonfatal')
save_time_series(split_df_covid_fatal, 'covid_fatal')
save_time_series(split_df_covid_nonfatal, 'covid_nonfatal')