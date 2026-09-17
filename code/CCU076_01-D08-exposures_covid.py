# Databricks notebook source
# MAGIC %md # CCU076_01-D08-exposures_covid
# MAGIC
# MAGIC **Description** This notebook creates the exposures, which comprise Covid-19 diagnoses and (number of) Covid-19 vaccinations.
# MAGIC
# MAGIC **Authors** Isabel Walter, Alexia Sampri, Yueying Li, Tom Bolton
# MAGIC
# MAGIC **Acknowledgements** Adapted from work by Stelios Boulitsakis Logothetis; Carmen Petitjean, Spencer Keene (CCU004_03); Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre). Based on previous work by Tom Bolton, John Nolan, Elena Raffetti, Alexia Sampri for CCU018_01, earlier CCU002 sub-projects and subsequently CCU002_07-D09-exposures.
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`out_exposures_covid`** : COVID-19 infection during follow-up for the cohort.
# MAGIC - **`out_exposures_vacc`** : Number of vaccinations during follow-up for the cohort.

# COMMAND ----------

# MAGIC %md # 0. Setup

# COMMAND ----------

# DBTITLE 1,Libraries
spark.sql('CLEAR CACHE')
import pyspark.sql.functions as f
from pyspark.sql import Window
from functools import reduce
import pyspark.pandas as ps

# COMMAND ----------

# DBTITLE 1,Functions
# MAGIC %run "./Spencers_common_functions"

# COMMAND ----------

# MAGIC %md # 1. Parameters

# COMMAND ----------

# MAGIC %run "./CCU076_01-D01-parameters"

# COMMAND ----------

# MAGIC %md # 2. Data Sources

# COMMAND ----------

# DBTITLE 1,Import data sources
cohort = spark.table(path_out_cohort)
covid = spark.table(path_cur_covid)
vacc = spark.table(path_cur_vacc)

# COMMAND ----------

# MAGIC %md # 3. Prepare

# COMMAND ----------

# DBTITLE 1,Cohort subset
# From the curated COVID tables, take the subset of PERSON_IDs corresponding to the study cohort
covid_prepared = covid.join(cohort.select('PERSON_ID'), on='PERSON_ID', how='inner')

# COMMAND ----------

# DBTITLE 1,Check curated tables
# Check the two imported curated tables have the columns required for this notebook to run
required_columns = ['PERSON_ID', 'DATE', 'covid_phenotype', 'covid_status', 'source']
assert all(_ in covid_prepared.columns for _ in required_columns)

# COMMAND ----------

# MAGIC %md # 4. Infection during follow-up

# COMMAND ----------

# MAGIC %md ## 4.2 Prepare

# COMMAND ----------

def confirmed_covid_cases(df_cur):
    # Restrict the curated COVID data to just confirmed cases, as identified by the phenotype 
    # In the case of SNOMED codes which could indicate suspected covid as well, use the covid_status to tell if it's confirmed
    result_confirmed = (
        df_cur
        .where(f.col('covid_phenotype').isin([
            '01_Covid_positive_test',
            '01_GP_covid_diagnosis',
            '02_Covid_admission_any_position',
            '02_Covid_admission_primary_position'
        ]) & (f.col('covid_status') != 'suspected'))
    )

    # Take the subset of COVID admissions with COVID as the primary cause 
    result_confirmed_admissions_primary = result_confirmed.where(
        f.col('covid_phenotype') == '02_Covid_admission_primary_position'
    )

    return result_confirmed, result_confirmed_admissions_primary

covid_confirmed, covid_confirmed_admissions_primary = confirmed_covid_cases(covid_prepared)

# COMMAND ----------

# MAGIC %md ## 4.3 Create

# COMMAND ----------

tab(covid_confirmed, 'covid_phenotype')

# COMMAND ----------

# DBTITLE 1,first cases and severity
# def first_covid_cases(df_confirmed, df_confirmed_admissions, name_prefix='exp_'):
#     window_1st = (
#         Window
#         .partitionBy('PERSON_ID')
#         .orderBy('DATE', 'covid_phenotype')
#     )

#     # filter to first (earliest) confirmed covid infection
#     # note: ignore ties in covid_phenotype for now
#     result_1st = (
#         df_confirmed
#         .withColumn('_row_num', f.row_number().over(window_1st))
#         .where(f.col('_row_num') == 1)
#         .withColumnRenamed('DATE', f'{name_prefix}covid_1st_date')
#         .withColumnRenamed('covid_phenotype', f'{name_prefix}covid_1st_phenotype')
#     )

#     # Take the first COVID-specific admission that comes after the first COVID infection per person
#     result_severity = (
#         df_confirmed_admissions
#         .join(result_1st, on='PERSON_ID', how='inner')
#         .where(f.col('DATE') >= f.col(f'{name_prefix}covid_1st_date'))
#         .withColumn('_row_num', f.row_number().over(window_1st))
#         .where(f.col('_row_num') == 1)
#     )

#     # Augment with the number of days it took from infection to admission, and indicator of whether this was over 28 days
#     result_severity = (
#         result_severity
#         .withColumnRenamed('DATE', f'{name_prefix}covid_admission_date')
#         .withColumn(f'{name_prefix}covid_admission_days', f.datediff(f.col(f'{name_prefix}covid_admission_date'), f.col(f'{name_prefix}covid_1st_date')))
#         .withColumn(f'{name_prefix}covid_admission_days_leq_28', f.when(f.col(f'{name_prefix}covid_admission_days') <= 28, 1).otherwise(0))
#         .select('PERSON_ID', f'{name_prefix}covid_admission_date', f'{name_prefix}covid_admission_days', f'{name_prefix}covid_admission_days_leq_28')
#     )

#     return (
#         result_1st
#         .join(result_severity, on='PERSON_ID', how='left')
#         .select('PERSON_ID', f'{name_prefix}covid_1st_date', f'{name_prefix}covid_1st_phenotype', f'{name_prefix}covid_admission_date', f'{name_prefix}covid_admission_days', f'{name_prefix}covid_admission_days_leq_28')
#     )


# covid_confirmed_1st = first_covid_cases(covid_confirmed, covid_confirmed_admissions_primary, name_prefix='exp_')

# COMMAND ----------

# DBTITLE 1,Washout function
from pyspark.sql import DataFrame

def washout(df: DataFrame, days: int) -> DataFrame:
  
  # person_id: str, name: str, date: str, code: str, 
  
  '''
  Applies washout period
  
  Args:
    df: Spark DataFrame.
    person_id: NOT CODED YET
    name: NOT CODED YET
    date: NOT CODED YET
    code: NOT CODED YET
    days: washout period (e.g., 30 days)   
    
  Example usage:
    washout(df, days = 30)
    >> 
    
  Notes:
    ...
  '''
  
  print('=================================================================================')
  print('washout')
  print('=================================================================================')  
  
  print('Note: This function currently uses PERSON_ID, DATE - arguments will be added at a later date to assign these')
  
  # check columns are not in df - as these will be overwritten and dropped
  common_cols = [col for col in df.columns if col in ['_rownum_DATE', '_rownum', '_diff', '_diff_cumsum', '_diff_cumsum_flag']]
  assert len(common_cols) == 0, f'common_cols = {common_cols}'
  
  # define windows to calculate row numbers and date differences with patient/outcome record sets
  # window for duplicate DATE
  _win_DATE = Window\
    .partitionBy('PERSON_ID', 'DATE')\
    .orderBy('covid_phenotype')

  # main window (with stable ordering for duplicate DATE)
  _win = Window\
    .partitionBy('PERSON_ID')\
    .orderBy('DATE', '_rownum_DATE')

  # initialise
  _df_master = []
  _df_washedout = []
  _df_working = df
  _counter_working = _df_working.count()
  print(f'{_counter_working:,} events in total initially'); print()

  # iterate through
  i = 0
  while _counter_working > 0:
    i += 1
    
    print('---------------------------------------------------------------------------------')
    print(f'iteration = {i}') 
    print('---------------------------------------------------------------------------------')
    # calculate row numbers, differences and flag
    _df_working = (
      _df_working
      .withColumn('_rownum_DATE', f.row_number().over(_win_DATE))
      .withColumn('_rownum', f.row_number().over(_win))
      .withColumn('_diff', f.datediff(f.col('DATE'), f.lag(f.col('DATE'), 1).over(_win)))
      .withColumn('_diff_cumsum', f.sum(f.col('_diff')).over(_win))
      .withColumn('_diff_cumsum_flag', f.when(f.col('_rownum') == 1, f.lit('0_first')).when(f.col('_diff_cumsum') > days, '2_gt_washout').otherwise('1_le_washout'))
    )

    # calculate maximum row number
    _rownum_max = _df_working.agg(f.max(f.col('_rownum'))).collect()[0][0]

    # print number of records and maximum row number for this iteration
    # the latter provides an idea of expected number of subsequent iterations
    print(f'{_counter_working:,} events in total (_rownum_max per person_id = {_rownum_max})'); print()

    # tabulate of records identified as within washout by name
    tmpt = tab(_df_working, '_diff_cumsum_flag'); print()

    # output first row for this iteration to the master dataframe
    _df_out = (
      _df_working
      .where(f.col('_rownum') == 1)
      .drop('_rownum_DATE', '_rownum', '_diff', '_diff_cumsum', '_diff_cumsum_flag')
    )
    _counter_firstevents = _df_out.count()
    if(i == 1): 
      _df_master = _df_out
    else:
      _df_master = (
        _df_master
        .unionByName(_df_out)
      )

    # remove the first row and rows within the washout period from the working dataframe (for the next iteration)
    # remove the first row
    _df_working = (
      _df_working
      .where(f.col('_rownum') != 1)
    )
    print(f'{_counter_firstevents:,} events stored for being the first event per person_id of iteration {i}')

    # count the rows within the washout period before removing below
    _df_washedout = (
      _df_working
      .where(f.col('_diff_cumsum') <= days)
    )
    _counter_washedout = _df_washedout.count()
    print(f'{_counter_washedout:,} events excluded for being within {days} days of the first event per person_id of iteration {i}'); print()

    # remove the rows within the washout period
    _df_working = (
      _df_working\
      .where(f.col('_diff_cumsum') > days)
    )
    _counter_working = _df_working.count()


  print('---------------------------------------------------------------------------------')
  print(f'check') 
  print('---------------------------------------------------------------------------------')      
  # check that differences between events are now all greater than the washout period
  
  # calculate row numbers, differences  
  _df_tmp = (
    _df_master
    .withColumn('_rownum_DATE', f.row_number().over(_win_DATE))
    .withColumn('_rownum', f.row_number().over(_win))
    .withColumn('_diff', f.datediff(f.col('DATE'), f.lag(f.col('DATE'), 1).over(_win)))
  )
  
  # check
  tmpt = tabstat(_df_tmp, '_diff'); print()
  _diff_min = _df_tmp.agg(f.min(f.col('_diff'))).collect()[0][0]
  assert _diff_min > days, '# # # # something went wrong - differences remain that are still less than the washout period # # # #'  
  print(f'differences between events per person_id and name > {days} days is satisfied')
  
  return _df_master

# COMMAND ----------

display(covid_confirmed)

# COMMAND ----------

covid_confirmed_washout = washout(df=covid_confirmed, days=42)

# COMMAND ----------

# DBTITLE 1,Right join cohort and remove diagnoses that occurred after death or where DATE is null
# Join covid_confirmed with cohort 
covid_confirmed_cohort = covid_confirmed.join(
    cohort.select('PERSON_ID', 'date_of_death'), 
    on='PERSON_ID', 
    how='inner'
)

total_initial = covid_confirmed_cohort.count()
print(f"Total COVID-confirmed diagnoses in cohort: {total_initial}")

# Filter out rows where diagnosis DATE is after date_of_death
covid_confirmed_filter1 = covid_confirmed_cohort.filter(
    (f.col('date_of_death').isNull()) | (f.col('DATE') <= f.col('date_of_death'))
)

total_after_filter1 = covid_confirmed_filter1.count()

excluded_rows1 = covid_confirmed_cohort.filter(
    f.col('date_of_death').isNotNull() & (f.col('DATE') > f.col('date_of_death'))
)
excluded_count1 = excluded_rows1.count()

print(f"Diagnoses excluded due to being after date_of_death: {excluded_count1}")
print(f"Total COVID-confirmed diagnoses in cohort after filtering after death: {total_after_filter1}")

# Filter out rows where DATE is null
covid_confirmed_out = covid_confirmed_filter1.filter(
    f.col('DATE').isNotNull()
)

total_after_filter2 = covid_confirmed_out.count()

excluded_rows2 = covid_confirmed_filter1.filter(
    f.col('DATE').isNull()
)
excluded_count2 = excluded_rows2.count()

print(f"Diagnoses excluded due to no date info: {excluded_count2}")
print(f"Total COVID-confirmed diagnoses in cohort after filtering out no DATE: {total_after_filter2}")

# Repeat similar steps for the washout dataset
# Join with cohort 
covid_confirmed_washout_cohort = covid_confirmed_washout.join(
    cohort.select('PERSON_ID', 'date_of_death'), 
    on='PERSON_ID', 
    how='inner'
)

total_initial = covid_confirmed_washout_cohort.count()
print(f"Total COVID-confirmed diagnoses in cohort: {total_initial}")

# Filter out rows where diagnosis DATE is after date_of_death
covid_confirmed_washout_filter1 = covid_confirmed_washout_cohort.filter(
    (f.col('date_of_death').isNull()) | (f.col('DATE') <= f.col('date_of_death'))
)

total_after_filter1_washout = covid_confirmed_washout_filter1.count()

excluded_rows1_washout = covid_confirmed_washout_cohort.filter(
    f.col('date_of_death').isNotNull() & (f.col('DATE') > f.col('date_of_death'))
)
excluded_count1_washout = excluded_rows1_washout.count()

print(f"Diagnoses excluded due to being after date_of_death: {excluded_count1_washout}")
print(f"Total COVID-confirmed diagnoses in cohort after filtering after death: {total_after_filter1_washout}")

# Filter out rows where DATE is null
covid_confirmed_washout_out = covid_confirmed_washout_filter1.filter(
    f.col('DATE').isNotNull()
)

total_after_filter2_washout = covid_confirmed_washout_out.count()

excluded_rows2_washout = covid_confirmed_washout_filter1.filter(
    f.col('DATE').isNull()
)
excluded_count2_washout = excluded_rows2_washout.count()

print(f"Diagnoses excluded due to no date info: {excluded_count2_washout}")
print(f"Total COVID-confirmed diagnoses in cohort after filtering out no DATE: {total_after_filter2_washout}")


# COMMAND ----------

# DBTITLE 1,7 day death flag
covid_confirmed_out = (covid_confirmed_out
              # Make flag = 1 if DOD is at or within 7 days after outcome event
              .withColumn('death_flag', f.when((f.col('date_of_death') >= f.col('DATE')) & (f.col('date_of_death') <= f.date_add(f.col("DATE"), 7)), 1).otherwise(None))

              .withColumn('death_flag_2week', f.when((f.col('date_of_death') >= f.col('DATE')) & (f.col('date_of_death') <= f.date_add(f.col("DATE"), 14)), 1).otherwise(None))

              .withColumn('death_flag_month', f.when((f.col('date_of_death') >= f.col('DATE')) & (f.col('date_of_death') <= f.date_add(f.col("DATE"), 30)), 1).otherwise(None))

              # Drop DOD column
              .drop('date_of_death')
)

covid_confirmed_washout_out = (covid_confirmed_washout_out
              # Make flag = 1 if DOD is at or within 7 days after outcome event
              .withColumn('death_flag', f.when((f.col('date_of_death') >= f.col('DATE')) & (f.col('date_of_death') <= f.date_add(f.col("DATE"), 7)), 1).otherwise(None))
              
              .withColumn('death_flag_2week', f.when((f.col('date_of_death') >= f.col('DATE')) & (f.col('date_of_death') <= f.date_add(f.col("DATE"), 14)), 1).otherwise(None))

              .withColumn('death_flag_month', f.when((f.col('date_of_death') >= f.col('DATE')) & (f.col('date_of_death') <= f.date_add(f.col("DATE"), 30)), 1).otherwise(None))

              # Drop DOD column
              .drop('date_of_death')
)

# COMMAND ----------

# MAGIC %md # 5. Vaccinations

# COMMAND ----------

# DBTITLE 1,Get a dataset with number of vaccinations during study period

# Get vaccination count per individual
vacc_wide = (
    vacc
    .filter(f.col('DATE') >= study_start_date)
    .filter(f.col('DATE') <= study_end_date)
    .groupBy('PERSON_ID').agg(
    f.count('PROCEDURE_CAT').alias('vaccination_count'))
)

# COMMAND ----------

display(vacc_wide)

# COMMAND ----------

vacc_wide_out = vacc_wide.join(cohort.select('PERSON_ID'), on='PERSON_ID', how='right').fillna({'vaccination_count': 0})


# COMMAND ----------

# MAGIC %md # 6. Save

# COMMAND ----------

save_table(df=covid_confirmed_out, out_name=f'{proj}_out_exposures_covid', save_previous=False)
save_table(df=covid_confirmed_washout_out, out_name=f'{proj}_out_exposures_covid_with_washout', save_previous=False)
save_table(df=vacc_wide_out, out_name=f'{proj}_out_exposures_vacc', save_previous=False)