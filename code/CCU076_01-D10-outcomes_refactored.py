# Databricks notebook source
# MAGIC %md # CCU076_01-D10-outcomes_refactored
# MAGIC
# MAGIC **Description** This notebook creates the outcomes tables that identifies cases of the outcomes of interest during the study period. For each case the events of each of the defined outcomes are stored in the ouput tables with a corresponding date. A flag is created in the long formatted outcomes table that indicates if someone died at the time of event or within 7 days after. 
# MAGIC
# MAGIC Outcomes are defined as follows:
# MAGIC
# MAGIC Arterial event: Myocardial infarction (MI), Arterial retinal infarction (RI), Arterial dissection and ruptured aneurysm (ADISS), Ischaemic stroke (stroke_IS), Stroke not otherwise specified (stroke_NOS), Other arterial thrombosis (AT).
# MAGIC
# MAGIC Venous event: Venous thrombosis (VT), Pulmonary embolism (PE), Intracranial venous thrombosis (ICVT)
# MAGIC
# MAGIC Myocarditis.
# MAGIC
# MAGIC **Authors** Isabel Walter
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Adapted from work by Stelios Boulitsakis Logothetis; Carmen Petitjean, Spencer Keene (CCU004_03); Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre.) Based on previous work by Tom Bolton (John Nolan, Elena Raffetti) for CCU018_01, and earlier CCU002 sub-projects and subsequently CCU002_07-D09-exposures.
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`out_outcomes`** : outcomes for the cohort (first event).
# MAGIC - **`out_outcomes_wide`** : outcomes for the cohort in wide format (first event).  
# MAGIC - **`out_outcomes_all_with_washout`** : All outcomes for the cohort in long format with washout period applied.

# COMMAND ----------

# MAGIC %md # 0. Setup

# COMMAND ----------

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
from pyspark.sql import Window

import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns


# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print(spark.version)

# COMMAND ----------

# MAGIC %md # 1. Parameters

# COMMAND ----------

# MAGIC %run "./CCU076_01-D01-parameters"

# COMMAND ----------

# MAGIC %md # 2. Data Sources

# COMMAND ----------

# DBTITLE 1,Import data sources
codelist    = spark.table(path_out_codelist_outcomes)
cohort       = spark.table(path_out_cohort)
hes_apc_long = spark.table(path_cur_hes_apc_long)
deaths_long     = spark.table(path_cur_deaths_long)

# COMMAND ----------

# MAGIC %md # 3. Prepare

# COMMAND ----------

# DBTITLE 1,Prepare HES-APC
hes_apc_prepared = (
    hes_apc_long
    # Select primary diagnoses only
    .where(f.col('DIAG_POSITION') == 1)
    # Select and rename columns
    .select(['PERSON_ID', 'EPISTART', 'CODE'])
    .withColumnRenamed('EPISTART', 'DATE')
    # Filter out episodes before the baseline date and after end date
    .where((f.col('DATE') > study_start_date) & (f.col('DATE') < study_end_date))
    # Restrict to cohort 
    .join(cohort.select('PERSON_ID'), on='PERSON_ID', how='inner')
)

hes_apc_prepared_myocarditis = (
    hes_apc_long
    # Select primary and secondary diagnoses
    .where((f.col('DIAG_POSITION') == 1) | (f.col('DIAG_POSITION') == 2))
    # Select and rename columns
    .select(['PERSON_ID', 'EPISTART', 'CODE'])
    .withColumnRenamed('EPISTART', 'DATE')
    # Filter out episodes before the baseline date and after end date
    .where((f.col('DATE') > study_start_date) & (f.col('DATE') < study_end_date))
    # Restrict to cohort 
    .join(cohort.select('PERSON_ID'), on='PERSON_ID', how='inner')
)

# COMMAND ----------

# DBTITLE 1,Prepare Deaths
deaths_prepared = (
    deaths_long
    # Select primary diagnoses only
    .where(f.col('DIAG_POSITION') == 'UNDERLYING')
    # Select and rename columns
    .select(['PERSON_ID', 'DATE', 'CODE'])
    # Filter out episodes before the baseline date and after end date
    .where((f.col('DATE') > study_start_date) & (f.col('DATE') < study_end_date))
    # Restrict to cohort 
    .join(cohort.select('PERSON_ID'), on='PERSON_ID', how='inner')
)

deaths_prepared_myocarditis = (
    deaths_long
    # Select primary and secondary diagnoses
    #.where((f.col('DIAG_POSITION') == 'UNDERLYING') | (f.col('DIAG_POSITION') == 'SECONDARY_1'))
    # Select and rename columns
    .select(['PERSON_ID', 'DATE', 'CODE'])
    # Filter out episodes before the baseline date and after end date
    .where((f.col('DATE') > study_start_date) & (f.col('DATE') < study_end_date))
    # Restrict to cohort 
    .join(cohort.select('PERSON_ID'), on='PERSON_ID', how='inner')
)

# COMMAND ----------

# Arterial composite 
arterial_events = ["MI", "RI", "ADISS", "stroke_IS", "stroke_NOS", "AT"]
codelist_arterial = codelist.filter(f.col("name").isin(arterial_events)).withColumn("name", f.lit("arterial_event"))

# Venous composite
venous_events = ["PE", "VT", "ICVT"]
codelist_venous = codelist.filter(f.col("name").isin(venous_events)).withColumn("name", f.lit("venous_event"))

# Make final codelist with composites, but also individual outcomes
codelist_final = codelist_arterial.union(codelist_venous).union(codelist)

# Filter rows with 'MC' or 'PC' in the 'name' column
codelist_mc_pc = codelist_final[codelist_final['name'].isin(['MC', 'PC'])]

# Creat combination group of myocarditis and pericarditis
mc_pc = ["MC", "PC"]
codelist_mc_pc_comb = codelist.filter(f.col("name").isin(mc_pc)).withColumn("name", f.lit("MC_PC"))

codelist_mc_pc = codelist_mc_pc.union(codelist_mc_pc_comb)

# Remove those rows from codelist_final
codelist_final = codelist_final[~codelist_final['name'].isin(['MC', 'PC'])]

display(codelist_final) 

# COMMAND ----------

display(codelist_mc_pc)

# COMMAND ----------

# MAGIC %md # 4. Create

# COMMAND ----------

# MAGIC %md ## 4.1 Match Codelist

# COMMAND ----------

# DBTITLE 1,Codelist matching
from mythoslib import codelist_match_v3, codelist_match_summary

dict_in = {
     'hes_apc': (hes_apc_prepared, codelist_final, 1),
     'deaths':  (deaths_prepared,  codelist_final, 2)
}

outcomes_all, outcomes_1st, outcomes_1st_wide = codelist_match_v3(dict_in, name_prefix='out_')
outcomes_summ_name, outcomes_summ_name_code = codelist_match_summary(dict_in, outcomes_all)

outcomes_all = temp_save(outcomes_all, f'{proj}_tmp_outcomes_all')
outcomes_1st = temp_save(outcomes_1st, f'{proj}_tmp_outcomes_1st')
outcomes_1st_wide = temp_save(outcomes_1st_wide, f'{proj}_tmp_outcomes_1st_wide')
outcomes_summ_name = temp_save(outcomes_summ_name, f'{proj}_tmp_outcomes_summ_name')
outcomes_summ_name_code = temp_save(outcomes_summ_name_code, f'{proj}_tmp_outcomes_summ_name_code')

dict_in = {
    'hes_apc': (hes_apc_prepared_myocarditis, codelist_mc_pc, 1),
    'deaths':  (deaths_prepared_myocarditis,  codelist_mc_pc, 2)
}

outcomes_all_mc_pc, outcomes_1st_mc_pc, outcomes_1st_wide_mc_pc = codelist_match_v3(dict_in, name_prefix='out_')
outcomes_summ_name_mc_pc, outcomes_summ_name_code_mc_pc = codelist_match_summary(dict_in, outcomes_all_mc_pc)

outcomes_all_mc_pc = temp_save(outcomes_all_mc_pc, f'{proj}_tmp_outcomes_all_myocarditis_pericarditis')
outcomes_summ_name_mc_pc = temp_save(outcomes_summ_name_mc_pc, f'{proj}_tmp_outcomes_summ_name_myocarditis_pericarditis')

# COMMAND ----------

display(outcomes_summ_name)

# COMMAND ----------

display(outcomes_summ_name_mc_pc)

# COMMAND ----------

# DBTITLE 1,Load (if re-running)
outcomes_all = spark.table(f'{dsa}.{proj}_tmp_outcomes_all')
outcomes_1st = spark.table(f'{dsa}.{proj}_tmp_outcomes_1st')
outcomes_1st_wide = spark.table(f'{dsa}.{proj}_tmp_outcomes_1st_wide')
outcomes_summ_name = spark.table(f'{dsa}.{proj}_tmp_outcomes_summ_name')
outcomes_summ_name_code = spark.table(f'{dsa}.{proj}_tmp_outcomes_summ_name_code')

# COMMAND ----------

display(outcomes_all)

# COMMAND ----------

# MAGIC %md # 5. Plots

# COMMAND ----------

# MAGIC %md ### 5.1 First event - Over age at event (years) by data source (stacked)

# COMMAND ----------

def hist_first_event_over_age(df_outcomes_1st, df_cohort, common_y_axis=False, colour_by='source'):
    df = (
        df_outcomes_1st
        .join(df_cohort.select('PERSON_ID', 'date_of_birth', "sex"), on='PERSON_ID', how='inner')
        .withColumn('age', f.datediff(f.col('DATE'), f.col('date_of_birth'))/365.25)
    ).toPandas()
    fig = sns.displot(
        # Plotting parameters: Histogram over age, faceted by outcome, coloured by provided variable ("source" or "sex")
        df, x='age', hue=colour_by, col='name', kind='hist', 
        # Appearance: 5 facets per row, 4x4 squares, x-axis drawn for range 0-100
        col_wrap=5, binwidth=1, height=4, binrange=(0,100), 
        # These keywords control whether the histograms share a common y-axis or not. Also, disable shared x-axes
        facet_kws=dict(sharey=common_y_axis, sharex=False), common_bins=common_y_axis
    )
    # By default, FacetGrid would title each sub-plot as "name=nonfatal_stroke". This command just removes the "name=" part. 
    fig.set_titles('{col_name}')
    fig.set_xlabels('Age (years)')

hist_first_event_over_age(outcomes_1st, cohort)

# COMMAND ----------

# MAGIC %md ### 5.2 First event - Over age at event (years) by sex (overlapping)

# COMMAND ----------

hist_first_event_over_age(outcomes_1st, cohort, colour_by="sex")

# COMMAND ----------

# MAGIC %md # 6. Curate multiple outcome events dataset
# MAGIC
# MAGIC Based on the dataset with all the outcome registrations (outcomes_all), create a dataset (outcomes_all_cur) that allows for multiple outcome events of the same outcome type per individual, with a washout period of 30 days.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Check numbers before
# Summary of total number of distinct individuals per outcome and the total number of registrations per outcome
summary_df = outcomes_all.groupBy('name').agg(
    f.countDistinct('PERSON_ID').alias('distinct_individuals'),
    f.count('*').alias('total_code_registrations')  # Count the number of rows
)

# Show the resulting summary
summary_df.show()

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
  
  print('Note: This function currently uses PERSON_ID, name, DATE, CODE - arguments will be added at a later date to assign these')
  
  # check columns are not in df - as these will be overwritten and dropped
  common_cols = [col for col in df.columns if col in ['_rownum_DATE', '_rownum', '_diff', '_diff_cumsum', '_diff_cumsum_flag']]
  assert len(common_cols) == 0, f'common_cols = {common_cols}'
  
  # define windows to calculate row numbers and date differences with patient/outcome record sets
  # window for duplicate DATE
  _win_DATE = Window\
    .partitionBy('PERSON_ID', 'name', 'DATE')\
    .orderBy('CODE')
  # main window (with stable ordering for duplicate DATE)
  _win = Window\
    .partitionBy('PERSON_ID', 'name')\
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
    print(f'{_counter_working:,} events in total (_rownum_max per person_id and name = {_rownum_max})'); print()

    # tabulate of records identified as within washout by name
    tmpt = tab(_df_working, 'name', '_diff_cumsum_flag'); print()

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
    print(f'{_counter_firstevents:,} events stored for being the first event per person_id and name of iteration {i}')

    # count the rows within the washout period before removing below
    _df_washedout = (
      _df_working
      .where(f.col('_diff_cumsum') <= days)
    )
    _counter_washedout = _df_washedout.count()
    print(f'{_counter_washedout:,} events excluded for being within {days} days of the first event per person_id and name of iteration {i}'); print()

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
  tmpt = tabstat(_df_tmp, '_diff', byvar='name'); print()
  _diff_min = _df_tmp.agg(f.min(f.col('_diff'))).collect()[0][0]
  assert _diff_min > days, '# # # # something went wrong - differences remain that are still less than the washout period # # # #'  
  print(f'differences between events per person_id and name > {days} days is satisfied')
  
  return _df_master

# COMMAND ----------

outcomes_all_cur = washout(df=outcomes_all, days=30)

# COMMAND ----------

# DBTITLE 1,Check individuals with multiple events
# 1. Group by person_id and name, and count the number of events per outcome type
event_counts = outcomes_all_cur.groupBy('PERSON_ID', 'name').agg(
    f.count('*').alias('event_count')
)

# 2. Filter to retain only those with more than one event per outcome type
multiple_events_df = event_counts.filter(f.col('event_count') > 1)

# 3. Inner join this filtered DataFrame into outcomes_all_cur
multiple_outcomes = outcomes_all_cur.join(multiple_events_df, on=['PERSON_ID', "name"], how="inner")

display(multiple_outcomes) 

# COMMAND ----------

multiple_outcomes.select('PERSON_ID').distinct().count()

# COMMAND ----------

# DBTITLE 1,Check numbers after applying washout period
# Summary of total number of distinct individuals per outcome and the total number of registrations per outcome
summary_df = outcomes_all_cur.groupBy('name').agg(
    f.countDistinct('PERSON_ID').alias('distinct_individuals'),
    f.count('*').alias('total_code_registrations')  # Count the number of rows
)

# Show the resulting summary
summary_df.show()

# COMMAND ----------

# DBTITLE 1,Exclude events that occurred after death
# Join with cohort to get date of death
outcomes_all_cur_cohort = outcomes_all_cur.join(
    cohort.select('PERSON_ID', 'date_of_death'), 
    on='PERSON_ID', 
    how='inner'
)

print("📊 Total diagnoses in cohort (per outcome):")
outcomes_all_cur_cohort.groupBy("name").count().show()

# Filter out rows where diagnosis DATE is after date_of_death
outcomes_all_cur_out = outcomes_all_cur_cohort.filter(
    (f.col('date_of_death').isNull()) | (f.col('DATE') <= f.col('date_of_death'))
)

print("📊 Diagnoses after filtering by death date (per outcome):")
outcomes_all_cur_out.groupBy("name").count().show()


excluded_rows = outcomes_all_cur_cohort.filter(
    f.col('date_of_death').isNotNull() & (f.col('DATE') > f.col('date_of_death'))
)

print("📊 Diagnoses excluded due to being after date_of_death (per outcome):")
excluded_rows.groupBy("name").count().show()

# COMMAND ----------

# MAGIC %md # 7. Flag death
# MAGIC
# MAGIC Make a flag for each event in the outcome table that indicates whether someone died at this outcome event or within 7 days after.

# COMMAND ----------

outcomes_all_cur_out = (outcomes_all_cur_out
              # Make flag = 1 if DOD is at or within 7 days after outcome event
              .withColumn('death_flag', f.when((f.col('date_of_death') >= f.col('DATE')) & (f.col('date_of_death') <= f.date_add(f.col("DATE"), 7)), 1).otherwise(None))
              
              .withColumn('death_flag_2week', f.when((f.col('date_of_death') >= f.col('DATE')) & (f.col('date_of_death') <= f.date_add(f.col("DATE"), 14)), 1).otherwise(None))

              .withColumn('death_flag_month', f.when((f.col('date_of_death') >= f.col('DATE')) & (f.col('date_of_death') <= f.date_add(f.col("DATE"), 30)), 1).otherwise(None))

              # Drop DOD column
              .drop('date_of_death')
)


# COMMAND ----------

display(outcomes_all_cur_out)

# COMMAND ----------

# MAGIC %md
# MAGIC # 8. Check

# COMMAND ----------

# DBTITLE 1,Summary of outcomes
# Aggregate
summary_df = (
    outcomes_all_cur_out
    .groupBy("name")
    .agg(
        f.count('*').alias("n_events"),
        f.count("PERSON_ID").alias("n_id"), 
        f.countDistinct("PERSON_ID").alias("n_id_distinct")
    )
)

#  Round to nearest 5
def round_and_mask(col):
    return f.when(col == 0, f.lit("0")) \
            .when((col > 0) & (col < 10), f.lit("<10")) \
            .otherwise((f.round(col / 5) * 5).cast("int").cast("string"))

# Apply masking logic
summary_masked = (
    summary_df
    .withColumn("n_events", round_and_mask(f.col("n_events")))
    .withColumn("n_id", round_and_mask(f.col("n_id")))
    .withColumn("n_id_distinct", round_and_mask(f.col("n_id_distinct")))
)

# Show result
display(summary_masked)

# COMMAND ----------

# MAGIC %md # 9. Save

# COMMAND ----------

# save all outcomes dataset with washout
save_table(df=outcomes_all_cur_out, out_name=f'{proj}_out_outcomes_all_with_washout', save_previous=True)