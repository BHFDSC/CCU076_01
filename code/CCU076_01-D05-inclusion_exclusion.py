# Databricks notebook source
# MAGIC %md # CCU076_01-D05-inclusion_exclusion
# MAGIC  
# MAGIC **Description** This notebook applies the inclusion/exclusion criteria.  
# MAGIC
# MAGIC 1. Exclude individuals not in GDPPR.  
# MAGIC 2. Exclude individuals who died before the start of follow-up.  
# MAGIC 3. Exclude individuals without LSOA between (study start date - 2 yrs) and study end date. 
# MAGIC 4. Exclude individuals with a non-English LSOA at study start date or during the study period.
# MAGIC 5. Exclude individuals who were aged <18 years of age at study start date. 
# MAGIC 6. Exclude individuals who failed quality assurance checks.
# MAGIC
# MAGIC **Authors** Isabel Walter, Fionna Chalmers, Alexia Sampri
# MAGIC  
# MAGIC **Reviewers**
# MAGIC
# MAGIC **Acknowledgements** Code adapted from Stelios Boulitsakis Logothetis and Alexia Sampri who based their work on CCU004_03 (Tom Bolton, Fionna Chalmers, Anna Stevenson).
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu076_01_tmp_inc_exc_cohort`** : cohort remaining after inclusion/exclusion criteria applied
# MAGIC - **`ccu076_01_tmp_inc_exc_flow`** : flowchart displaying total n of cohort after each inclusion/exclusion rule is applied

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

# MAGIC %run "/Shared/SHDS/common/functions"

# COMMAND ----------

# MAGIC %md # 0. Parameters

# COMMAND ----------

# MAGIC %run "./CCU076_01-D01-parameters"

# COMMAND ----------

# MAGIC %md # 1. Data

# COMMAND ----------

skinny              = spark.table(hds_curated_assets_demographic)
deaths              = spark.table(path_cur_deaths_sing)
qa                  = spark.table(f'{dsa}.{proj}_tmp_quality_assurance')
lsoa_multisource    = spark.table(path_cur_lsoa_multisource)


# COMMAND ----------

# MAGIC %md # 2. Prepare

# COMMAND ----------

# DBTITLE 1,Prepare variables for inclusion and exclusion
# Get date two years before study start date 
start_date_lit = f.to_date(f.lit(study_start_date), 'yyyy-MM-dd')
start_two_year_before = spark.range(1).select(f.add_months(start_date_lit, -(12*2)).alias("two_year_before")).collect()[0]["two_year_before"]

# Everyone with an LSOA between (study_start_date - 2 years) and study_end_date
individuals_with_lsoa = (
    lsoa_multisource
    .where(
        (f.col('record_date') >= start_two_year_before) &
        (f.col('record_date') <= study_end_date)
    )
    .select('person_id').distinct()
    .withColumnRenamed('person_id', 'PERSON_ID')
    .withColumn('with_lsoa', f.lit(1))
)

# Get most recent LSOA date for each person at study start date
#window_spec = Window.partitionBy('PERSON_ID').orderBy(f.col('record_date').desc())

# Get most recent record at study start date
#individual_start_dates = (
#      lsoa_multisource
#      .filter(f.col('record_date') <= study_start_date)
#      .withColumn('rank', f.rank().over(window_spec))
#      .filter(f.col('rank') == 1).select('person_id', f.col('record_date').alias('start_date'))
#      .distinct()
#      .orderBy('person_id','rank')
#)

# Window spec to rank by closeness to study_start_date
window_spec = Window.partitionBy('PERSON_ID').orderBy(f.col('date_diff').asc())

# Get LSOA closest to study_start_date within [study_start_date-2y, study_end_date]
individual_start_dates = (
    lsoa_multisource
    .filter(
        (f.col('record_date') >= start_two_year_before) &
        (f.col('record_date') <= study_end_date)
    )
    .withColumn('date_diff', f.abs(f.datediff(f.col('record_date'), start_date_lit)))
    .withColumn('rank', f.rank().over(window_spec))
    .filter(f.col('rank') == 1)
    .select('person_id', f.col('record_date').alias('closest_date'))
    .distinct()
)

# Filter to closest LSOA to study start and LSOAs during study period
lsoa_multisource_filtered = (
      lsoa_multisource
      .filter(f.col('record_date') <= study_end_date)
      .join(individual_start_dates, on='person_id', how='left')
      .filter(f.col('record_date') >= f.col('closest_date'))
)

# Everyone with any recent LSOA outside England
individuals_outside_england = (
      lsoa_multisource_filtered
      .filter(~f.col('lsoa').rlike('^E'))
      .select('person_id').distinct()
      .withColumnRenamed('person_id', 'PERSON_ID')
      .withColumn('outside_england', f.lit(1))
      )

# COMMAND ----------

df_starting = (
    skinny 
    .select('PERSON_ID', 'date_of_birth', 'date_of_death', 'in_gdppr', 'region', 'sex', 'ethnicity_5_group', 'ethnicity_19_group', 'ethnicity_19_code')
    .join(qa.select('PERSON_ID', '_rule_total'), on='PERSON_ID', how='inner')
    .join(individuals_with_lsoa, on ='PERSON_ID', how='left')
    .join(individuals_outside_england, on ='PERSON_ID', how='left')
)

df_starting = temp_save(df=df_starting, out_name=f'{proj}_tmp_inc_exc_merged')

# COMMAND ----------

df_starting = spark.table(f'{dsa}.{proj}_tmp_inc_exc_merged')

# COMMAND ----------

# MAGIC %md # 3. Set inclusion / exclusion conditions

# COMMAND ----------

conditions, condition_descriptions = {}, {}
baseline_date = f.to_date(f.lit(study_start_date))

# Condition 1: Exclude individuals not in GDPPR
conditions[1] = (f.col('in_gdppr') == 1)
condition_descriptions[1] = "Post exclusion of individuals not in GDPPR"

# Condition 2: Exclude individuals who died before baseline 
conditions[2] = (f.col('date_of_death').isNull() | (f.col('date_of_death') > baseline_date))
condition_descriptions[2] = "Post exlusion of individuals who died before baseline"

# Condition 3: Exclude individuals without an LSOA between study start date - 2yrs and study end date
conditions[3] = f.col('with_lsoa').isNotNull()
condition_descriptions[3] = 'Post exclusion of individuals without registered LSOA between (study start date - 2yrs) and study end date'

# Condition 4: Exclude individuals with an LSOA outside England at study start date or during follow-up
conditions[4] = f.col('outside_england').isNull()
condition_descriptions[4] = 'Post exclusion of individuals with an LSOA outside England at study start date or during follow-up'
 
# Condition 5: Exclude individuals aged < 18 years at baseline 
index_minus_18y = f.add_months(baseline_date, -18*12)
conditions[5] = (f.col('date_of_birth') <= index_minus_18y)
condition_descriptions[5] = "Post exclusion of individuals aged < 18 at study start date"

# Condition 6: Exclude individuals who failed the quality assurance
# This step implicitly removes ~19m individuals from the skinny table with missing DOB or sex.
conditions[6] = (f.col('_rule_total').isNull())
condition_descriptions[6] = "Post exclusion of individuals who failed the quality assurance"

# COMMAND ----------

# MAGIC %md # 4. Execute inclusion / exclusion

# COMMAND ----------

# DBTITLE 1,Execute exclusions and run tally
# Initialise working state and running tally
tally = count_var(df_starting, 'PERSON_ID', ret=1, df_desc='Original', indx=0)
df_working = df_starting

# Execute the inclusion/exclusion rules properly
for idx, condition in conditions.items():
    print(f'{idx}: {condition_descriptions[idx]}')
    df_working = df_working.where(condition)
    tally = tally.unionByName(
        count_var(df_working, 'PERSON_ID', ret=1, df_desc=condition_descriptions[idx], indx=idx)
    )

# COMMAND ----------

# DBTITLE 1,check
# Ensure we haven't accidentally ended up with an empty cohort
assert df_working.count() > 0

# COMMAND ----------

# MAGIC %md # 5. Flow diagram

# COMMAND ----------

def flow_diagram_diffs(df):
    """ Given the running tally dataframe, augments with with deltas for every colunm (the difference between the current row and the previous row) """
    r = df.copy()
    for col in ['n', 'n_id', 'n_id_distinct']:
        r[col] = df[col].astype(int)
        
        # Create n_diff, n_id_diff, n_id_distinct_diff
        diff_col = f'{col}_diff'
        r[diff_col] = (r[col] - r[col].shift(1)).fillna(0).astype(int)
    return r

def format_flow_diagram(df):
    """ Given the running tally dataframe, formats the large numerical values with commas """
    r = df.copy()
    for col in ['n', 'n_id', 'n_id_distinct', 'n_diff', 'n_id_diff', 'n_id_distinct_diff']:
        # Reformat to add commas (e.g. 53166323 -> "51,166,323")
        r[col] = df[col].map('{:,.0f}'.format).astype(str)

    return r.set_index('indx')

df_flow = flow_diagram_diffs(
    tally
    .orderBy('indx')
    .select('indx', 'df_desc', 'n', 'n_id', 'n_id_distinct')
    .withColumnRenamed('df_desc', 'stage')
    .toPandas()
)

print(format_flow_diagram(df_flow).to_string())

# COMMAND ----------

# DBTITLE 1,Round
from mythoslib import censor_for_sde

df_flow_censored = df_flow.copy()
# For each numeric column 
for col in df_flow_censored.select_dtypes('number'):
    # Censor the numeric values
    df_flow_censored[col] = df_flow_censored[col].apply(censor_for_sde)

print(format_flow_diagram(df_flow_censored).to_string())

# COMMAND ----------

# MAGIC %md # 6. Save

# COMMAND ----------

df_final = df_working.select('PERSON_ID', 'date_of_birth', 'date_of_death', 'sex', 'ethnicity_5_group', 'ethnicity_19_group', 'ethnicity_19_code')
save_table(df=df_final, out_name=f'{proj}_tmp_inc_exc_cohort', save_previous=True)

# COMMAND ----------

save_table(df=tally, out_name=f'{proj}_tmp_inc_exc_flow', save_previous=True)