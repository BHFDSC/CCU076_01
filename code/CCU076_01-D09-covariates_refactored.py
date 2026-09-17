# Databricks notebook source
# MAGIC %md # CCU076_01-D09-covariates_refactored
# MAGIC  
# MAGIC **Description** This notebook creates following covariates at baseline:
# MAGIC   - Smoking status
# MAGIC   - Medical history: overweight/obesity, history of hypertension, history of heart disease, history of stroke, history of depression, composite: diabetes, copd, liver disease, CKD, cancer, asthma. 
# MAGIC
# MAGIC **Authors**  Yueying Li, Isabel Walter
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Adapted from CCU004_03 (Stelios Boulitsakis Logothetis; Carmen Petitjean, Spencer Keene; Tom Bolton, Fionna Chalmers, Anna Stevenson).
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`out_covariates`** : Covariates for the cohort.

# COMMAND ----------

# MAGIC %md # 0. Setup

# COMMAND ----------

# DBTITLE 1,Libraries
spark.sql('CLEAR CACHE')

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import databricks.koalas as ks
# import pandas as pd
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

# DBTITLE 1,Common Functions
# MAGIC %run "/Shared/SHDS/common/functions"

# COMMAND ----------

# MAGIC %md # 1. Parameters

# COMMAND ----------

# MAGIC %run "./CCU076_01-D01-parameters"

# COMMAND ----------

# MAGIC %md # 2. Data Sources

# COMMAND ----------

# DBTITLE 1,Import data sources
codelist = spark.table(path_out_codelist_covariates)
cohort = spark.table(path_out_cohort)
hes_apc_long = spark.table(path_cur_hes_apc_long)
gdppr   = extract_batch_from_archive(parameters_df_datasets, 'gdppr')

# COMMAND ----------

tab(codelist,"name")

# COMMAND ----------

# MAGIC %md # 3. Prepare

# COMMAND ----------

# MAGIC %md ## 3.1 Codelist
# MAGIC
# MAGIC We select the codelists for smoking groups (current, ex, or never-smoker) and for different sources

# COMMAND ----------

# DBTITLE 1,Partition codelist by covariate
codelist_smoking = (
    codelist
    .where(f.col('name').isin([
        'smoking_current','smoking_ex','smoking_never'
    ]))
)

codelist_bmi = (
    codelist
    .where(f.col('name').isin(['bmi']))
)

# COMMAND ----------

# prepare codelist medical history incl composites
codelist_heart = (
    codelist
    .where(f.col('name').isin([
       'CAD','VHD','HCD','CM', 'ARR', 'PH', 'HF', 'AD', 'HT', 'MC', 'PC', 'CHD' 
    ])).withColumn('name', f.lit("heart_disease")))

codelist_cov = (
    codelist
    .where(f.col('name').isin([
        'hypertension','depression','BMI_obesity','stroke', 'diabetes', 'CKD'
    ])))
    
codelist_comp = (
    codelist
    .where(f.col('name').isin([
       'copd', 'diabetes', 'cancer', 'liver_disease', 'CKD','asthma'
    ])).withColumn('name', f.lit('comorb_comp')))

display(codelist_cov)

codelist_hx = codelist_cov.union(codelist_heart).union(codelist_comp)

# CAREFUL: Having capitals in the covariate names causes weird bugs in the codelist matching summarisation. Just make them lowercase for convenience
codelist_prepared = codelist_hx.withColumn('name', f.lower(f.col('name')))
codelist_icd, codelist_snomed = (
    codelist_prepared.where(f.col('terminology') == 'ICD10'),
    codelist_prepared.where(f.col('terminology') == 'SNOMED')
)
condition_names = codelist_prepared.select('name').distinct().toPandas().name.values
condition_names

# COMMAND ----------

display(codelist_comp)

# COMMAND ----------

# MAGIC %md ## 3.2 Curate Source Data

# COMMAND ----------

# We perform identical preparation steps for each data source, so we define a function to do it
prepare_dataset = lambda df: (
    df
    # Filter nulls
    .where(f.col('PERSON_ID').isNotNull() & f.col('DATE').isNotNull())
    # Join with cohort to limit size a bit
    .join(cohort.select('PERSON_ID', 'date_of_birth'), on='PERSON_ID', how='inner')
    # Necessary column for `codelist_match()`. Might as well use it to filter dates prior to the date of birth
    .withColumnRenamed('date_of_birth', 'CENSOR_DATE_START')
    .withColumn('CENSOR_DATE_END', f.lit(study_start_date))
    # Filter to time window - record must be after the patient's DOB but before the baseline date
    .where((f.col('DATE') > f.col('CENSOR_DATE_START')) & (f.col('DATE') <= study_start_date))
)

gdppr_prepared, hes_prepared = (
    prepare_dataset(gdppr.select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 'DATE', 'CODE')),
    prepare_dataset(hes_apc_long.select('PERSON_ID', f.col('EPISTART').alias('DATE'), 'CODE'))
)

# COMMAND ----------

# MAGIC %md # 4. Smoking
# MAGIC

# COMMAND ----------

# MAGIC %md ## 4.1 Codematch all

# COMMAND ----------

# MAGIC %md First, we extract all the smoking records for each individual between the censor start and end dates via simple inner join with the codelist.

# COMMAND ----------

# DBTITLE 1,Match all smoking records
gdppr_smoking = gdppr_prepared.join(codelist_smoking, on='code', how='inner')

# COMMAND ----------

display(gdppr_smoking)

# COMMAND ----------

# MAGIC %md ## 4.2 Prioritise records

# COMMAND ----------

# DBTITLE 1,Select the smoking record with highest priority
# Calculate time difference between smoking record and study start
smoking_history = (
    gdppr_smoking
    .withColumn('diff_abs', f.abs(f.datediff(f.col('DATE'), f.lit(study_start_date))/365.25))
)

# Set how records should be ordered
smoking_window_spec = (
    Window
    .partitionBy('PERSON_ID')
    .orderBy(f.col("PERSON_ID"),f.col("diff_abs").asc())  
)

# Pick most recent record
smoking_prioritised = (
    smoking_history
    .withColumn('_rank', f.dense_rank().over(smoking_window_spec))
    .where(f.col('_rank') == 1)
    .drop('_rank')
)

# COMMAND ----------

# MAGIC %md ##4.3 Smoking history censoring date

# COMMAND ----------

# MAGIC %md Given the highest priority smoking record per patient, we store the date of that record. This will be the "censoring date" for the patient's smoking history. We use this patient-individual date to further prepare GDPRR (filtering out records dated after that censoring date).

# COMMAND ----------

# DBTITLE 1,Obtain patient-specific smoking history censoring date
smoking_individual_censor_dates = (
    smoking_prioritised
    .select(f.col("PERSON_ID"),f.col("DATE").alias("CENSOR_DATE_END_SMOKING"))
)

# COMMAND ----------

# MAGIC %md ##4.4 Full smoking history

# COMMAND ----------

# MAGIC %md We use the censoring dates to extract each patient's full smoking history from GDPPR. First we filter GDPPR to just all records up to the patient-individual censoring date for each patient. Then we finally match with the smoking codelist. 

# COMMAND ----------

# DBTITLE 1,Prepare GDPPR
# Filter GDPPR to exclude records > than an individual's CENSOR_DATE_END - note gdppr here includes all codes (not just smoking)

gdppr_censored = (
    gdppr_prepared
    .join((smoking_individual_censor_dates), on=["PERSON_ID"], how="inner")
    .where(f.col('DATE') <= f.col('CENSOR_DATE_END_SMOKING'))
)

# COMMAND ----------

# DBTITLE 1,Codelist match
from mythoslib import codelist_match_v3, codelist_match_summary
_smoking_in = {
    'gdppr': (gdppr_censored, codelist_smoking,  1)
}

# Complete matching table, Long-format table of the latest matched record
smoking_all, smoking_latest, smoking_latest_wide = codelist_match_v3(_smoking_in, name_prefix=f'cov_', latest_event=True)
_,smoking_summary_name_code = codelist_match_summary(_smoking_in, smoking_all) # there are two outputs of this command: summary by name and summary by code. The first output is replaced by _ because we don't need it.

# COMMAND ----------

display(smoking_latest)
display(smoking_latest_wide)
display(smoking_summary_name_code)

# COMMAND ----------

# DBTITLE 1,Save
# temp save
smoking_latest = temp_save(df=smoking_latest, out_name=f'{proj}_tmp_covariates_smoking_latest')
smoking_latest_wide = temp_save(df=smoking_latest_wide, out_name=f'{proj}_tmp_covariates_smoking_latest_wide')
smoking_summary_name_code = temp_save(df=smoking_summary_name_code, out_name=f'{proj}_tmp_covariates_smoking_summary_name_code')

# COMMAND ----------

# DBTITLE 1,Load (if re-running)
# Re-load tables if re-running
smoking_latest = spark.table(f'{dsa}.{proj}_tmp_covariates_smoking_latest')
smoking_latest_wide = spark.table(f'{dsa}.{proj}_tmp_covariates_smoking_latest_wide')
smoking_summary_name_code = spark.table(f'{dsa}.{proj}_tmp_covariates_smoking_summary_name_code')

# COMMAND ----------

# MAGIC %md ##4.5 Smoking status

# COMMAND ----------

# MAGIC %md We obtain the "smoking status" per person. I.e., their latest smoking category (Current, Ex, or Never). We further define a "never smoking upgrade" indicator - it's value is `1` if the latest smoking category is Never but there's past data indicating past smoking (thus the Never category is erroneous). In such cases, we set the smoking status to Ex. 

# COMMAND ----------

# DBTITLE 1,Aux function - Map a column to a new one with a dictionary
from itertools import chain
def map_dict(column, mapping):
    """ Given the name of a column and a dictionary, set up the expression to generate a new column using the dictionary 
    Use like so: df.withColumn('new_column', map_dict('old_column', my_dictionary))
    """
    mapping_expr = f.create_map([f.lit(x) for x in chain(*mapping.items())])
    return mapping_expr[f.col(column)]

# COMMAND ----------

# DBTITLE 1,Derive smoking status
def smoking_status(df_smoking_latest, df_smoking_latest_wide):
    """ Given the matched codelist tables:
    smoking_last: (PERSON_ID, CODE, NAME, DATE) - (long format, one record for smoking_never, one for smoking_current, etc)
    smoking_last_wide: (PERSON_ID, SMOKING_CURRENT_DATE, SMOKING_EX_DATE, SMOKING_NEVER_DATE) - (wide format, one record per PERSON_ID)
    Identifies the latest smoking category per PERSON_ID and produces an indicator of whether the latest is an "erroneous" "Never Smoker" record
    I.e. the latest is "Never Smoker" but there's past data indicating past smoking.
    If two smoking records have identical dates, we prioritise SMOKING_CURRENT, then EX, then NEVER. 
    """

    # First, obtain an indicator of whether there's a "Never Smoker" record dated later than current or ex smoker records
    current, ex, never = f.col('cov_smoking_current_date'), f.col('cov_smoking_ex_date'), f.col('cov_smoking_never_date')
    latest = f.greatest(ex, current)

    # We access the `smoking_last_wide` dataframe, (PERSON_ID, SMOKING_CURRENT_DATE, SMOKING_EX_DATE, SMOKING_NEVER_DATE)
    # We flag records where SMOKING_NEVER_DATE is not null and greater than SMOKING_CURRENT_DATE or SMOKING_EX_DATE (assuming at least one is not null)
    df_upgrade = (
        df_smoking_latest_wide
        .withColumn('cov_smoking_never_upgrade', (never.isNotNull() & latest.isNotNull() & (never > latest)).cast('integer'))
        .select('PERSON_ID', 'cov_smoking_never_upgrade')
    )

    # Second, we obtain the latest smoking record per person
    # We partition by PERSON_ID and order by date (descending)
    smoking_window = (
        Window.partitionBy('PERSON_ID').orderBy(f.col("DATE").desc(), f.col('_priority').desc())  
    )

    tie_priority = {
        'smoking_current': 1,
        'smoking_ex': 2,
        'smoking_never': 3
    }

    # Then we simply pick the top row per partition
    # CAREFUL: We use _priority to explicitly resolve ties. row_number would otherwise implicitly pick a random option
    # Instead, we use tie_priority to specify that smoking_current wins out in ties
    df_latest = (
        df_smoking_latest
        .withColumn('_priority', map_dict('name', tie_priority))
        .withColumn('_rank', f.row_number().over(smoking_window)).where(f.col('_rank') == 1).drop('_rank')
        .withColumnRenamed('name', 'cov_smoking_status')
        .withColumnRenamed('DATE', 'cov_smoking_date')
    )

    # We inner join the resulting tables to obtain (PERSON_ID, COV_SMOKING_STATUS, COV_SMOKING_NEVER_UPGRADE)
    # Result table has only one record per PERSON_ID
    result = (
        df_latest
        .join(df_upgrade, how='inner', on='PERSON_ID')
        # We upgrade never-smokers to ex-smokers if they have a smoking history we identified
        .withColumn('cov_smoking_status', f.when(
            f.col('cov_smoking_never_upgrade') == 1, "smoking_ex"
        ).otherwise(f.col('cov_smoking_status')))

        .select('PERSON_ID', 'cov_smoking_date', 'cov_smoking_status', 'cov_smoking_never_upgrade')
    ) 

    return result

# COMMAND ----------

# DBTITLE 1,Save
# save
smoking_latest_status = smoking_status(smoking_latest, smoking_latest_wide)
smoking_latest_status = temp_save(df=smoking_latest_status, out_name=f'{proj}_tmp_covariates_smoking_latest_status')

# COMMAND ----------

# DBTITLE 1,Check
# checks
smoking_latest_status = spark.table(f'{dsa}.{proj}_tmp_covariates_smoking_latest_status')
tab(smoking_latest_status, 'cov_smoking_status', 'cov_smoking_never_upgrade')

# COMMAND ----------

# MAGIC %md ##4.6 Check

# COMMAND ----------

# MAGIC %md ###4.6.1 Distinct code matches
# MAGIC
# MAGIC We inspect how many records and how many distinct PERSON_IDs per smoking SNOMED code.

# COMMAND ----------

display(smoking_summary_name_code.orderBy('name', 'terminology', 'code'))

# COMMAND ----------

# MAGIC %md ### 4.6.2 Distinct PERSON_IDs
# MAGIC
# MAGIC We ensure the final result is a subset of the cohort table and has no duplicate PERSON_IDs

# COMMAND ----------

smoking_latest_status_unique_ids = smoking_latest_status.select('PERSON_ID').distinct().count()

assert smoking_latest_status_unique_ids == smoking_latest_status.count()
assert smoking_latest_status_unique_ids <= cohort.count()

# COMMAND ----------

# MAGIC %md # 5. Medical history
# MAGIC

# COMMAND ----------

# MAGIC %md ##5.1 Time windows
# MAGIC
# MAGIC We define time windows for each condition, in case we want them to limit some of them to a specific look-back. E.g., only consider AF up to 5 years before baseline. The given min and max values are measured in months relative to the baseline date.

# COMMAND ----------

# Time-windows (in months, relative to the study start date) for matching chronic conditions.
# Expected format: "name": (start_months, end_months). 
# Set end_months to 0 to stop at the baseline date. 
# Set a large (negative) value for start_months to have unlimited look-back (e.g. -100*12)

condition_windows = {
    'bmi_obesity': (-5*12, 0),
    'hypertension': (-5*12, 0),
    'heart_disease': (-100*12, 0),
    'stroke': (-100*12, 0),
    'depression': (-5*12, 0),
    'diabetes': (-100*12,0),
    'ckd': (-100*12, 0),
    'comorb_comp': (-5*12, 0)
}

assert set(condition_names) == set(condition_windows.keys()), "There are missing or extraneous covariates in the condition_windows dictionary. Make sure it exactly matches the contents of the codelist."

# COMMAND ----------

# MAGIC %md ##5.2 Codelist match

# COMMAND ----------

from mythoslib import codelist_match_v3_time_filtered, codelist_match_summary
# dictionary - dataset, codelist, and ordering in the event of tied records
dict_in = {
    'hes_apc':  (hes_prepared,   codelist_icd,    1),
    'gdppr':    (gdppr_prepared, codelist_snomed, 2)
}

# run codelist match and codelist match summary functions
hx_out_all, hx_out_last, hx_out_last_wide = codelist_match_v3_time_filtered(dict_in, censor_windows=condition_windows, baseline_date=study_start_date, latest_event=1, name_prefix='cov_')
hx_out_summ_name, hx_out_summ_name_code = codelist_match_summary(dict_in, hx_out_all)

# Temp save
hx_out_all = temp_save(df=hx_out_all, out_name=f'{proj}_tmp_covariates_all')
hx_out_last = temp_save(df=hx_out_last, out_name=f'{proj}_tmp_covariates_last')
hx_out_last_wide = temp_save(df=hx_out_last_wide, out_name=f'{proj}_tmp_covariates_last_wide')
hx_out_summ_name = temp_save(df=hx_out_summ_name, out_name=f'{proj}_tmp_covariates_summ_name')
hx_out_summ_name_code = temp_save(df=hx_out_summ_name_code, out_name=f'{proj}_tmp_covariates_summ_name_code')

# COMMAND ----------

# MAGIC %md ## 5.3 check
# MAGIC We inspect the summary table counting how many matches were found per covariate per dataset, and how many distinct PERSON_IDs these correspond to. We also run the following automated checks:
# MAGIC
# MAGIC We ensure that all the matched records lie within the specified covariate-specific time windows. _check_enforce_condition_specific_dates
# MAGIC We ensure we didn't miss any valid records. We do this by manually matching the code list in a simply verifiable way and checking that the output is the same. It's slow to do this for all covariates so we sample one or two. _check_manually_compare_match_counts
# MAGIC We ensure the wide-format output table matches the results of the long format table (as a sanity check for our filtering).

# COMMAND ----------

# check codelist match summary by name and source
display(hx_out_summ_name)

# COMMAND ----------

from dateutil.relativedelta import relativedelta

def _check_get_condition_specific_dates(condition_name):
    baseline_date = datetime.datetime.strptime(study_start_date, '%Y-%m-%d')
    start_date, stop_date = condition_windows[condition_name]
    start_date, stop_date = baseline_date + relativedelta(months=start_date), baseline_date + relativedelta(months=stop_date)
    return start_date, stop_date

def _check_enforce_condition_specific_dates(df_all):
    col_start, col_stop = f.when(f.lit(False), study_start_date), f.when(f.lit(False), study_start_date) #initialize columns
    for name in condition_windows.keys():
        start_date, stop_date = _check_get_condition_specific_dates(name)
        col_start = col_start.when(f.col('name') == name, start_date)
        col_stop = col_stop.when(f.col('name') == name, stop_date)
    
    df_all_aug = df_all.withColumn('start_date', col_start).withColumn('stop_date', col_stop)
    assert df_all_aug.where(
        (f.col('DATE') < f.col('start_date')) | (f.col('DATE') > f.col('stop_date'))
    ).count() == 0

_check_enforce_condition_specific_dates(hx_out_all)


# COMMAND ----------

tab(codelist_prepared, "terminology","name")

# COMMAND ----------

def _check_manually_compare_match_counts(condition_name, df_all):
    start_date, stop_date = _check_get_condition_specific_dates(condition_name)
    codes =  codelist_prepared.where((f.col('terminology') == "SNOMED") | (f.col('terminology') == "ICD10")).where(f.col('name') == condition_name)

    manual_match = (
        gdppr_prepared
        .unionByName(hes_prepared)
        .where((f.col('DATE') > f.lit(start_date)) & (f.col('DATE') <= f.lit(stop_date)))
        .join(codes, on='code', how='inner')
    )

    auto_match = df_all.where(f.col('name') == condition_name)

    # Ensure same number of matches overall
    n_manual_matches, n_auto_matches = manual_match.count(), auto_match.count()
    try:
        assert n_manual_matches == n_auto_matches
    except AssertionError:
        raise AssertionError(f'{condition_name}: Manual matches: {n_manual_matches}, Auto matches: {n_auto_matches}')


    # Ensure same PERSON_IDs 
    manual_ids, auto_ids = manual_match.select('PERSON_ID').distinct().count(), auto_match.select('PERSON_ID').distinct().count()
    try:
        assert manual_ids == auto_ids
    except AssertionError:
        raise AssertionError(f'{condition_name}: Manual IDs: {manual_ids}, Auto IDs: {auto_ids}')

_check_manually_compare_match_counts('heart_disease', hx_out_all)
_check_manually_compare_match_counts('comorb_comp', hx_out_all)

# COMMAND ----------

def _check_long_vs_wide(df_long, df_wide):
    # Messing around with column names
    wide_columns = [f'cov_{name}_flag' for name in condition_names]
    column_map = {f'sum(cov_{name}_flag)': name for name in condition_names}

    # Obtain the sum per column in the wide-format table
    wide_counts = df_wide.select(wide_columns).groupby().sum().toPandas()
    # Obtain the aggregate count of rows per covariate in the long-format table
    long_counts =  df_long.groupby('name').count().toPandas()
    
    # Messing around with pandas to make the comparison
    wide_counts = wide_counts.T[0].rename('wide') #transpose
    wide_counts.index = wide_counts.index.map(column_map)
    long_counts = long_counts.set_index('name')['count'].rename('long')
    
    # Join the two sets of counts and take the difference
    both = pd.concat((wide_counts, long_counts), axis=1).fillna(0)
    both['diff'] = (both.wide - both.long)

    # Ensure the difference is zero for all covariates
    try:
        assert not (both['diff'] > 0).any() 
    except AssertionError:
        return both

_check_long_vs_wide(hx_out_last, hx_out_last_wide)

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. BMI

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Prepare

# COMMAND ----------

markers_start_date = f.add_months(f.to_date(f.lit(study_start_date)), int(-12*1.5))
markers_end_date = f.add_months(f.to_date(f.lit(study_start_date)), 3)

# We prepare GDPPR into a suitable format and constrain the date range
gdppr_prepared = (
    gdppr
    .select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 'DATE', 'CODE', 'VALUE1_CONDITION', 'VALUE2_CONDITION')
    .where(f.col('PERSON_ID').isNotNull() & f.col('DATE').isNotNull())  
    .withColumn('mono_id', f.monotonically_increasing_id())
    # Filter down to cohort-members only and add the inclusion date column
    .join(cohort, on='PERSON_ID', how='inner')
    .where(
        (f.col('DATE') >= markers_start_date) &
        (f.col('DATE') < markers_end_date)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 Codelist match

# COMMAND ----------

from mythoslib import _codelist_match_all, codelist_match_summary
# dictionary - key: dataset, codelist, and ordering in the event of tied records
dict_in = {
    'gdppr': (gdppr_prepared, codelist_bmi, 1)
}

# Run codelist match and codelist match summary functions
# We only import and run the first stage of codelist matching, giving us all the matches across the input datasets. We don't need the 1st match or the wide format.
bmi_matched_all = _codelist_match_all(dict_in)
bmi_matched_all_summ_name, bmi_matched_all_summ_name_code = codelist_match_summary(dict_in, bmi_matched_all)

# temp save
bmi_matched_all = temp_save(df=bmi_matched_all, out_name=f'{proj}_tmp_covariates_bmi_matched_all')
bmi_matched_all_summ_name  = temp_save(df=bmi_matched_all_summ_name, out_name=f'{proj}_tmp_covariates_bmi_matched_all_summ_name')
matched_all_summ_name_code = temp_save(df=bmi_matched_all_summ_name_code, out_name=f'{proj}_tmp_covariates_bmi_matched_all_summ_name_code')


# COMMAND ----------

# Load
matched_all = spark.table(f'{dsa}.{proj}_tmp_covariates_bmi_matched_all')
matched_all_summ_name = spark.table(f'{dsa}.{proj}_tmp_covariates_bmi_matched_all_summ_name')
matched_all_summ_name_code = spark.table(f'{dsa}.{proj}_tmp_covariates_bmi_matched_all_summ_name_code')

# COMMAND ----------

display(matched_all_summ_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.3 Filter nulls and OOR

# COMMAND ----------

# filter out nulls or out of range (OOR) values to avoid selecting these in the next stage
def constrain_marker_to_range(df, marker_name, range_low, range_high):
    """ Given the table of matched biomarker records, the name of a biomarker, and low & high ends for the valid range of values,
    Filters out any records for that biomarker that lie outside the given range (inclusive). Ignores other biomarkers in the table.
    Also removes NULLs.
    """
    col_name, col_value = f.col('name'), f.col('VALUE1_CONDITION')
    # Logical mask for the biomarker: A given record can either belong to the biomarker and be in the given range
    # OR not belong to this biomarker (in which case we ignore it)
    marker_within_range = (col_name == marker_name) & (col_value >= range_low) & (col_value <= range_high)
    different_marker = (col_name != marker_name)
    marker_not_null = (col_value.isNotNull())

    return df.where((marker_not_null & marker_within_range) | different_marker)

def constrain_all_markers_to_ranges(df, ranges):
    """ Given the table of matched biomarker records, and a dictionary of biomarker names and low & high ends for the valid ranges of values,
    Filters out any records for those biomarkers that lie outside the given ranges (inclusive). Ignores other biomarkers in the table.
    Expects ranges: Dict[string, Tuple[Numeric, Numeric]] with the name of the biomarker as the key (e.g. 'bmi') and the lower and upper ends of the range (inclusive) as the value.
    """
    # Iterate the different biomarker types
    result = df
    for marker_name, (range_low, range_high) in ranges.items():
        result = constrain_marker_to_range(result, marker_name, range_low, range_high)
    
    return result

marker_ranges = {
    'bmi': (10, 60)
}

matched_2_constrained = constrain_all_markers_to_ranges(matched_all, marker_ranges)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.4 Round

# COMMAND ----------

matched_3_rounded = (
    matched_2_constrained
    .withColumn('VALUE1_CONDITION', f.round(f.col('VALUE1_CONDITION'), 2))
    .withColumn('VALUE2_CONDITION', f.round(f.col('VALUE2_CONDITION'), 2))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.5 Remove duplicates

# COMMAND ----------

window_duplicates = (
    Window
    .partitionBy('PERSON_ID', 'name', 'DATE', 'VALUE1_CONDITION')
    .orderBy('mono_id')
)
matched_4_deduplicated = (
    matched_3_rounded
    .withColumn('_rank', f.row_number().over(window_duplicates))
    .where(f.col('_rank') == 1)
    .drop('_rank')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.6 Prioritise values

# COMMAND ----------

matched_5_selection_groups = (
    matched_4_deduplicated
    .withColumn('_selection_group',
        f.when((f.col('DATE') >= markers_start_date) & (f.col('DATE') < study_start_date), 1)
        .when((f.col('DATE') >= study_start_date) & (f.col('DATE') < markers_end_date), 2)
        .otherwise(-1))
    .withColumn('_diff_abs', f.abs(f.datediff(f.col('DATE'), f.lit(study_start_date))/365.25))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.7 Select top selection group

# COMMAND ----------

# Densely rank the records (per biomarker per person) by selection group and date. 
# Use a dense rank to allow multiple records for the same biomarker if they have the same date. Will resolve these ties later.
window_dense_rank = (
    Window
    .partitionBy('PERSON_ID', 'name')
    .orderBy('_selection_group', '_diff_abs')  
)
# Pick the records per marker that have the top rank. 
history_1_dense_ranked = (
    matched_5_selection_groups
    .withColumn('_dense_rank', f.dense_rank().over(window_dense_rank))
    .where(f.col('_dense_rank') == 1)
    .drop('_dense_rank')
)  

# COMMAND ----------

# Densely rank the records (per biomarker per person) by selection group and date. 
# Use a dense rank to allow multiple records for the same biomarker if they have the same date. Will resolve these ties later.
window_dense_rank = (
    Window
    .partitionBy('PERSON_ID', 'name')
    .orderBy('_selection_group', '_diff_abs')  
)
# Pick the records per marker that have the top rank. 
history_1_dense_ranked = (
    matched_5_selection_groups
    .withColumn('_dense_rank', f.dense_rank().over(window_dense_rank))
    .where(f.col('_dense_rank') == 1)
    .drop('_dense_rank')
)  

# COMMAND ----------

# Augment with row numbers & a count of how many conflicting records per biomarker per person.
window_row_num = (
    Window
    .partitionBy('PERSON_ID', 'name')
    .orderBy('_selection_group', '_diff_abs', 'mono_id')  
)
window_row_num_max = (
    Window
    .partitionBy('PERSON_ID', 'name', 'DATE')
)

history_2_row_numbers = (
    history_1_dense_ranked
    # Index distinct values for each biomarker that had the same date
    .withColumn('_row_num', f.row_number().over(window_row_num))
    # Augment with the maximum value of _row_num per biomarker per person
    .withColumn('_row_num_max', f.count(f.lit(1)).over(window_row_num_max))
)

# COMMAND ----------

tabstat(history_2_row_numbers, '_row_num_max', 'name')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.8 BMI rounding 
# MAGIC A lot of patients have ties in the BMI records - multiple values for the same date. We want to prioritise the most precise/non-rounded value. Keep in mind there may be multiple distinct values with the same precision (e.g. 28.53 and 31.39). We order the most precise value(s) first and remove the rounded values below. Distinct but equally precise values will not be removed but flagged for later. 

# COMMAND ----------

# Separate the working table into 2: 
# 1. The BMI records where the same PERSON_ID has multiple tied records, and 
# 2. Everything else (BMI records with no ties and also all other markers)
# We will work on bmi_conflicts and then re-join it with bmi_outer to obtain a complete table again.
bmi_conflicts, bmi_outer = (
    history_2_row_numbers.where((f.col('name') == 'bmi') & (f.col('_row_num_max') > 1)),
    history_2_row_numbers.where((f.col('name') != 'bmi') | (f.col('_row_num_max') == 1))
)

# Augment BMIs with indicator of how many decimal places they are precise to
bmi_conflicts_dp = (
    bmi_conflicts
    .withColumn('VALUE1_CONDITION_str', f.col('VALUE1_CONDITION').cast(t.StringType()))
    .withColumn('first_decimal_point', f.substring(f.col('VALUE1_CONDITION_str'), -2, 1)) 
    .withColumn('second_decimal_point', f.substring(f.col('VALUE1_CONDITION_str'), -1, 1))
    .withColumn('n_decimal_points', 
        f.when(f.col('second_decimal_point') != '0', 2)
        .when(f.col('first_decimal_point') != '0', 1)
        .otherwise(0)
    )
)

# This regex will match numbers with up to 2 decimal digits and up to 3 integer digits 
# We use it to ensure BMIs have been rounded correctly. Should be rounded to 2 decimal places. 
evil_regex = r'^(\d)?\d\d\.\d\d$'
assert bmi_conflicts_dp.where(~f.col('VALUE1_CONDITION_str').rlike(evil_regex)).count() == 0

# Window will prioritise values precise to more decimal digits
win_bmi = (
    Window
    .partitionBy('PERSON_ID', 'DATE')
    .orderBy(f.desc('n_decimal_points'), 'mono_id')
)

bmi_conflicts_rownum = (
    bmi_conflicts_dp
    .withColumn('rownum', f.row_number().over(win_bmi))
    .withColumn('rownum1_round1', f.when((f.col('rownum') == 1) & (f.col('n_decimal_points') == 2), f.round(f.col('VALUE1_CONDITION'), 1)).otherwise(None))
    .withColumn('rownum1_round0', f.when((f.col('rownum') == 1) & (f.col('n_decimal_points').isin([1,2])), f.round(f.col('VALUE1_CONDITION'), 0)).otherwise(None))
    .withColumn('rownum1_round1_egen', f.min(f.col('rownum1_round1')).over(win_bmi))
    .withColumn('rownum1_round0_egen', f.min(f.col('rownum1_round0')).over(win_bmi))
    .withColumn('to_drop', f.when(
        (f.col('VALUE1_CONDITION') == f.col('rownum1_round1_egen')) | (f.col('VALUE1_CONDITION') == f.col('rownum1_round0_egen')), 1).otherwise(0)
    )
)

# Drop the flagged records
bmi_clean = (
    bmi_conflicts_rownum
    .where(f.col('to_drop') == 0)
    .select(bmi_outer.columns)
)  

# COMMAND ----------

# reassemble the complete marker table, now with cleaned BMI ties 
history_3_bmi = bmi_outer.unionByName(bmi_clean) 

# temp save
history_3_bmi = temp_save(df=history_3_bmi, out_name=f'{proj}_tmp_covariates_history_3_bmi')

# check
assert (bmi_conflicts.select('PERSON_ID').distinct().count() - bmi_clean.select('PERSON_ID').distinct().count()) == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.9 Row number
# MAGIC
# MAGIC We take the quick and easy approach of just choosing the first record per person per marker whenever we have conflicts. Hopefully our previous curation will have prioritised the most accurate & within-range records. 

# COMMAND ----------

history_4_rowed = (
    history_3_bmi
    # Index distinct values for each biomarker that had the same date
    .withColumn('_row_num', f.row_number().over(window_row_num))
    # Augment with the maximum value of _row_num per biomarker per person
    .withColumn('_row_num_max', f.count(f.lit(1)).over(window_row_num_max))
    .where(f.col('_row_num') == 1)
    .withColumnRenamed('_row_num_max', 'n_distinct_values_on_date')
    .withColumn('flag_multi_distinct_values_on_date', f.when(f.col('n_distinct_values_on_date') > 1, 1).otherwise(0))
)

# Temp save
history_4_rowed = temp_save(history_4_rowed, f'{proj}_tmp_covariates_bmi_history_4_rowed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.10 Reshape

# COMMAND ----------

# Pivot the markers table from long format (PERSON_ID, MARKER_NAME, VALUE) to wide format (PERSON_ID, COV_MARKER_1_DATE, COV_MARKER_1_VALUE, COV_MARKER_2_DATE, ...)
history_5_pivoted = (
    history_4_rowed
    .select('PERSON_ID', 'name', 'DATE', 'VALUE1_CONDITION', 'flag_multi_distinct_values_on_date')  
    # Prepend 'cov_mark_' prefix to all biomarker names
    .withColumn('name', f.concat(f.lit('cov_mark_'), f.lower(f.col('name'))))
    # Pivot into wide format
    .groupBy('PERSON_ID')
    .pivot('name')
    .agg(
        f.min('DATE').alias('date'), f.first('VALUE1_CONDITION').alias('value')
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.11 Add cohort

# COMMAND ----------

bmi_cohort = history_5_pivoted.join(cohort.select('PERSON_ID'), how='right', on='PERSON_ID')

# temp save
bmi_cohort = temp_save(df=bmi_cohort, out_name=f'{proj}_tmp_covariates_bmi_cohort')

# COMMAND ----------

bmi_cohort = spark.table(f'{dsa}.{proj}_tmp_covariates_bmi_cohort')

# COMMAND ----------

# MAGIC %md # 7. Save
# MAGIC
# MAGIC Finally, we take the outer join of all the produced tables of covariates and save to the database.

# COMMAND ----------

# DBTITLE 1,Join all produced covariate tables
covariates_final = (
    cohort.select('PERSON_ID')
    .join(smoking_latest_status, on='PERSON_ID', how='outer') # PERSON_ID cov_smoking_date, cov_smoking_status, cov_smoking_never_upgrade
    .join(hx_out_last_wide.drop("CENSOR_DATE_START","CENSOR_DATE_END"), on='PERSON_ID', how='outer') # PERSON_ID, cov_{covariate}_flag, cov_{covariate}_date
    .join(bmi_cohort, on = 'PERSON_ID', how='outer')
)

# Adding obesity flag based on bmi and icd obesity codes
covariates_final = covariates_final.withColumn(
    'cov_obesity_combined_flag',
    f.when(
        (f.col('cov_bmi_obesity_flag') == 1) | (f.col('cov_mark_bmi_value') > 25),
        1).otherwise(0)
    )

  # Count of people with cov_bmi_obesity_flag == 1
bmi_flag_count = covariates_final.filter(f.col("cov_bmi_obesity_flag") == 1).count()

# Count of people with cov_obesity_combined_flag == 1
combined_flag_count = covariates_final.filter(f.col("cov_obesity_combined_flag") == 1).count()

print(f"Number of people with cov_bmi_obesity_flag == 1: {bmi_flag_count}")
print(f"Number of people with cov_obesity_combined_flag == 1: {combined_flag_count}")
  

# COMMAND ----------

# DBTITLE 1,Save
save_table(df=covariates_final, out_name=f'{proj}_out_covariates', save_previous=True)

# COMMAND ----------

# DBTITLE 1,Load (if re-running)
covariates_final = spark.table(f'{dsa}.{proj}_out_covariates')

# COMMAND ----------

# DBTITLE 1,Check
covariates_final_unique_ids = covariates_final.select("PERSON_ID").distinct().count()

# Ensure we haven't lost any cohort members 
assert covariates_final_unique_ids == cohort.select('PERSON_ID').distinct().count()

# Ensure we haven't created a cartesian product 
assert covariates_final.count() <= cohort.count()

# Ensure there are no duplicate PERSON_IDs
assert covariates_final_unique_ids == covariates_final.count()