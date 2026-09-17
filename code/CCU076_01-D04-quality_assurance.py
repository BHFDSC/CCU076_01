# Databricks notebook source
# MAGIC %md # CCU076_01-D04-quality_assurance
# MAGIC  
# MAGIC **Description** This notebook creates the quality assurance table, which indentifies individuals to remove from the analyses due to erroneous or conflicting data, with reference to previous work/coding by CCU002 and Spiros Denaxas.
# MAGIC
# MAGIC **Authors** Alexia Sampri, Isabel Walter  
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Code from Stelios Boulitsakis Logothetis, who adapted from CCU004_03 and the Health Data Science Team, BHF Data Science Centre.
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu076_01_tmp_quality_assurance`**: table with flags for individuals meeting exclusion criteria based on each of the ten exclusion rules

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

# MAGIC %run "Workspace/Shared/SHDS/common/functions"

# COMMAND ----------

# MAGIC %md 
# MAGIC # 0. Parameters

# COMMAND ----------

# MAGIC %run "./CCU076_01-D01-parameters"

# COMMAND ----------

# MAGIC %md # 1. Data

# COMMAND ----------

codelist_qa = spark.table(path_out_codelist_quality_assurance)
skinny      = spark.table(hds_curated_assets_demographic)
deaths      = spark.table(path_cur_deaths_sing)
gdppr       = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
pmeds       = extract_batch_from_archive(parameters_df_datasets, 'pmeds')

# COMMAND ----------

# MAGIC %md # 2. Prepare

# COMMAND ----------

gdppr_prepared = (
    gdppr
    .select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 'DATE', 'RECORD_DATE', 'CODE')
    .withColumn('DATE', f.when(f.col('DATE').isNull(), f.col('RECORD_DATE')).otherwise(f.col('DATE')))
    .drop(f.col('RECORD_DATE'))
    .where(f.col('PERSON_ID').isNotNull()) # Careful: Not excluding NULL dates here. Do it for matching.
)

pmeds_prepared = (
    pmeds
    .select(
        f.col('Person_ID_DEID').alias('PERSON_ID'), 
        f.col('ProcessingPeriodDate').alias('DATE'), 
        f.col('PrescribedBNFCode').alias('CODE')
    )
    .where(f.col('PERSON_ID').isNotNull())
)

# COMMAND ----------

deaths_prepared = (
    deaths
    .select('PERSON_ID', 'REG_DATE', 'REG_DATE_OF_DEATH')
    .withColumn('_in_deaths', f.lit(1))
)
skinny_prepared = (
    skinny
    .select('PERSON_ID', 'date_of_birth', 'sex_code', 'date_of_death')
    .join(deaths_prepared, on='PERSON_ID', how='left')
)

# COMMAND ----------

# MAGIC %md # 3. Medical conditions
# MAGIC
# MAGIC We identify all patients in GDPPR and PMEDS who are recorded as having the medical conditions or prescriptions of interest: pregnancy, prostate cancer, HRT, or COCP.

# COMMAND ----------

# MAGIC %md ## 3.1 Codelist

# COMMAND ----------

# DBTITLE 1,Split codelist by terminology
codelist_qa_gdppr, codelist_qa_pmeds = (
    codelist_qa.filter(f.col('terminology') == 'SNOMED'), 
    codelist_qa.filter((f.col('terminology') == 'BNF'))
)

# COMMAND ----------

# MAGIC %md ## 3.2 Create

# COMMAND ----------

from mythoslib import codelist_match_v3, codelist_match_summary
gdppr_prepared_matching = gdppr_prepared.where(f.col('DATE').isNotNull())
dict_in = {
    'gdppr': (gdppr_prepared_matching, codelist_qa_gdppr, 1),
    'pmeds': (pmeds_prepared, codelist_qa_pmeds, 2)
}

# run codelist match and codelist match summary functions
hx_all, hx_1st, hx_1st_wide = codelist_match_v3(dict_in, name_prefix='hx_')
hx_summ_name, hx_summ_name_code = codelist_match_summary(dict_in, hx_all)

# COMMAND ----------

# temp save
hx_all = temp_save(df=hx_all, out_name=f'{proj}_tmp_quality_assurance_hx_all')
hx_1st = temp_save(df=hx_1st, out_name=f'{proj}_tmp_quality_assurance_hx_1st')
hx_1st_wide = temp_save(df=hx_1st_wide, out_name=f'{proj}_tmp_quality_assurance_hx_1st_wide')
hx_summ_name = temp_save(df=hx_summ_name, out_name=f'{proj}_tmp_quality_assurance_hx_summ_name')

# COMMAND ----------

hx_all = spark.table(f'{dsa}.{proj}_tmp_quality_assurance_hx_all')
hx_1st = spark.table(f'{dsa}.{proj}_tmp_quality_assurance_hx_1st')
hx_1st_wide = spark.table(f'{dsa}.{proj}_tmp_quality_assurance_hx_1st_wide')
hx_summ_name = spark.table(f'{dsa}.{proj}_tmp_quality_assurance_hx_summ_name')

# COMMAND ----------

# MAGIC %md ## 3.3 Check

# COMMAND ----------

# Ensure all the required attributes have been matched
required_columns = ['hx_pregnancy_flag', 'hx_prostate_cancer_flag', 'hx_hrt_flag', 'hx_cocp_flag']
assert set(required_columns) <= set(hx_1st_wide.columns)

# COMMAND ----------

# check codelist match summary by name
display(hx_summ_name)

# COMMAND ----------

# MAGIC %md ## 3.4 Merge

# COMMAND ----------

# Left join because hx_1st_wide will also contain some patients that exist in pmeds but not in GDPPR
skinny_2_conditions = skinny_prepared.join(
    hx_1st_wide.select('PERSON_ID', 'hx_pregnancy_flag', 'hx_prostate_cancer_flag', 'hx_cocp_flag','hx_hrt_flag'),
    on='PERSON_ID', how='left'
)

# COMMAND ----------

# MAGIC %md # 4. Rules
# MAGIC
# MAGIC We define the quality assurance rules that each patient record must fulfill in order to be included in the study. These are:
# MAGIC
# MAGIC Rule 1: Date of birth is after the date of death  
# MAGIC Rule 2: Patient does not have mandatory fields completed (nhs_number, sex, Date of birth)  
# MAGIC Rule 3: Date of birth Predates NHS Established Year or is over the Current Date  
# MAGIC Rule 4: Remove those with only null/invalid dates of death  
# MAGIC Rule 5: Remove those where registered date of death before the actual date of death  
# MAGIC Rule 6: Patients have all missing record_dates and dates  
# MAGIC Rule 7: Pregnancy/birth codes for men  
# MAGIC Rule 8: HRT codes codes for men  
# MAGIC Rule 9: COCP codes codes for men  
# MAGIC Rule 10: Prostate cancer codes for women

# COMMAND ----------

# MAGIC %md ## 4.1 Prepare

# COMMAND ----------

# DBTITLE 1,Prepare GDPPR for rule 6
gdppr_null_dates = (
    gdppr_prepared
    .groupby('PERSON_ID')
    .agg(
        f.sum(f.col('DATE').isNotNull().cast('integer')).alias('_n_gdppr_notnull'),
        f.sum(f.col('DATE').isNull().cast('integer')).alias('_n_gdppr_null')
    )
)

skinny_3_null_dates = skinny_2_conditions.join(gdppr_null_dates, on="PERSON_ID", how='left')

# COMMAND ----------

# MAGIC %md ## 4.2 Create

# COMMAND ----------

# DBTITLE 1,Define rules
rules = {}

# Rule 1: Date of birth is after the year of death
rules[1] = (f.col('date_of_birth') > f.col('date_of_death'))

# Rule 2: Patient does not have mandatory fields completed (nhs_number, sex, Date of birth)
rules[2] = (
    (f.col('sex_code').isNull()) | (~f.col('sex_code').isin([1,2])) |
    (f.col('date_of_birth').isNull()) |
    (f.col('PERSON_ID').isNull()) | (f.trim(f.col('PERSON_ID')) == '')
)

# Rule 3: Date of Birth Predates NHS Established Year or Year is over the Current Date
current_year = datetime.datetime.today()
rules[3] = (
    (f.col('date_of_birth') < '01-01-1793') | (f.col('date_of_birth') > f.current_date())
)

# Rule 4: Remove those with only null/invalid dates of death
rules[4] = (f.col('_in_deaths') == 1) & (
    f.col('date_of_death').isNull() | 
    (f.col('date_of_death') <= f.to_date(f.lit('1900-01-01'))) |
    ((f.col('REG_DATE_OF_DEATH') > f.current_date()))
)

# Rule 5: Remove those where registered date of death before the actual date of death
rules[5] = (f.col('REG_DATE_OF_DEATH') > f.col('REG_DATE'))

# Rule 6: Patients have all missing record_dates and dates
rules[6] = ((f.col('_n_gdppr_null') > 0) & (f.col('_n_gdppr_notnull') == 0))

# Rule 7: Pregnancy/birth codes for men
is_male, is_female = f.col('sex_code') == 1, f.col('sex_code') == 2
rules[7] = (is_male & (f.col('hx_pregnancy_flag') == 1))

# Rule 8: HRT codes codes for men
rules[8] = (is_male & (f.col('hx_hrt_flag') == 1))

# Rule 9: COCP codes codes for men
rules[9] = (is_male & (f.col('hx_cocp_flag') == 1))

# Rule 10: Prostate cancer codes for women
rules[10] = (is_female & (f.col('hx_prostate_cancer_flag') == 1))


# COMMAND ----------

# DBTITLE 1,Parse defined rules into skinny
qax = skinny_3_null_dates
for rule_idx, rule in rules.items():
    qax = qax.withColumn(f'_rule_{rule_idx}', f.when(rule, 1).otherwise(0))

rule_columns = [f'_rule_{rule_idx}' for rule_idx in rules.keys()]
qax = qax.withColumn('_rule_total', sum(f.col(_) for _ in rule_columns))
rule_columns.append('_rule_total')

# COMMAND ----------

# temp save
qax = temp_save(df = qax, out_name=f'{proj}_tmp_quality_assurance_qax')

# COMMAND ----------

# MAGIC %md ## 4.3 Checks

# COMMAND ----------

# Ensure there is no rule that *everyone* violates - indicating a definitional problem.
assert qax.where(f.col('_rule_total').isNull() | (f.col('_rule_total') == 0)).count() != 0 

# COMMAND ----------

# MAGIC %md # 5. Save 

# COMMAND ----------

# DBTITLE 1,Reduce and recode rule columns
# Select only the ID and rules columns
qax_out = qax.select(['PERSON_ID'] + rule_columns)

# Recode zeros to NULLs
for col in rule_columns: 
    qax_out = qax_out.withColumn(
        col, f.when(f.col(col) == 0, f.lit(None)).otherwise(f.col(col))
    )

# COMMAND ----------

save_table(df=qax_out, out_name=f'{proj}_tmp_quality_assurance', save_previous=True)