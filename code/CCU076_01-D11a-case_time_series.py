# Databricks notebook source
# MAGIC %md # CCU076_01_-D11a-time_series
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
covariates = spark.table(path_out_covariates)
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

mi_fatal = outcomes_extended.where(f.col('name') == 'MI').drop('death_flag')
mi_nonfatal = outcomes_extended.where(f.col('name') == 'MI').where(f.col('death_flag').isNull()).drop('death_flag')
mi_nonfatal_2week = outcomes_extended.where(f.col('name') == 'MI').where(f.col('death_flag_2week').isNull()).drop('death_flag_2week')
mi_nonfatal_month = outcomes_extended.where(f.col('name') == 'MI').where(f.col('death_flag_month').isNull()).drop('death_flag_month')

stroke_fatal = outcomes_extended.where(f.col('name') == 'stroke_IS').drop('death_flag')
stroke_nonfatal = outcomes_extended.where(f.col('name') == 'stroke_IS').where(f.col('death_flag').isNull()).drop('death_flag')
stroke_nonfatal_2week = outcomes_extended.where(f.col('name') == 'stroke_IS').where(f.col('death_flag_2week').isNull()).drop('death_flag_2week')
stroke_nonfatal_month = outcomes_extended.where(f.col('name') == 'stroke_IS').where(f.col('death_flag_month').isNull()).drop('death_flag_month')

at_fatal = outcomes_extended.where(f.col('name') == 'AT').drop('death_flag')
at_nonfatal = outcomes_extended.where(f.col('name') == 'AT').where(f.col('death_flag').isNull()).drop('death_flag')
at_nonfatal_2week = outcomes_extended.where(f.col('name') == 'AT').where(f.col('death_flag_2week').isNull()).drop('death_flag_2week')
at_nonfatal_month = outcomes_extended.where(f.col('name') == 'AT').where(f.col('death_flag_month').isNull()).drop('death_flag_month')

ae_fatal = outcomes_extended.where(f.col('name') == 'arterial_event').drop('death_flag')
ae_nonfatal = outcomes_extended.where(f.col('name') == 'arterial_event').where(f.col('death_flag').isNull()).drop('death_flag')
ae_nonfatal_2week = outcomes_extended.where(f.col('name') == 'arterial_event').where(f.col('death_flag_2week').isNull()).drop('death_flag_2week')
ae_nonfatal_month = outcomes_extended.where(f.col('name') == 'arterial_event').where(f.col('death_flag_month').isNull()).drop('death_flag_month')

ve_fatal = outcomes_extended.where(f.col('name') == 'venous_event').drop('death_flag')
ve_nonfatal = outcomes_extended.where(f.col('name') == 'venous_event').where(f.col('death_flag').isNull()).drop('death_flag')
ve_nonfatal_2week = outcomes_extended.where(f.col('name') == 'venous_event').where(f.col('death_flag_2week').isNull()).drop('death_flag_2week')
ve_nonfatal_month = outcomes_extended.where(f.col('name') == 'venous_event').where(f.col('death_flag_month').isNull()).drop('death_flag_month')

covid_fatal = outcomes_extended.where(f.col('name') == 'covid').drop('death_flag')
covid_nonfatal = outcomes_extended.where(f.col('name') == 'covid').where(f.col('death_flag').isNull()).drop('death_flag')
covid_nonfatal_2week = outcomes_extended.where(f.col('name') == 'covid').where(f.col('death_flag_2week').isNull()).drop('death_flag_2week')
covid_nonfatal_month = outcomes_extended.where(f.col('name') == 'covid').where(f.col('death_flag_month').isNull()).drop('death_flag_month')


# COMMAND ----------

# DBTITLE 1,Cases per month
datasets = {
    "mi_fatal": mi_fatal,
    "mi_nonfatal": mi_nonfatal,
    "stroke_fatal": stroke_fatal,
    "stroke_nonfatal": stroke_nonfatal,
    "ve_fatal": ve_fatal,
    "ve_nonfatal": ve_nonfatal,
    "ae_fatal": ae_fatal,
    "ae_nonfatal": ae_nonfatal,
    "covid_fatal": covid_fatal,
    "covid_nonfatal": covid_nonfatal
}

def get_rows_per_month(dataset):
    return dataset.withColumn("YearMonth", f.date_format(f.col("DATE"), "yyyy-MM")) \
                  .groupBy("YearMonth") \
                  .count() \
                  .orderBy("YearMonth") \
                  .toPandas()

# Create subplots
num_datasets = len(datasets)
fig, axes = plt.subplots(nrows=num_datasets, ncols=1, figsize=(10, 6 * num_datasets))

# If there is only one dataset, make axes a list for consistency
if num_datasets == 1:
    axes = [axes]

# Plot each dataset in a separate subplot
for ax, (name, dataset) in zip(axes, datasets.items()):
    pdf = get_rows_per_month(dataset)
    ax.bar(pdf["YearMonth"], pdf["count"], color="skyblue")
    ax.set_title(name, fontsize=14)
    ax.set_xlabel("Month")
    ax.set_ylabel("Number of Case (first event)")
    ax.tick_params(axis="x", rotation=45)

# Adjust layout to prevent overlap
plt.tight_layout()
plt.show()

# COMMAND ----------

# Dictionary mapping dataset names to DataFrame objects
datasets = {
    "mi_fatal": mi_fatal,
    "mi_nonfatal": mi_nonfatal,
    "stroke_fatal": stroke_fatal,
    "stroke_nonfatal": stroke_nonfatal,
    "ve_fatal": ve_fatal,
    "ve_nonfatal": ve_nonfatal,
    "ae_fatal": ae_fatal,
    "ae_nonfatal": ae_nonfatal,
    "covid_fatal": covid_fatal,
    "covid_nonfatal": covid_nonfatal
}

# Create an empty Pandas DataFrame to store results
results = pd.DataFrame(columns=["Dataset", "YearMonth", "Number_of_Cases"])

# Function to calculate and round rows per month
def get_rows_per_month(dataset, name):
    # Aggregate rows per month and round to nearest five
    data_by_month = dataset.withColumn("YearMonth", f.date_format(f.col("DATE"), "yyyy-MM")) \
                           .groupBy("YearMonth") \
                           .count() \
                           .withColumn("RoundedCount", (f.round(f.col("count") / 5) * 5).cast("int"))
    # Convert to Pandas and add the dataset name
    pdf = data_by_month.select("YearMonth", "RoundedCount") \
                       .toPandas()
    pdf["Dataset"] = name
    pdf.rename(columns={"RoundedCount": "Number_of_Cases"}, inplace=True)
    return pdf

# Iterate over datasets and collect results
for name, dataset in datasets.items():
    dataset_result = get_rows_per_month(dataset, name)
    results = pd.concat([results, dataset_result], ignore_index=True)

# Order results by Dataset and YearMonth
results["YearMonth"] = pd.to_datetime(results["YearMonth"])  # Convert to datetime for sorting
results = results.sort_values(by=["Dataset", "YearMonth"]).reset_index(drop=True)

# Display the combined table
display(results)

# COMMAND ----------

# DBTITLE 1,Save cases per month
results = spark.createDataFrame(results)

save_table(df=results, out_name=f'{proj}_out_cases_per_month', save_previous=False)

# COMMAND ----------

# DBTITLE 1,Incidence rates function
def year_diff(end_col, start_col):
    return f.datediff(end_col, start_col) / 365.25

def get_period_summary(df_cohort, outcomes, start_date_str, end_date_str, label):
    start_date = f.lit(start_date_str).cast("date")
    end_date = f.lit(end_date_str).cast("date")

    # People alive at start of period
    pop = df_cohort.filter(
        f.col("date_of_death").isNull() | (f.col("date_of_death") >= start_date_str)
    )

    # Compute exit date and person-time
    pop = pop.withColumn(
        f"exit_{label}",
        f.least(f.coalesce(f.col("date_of_death"), end_date), end_date)
        ).withColumn(
            f"person_time_{label}",
            year_diff(f.col(f"exit_{label}"), start_date)
            ).filter(f.col(f"person_time_{label}") > 0)

    # Aggregate population stats
    pop_summary = pop.agg(
        f.sum(f.col(f"person_time_{label}")).alias(f"person_years_{label}"),
        f.countDistinct("PERSON_ID").alias(f"n_people_{label}")
    )

    # Count events during the period
    events = outcomes.filter(
        (f.col("DATE") >= start_date_str) & (f.col("DATE") <= end_date_str)
    ).groupBy("name").agg(
        f.count("*").alias(f"events_{label}")
    )

    # Combine and compute incidence rate
    summary = pop_summary.crossJoin(events).withColumn(
        f"incidence_per_100k_{label}",
        (f.col(f"events_{label}") / f.col(f"person_years_{label}")) * 100000
    )

    return summary


# COMMAND ----------

# DBTITLE 1,Incidence rates England
# Define the periods you want
periods = [
    (study_start_date, study_end_date, "full"),
    (study_start_date, "2020-12-31", "2020"),
    ("2021-01-01", "2021-12-31", "2021"),
    ("2022-01-01", "2022-12-31", "2022"),
    (study_start_date, "2021-12-31", "2020_2021"),
]

# Generate all summaries
period_summaries = [get_period_summary(cohort, outcomes_extended, start, end, label) for start, end, label in periods]

# Join them all on outcome name
from functools import reduce

final_df = reduce(lambda df1, df2: df1.join(df2, on="name", how="outer"), period_summaries)


# COMMAND ----------

display(final_df)

# COMMAND ----------

# DBTITLE 1,Incidence rates per region
regions = [row["region"] for row in cohort.select("region").distinct().collect()]

periods = [
    (study_start_date, study_end_date, "full"),
    (study_start_date, "2020-12-31", "2020"),
    ("2021-01-01", "2021-12-31", "2021"),
    ("2022-01-01", "2022-12-31", "2022"),
    (study_start_date, "2021-12-31", "2020_2021"),
]

# Collect all region-wise summaries
region_results = []

for region_value in regions:
    # Filter cohort to region (no change to the function itself)
    region_cohort = cohort.filter(f.col("region") == region_value)

    # Filter outcomes dataset to people in region
    filtered_outcomes = outcomes_extended.join(region_cohort.select("PERSON_ID"), on="PERSON_ID", how="inner")

    # Run summaries for all periods using original function
    summaries = [get_period_summary(region_cohort, filtered_outcomes, start, end, label) for start, end, label in periods]

    # Join results on 'name'
    region_summary_df = reduce(lambda df1, df2: df1.join(df2, on="name", how="outer"), summaries)

    # Add region column
    region_summary_df = region_summary_df.withColumn("region", f.lit(region_value))

    # Append to list
    region_results.append(region_summary_df)

# Combine all regional summaries
regional_final_df = reduce(lambda df1, df2: df1.unionByName(df2), region_results)




# COMMAND ----------

display(regional_final_df)

# COMMAND ----------

# DBTITLE 1,incidence rates by ethnic group
ethnic_groups = [row["ethnicity_5_group"] for row in cohort.select("ethnicity_5_group").distinct().collect()]

periods = [
    (study_start_date, study_end_date, "full"),
    (study_start_date, "2020-12-31", "2020"),
    ("2021-01-01", "2021-12-31", "2021"),
    ("2022-01-01", "2022-12-31", "2022"),
    (study_start_date, "2021-12-31", "2020_2021"),
]

# Collect all subgroup-wise summaries
group_results = []

for ethnic_group in ethnic_groups:
    # Filter cohort to ethnic group (no change to the function itself)
    group_cohort = cohort.filter(f.col("ethnicity_5_group") == ethnic_group)

    # Filter outcomes dataset to people in subgroup
    filtered_outcomes = outcomes_extended.join(group_cohort.select("PERSON_ID"), on="PERSON_ID", how="inner")

    # Run summaries for all periods using original function
    summaries = [get_period_summary(group_cohort, filtered_outcomes, start, end, label) for start, end, label in periods]

    # Join results on 'name'
    group_summary_df = reduce(lambda df1, df2: df1.join(df2, on="name", how="outer"), summaries)

    # Add group column column
    group_summary_df = group_summary_df.withColumn("subgroup", f.lit(ethnic_group))

    # Append to list
    group_results.append(group_summary_df)

# Combine all regional summaries
ethnic_group_final_df = reduce(lambda df1, df2: df1.unionByName(df2), group_results)

# COMMAND ----------

display(ethnic_group_final_df)

# COMMAND ----------

# DBTITLE 1,incidence rates by sex
groups = [row["sex"] for row in cohort.select("sex").distinct().collect()]

periods = [
    (study_start_date, study_end_date, "full"),
    (study_start_date, "2020-12-31", "2020"),
    ("2021-01-01", "2021-12-31", "2021"),
    ("2022-01-01", "2022-12-31", "2022"),
    (study_start_date, "2021-12-31", "2020_2021"),
]

# Collect all subgroup-wise summaries
group_results = []

for group in groups:
    # Filter cohort to subgroup (no change to the function itself)
    group_cohort = cohort.filter(f.col("sex") == group)

    # Filter outcomes dataset to people in subgroup
    filtered_outcomes = outcomes_extended.join(group_cohort.select("PERSON_ID"), on="PERSON_ID", how="inner")

    # Run summaries for all periods using original function
    summaries = [get_period_summary(group_cohort, filtered_outcomes, start, end, label) for start, end, label in periods]

    # Join results on 'name'
    group_summary_df = reduce(lambda df1, df2: df1.join(df2, on="name", how="outer"), summaries)

    # Add group column column
    group_summary_df = group_summary_df.withColumn("subgroup", f.lit(group))

    # Append to list
    group_results.append(group_summary_df)

# Combine all regional summaries
sex_final_df = reduce(lambda df1, df2: df1.unionByName(df2), group_results)

# COMMAND ----------

display(sex_final_df)

# COMMAND ----------

# DBTITLE 1,Create stratification variables to calculate furter incidence rates
# Join with covariates on PERSON_ID
cohort = cohort.join(covariates, on="PERSON_ID", how="left")

# Create derived variables
cohort = cohort.withColumn(
    "age_group",
    f.when(f.col("study_start_age").isNull(), "Missing age group")
    .when((f.col("study_start_age") >= 18) & (f.col("study_start_age") < 65), "Age 18–64 years")
    .when((f.col("study_start_age") >= 65) & (f.col("study_start_age") < 75), "Age 65–74 years")
    .when((f.col("study_start_age") >= 75) & (f.col("study_start_age") < 85), "Age 75–84 years")
    .when(f.col("study_start_age") >= 85, "Age 85 years or older")
).withColumn(
    "age_group_bin",
    f.when(f.col("study_start_age").isNull(), "Missing binary age group")
    .when(f.col("study_start_age") < 65, "Binary age: Age 18–64 years")
    .otherwise("Binary age: Age 65 years or older")
).withColumn(
    "deprivation_bin",
    f.when(f.col("deprivation_index_quintiles").isNull(), "Missing deprivation index")
    .when(f.col("deprivation_index_quintiles").isin(1, 2, 3), "Low deprivation index (1–3)")
    .when(f.col("deprivation_index_quintiles").isin(4, 5), "High deprivation index (4–5)")
).withColumn(
    "cov_smoking_status",
    f.when(f.col("cov_smoking_status").isNull(), "Missing smoking status")
    .when(f.col("cov_smoking_status") == "smoking_current", "Current smoker")
    .when(f.col("cov_smoking_status") == "smoking_never", "Never smoked")
    .when(f.col("cov_smoking_status") == "smoking_ex", "Ex-smoker")
).withColumn(
    "cov_comorb_comp_flag",
    f.when(f.col("cov_comorb_comp_flag").isNull(), "No comorbidities")
    .when(f.col("cov_comorb_comp_flag") == 0, "No comorbidities")
    .when(f.col("cov_comorb_comp_flag") == 1, "Comorbidities")
).withColumn(
    "cov_obesity_combined_flag",
    f.when(f.col("cov_obesity_combined_flag") == 0, "No excess weight")
    .when(f.col("cov_obesity_combined_flag") == 1, "Excess weight")
)


# COMMAND ----------

# DBTITLE 1,Incidence rates by additional stratification variables
# List of variables to run subgroup summaries for
group_vars = [
    "age_group",
    "age_group_bin",
    "deprivation_bin",
    "cov_smoking_status",
    "cov_comorb_comp_flag",
    "cov_obesity_combined_flag",
    "rural_urban_class"
]

# Define time periods (adjust your study_start_date/study_end_date accordingly)
periods = [
    (study_start_date, study_end_date, "full"),
    (study_start_date, "2020-12-31", "2020"),
    ("2021-01-01", "2021-12-31", "2021"),
    ("2022-01-01", "2022-12-31", "2022"),
    (study_start_date, "2021-12-31", "2020_2021"),
]

# Store final results
all_group_results = []

# Loop over each variable
for group_var in group_vars:
    
    # Get distinct values for the current variable
    groups = [row[group_var] for row in cohort.select(group_var).distinct().collect()]
    
    # Store results for each value of this variable
    group_results = []

    for group in groups:
        # Filter the cohort by current group value
        group_cohort = cohort.filter(f.col(group_var) == group)

        # Filter outcomes dataset to people in this subgroup
        filtered_outcomes = outcomes_extended.join(
            group_cohort.select("PERSON_ID"),
            on="PERSON_ID",
            how="inner"
        )

        # Run summaries for all time periods
        summaries = [
            get_period_summary(group_cohort, filtered_outcomes, start, end, label)
            for start, end, label in periods
        ]

        # Combine all time period summaries for this group
        group_summary_df = reduce(lambda df1, df2: df1.join(df2, on="name", how="outer"), summaries)

        # Add columns to identify the group value and variable
        group_summary_df = group_summary_df.withColumn("subgroup", f.lit(group))
        group_summary_df = group_summary_df.withColumn("group_var", f.lit(group_var))

        # Append to results list
        group_results.append(group_summary_df)

    # Combine results for all values of this variable
    group_var_df = reduce(lambda df1, df2: df1.unionByName(df2), group_results)
    
    # Append to overall results
    all_group_results.append(group_var_df)

# Combine all results into one final DataFrame
final_subgroup_df = reduce(lambda df1, df2: df1.unionByName(df2), all_group_results)


# COMMAND ----------

display(final_subgroup_df)

# COMMAND ----------

# DBTITLE 1,Round for output
def round_and_mask_columns(df):
    # Identify event or n_people columns
    cols_to_mask = [col for col in df.columns if re.match(r"^(events|n_people|person_years)_", col)]

    for col in cols_to_mask:
        rounded = (f.round(f.col(col) / 5) * 5).cast("int")
        df = df.withColumn(
            col,
            f.when(rounded == 0, f.lit("0"))
             .when(rounded < 10, f.lit("<10"))
             .otherwise(rounded.cast("string"))
        )

    return df

final_df_rounded = round_and_mask_columns(final_df)
regional_final_df_rounded = round_and_mask_columns(regional_final_df)
ethnic_group_final_df_rounded = round_and_mask_columns(ethnic_group_final_df)
sex_final_df_rounded = round_and_mask_columns(sex_final_df)
final_subgroup_df_rounded = round_and_mask_columns(final_subgroup_df)

# COMMAND ----------

save_table(df=final_df_rounded, out_name=f'{proj}_out_incidence_rates_rounded', save_previous=False)
save_table(df=regional_final_df_rounded, out_name=f'{proj}_out_incidence_rates_region_rounded', save_previous=False)
save_table(df=ethnic_group_final_df_rounded, out_name=f'{proj}_out_incidence_rates_ethnicity_rounded', save_previous=False)
save_table(df=sex_final_df_rounded, out_name=f'{proj}_out_incidence_rates_sex_rounded', save_previous=False)
save_table(df=final_subgroup_df_rounded, out_name=f'{proj}_out_incidence_rates_additional_subgroups_rounded', save_previous=False)

# COMMAND ----------

# DBTITLE 1,Save incidence rates
save_table(df=final_df, out_name=f'{proj}_out_incidence_rates', save_previous=False)
save_table(df=regional_final_df, out_name=f'{proj}_out_incidence_rates_region', save_previous=False)
save_table(df=final_df_rounded, out_name=f'{proj}_out_incidence_rates_rounded', save_previous=False)
save_table(df=regional_final_df_rounded, out_name=f'{proj}_out_incidence_rates_region_rounded', save_previous=False)
save_table(df=ethnic_group_final_df, out_name=f'{proj}_out_incidence_rates_ethnicity', save_previous=False)
save_table(df=ethnic_group_final_df_rounded, out_name=f'{proj}_out_incidence_rates_ethnicity_rounded', save_previous=False)
save_table(df=sex_final_df, out_name=f'{proj}_out_incidence_rates_sex', save_previous=False)
save_table(df=sex_final_df_rounded, out_name=f'{proj}_out_incidence_rates_sex_rounded', save_previous=False)
save_table(df=final_subgroup_df, out_name=f'{proj}_out_incidence_rates_additional_subgroups', save_previous=False)
save_table(df=final_subgroup_df_rounded, out_name=f'{proj}_out_incidence_rates_additional_subgroups_rounded', save_previous=False)

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
        .withColumn('last_day_of_death_month', f.when(f.col('date_of_death').isNotNull(), f.last_day(f.col('date_of_death'))).otherwise(None))
        .withColumn('study_start_date', f.to_date(f.lit(study_start), 'yyyy-MM-dd'))
        .withColumn('fu_start_date', f.date_sub(f.trunc(f.col('study_start_date'), 'MM'), 21))
        .withColumn('study_end_date', f.to_date(f.lit(study_end), 'yyyy-MM-dd'))
        .withColumn('end_date', f.when(f.col('date_of_death').isNull(), f.col('study_end_date'))
        .otherwise(f.least(f.col('study_end_date'), f.col('last_day_of_death_month'))))
        .withColumn('date_month_seq', f.expr('sequence(fu_start_date, end_date, interval 1 day)'))
        .drop('date_of_death', 'last_day_of_death_month', 'study_end_date'))
    
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

time_series_mi_fatal.select('PERSON_ID').distinct().count()

# COMMAND ----------

time_series_ve_fatal.select('PERSON_ID').distinct().count()

# COMMAND ----------

# DBTITLE 1,Make time series
time_series_mi_fatal = make_time_series(mi_fatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_mi_nonfatal = make_time_series(mi_nonfatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_mi_nonfatal_2week = make_time_series(mi_nonfatal_2week, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_mi_nonfatal_month = make_time_series(mi_nonfatal_month, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)

time_series_stroke_fatal = make_time_series(stroke_fatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_stroke_nonfatal = make_time_series(stroke_nonfatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_stroke_nonfatal_2week = make_time_series(stroke_nonfatal_2week, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_stroke_nonfatal_month = make_time_series(stroke_nonfatal_month, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)

time_series_at_fatal = make_time_series(at_fatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_at_nonfatal = make_time_series(at_nonfatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_at_nonfatal_2week = make_time_series(at_nonfatal_2week, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_at_nonfatal_month = make_time_series(at_nonfatal_month, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)

time_series_ae_fatal = make_time_series(ae_fatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_ae_nonfatal = make_time_series(ae_nonfatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_ae_nonfatal_2week = make_time_series(ae_nonfatal_2week, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_ae_nonfatal_month = make_time_series(ae_nonfatal_month, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)

time_series_ve_fatal = make_time_series(ve_fatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_ve_nonfatal = make_time_series(ve_nonfatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_ve_nonfatal_2week = make_time_series(ve_nonfatal_2week, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_ve_nonfatal_month = make_time_series(ve_nonfatal_month, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)

time_series_covid_fatal = make_time_series(covid_fatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_covid_nonfatal = make_time_series(covid_nonfatal, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_covid_nonfatal_2week = make_time_series(covid_nonfatal_2week, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)
time_series_covid_nonfatal_month = make_time_series(covid_nonfatal_month, lsoa_dynamic, exposures_covid, env, study_start_date, study_end_date)


# COMMAND ----------

# DBTITLE 1,Save
# List of (variable_name_string, DataFrame_variable)
time_series_datasets = [
    ("time_series_mi_fatal", time_series_mi_fatal),
    ("time_series_mi_nonfatal", time_series_mi_nonfatal),
    ("time_series_mi_nonfatal_2week", time_series_mi_nonfatal_2week),
    ("time_series_mi_nonfatal_month", time_series_mi_nonfatal_month),

    ("time_series_stroke_fatal", time_series_stroke_fatal),
    ("time_series_stroke_nonfatal", time_series_stroke_nonfatal),
    ("time_series_stroke_nonfatal_2week", time_series_stroke_nonfatal_2week),
    ("time_series_stroke_nonfatal_month", time_series_stroke_nonfatal_month),

    ("time_series_at_fatal", time_series_at_fatal),
    ("time_series_at_nonfatal", time_series_at_nonfatal),
    ("time_series_at_nonfatal_2week", time_series_at_nonfatal_2week),
    ("time_series_at_nonfatal_month", time_series_at_nonfatal_month),

    ("time_series_ae_fatal", time_series_ae_fatal),
    ("time_series_ae_nonfatal", time_series_ae_nonfatal),
    ("time_series_ae_nonfatal_2week", time_series_ae_nonfatal_2week),
    ("time_series_ae_nonfatal_month", time_series_ae_nonfatal_month),

    ("time_series_ve_fatal", time_series_ve_fatal),
    ("time_series_ve_nonfatal", time_series_ve_nonfatal),
    ("time_series_ve_nonfatal_2week", time_series_ve_nonfatal_2week),
    ("time_series_ve_nonfatal_month", time_series_ve_nonfatal_month),

    ("time_series_covid_fatal", time_series_covid_fatal),
    ("time_series_covid_nonfatal", time_series_covid_nonfatal),
    ("time_series_covid_nonfatal_2week", time_series_covid_nonfatal_2week),
    ("time_series_covid_nonfatal_month", time_series_covid_nonfatal_month),
]


# Save with correct variable name string
for varname, df in time_series_datasets:
    save_table(df=df, out_name=f"{proj}_tmp_{varname}", save_previous=False)


# COMMAND ----------

# DBTITLE 1,Re-load
time_series_mi_fatal = spark.table(f"{dsa}.{proj}_tmp_time_series_mi_fatal")
time_series_mi_nonfatal = spark.table(f"{dsa}.{proj}_tmp_time_series_mi_nonfatal")
time_series_mi_nonfatal_2week = spark.table(f"{dsa}.{proj}_tmp_time_series_mi_nonfatal_2week")
time_series_mi_nonfatal_month = spark.table(f"{dsa}.{proj}_tmp_time_series_mi_nonfatal_month")

time_series_stroke_fatal = spark.table(f"{dsa}.{proj}_tmp_time_series_stroke_fatal")
time_series_stroke_nonfatal = spark.table(f"{dsa}.{proj}_tmp_time_series_stroke_nonfatal")
time_series_stroke_nonfatal_2week = spark.table(f"{dsa}.{proj}_tmp_time_series_stroke_nonfatal_2week")
time_series_stroke_nonfatal_month = spark.table(f"{dsa}.{proj}_tmp_time_series_stroke_nonfatal_month")

time_series_at_fatal = spark.table(f"{dsa}.{proj}_tmp_time_series_at_fatal")
time_series_at_nonfatal = spark.table(f"{dsa}.{proj}_tmp_time_series_at_nonfatal")
time_series_at_nonfatal_2week = spark.table(f"{dsa}.{proj}_tmp_time_series_at_nonfatal_2week")
time_series_at_nonfatal_month = spark.table(f"{dsa}.{proj}_tmp_time_series_at_nonfatal_month")

time_series_ae_fatal = spark.table(f"{dsa}.{proj}_tmp_time_series_ae_fatal")
time_series_ae_nonfatal = spark.table(f"{dsa}.{proj}_tmp_time_series_ae_nonfatal")
time_series_ae_nonfatal_2week = spark.table(f"{dsa}.{proj}_tmp_time_series_ae_nonfatal_2week")
time_series_ae_nonfatal_month = spark.table(f"{dsa}.{proj}_tmp_time_series_ae_nonfatal_month")

time_series_ve_fatal = spark.table(f"{dsa}.{proj}_tmp_time_series_ve_fatal")
time_series_ve_nonfatal = spark.table(f"{dsa}.{proj}_tmp_time_series_ve_nonfatal")
time_series_ve_nonfatal_2week = spark.table(f"{dsa}.{proj}_tmp_time_series_ve_nonfatal_2week")
time_series_ve_nonfatal_month = spark.table(f"{dsa}.{proj}_tmp_time_series_ve_nonfatal_month")

time_series_covid_fatal = spark.table(f"{dsa}.{proj}_tmp_time_series_covid_fatal")
time_series_covid_nonfatal = spark.table(f"{dsa}.{proj}_tmp_time_series_covid_nonfatal")
time_series_covid_nonfatal_2week = spark.table(f"{dsa}.{proj}_tmp_time_series_covid_nonfatal_2week")
time_series_covid_nonfatal_month = spark.table(f"{dsa}.{proj}_tmp_time_series_covid_nonfatal_month")



# COMMAND ----------

display(time_series_mi_fatal)

# COMMAND ----------

# DBTITLE 1,Make time series individual level summary dataset
def process_time_series(df):
    """
    Function to process a time series dataset and extract individual-level summary statistics.

    Parameters:
    df (DataFrame): A PySpark DataFrame containing time series data with multiple follow-up days per individual.

    Returns:
    DataFrame: A processed DataFrame with one row per individual and a count of outcome events.
    """

    # Cutoff date for air pollution analysis
    cutoff_2021 = f.lit("2021-12-31").cast("date")


    # Grouping by PERSON_ID and aggregating necessary columns
    result_df = df.select('PERSON_ID', 'study_start_date', 'end_date', 'time_series_date', 'outcome_ind', 'tmean', 'pm2p5').groupBy('PERSON_ID').agg(
        
        # Earliest follow-up date for each person
        f.min('study_start_date').alias('fu_start'),
        
        # Latest follow-up date for each person
        f.max('time_series_date').alias('fu_end_date'),

        # Follow-up duration in days
        f.datediff(f.max("time_series_date"), f.min("study_start_date")).alias("ts_fu_days"),
        
        # Counting the number of outcome events per person

        # tmean analysis study period

        # Did an outcome occur between study start and december 2021 (air pollution analysis)
        f.max(
            f.when(
                (f.col("time_series_date") >= f.col("study_start_date")) &
                (f.col("time_series_date") <= cutoff_2021) &
                (f.col("outcome_ind") == 1), 1).otherwise(0))
        .alias("outcome_event_20_21"),

        # Counting the number of outcome events per person

        #  Total number of outcome events temperature analysis
        f.sum(f.when(
            (f.col("time_series_date") >= f.col("study_start_date")) &
            (f.col("time_series_date") <= f.col("end_date")) &
            (f.col("outcome_ind") == 1), 1).otherwise(0))
        .alias("outcome_event_n"),

        # Total number of outcome events air pollution analysis
        f.sum(f.when(
            (f.col("time_series_date") >= f.col("study_start_date")) &
            (f.col("time_series_date") <= cutoff_2021) &
            (f.col("outcome_ind") == 1), 1).otherwise(0))
        .alias("outcome_event_n_20_21"),

         # Any missing tmean between study_start and fu_end?
        f.max(
            f.when(
            (f.col("time_series_date") >= f.col("study_start_date")) &
            (f.col("time_series_date") <= f.col("end_date")) &
            (f.col("tmean").isNull()), 1).otherwise(0))
        .alias("missing_tmean"),

        # Any missing pm2p5 between study_start and 2021-12-31?
        f.max(
            f.when(
            (f.col("time_series_date") >= f.col("study_start_date")) &
            (f.col("time_series_date") <= cutoff_2021) &
            f.col("pm2p5").isNull(), 1).otherwise(0))
        .alias("pm2p5_missing_flag"),

    ).withColumn(
        # Creating 'tmean_analysis' column (1 if no missing 'tmean', otherwise 0)
        'tmean_analysis', f.when(f.col('missing_tmean') == 0, 1).otherwise(0)
    ).withColumn(
        "missing_pm2p5",
        f.when(
            (f.col("outcome_event_20_21") == 1) &
            (f.col("pm2p5_missing_flag") == 1), 1).otherwise(0)
    ).withColumn(
        # Creating 'air_pollution_analysis' column (if someone has an outcome event between jan 2020 and dec 2021 and 'pm2p5' is not missing in this period, otherwise 0)
        'air_pollution_analysis', f.when((f.col('missing_pm2p5') == 0) & (f.col('outcome_event_20_21') == 1), 1).otherwise(0)).drop("pm2p5_missing_flag")
    
    return result_df  # Returning the processed DataFrame
  
  # Processing each time series dataset and storing the results in separate DataFrame variables

time_series_mi_fatal_individuals = process_time_series(time_series_mi_fatal)
time_series_mi_nonfatal_individuals = process_time_series(time_series_mi_nonfatal)
time_series_mi_nonfatal_2week_individuals = process_time_series(time_series_mi_nonfatal_2week)
time_series_mi_nonfatal_month_individuals = process_time_series(time_series_mi_nonfatal_month)

time_series_stroke_fatal_individuals = process_time_series(time_series_stroke_fatal)
time_series_stroke_nonfatal_individuals = process_time_series(time_series_stroke_nonfatal)
time_series_stroke_nonfatal_2week_individuals = process_time_series(time_series_stroke_nonfatal_2week)
time_series_stroke_nonfatal_month_individuals = process_time_series(time_series_stroke_nonfatal_month)

time_series_at_fatal_individuals = process_time_series(time_series_at_fatal)
time_series_at_nonfatal_individuals = process_time_series(time_series_at_nonfatal)
time_series_at_nonfatal_2week_individuals = process_time_series(time_series_at_nonfatal_2week)
time_series_at_nonfatal_month_individuals = process_time_series(time_series_at_nonfatal_month)

time_series_ae_fatal_individuals = process_time_series(time_series_ae_fatal)
time_series_ae_nonfatal_individuals = process_time_series(time_series_ae_nonfatal)
time_series_ae_nonfatal_2week_individuals = process_time_series(time_series_ae_nonfatal_2week)
time_series_ae_nonfatal_month_individuals = process_time_series(time_series_ae_nonfatal_month)

time_series_ve_fatal_individuals = process_time_series(time_series_ve_fatal)
time_series_ve_nonfatal_individuals = process_time_series(time_series_ve_nonfatal)
time_series_ve_nonfatal_2week_individuals = process_time_series(time_series_ve_nonfatal_2week)
time_series_ve_nonfatal_month_individuals = process_time_series(time_series_ve_nonfatal_month)

time_series_covid_fatal_individuals = process_time_series(time_series_covid_fatal)
time_series_covid_nonfatal_individuals = process_time_series(time_series_covid_nonfatal)
time_series_covid_nonfatal_2week_individuals = process_time_series(time_series_covid_nonfatal_2week)
time_series_covid_nonfatal_month_individuals = process_time_series(time_series_covid_nonfatal_month)


# COMMAND ----------

# DBTITLE 1,Make time series overall summary dataset
# List of datasets and their labels
summary_specs = [
    ("ve", time_series_ve_fatal_individuals),
    ("ve_nonfatal", time_series_ve_nonfatal_individuals),
    ("ve_nonfatal_2week", time_series_ve_nonfatal_2week_individuals),
    ("ve_nonfatal_month", time_series_ve_nonfatal_month_individuals),

    ("ae", time_series_ae_fatal_individuals),
    ("ae_nonfatal", time_series_ae_nonfatal_individuals),
    ("ae_nonfatal_2week", time_series_ae_nonfatal_2week_individuals),
    ("ae_nonfatal_month", time_series_ae_nonfatal_month_individuals),

    ("stroke", time_series_stroke_fatal_individuals),
    ("stroke_nonfatal", time_series_stroke_nonfatal_individuals),
    ("stroke_nonfatal_2week", time_series_stroke_nonfatal_2week_individuals),
    ("stroke_nonfatal_month", time_series_stroke_nonfatal_month_individuals),

    ("mi", time_series_mi_fatal_individuals),
    ("mi_nonfatal", time_series_mi_nonfatal_individuals),
    ("mi_nonfatal_2week", time_series_mi_nonfatal_2week_individuals),
    ("mi_nonfatal_month", time_series_mi_nonfatal_month_individuals),

    ("at", time_series_at_fatal_individuals),
    ("at_nonfatal", time_series_at_nonfatal_individuals),
    ("at_nonfatal_2week", time_series_at_nonfatal_2week_individuals),
    ("at_nonfatal_month", time_series_at_nonfatal_month_individuals),

    ("covid", time_series_covid_fatal_individuals),
    ("covid_nonfatal", time_series_covid_nonfatal_individuals),
    ("covid_nonfatal_2week", time_series_covid_nonfatal_2week_individuals),
    ("covid_nonfatal_month", time_series_covid_nonfatal_month_individuals),
]


# Helper to compute summary row per dataset
def summarise_dataset(label, df):
    def round_or_suppress(n):
        if n == 0:
            return 0
        elif n is None:
            return None
        elif n < 10:
            return "<10"
        else:
            return int(round(n / 5.0) * 5)

    # Extract values
    total_persons_tmean = df.select("PERSON_ID").distinct().count()
    total_events_tmean = df.agg(f.sum("outcome_event_n")).first()[0]
    persons_analysis_tmean = df.filter(f.col("tmean_analysis") == 1).select("PERSON_ID").distinct().count()
    events_analysis_tmean = df.filter(f.col("tmean_analysis") == 1).agg(f.sum("outcome_event_n")).first()[0]
    missing_tmean = df.filter(f.col("missing_tmean") == 1).select("PERSON_ID").distinct().count()

    total_persons_pm2p5 = df.filter(f.col("outcome_event_20_21") == 1).select("PERSON_ID").distinct().count()
    total_events_pm2p5 = df.agg(f.sum("outcome_event_n_20_21")).first()[0]
    persons_analysis_pm2p5 = df.filter(f.col("air_pollution_analysis") == 1).select("PERSON_ID").distinct().count()
    events_analysis_pm2p5 = df.filter(f.col("air_pollution_analysis") == 1).agg(f.sum("outcome_event_n_20_21")).first()[0]
    missing_air_pollution = df.filter(f.col("missing_pm2p5") == 1).select("PERSON_ID").distinct().count()

    return spark.createDataFrame([{
        "time_series": label,
        "n_total_persons_tmean": round_or_suppress(total_persons_tmean),
        "n_total_events_tmean": round_or_suppress(total_events_tmean),
        "n_persons_analysis_tmean": round_or_suppress(persons_analysis_tmean),
        "n_events_analysis_tmean": round_or_suppress(events_analysis_tmean),
        "missing_tmean": round_or_suppress(missing_tmean),
        "n_total_persons_pm2p5": round_or_suppress(total_persons_pm2p5),
        "n_total_events_pm2p5": round_or_suppress(total_events_pm2p5),
        "n_persons_analysis_pm2p5": round_or_suppress(persons_analysis_pm2p5),
        "n_events_analysis_pm2p5": round_or_suppress(events_analysis_pm2p5),
        "missing_air_pollution": round_or_suppress(missing_air_pollution)
    }])


# Run the summarization across all datasets
summary_rows = [summarise_dataset(label, df) for label, df in summary_specs]

# Union all rows into one final DataFrame
summary_df = summary_rows[0]
for row_df in summary_rows[1:]:
    summary_df = summary_df.unionByName(row_df)

# Save
save_table(df=summary_df, out_name=f"{proj}_out_time_series_summary", save_previous=False)

# View result
display(summary_df)


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
    save_table(df=split_df, out_name=f'{proj}_out_time_series_{name}_{i+1}', save_previous=False)

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

# COMMAND ----------

time_series_ve_fatal_1 = spark.table(f"{dsa}.{proj}_out_time_series_ve_fatal_1")
time_series_ve_fatal_1.select("PERSON_ID").distinct().count()

# COMMAND ----------

time_series_ve_fatal_2 = spark.table(f"{dsa}.{proj}_out_time_series_ve_fatal_2")
time_series_ve_fatal_2.select("PERSON_ID").distinct().count()

# COMMAND ----------

time_series_ve_fatal_3 = spark.table(f"{dsa}.{proj}_out_time_series_ve_fatal_3")
time_series_ve_fatal_3.select("PERSON_ID").distinct().count()

# COMMAND ----------

time_series_ve_fatal_4 = spark.table(f"{dsa}.{proj}_out_time_series_ve_fatal_4")
time_series_ve_fatal_4.select("PERSON_ID").distinct().count()

# COMMAND ----------

time_series_ve_fatal_5 = spark.table(f"{dsa}.{proj}_out_time_series_ve_fatal_5")
time_series_ve_fatal_5.select("PERSON_ID").distinct().count()

# COMMAND ----------

# DBTITLE 1,Save full time series
save_table(df=time_series_mi_fatal, out_name=f'{proj}_out_time_series_mi_fatal', save_previous=False)
save_table(df=time_series_mi_nonfatal, out_name=f'{proj}_out_time_series_mi_nonfatal', save_previous=False)
save_table(df=time_series_stroke_fatal, out_name=f'{proj}_out_time_series_stroke_fatal', save_previous=False)
save_table(df=time_series_stroke_nonfatal, out_name=f'{proj}_out_time_series_stroke_nonfatal', save_previous=False)
save_table(df=time_series_at_fatal, out_name=f'{proj}_out_time_series_at_fatal', save_previous=False)
save_table(df=time_series_at_nonfatal, out_name=f'{proj}_out_time_series_at_nonfatal', save_previous=False)
save_table(df=time_series_ae_fatal, out_name=f'{proj}_out_time_series_ae_fatal', save_previous=False)
save_table(df=time_series_ae_nonfatal, out_name=f'{proj}_out_time_series_ae_nonfatal', save_previous=False)
save_table(df=time_series_ve_fatal, out_name=f'{proj}_out_time_series_ve_fatal', save_previous=False)
save_table(df=time_series_ve_nonfatal, out_name=f'{proj}_out_time_series_ve_nonfatal', save_previous=False)
save_table(df=time_series_covid_fatal, out_name=f'{proj}_out_time_series_covid_fatal', save_previous=False)
save_table(df=time_series_covid_nonfatal, out_name=f'{proj}_out_time_series_covid_nonfatal', save_previous=False)

# COMMAND ----------

# DBTITLE 1,Save time series individual level summary
save_table(df=time_series_mi_fatal_individuals, out_name=f"{proj}_out_time_series_mi_fatal_individual_level_summary", save_previous=False)
save_table(df=time_series_mi_nonfatal_individuals, out_name=f"{proj}_out_time_series_mi_nonfatal_individual_level_summary", save_previous=False)
save_table(df=time_series_mi_nonfatal_2week_individuals, out_name=f"{proj}_out_time_series_mi_nonfatal_2week_individual_level_summary", save_previous=False)
save_table(df=time_series_mi_nonfatal_month_individuals, out_name=f"{proj}_out_time_series_mi_nonfatal_month_individual_level_summary", save_previous=False)

save_table(df=time_series_stroke_fatal_individuals, out_name=f"{proj}_out_time_series_stroke_fatal_individual_level_summary", save_previous=False)
save_table(df=time_series_stroke_nonfatal_individuals, out_name=f"{proj}_out_time_series_stroke_nonfatal_individual_level_summary", save_previous=False)
save_table(df=time_series_stroke_nonfatal_2week_individuals, out_name=f"{proj}_out_time_series_stroke_nonfatal_2week_individual_level_summary", save_previous=False)
save_table(df=time_series_stroke_nonfatal_month_individuals, out_name=f"{proj}_out_time_series_stroke_nonfatal_month_individual_level_summary", save_previous=False)

save_table(df=time_series_at_fatal_individuals, out_name=f"{proj}_out_time_series_at_fatal_individual_level_summary", save_previous=False)
save_table(df=time_series_at_nonfatal_individuals, out_name=f"{proj}_out_time_series_at_nonfatal_individual_level_summary", save_previous=False)
save_table(df=time_series_at_nonfatal_2week_individuals, out_name=f"{proj}_out_time_series_at_nonfatal_2week_individual_level_summary", save_previous=False)
save_table(df=time_series_at_nonfatal_month_individuals, out_name=f"{proj}_out_time_series_at_nonfatal_month_individual_level_summary", save_previous=False)

save_table(df=time_series_ae_fatal_individuals, out_name=f"{proj}_out_time_series_ae_fatal_individual_level_summary", save_previous=False)
save_table(df=time_series_ae_nonfatal_individuals, out_name=f"{proj}_out_time_series_ae_nonfatal_individual_level_summary", save_previous=False)
save_table(df=time_series_ae_nonfatal_2week_individuals, out_name=f"{proj}_out_time_series_ae_nonfatal_2week_individual_level_summary", save_previous=False)
save_table(df=time_series_ae_nonfatal_month_individuals, out_name=f"{proj}_out_time_series_ae_nonfatal_month_individual_level_summary", save_previous=False)

save_table(df=time_series_ve_fatal_individuals, out_name=f"{proj}_out_time_series_ve_fatal_individual_level_summary", save_previous=False)
save_table(df=time_series_ve_nonfatal_individuals, out_name=f"{proj}_out_time_series_ve_nonfatal_individual_level_summary", save_previous=False)
save_table(df=time_series_ve_nonfatal_2week_individuals, out_name=f"{proj}_out_time_series_ve_nonfatal_2week_individual_level_summary", save_previous=False)
save_table(df=time_series_ve_nonfatal_month_individuals, out_name=f"{proj}_out_time_series_ve_nonfatal_month_individual_level_summary", save_previous=False)

save_table(df=time_series_covid_fatal_individuals, out_name=f"{proj}_out_time_series_covid_fatal_individual_level_summary", save_previous=False)
save_table(df=time_series_covid_nonfatal_individuals, out_name=f"{proj}_out_time_series_covid_nonfatal_individual_level_summary", save_previous=False)
save_table(df=time_series_covid_nonfatal_2week_individuals, out_name=f"{proj}_out_time_series_covid_nonfatal_2week_individual_level_summary", save_previous=False)
save_table(df=time_series_covid_nonfatal_month_individuals, out_name=f"{proj}_out_time_series_covid_nonfatal_month_individual_level_summary", save_previous=False)


# COMMAND ----------

# DBTITLE 1,Update and save tally
tally = spark.table(f'{dsa}.{proj}_tmp_inc_exc_flow')

new_rows = {}

# Get the row where time_series == "mi"
mi_row = summary_df.filter(f.col("time_series") == "mi").collect()[0]

new_rows[1] = spark.createDataFrame([(8, 'identification_of_cases_tmean', 'Post exclusion of individuals without myocardial infarction during study period 2020-2022', 'PERSON_ID', mi_row["n_total_events_tmean"], mi_row["n_total_events_tmean"], mi_row["n_total_persons_tmean"])], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])
new_rows[2] = spark.createDataFrame([(9, 'identification_of_cases_pm2p5', 'Post exclusion of individuals without myocardial infarction during study period 2020-2021', 'PERSON_ID', mi_row["n_total_events_pm2p5"], mi_row["n_total_events_pm2p5"], mi_row["n_total_persons_pm2p5"])], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])

mi_nonfatal_row = summary_df.filter(f.col("time_series") == "mi_nonfatal").collect()[0]

new_rows[3] = spark.createDataFrame([(10, 'identification_of_cases_tmean', 'Post exclusion of individuals without myocardial infarction during study period and individuals who died at outcome or within 7 days after, during study period 2020-2022', 'PERSON_ID',  mi_nonfatal_row["n_total_events_tmean"], mi_nonfatal_row["n_total_events_tmean"], mi_nonfatal_row["n_total_persons_tmean"])], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])
new_rows[4] = spark.createDataFrame([(11, 'identification_of_cases_pm2p5', 'Post exclusion of individuals without myocardial infarction and individuals who died at outcome or within 7 days after, during study period 2020-2021', 'PERSON_ID', mi_nonfatal_row["n_total_events_pm2p5"], mi_nonfatal_row["n_total_events_pm2p5"], mi_nonfatal_row["n_total_persons_pm2p5"])], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])

stroke_row = summary_df.filter(f.col("time_series") == "stroke").collect()[0]

new_rows[5] = spark.createDataFrame([(12, 'identification_of_cases_tmean', 'Post exclusion of individuals without stroke during study period 2020-2022', 'PERSON_ID',  stroke_row["n_total_events_tmean"], stroke_row["n_total_events_tmean"], stroke_row["n_total_persons_tmean"])], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])
new_rows[6] = spark.createDataFrame([(13, 'identification_of_cases_pm2p5', 'Post exclusion of individuals without stroke during study period 2020-2021', 'PERSON_ID', stroke_row["n_total_events_pm2p5"], stroke_row["n_total_events_pm2p5"], stroke_row["n_total_persons_pm2p5"])], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])

stroke_nonfatal_row = summary_df.filter(f.col("time_series") == "stroke_nonfatal").collect()[0]

new_rows[7] = spark.createDataFrame([(14, 'identification_of_cases_tmean', 'Post exclusion of individuals without stroke during study period and individuals who died at outcome or within 7 days after, during study period 2020-2022', 'PERSON_ID', stroke_nonfatal_row["n_total_events_tmean"], stroke_nonfatal_row["n_total_events_tmean"], stroke_nonfatal_row["n_total_persons_tmean"])], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])
new_rows[8] = spark.createDataFrame([(15, 'identification_of_cases_pm2p5', 'Post exclusion of individuals without stroke during study period and individuals who died at outcome or within 7 days after, during study period 2020-2021', 'PERSON_ID',  stroke_nonfatal_row["n_total_events_pm2p5"], stroke_nonfatal_row["n_total_events_pm2p5"], stroke_nonfatal_row["n_total_persons_pm2p5"])], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])

ae_row = summary_df.filter(f.col("time_series") == "ae").collect()[0]

new_rows[9] = spark.createDataFrame([(16, 'identification_of_cases_tmean', 'Post exclusion of individuals without arterial thrombotic event during study period 2020-2022', 'PERSON_ID',  ae_row["n_total_events_tmean"], ae_row["n_total_events_tmean"], ae_row["n_total_persons_tmean"])], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])
new_rows[10] = spark.createDataFrame([(17, 'identification_of_cases_pm2p5', 'Post exclusion of individuals without arterial thrombotic event during study period 2020-2021', 'PERSON_ID', ae_row["n_total_events_pm2p5"], ae_row["n_total_events_pm2p5"], ae_row["n_total_persons_pm2p5"])], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])

ae_nonfatal_row = summary_df.filter(f.col("time_series") == "ae_nonfatal").collect()[0]

new_rows[11] = spark.createDataFrame([(18, 'identification_of_cases_tmean', 'Post exclusion of individuals without arterial thrombotic event during study period and individuals who died at outcome or within 7 days after, during study period 2020-2022', 'PERSON_ID', ae_nonfatal_row["n_total_events_tmean"], ae_nonfatal_row["n_total_events_tmean"], ae_nonfatal_row["n_total_persons_tmean"])], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])
new_rows[12] = spark.createDataFrame([(19, 'identification_of_cases_pm2p5', 'Post exclusion of individuals without arterial thrombotic event during study period and individuals who died at outcome or within 7 days after, during study period 2020-2021', 'PERSON_ID',  ae_nonfatal_row["n_total_events_pm2p5"], ae_nonfatal_row["n_total_events_pm2p5"], ae_nonfatal_row["n_total_persons_pm2p5"])], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])

ve_row = summary_df.filter(f.col("time_series") == "ve").collect()[0]

new_rows[13] = spark.createDataFrame([(20, 'identification_of_cases_tmean', 'Post exclusion of individuals without venous thrombotic event during study period 2020-2022', 'PERSON_ID',  ve_row["n_total_events_tmean"], ve_row["n_total_events_tmean"], ve_row["n_total_persons_tmean"])], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])
new_rows[14] = spark.createDataFrame([(21, 'identification_of_cases_pm2p5', 'Post exclusion of individuals without venous thrombotic event during study period 2020-2021', 'PERSON_ID', ve_row["n_total_events_pm2p5"], ve_row["n_total_events_pm2p5"], ve_row["n_total_persons_pm2p5"])], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])

ve_nonfatal_row = summary_df.filter(f.col("time_series") == "ve_nonfatal").collect()[0]

new_rows[15] = spark.createDataFrame([(22, 'identification_of_cases_tmean', 'Post exclusion of individuals without venous thrombotic event during study period and individuals who died at outcome or within 7 days after, during study period 2020-2022', 'PERSON_ID', ve_nonfatal_row["n_total_events_tmean"], ve_nonfatal_row["n_total_events_tmean"], ve_nonfatal_row["n_total_persons_tmean"])], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])
new_rows[16] = spark.createDataFrame([(23, 'identification_of_cases_pm2p5', 'Post exclusion of individuals without venous thrombotic event during study period and individuals who died at outcome or within 7 days after, during study period 2020-2021', 'PERSON_ID',  ve_nonfatal_row["n_total_events_pm2p5"], ve_nonfatal_row["n_total_events_pm2p5"], ve_nonfatal_row["n_total_persons_pm2p5"])], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])

covid_row = summary_df.filter(f.col("time_series") == "covid").collect()[0]

new_rows[17] = spark.createDataFrame([(24, 'identification_of_cases_tmean', 'Post exclusion of individuals without COVID19 diagnosis during study period 2020-2022', 'PERSON_ID',  covid_row["n_total_events_tmean"], covid_row["n_total_events_tmean"], covid_row["n_total_persons_tmean"])], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])
new_rows[18] = spark.createDataFrame([(25, 'identification_of_cases_pm2p5', 'Post exclusion of individuals without COVID19 diagnosis during study period 2020-2021', 'PERSON_ID', covid_row["n_total_events_pm2p5"], covid_row["n_total_events_pm2p5"], covid_row["n_total_persons_pm2p5"])], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])

covid_nonfatal_row = summary_df.filter(f.col("time_series") == "covid_nonfatal").collect()[0]

new_rows[19] = spark.createDataFrame([(26, 'identification_of_cases_tmean', 'Post exclusion of individuals without COVID19 diagnosis during study period and individuals who died at outcome or within 7 days after, during study period 2020-2022', 'PERSON_ID', covid_nonfatal_row["n_total_events_tmean"], covid_nonfatal_row["n_total_events_tmean"], covid_nonfatal_row["n_total_persons_tmean"])], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])
new_rows[20] = spark.createDataFrame([(27, 'identification_of_cases_pm2p5', 'Post exclusion of individuals without COVID19 diagnosis during study period and individuals who died at outcome or within 7 days after, during study period 2020-2021', 'PERSON_ID',  covid_nonfatal_row["n_total_events_pm2p5"], covid_nonfatal_row["n_total_events_pm2p5"], covid_nonfatal_row["n_total_persons_pm2p5"])], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])

# Union them together
joined_rows = reduce(lambda df1, df2: df1.union(df2), new_rows.values())

# Union into tally dataframe
tally_update = tally.union(joined_rows)
display(tally_update)

# COMMAND ----------

save_table(df=tally_update, out_name=f'{proj}_tmp_inc_exc_flow_cases', save_previous=True)