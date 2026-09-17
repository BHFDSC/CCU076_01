# Databricks notebook source
# MAGIC %md # CCU076_01-D07-LSOA_multisource_curation
# MAGIC
# MAGIC **Description** This notebook creates a table with the LSOA at study start date (max 2 year before) and further LSOA records throughout the study period for each individual in the cohort. The notebook deals with tied LSOA records.
# MAGIC  
# MAGIC **Authors** Fionna Chalmers, Jamie Farrell, Isabel Walter
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Data output**  
# MAGIC **`out_lsoa_dynamic`** : Curated LSOA records in long format of individuals in cohort with records from at the earliest 2 years before start of study period until study end date at the latest.   
# MAGIC **`out_cohort_new`** : Updated cohort based on exclusion of individuals who do not have a recent LSOA record for time series creation.  
# MAGIC **`tmp_inc_exc_flow_new`** : Updated flowchart based on above.   

# COMMAND ----------

# MAGIC %md # 0. Setup

# COMMAND ----------

spark.sql('CLEAR CACHE')
import pyspark.sql.functions as f
from pyspark.sql import Window
from functools import reduce
import pyspark.pandas as ps

# COMMAND ----------

# MAGIC %run "./Spencers_common_functions"

# COMMAND ----------

# MAGIC %md # 1. Parameters

# COMMAND ----------

# MAGIC %run "./CCU076_01-D01-parameters"

# COMMAND ----------

# MAGIC %md # 2. Data sources

# COMMAND ----------

cohort = spark.table(path_out_cohort)
lsoa_multisource = spark.table(path_cur_lsoa_multisource)

# COMMAND ----------

# MAGIC %md # 3. Data curation

# COMMAND ----------

# DBTITLE 1,Restrict LSOA dataset to cohort and study end date
# Restrict to PERSON_ID in cohort and to LSOA records from before study end date
lsoa_multisource = (lsoa_multisource
                    .withColumnRenamed('person_id', 'PERSON_ID')
                    .join(cohort.select('PERSON_ID'), on='PERSON_ID', how='inner')
                    .filter(f.col('record_date') <= (f.lit(study_end_date))))

# Remove the SSNAP data source and null values
lsoa_multisource = lsoa_multisource.filter(f.col('data_source')!='ssnap').filter(f.col('lsoa').isNotNull())

# Filter out non coding lsoa
lsoa_multisource = lsoa_multisource.filter(f.col("lsoa") != "E99999999")

# COMMAND ----------

# DBTITLE 1,Restrict LSOA dataset to records within study period and two years before
# Individual censor dates

# Get date two years before study start date 
#start_date_lit = f.to_date(f.lit(study_start_date), 'yyyy-MM-dd')
#start_two_year_before = spark.range(1).select(f.add_months(start_date_lit, -(12*2)).alias("two_year_before")).#collect()[0]["two_year_before"]

# Get most recent LSOA date for each person at study start date and make this the first LSOA record date
#window_spec = Window.partitionBy('PERSON_ID').orderBy(f.col('record_date').desc())

#individual_censor_dates = (
#  lsoa_multisource
#  .filter(f.col('record_date') <= study_start_date)
#  .filter(f.col('record_date') >= start_two_year_before)
#  .withColumn('rank', f.rank().over(window_spec))
#  .filter(f.col('rank') == 1).select('PERSON_ID', f.col('record_date').alias('first_record_date'))
#  .distinct()
#  .orderBy('PERSON_ID','rank')
#)

# Get date two years before study start date
start_date_lit = f.to_date(f.lit(study_start_date), "yyyy-MM-dd")
start_two_year_before = (
    spark.range(1)
    .select(f.add_months(start_date_lit, -(12*2)).alias("two_year_before"))
    .collect()[0]["two_year_before"]
)

# Window spec: order by closeness to study_start_date
window_spec = Window.partitionBy("PERSON_ID").orderBy(f.col("date_diff").asc())

# Get the LSOA closest to study_start_date within [study_start_date - 2y, study_end_date]
individual_censor_dates = (
    lsoa_multisource
    .filter(
        (f.col("record_date") >= start_two_year_before) &
        (f.col("record_date") <= study_end_date)
    )
    .withColumn("date_diff", f.abs(f.datediff(f.col("record_date"), start_date_lit)))
    .withColumn("rank", f.row_number().over(window_spec))
    .filter(f.col("rank") == 1)
    .select(
        "PERSON_ID",
        f.col("record_date").alias("closest_record_date")
    )
    .distinct()
    .orderBy("PERSON_ID")
)

# We restrict LSOA multisource to records between the first record date and study end date.
# For those without a first_record_date - add the first_record_date as the study start date allowing us to filter out the records for them that came post study start. Note: this step will filter out any persons who had an LSOA before two years prior to study start date, but no LSOA recorded during study period or within 2 years before that.   

lsoa_multisource_restricted = (
  lsoa_multisource.join(individual_censor_dates,on='PERSON_ID',how='left')
  .fillna({'closest_record_date': study_start_date})
  .filter(f.col('record_date') >= f.col('closest_record_date'))
  ) 

cohort_count = cohort.count()
lsoa_multisource_count = lsoa_multisource.select('PERSON_ID').distinct().count()
lsoa_multisource_restr_count = lsoa_multisource_restricted.select('PERSON_ID').distinct().count()

print(f'Cohort has {cohort_count} individuals. All of whom had an LSOA registered between study start date - 2 yrs and study end date.')
print(f'LSOA multisource has {lsoa_multisource_count} individuals. This should be same number as cohort after inner join with cohort.')
print(f'LSOA multisource restricted has {lsoa_multisource_restr_count} individuals. This further excluded anyone who does not have an LSOA within the period of two years before study start date until study end date.')    

# COMMAND ----------

# MAGIC %md # 4. Resolve LSOA ties

# COMMAND ----------

# DBTITLE 1,Resolve ties based on data source
# Data source priorty order
priority_mapping = {
    'gdppr': 1,
    'vaccine_status': 2,
    'hes_apc': 3,
    'hes_op': 4,
    'hes_ae': 4
}

window_spec = Window.partitionBy('PERSON_ID','record_date').orderBy('priority')

lsoa_multisource_restricted = (lsoa_multisource_restricted.withColumn('priority', 
                   f.when(f.col('data_source') == 'gdprr', priority_mapping['gdppr'])
                   .when(f.col('data_source') == 'vaccine_status', priority_mapping['vaccine_status'])
                   .when(f.col('data_source') == 'hes_apc', priority_mapping['hes_apc'])
                   .when(f.col('data_source') == 'hes_op', priority_mapping['hes_op'])
                   .when(f.col('data_source') == 'hes_ae', priority_mapping['hes_ae']))
                   
)

lsoa_multisource_ranked = (
    lsoa_multisource_restricted
    .withColumn('rank', f.rank().over(window_spec)).orderBy('PERSON_ID','record_date','rank')
    .filter(f.col('rank') == 1).drop('rank','priority')
)

# COMMAND ----------

# DBTITLE 1,Resolve remaining ties based on random selection
# Randomly select one value

# Specify window function to collect ties
_win_collect_ties = (
        Window
        .partitionBy('PERSON_ID','record_date','data_source')
       
)

# Create tie flag and collect ties in arrays
lsoa_ties = (
        lsoa_multisource_ranked
        .withColumn(
            'lsoa_distinct_value',
            f.collect_set(f.col('lsoa')).over(_win_collect_ties)
        )
        .withColumn(
            'lsoa_tie_flag',
            f.when(f.size(f.col('lsoa_distinct_value')) > f.lit(1), f.lit(1))
        )
        .withColumn(
            'lsoa_tie_value',
            f.when(f.col('lsoa_tie_flag') == f.lit(1), f.collect_list(f.col('lsoa')).over(_win_collect_ties))
        )
        .withColumn(
            'lsoa_tie_data_source',
            f.when(f.col('lsoa_tie_flag') == f.lit(1), f.collect_list(f.col('data_source')).over(_win_collect_ties))
        )
    )

seed = 124910
window_spec = Window.partitionBy('PERSON_ID', 'record_date').orderBy(f.rand(seed))

lsoa_untied = (
    lsoa_ties.withColumn('row_num', f.row_number().over(window_spec))
    .filter(f.col('row_num') == 1)
    .drop('row_num','lsoa_tie_value','lsoa_tie_data_source')
)

# COMMAND ----------

lsoa_untied.printSchema()

# COMMAND ----------

# DBTITLE 1,Add baseline record
# Step 1: Create synthetic records at study_start_date
synthetic_records = (
    lsoa_untied
    .filter(f.col("record_date") == f.col("closest_record_date"))
    .withColumn("record_date", f.lit(study_start_date))  # overwrite date
)

# Step 2: Assert no person is lost between synthetic_records and lsoa_untied
unique_persons_untied = lsoa_untied.select("PERSON_ID").distinct().count()
unique_persons_synthetic = synthetic_records.select("PERSON_ID").distinct().count()
assert unique_persons_untied == unique_persons_synthetic, (
    f"Mismatch: {unique_persons_untied} in lsoa_untied vs "
    f"{unique_persons_synthetic} in synthetic_records"
)

# Step 3: Add synthetic records to dataset
lsoa_with_start = lsoa_untied.unionByName(synthetic_records)

# Step 4: Remove any records before study_start_date
lsoa_final = lsoa_with_start.filter(f.col("record_date") >= study_start_date)

# ---- Sanity checks ----

# Unique individuals before and after
unique_before = lsoa_untied.select("PERSON_ID").distinct().count()
unique_after = lsoa_final.select("PERSON_ID").distinct().count()
print(f"Unique individuals before: {unique_before}")
print(f"Unique individuals after:  {unique_after}")

# Total records (rows)
print(f"Total records in lsoa_untied: {lsoa_untied.count()}")
print(f"Total records in lsoa_final:  {lsoa_final.count()}")

# ---- Count registrations per person ----
registrations_per_person = (
    lsoa_final
    .groupBy("PERSON_ID")
    .count()
    .withColumnRenamed("count", "num_registrations")
)

# Distribution: how many persons have 0, 1, 2... registrations
registration_distribution = (
    registrations_per_person
    .groupBy("num_registrations")
    .count()
    .orderBy("num_registrations")
)

registration_distribution.show(truncate=False)


# COMMAND ----------

lsoa_final.printSchema()

# COMMAND ----------

# MAGIC %md # 5. Save

# COMMAND ----------

lsoa_dynamic = lsoa_final.drop('closest_record_date')

# COMMAND ----------

# DBTITLE 1,Save dynamic LSOA registrations
save_table(df=lsoa_dynamic, out_name=f'{proj}_out_lsoa_dynamic', save_previous=True)

# COMMAND ----------

# MAGIC %md # 6. Update cohort and flowchart

# COMMAND ----------

# DBTITLE 1,Cohort update based on recent lsoa availability in lsoa multisource
#cohort_update = cohort.join(lsoa_multisource_restricted.select('PERSON_ID').distinct(), on='PERSON_ID', how='right')
#final_count = cohort_update.count() # about 450000 individuals excluded

# COMMAND ----------

# DBTITLE 1,Flowchart update based on recent lsoa availability in lsoa multisource
#tally = spark.table(f'{dsa}.{proj}_tmp_inc_exc_flow')
#ew_row = spark.createDataFrame([(7, 'post_incl_excl_notebook', 'Post exclusion of individual without registered LSOA within study period or 2 years before', 'PERSON_ID', final_count, final_count, final_count)], ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct'])
#tally_update = tally.union(new_row)
#display(tally_update)

# COMMAND ----------

# DBTITLE 1,Save updated versions
#save_table(df=cohort_update, out_name=f'{proj}_out_cohort', save_previous=True)
#save_table(df=tally_update, out_name=f'{proj}_tmp_inc_exc_flow', save_previous=True)