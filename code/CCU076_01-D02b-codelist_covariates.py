# Databricks notebook source
# MAGIC %md # CCU076_01-D02b-codelist_covariates
# MAGIC
# MAGIC **Description** This notebook creates the time invariant covariates codelists. These include smoking status (three categories: smoker, former smoker, non-smoker), history of hypertensive disorders (categories: yes or no), history of heart disease (categories: yes or no), history of being overweight/obesity (categories: yes or no), history of depression (categories: yes or no) and a composite of other comorbidities (categories: yes or no). The composite includes at least one of the following health conditions: history of diabetic disorders, chronic obstructive pulmonary disease, liver disease, chronic kidney disease, cancer and surgical intervention. The codelists of individual health conditions of this composite are created in this notebook, but the composite itself + surgical intervention are created in covariates notebook.
# MAGIC
# MAGIC **Authors** Alexia Sampri, Elena Raffetti, Yueying Li, Isabel Walter
# MAGIC
# MAGIC **Reviewers**  Alexia Sampri
# MAGIC
# MAGIC **Acknowledgements** Based on code from CCU004_03, CCU002_07 and CCU018_01.
# MAGIC
# MAGIC **Data Input**
# MAGIC - **`bhf_covid_uk_phenotypes_20210127`, `pmeds`, `map_ctv3_snome`**
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`ccu076_01_out_codelist`** 

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

# DBTITLE 1,Libraries
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

# MAGIC %md # 0 Parameters

# COMMAND ----------

# DBTITLE 1,Parameters
# MAGIC %run ./CCU076_01-D01-parameters

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

bhf_phenotypes  = spark.table(path_ref_bhf_phenotypes)
gdppr_refset = spark.table('dss_corporate.gdppr_cluster_refset')
map_ctv3_snomed =  spark.table('dss_corporate.read_codes_map_ctv3_to_snomed')

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# ------------------------------------------------------------------------------
# bhf_phenotypes
# ------------------------------------------------------------------------------
bhf_phenotypes = bhf_phenotypes\
  .select(['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate'])\
  .dropDuplicates()

# cache
bhf_phenotypes.cache()
print(f'{bhf_phenotypes.count():,}'); print()

# check number of codes
tmpt = tab(bhf_phenotypes, 'name', 'terminology', var2_unstyled=1); print()

# # -----------------------------------------------------------------------------
# # CCU004_3 covariates codelists
# # -----------------------------------------------------------------------------
# ccu004_03_covs = spark.table(f'{dsa}.ccu004_03_out_codelist_covariates_chronic_conditions')

# # cache
# ccu004_03_covs.cache()
# print(f'{ccu004_03_covs.count():,}'); print()

# # check number of codes
# tmpt = tab(ccu004_03_covs, 'name', 'terminology', var2_unstyled=1); print()

# ------------------------------------------------------------------------------
# pmeds
# ------------------------------------------------------------------------------
"""_pmeds = pmeds\
  .select(['PrescribedBNFCode', 'PrescribedBNFName'])\
  .dropDuplicates(['PrescribedBNFCode'])

# cache
_pmeds.cache()
print(f'{_pmeds.count():,}'); print()"""

# COMMAND ----------

# check
display(bhf_phenotypes)

# COMMAND ----------

# MAGIC %md # 3 Codelists

# COMMAND ----------

# MAGIC %md ## 3.1 Demographics

# COMMAND ----------

# BMI obesity
codelist_BMI_obesity = bhf_phenotypes.where(f.col('name') == 'BMI_obesity')
tmp_BMI_obesity = spark.createDataFrame(
  [  
    ('BMI_obesity', 'ICD10', 'E66', 'Diagnosis of obesity', '1', '20210127')
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)
codelist_BMI_obesity = codelist_BMI_obesity.unionByName(tmp_BMI_obesity)


# DBTITLE 1,Derive BMI codelist
# to be picked by Carmen for CCU004
bmi_snomed_codes = [
    '722595002',
    '914741000000103',
    '914731000000107',
    '914721000000105',
    '35425004',
    '48499001',
    '301331008',
    '6497000',
    '310252000',
    '427090001',
    '408512008',
    '162864005',
    '162863004',
    '412768003',
    '60621009',
    '846931000000101',
]

# Select from refset based on the curated list of SNOMED codes
bmi_snomed = gdppr_refset.where(
    f.col('ConceptId').isin(bmi_snomed_codes)
).dropDuplicates(['ConceptId'])

# Select based on cluster label BMIVAL_COD
bmi_cluster = gdppr_refset.where(
    f.col('Cluster_ID') == 'BMIVAL_COD'
).dropDuplicates(['ConceptId']).select('ConceptId')

# Merge the two (avoiding duplicates) and re-format to fit our usual codelist table format
codelist_bmi = (
    bmi_snomed.join(bmi_cluster, on='ConceptId', how='left')
    .withColumn('name', f.lit('bmi'))
    .withColumn('terminology', f.lit('SNOMED'))
    .withColumnRenamed('ConceptId', 'code')
    .withColumnRenamed('ConceptId_description', 'term')
    .select('name', 'terminology', 'code', 'term')
)

# smoking_(current|ex|never)
# 20220707 ER updated 6 smoking_ex SNOMED codes that had been rounded
tmp_smoking_status = spark.createDataFrame(
  [
    ("160603005","Light cigarette smoker (1-9 cigs/day) (finding)","Current-smoker","Light"),
    ("160606002","Very heavy cigarette smoker (40+ cigs/day) (finding)","Current-smoker","Heavy"),
    ("160613002","Admitted tobacco consumption possibly untrue (finding)","Current-smoker","Unknown"),
    ("160619003","Rolls own cigarettes (finding)","Current-smoker","Unknown"),
    ("230056004","Cigarette consumption (observable entity)","Current-smoker","Unknown"),
    ("230057008","Cigar consumption (observable entity)","Current-smoker","Unknown"),
    ("230058003","Pipe tobacco consumption (observable entity)","Current-smoker","Unknown"),
    ("230060001","Light cigarette smoker (finding)","Current-smoker","Light"),
    ("230062009","Moderate cigarette smoker (finding)","Current-smoker","Moderate"),
    ("230065006","Chain smoker (finding)","Current-smoker","Heavy"),
    ("266918002","Tobacco smoking consumption (observable entity)","Current-smoker","Unknown"),
    ("446172000","Failed attempt to stop smoking (finding)","Current-smoker","Unknown"),
    ("449868002","Smokes tobacco daily (finding)","Current-smoker","Unknown"),
    ("56578002","Moderate smoker (20 or less per day) (finding)","Current-smoker","Moderate"),
    ("56771006","Heavy smoker (over 20 per day) (finding)","Current-smoker","Heavy"),
    ("59978006","Cigar smoker (finding)","Current-smoker","Unknown"),
    ("65568007","Cigarette smoker (finding)","Current-smoker","Unknown"),
    ("134406006","Smoking reduced (finding)","Current-smoker","Unknown"),
    ("160604004","Moderate cigarette smoker (10-19 cigs/day) (finding)","Current-smoker","Moderate"),
    ("160605003","Heavy cigarette smoker (20-39 cigs/day) (finding)","Current-smoker","Heavy"),
    ("160612007","Keeps trying to stop smoking (finding)","Current-smoker","Unknown"),
    ("160616005","Trying to give up smoking (finding)","Current-smoker","Unknown"),
    ("203191000000107","Wants to stop smoking (finding)","Current-smoker","Unknown"),
    ("225934006","Smokes in bed (finding)","Current-smoker","Unknown"),
    ("230059006","Occasional cigarette smoker (finding)","Current-smoker","Light"),
    ("230063004","Heavy cigarette smoker (finding)","Current-smoker","Heavy"),
    ("230064005","Very heavy cigarette smoker (finding)","Current-smoker","Heavy"),
    ("266920004","Trivial cigarette smoker (less than one cigarette/day) (finding)","Current-smoker","Light"),
    ("266929003","Smoking started (finding)","Current-smoker","Unknown"),
    ("308438006","Smoking restarted (finding)","Current-smoker","Unknown"),
    ("394871007","Thinking about stopping smoking (finding)","Current-smoker","Unknown"),
    ("394872000","Ready to stop smoking (finding)","Current-smoker","Unknown"),
    ("394873005","Not interested in stopping smoking (finding)","Current-smoker","Unknown"),
    ("401159003","Reason for restarting smoking (observable entity)","Current-smoker","Unknown"),
    ("413173009","Minutes from waking to first tobacco consumption (observable entity)","Current-smoker","Unknown"),
    ("428041000124106","Occasional tobacco smoker (finding)","Current-smoker","Light"),
    ("77176002","Smoker (finding)","Current-smoker","Unknown"),
    ("82302008","Pipe smoker (finding)","Current-smoker","Unknown"),
    ("836001000000109","Waterpipe tobacco consumption (observable entity)","Current-smoker","Unknown"),
    ("160603005","Light cigarette smoker (1-9 cigs/day) (finding)","Current-smoker","Light"),
    ("160612007","Keeps trying to stop smoking (finding)","Current-smoker","Unknown"),
    ("160613002","Admitted tobacco consumption possibly untrue (finding)","Current-smoker","Unknown"),
    ("160616005","Trying to give up smoking (finding)","Current-smoker","Unknown"),
    ("160619003","Rolls own cigarettes (finding)","Current-smoker","Unknown"),
    ("160625004","Date ceased smoking (observable entity)","Current-smoker","Unknown"),
    ("225934006","Smokes in bed (finding)","Current-smoker","Unknown"),
    ("230056004","Cigarette consumption (observable entity)","Current-smoker","Unknown"),
    ("230057008","Cigar consumption (observable entity)","Current-smoker","Unknown"),
    ("230059006","Occasional cigarette smoker (finding)","Current-smoker","Light"),
    ("230060001","Light cigarette smoker (finding)","Current-smoker","Light"),
    ("230062009","Moderate cigarette smoker (finding)","Current-smoker","Moderate"),
    ("230063004","Heavy cigarette smoker (finding)","Current-smoker","Heavy"),
    ("230064005","Very heavy cigarette smoker (finding)","Current-smoker","Heavy"),
    ("266920004","Trivial cigarette smoker (less than one cigarette/day) (finding)","Current-smoker","Light"),
    ("266929003","Smoking started (finding)","Current-smoker","Unknown"),
    ("394872000","Ready to stop smoking (finding)","Current-smoker","Unknown"),
    ("401159003","Reason for restarting smoking (observable entity)","Current-smoker","Unknown"),
    ("449868002","Smokes tobacco daily (finding)","Current-smoker","Unknown"),
    ("65568007","Cigarette smoker (finding)","Current-smoker","Unknown"),
    ("134406006","Smoking reduced (finding)","Current-smoker","Unknown"),
    ("160604004","Moderate cigarette smoker (10-19 cigs/day) (finding)","Current-smoker","Moderate"),
    ("160605003","Heavy cigarette smoker (20-39 cigs/day) (finding)","Current-smoker","Heavy"),
    ("160606002","Very heavy cigarette smoker (40+ cigs/day) (finding)","Current-smoker","Heavy"),
    ("203191000000107","Wants to stop smoking (finding)","Current-smoker","Unknown"),
    ("308438006","Smoking restarted (finding)","Current-smoker","Unknown"),
    ("394871007","Thinking about stopping smoking (finding)","Current-smoker","Unknown"),
    ("394873005","Not interested in stopping smoking (finding)","Current-smoker","Unknown"),
    ("401201003","Cigarette pack-years (observable entity)","Current-smoker","Unknown"),
    ("413173009","Minutes from waking to first tobacco consumption (observable entity)","Current-smoker","Unknown"),
    ("428041000124106","Occasional tobacco smoker (finding)","Current-smoker","Light"),
    ("446172000","Failed attempt to stop smoking (finding)","Current-smoker","Unknown"),
    ("56578002","Moderate smoker (20 or less per day) (finding)","Current-smoker","Moderate"),
    ("56771006","Heavy smoker (over 20 per day) (finding)","Current-smoker","Heavy"),
    ("59978006","Cigar smoker (finding)","Current-smoker","Unknown"),
    ("77176002","Smoker (finding)","Current-smoker","Unknown"),
    ("82302008","Pipe smoker (finding)","Current-smoker","Unknown"),
    ("836001000000109","Waterpipe tobacco consumption (observable entity)","Current-smoker","Unknown"),
    ("53896009","Tolerant ex-smoker (finding)","Ex-smoker","Unknown"),
    ("1092041000000104","Ex-very heavy smoker (40+/day) (finding)","Ex-smoker","Unknown"),
    ("1092091000000109","Ex-moderate smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
    ("160620009","Ex-pipe smoker (finding)","Ex-smoker","Unknown"),
    ("160621008","Ex-cigar smoker (finding)","Ex-smoker","Unknown"),
    ("228486009","Time since stopped smoking (observable entity)","Ex-smoker","Unknown"),
    ("266921000","Ex-trivial cigarette smoker (<1/day) (finding)","Ex-smoker","Unknown"),
    ("266922007","Ex-light cigarette smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
    ("266923002","Ex-moderate cigarette smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
    ("266928006","Ex-cigarette smoker amount unknown (finding)","Ex-smoker","Unknown"),
    ("281018007","Ex-cigarette smoker (finding)","Ex-smoker","Unknown"),
    ("735128000","Ex-smoker for less than 1 year (finding)","Ex-smoker","Unknown"),
    ("8517006","Ex-smoker (finding)","Ex-smoker","Unknown"),
    ("1092031000000108","Ex-smoker amount unknown (finding)","Ex-smoker","Unknown"),
    ("1092071000000105","Ex-heavy smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
    ("1092111000000104","Ex-light smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
    ("1092131000000107","Ex-trivial smoker (<1/day) (finding)","Ex-smoker","Unknown"),
    ("160617001","Stopped smoking (finding)","Ex-smoker","Unknown"),
    ("160625004","Date ceased smoking (observable entity)","Ex-smoker","Unknown"),
    ("266924008","Ex-heavy cigarette smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
    ("266925009","Ex-very heavy cigarette smoker (40+/day) (finding)","Ex-smoker","Unknown"),
    ("360890004","Intolerant ex-smoker (finding)","Ex-smoker","Unknown"),
    ("360900008","Aggressive ex-smoker (finding)","Ex-smoker","Unknown"),
    ("48031000119106","Ex-smoker for more than 1 year (finding)","Ex-smoker","Unknown"),
    ("492191000000103","Ex roll-up cigarette smoker (finding)","Ex-smoker","Unknown"),
    ("53896009","Tolerant ex-smoker (finding)","Ex-smoker","Unknown"),
    ("735112005","Date ceased using moist tobacco (observable entity)","Ex-smoker","Unknown"),
    ("228486009","Time since stopped smoking (observable entity)","Ex-smoker","Unknown"),
    ("266921000","Ex-trivial cigarette smoker (<1/day) (finding)","Ex-smoker","Unknown"),
    ("266923002","Ex-moderate cigarette smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
    ("266928006","Ex-cigarette smoker amount unknown (finding)","Ex-smoker","Unknown"),
    ("360900008","Aggressive ex-smoker (finding)","Ex-smoker","Unknown"),
    ("492191000000103","Ex roll-up cigarette smoker (finding)","Ex-smoker","Unknown"),
    ("735112005","Date ceased using moist tobacco (observable entity)","Ex-smoker","Unknown"),
    ("735128000","Ex-smoker for less than 1 year (finding)","Ex-smoker","Unknown"),
    ("160617001","Stopped smoking (finding)","Ex-smoker","Unknown"),
    ("160620009","Ex-pipe smoker (finding)","Ex-smoker","Unknown"),
    ("160621008","Ex-cigar smoker (finding)","Ex-smoker","Unknown"),
    ("230058003","Pipe tobacco consumption (observable entity)","Ex-smoker","Unknown"),
    ("230065006","Chain smoker (finding)","Ex-smoker","Unknown"),
    ("266918002","Tobacco smoking consumption (observable entity)","Ex-smoker","Unknown"),
    ("266922007","Ex-light cigarette smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
    ("266924008","Ex-heavy cigarette smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
    ("266925009","Ex-very heavy cigarette smoker (40+/day) (finding)","Ex-smoker","Unknown"),
    ("281018007","Ex-cigarette smoker (finding)","Ex-smoker","Unknown"),
    ("360890004","Intolerant ex-smoker (finding)","Ex-smoker","Unknown"),
    ("48031000119106","Ex-smoker for more than 1 year (finding)","Ex-smoker","Unknown"),
    ("8517006","Ex-smoker (finding)","Ex-smoker","Unknown"),
    ("221000119102","Never smoked any substance (finding)","Never-smoker","NA"),
    ("266919005","Never smoked tobacco (finding)","Never-smoker","NA"),
    ("221000119102","Never smoked any substance (finding)","Never-smoker","NA"),
    ("266919005","Never smoked tobacco (finding)","Never-smoker","NA")
  ],
  ['code', 'term', 'smoking_status', 'severity']
)
codelist_smoking_status = tmp_smoking_status\
  .distinct()\
  .withColumn('name',\
    f.when(f.col('smoking_status') == 'Current-smoker', f.lit('smoking_current'))\
     .when(f.col('smoking_status') == 'Ex-smoker', f.lit('smoking_ex'))\
     .when(f.col('smoking_status') == 'Never-smoker', f.lit('smoking_never'))\
     .otherwise(f.col('smoking_status'))\
  )\
  .withColumn('terminology', f.lit('SNOMED'))\
  .withColumn('code_type', f.lit(''))\
  .withColumn('RecordDate', f.lit(''))\
  .select(['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate'])

# COMMAND ----------

# MAGIC %md ## 3.2 Comorbidities

# COMMAND ----------

# chronic kidney disease (CKD)
codelist_ckd = spark.createDataFrame(pd.read_csv('/Workspace/Users/ijw36@cam.ac.uk/CCU076/CCU076_01/Codelists/codelist_ckd.csv').fillna("")) 

# chronic obstructive pulmonary disease (COPD)
codelist_copd = spark.createDataFrame(
  pd.read_csv('/Workspace/Users/ijw36@cam.ac.uk/CCU076/CCU076_01/Codelists/codelist_copd.csv').fillna("")
  [['name', 'terminology', 'code', 'term']])

# Asthma
codelist_asthma = spark.createDataFrame(
  pd.read_csv('/Workspace/Users/ijw36@cam.ac.uk/CCU076/CCU076_01/Codelists/codelist_asthma.csv').fillna("")
  [['name', 'terminology', 'code', 'term']])

# depression
codelist_depression = bhf_phenotypes\
  .where(f.col('name') == 'depression')
# exclude remission codes
codelist_depression = (
  codelist_depression.filter(~f.col("code").isin("19527009", "698957003"))
)


# cancer
# 20220616 ER advised to remove code 417036008 "Liquid based cervical cytology screening (procedure)"
codelist_cancer = bhf_phenotypes\
  .where(f.col('name') == 'cancer')\
  .where(f.col('code') != '417036008')

# diabetes
codelist_diabetes = spark.createDataFrame(pd.read_csv('/Workspace/Users/ijw36@cam.ac.uk/CCU076/CCU076_01/Codelists/codelist_diabetes.csv').fillna(""))
 
# codelist_diabetes = codelist_diabetes\
#  .unionByName(tmp_diabetes)

# hypertension
codelist_hypertension = bhf_phenotypes\
  .where(f.col('name').rlike('^hypertension.*$'))
tmp_hypertension = spark.createDataFrame(
  [
    ('hypertension', 'ICD10', 'I10', 'Essential (primary) hypertension','1','20210127'),
    ('hypertension', 'ICD10', 'I11', 'Hypertensive heart disease','1','20210127'),
    ('hypertension', 'ICD10', 'I12', 'Hypertensive renal disease','1','20210127'),
    ('hypertension', 'ICD10', 'I13', 'Hypertensive heart and renal disease','1','20210127'),
    ('hypertension', 'ICD10', 'I15', 'Secondary hypertension','1','20210127')    
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)
codelist_hypertension = codelist_hypertension\
  .unionByName(tmp_hypertension)

# Heart disease
codelist_heart_disease = spark.createDataFrame(pd.read_csv('/Workspace/Users/ijw36@cam.ac.uk/CCU076/CCU076_01/Codelists/codelist_heart_disease.csv').fillna(""))

# Stroke
codelist_stroke = spark.createDataFrame(pd.read_csv('/Workspace/Users/ijw36@cam.ac.uk/CCU076/CCU076_01/Codelists/codelist_stroke.csv').fillna(""))

# COMMAND ----------

# liver_disease

# get mapping file for CTV3 to SNOMED
map_ctv3_snomed_assured = map_ctv3_snomed\
  .where(f.col('IS_ASSURED') == 1)\
  .select(['CTV3_CONCEPTID', 'SCT_CONCEPTID'])\
  .withColumnRenamed('CTV3_CONCEPTID', 'code')\
  .dropDuplicates()
# display(map_ctv3_snomed_assured)

# get liver disease CTV3 codelist
liver_ctv3 = bhf_phenotypes\
  .where((f.col('name') == 'liver_disease') & (f.col('terminology') == 'CTV3'))\
  .select(['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate'])
# display(liver_ctv3)

# map CTV3 to SNOMED
liver_ctv3_snomed = merge(liver_ctv3, map_ctv3_snomed_assured, ['code'])

# check
# display(liver_ctv3_snomed.where(f.col('_merge') == 'left_only').drop('_merge'))
# display(liver_ctv3_snomed.where(f.col('_merge') == 'both').drop(['_merge']))

# reformat
liver_ctv3_snomed = liver_ctv3_snomed\
  .where(f.col('_merge') == 'both')\
  .drop('_merge', 'code')\
  .withColumnRenamed('SCT_CONCEPTID', 'code')\
  .withColumn('terminology', f.lit('SNOMED'))\
  .select(['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate'])\
  .dropDuplicates(['code'])
# display(liver_ctv3_snomed)

# get liver disease CTV3 codelist
liver_icd10 = bhf_phenotypes\
  .where((f.col('name') == 'liver_disease') & (f.col('terminology') == 'ICD10'))\
  .select(['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate'])
# display(liver_icd10)

# combine
codelist_liver_disease = liver_ctv3_snomed.unionByName(liver_icd10)

# COMMAND ----------

# thrombophilia
"""codelist_thrombophilia = spark.createDataFrame(
  [
    ("thrombophilia","ICD10","D68.5","Primary thrombophilia","1","20210127"),
    ("thrombophilia","ICD10","D68.6","Other thrombophilia","1","20210127"),
    ("thrombophilia","SNOMED","439001009","Acquired thrombophilia","1","20210127"),
    ("thrombophilia","SNOMED","441882000","History of thrombophilia","1","20210127"),
    ("thrombophilia","SNOMED","439698008","Primary thrombophilia","1","20210127"),
    ("thrombophilia","SNOMED","234467004","Thrombophilia","1","20210127"),
    ("thrombophilia","SNOMED","441697004","Thrombophilia associated with pregnancy","1","20210127"),
    ("thrombophilia","SNOMED","442760001","Thrombophilia caused by antineoplastic agent therapy","1","20210127"),
    ("thrombophilia","SNOMED","442197003","Thrombophilia caused by drug therapy","1","20210127"),
    ("thrombophilia","SNOMED","442654007","Thrombophilia caused by hormone therapy","1","20210127"),
    ("thrombophilia","SNOMED","442363001","Thrombophilia caused by vascular device","1","20210127"),
    ("thrombophilia","SNOMED","439126002","Thrombophilia due to acquired antithrombin III deficiency","1","20210127"),
    ("thrombophilia","SNOMED","439002002","Thrombophilia due to acquired protein C deficiency","1","20210127"),
    ("thrombophilia","SNOMED","439125003","Thrombophilia due to acquired protein S deficiency","1","20210127"),
    ("thrombophilia","SNOMED","441079006","Thrombophilia due to antiphospholipid antibody","1","20210127"),
    ("thrombophilia","SNOMED","441762006","Thrombophilia due to immobilisation","1","20210127"),
    ("thrombophilia","SNOMED","442078001","Thrombophilia due to malignant neoplasm","1","20210127"),
    ("thrombophilia","SNOMED","441946009","Thrombophilia due to myeloproliferative disorder","1","20210127"),
    ("thrombophilia","SNOMED","441990004","Thrombophilia due to paroxysmal nocturnal haemoglobinuria","1","20210127"),
    ("thrombophilia","SNOMED","441945008","Thrombophilia due to trauma","1","20210127"),
    ("thrombophilia","SNOMED","442121006","Thrombophilia due to vascular anomaly","1","20210127"),    
    ("thrombophilia","SNOMED","783250007","Hereditary thrombophilia due to congenital histidine-rich (poly-L) glycoprotein deficiency","1",	"20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

# thrombocytopenia
codelist_thrombocytopenia = spark.createDataFrame(
  [
    ("thrombocytopenia","ICD10","D69.3","Thrombocytopenia","1","20210127"),
    ("thrombocytopenia","ICD10","D69.4","Thrombocytopenia","1","20210127"),
    ("thrombocytopenia","ICD10","D69.5","Thrombocytopenia","1","20210127"),
    ("thrombocytopenia","ICD10","D69.6","Thrombocytopenia","1","20210127"),
    ("thrombocytopenia","SNOMED","74576004","Acquired thrombocytopenia","1","20210127"),
    ("thrombocytopenia","SNOMED","439007008","Acquired thrombotic thrombocytopenic purpura","1","20210127"),
    ("thrombocytopenia","SNOMED","28505005","Acute idiopathic thrombocytopenic purpura","1","20210127"),
    ("thrombocytopenia","SNOMED","128091003","Autoimmune thrombocytopenia","1","20210127"),
    ("thrombocytopenia","SNOMED","13172003","Autoimmune thrombocytopenic purpura","1","20210127"),
    ("thrombocytopenia","SNOMED","438476003","Autoimmune thrombotic thrombocytopenic purpura","1","20210127"),
    ("thrombocytopenia","SNOMED","111588002","Heparin associated thrombotic thrombocytopenia","1","20210127"),
    ("thrombocytopenia","SNOMED","73397007","Heparin induced thrombocytopaenia","1","20210127"),
    ("thrombocytopenia","SNOMED","438492008","Hereditary thrombocytopenic disorder","1","20210127"),
    ("thrombocytopenia","SNOMED","441511006","History of immune thrombocytopenia","1","20210127"),
    ("thrombocytopenia","SNOMED","49341000119108","History of thrombocytopaenia",	"1","20210127"),
    ("thrombocytopenia","SNOMED","726769004","HIT (Heparin induced thrombocytopenia) antibody","1","20210127"),
    ("thrombocytopenia","SNOMED","371106008","Idiopathic maternal thrombocytopenia","1","20210127"),
    ("thrombocytopenia","SNOMED","32273002","Idiopathic thrombocytopenic purpura","1","20210127"),
    ("thrombocytopenia","SNOMED","2897005","Immune thrombocytopenia","1","20210127"),
    ("thrombocytopenia","SNOMED","36070007","Immunodeficiency with thrombocytopenia AND eczema","1","20210127"),
    ("thrombocytopenia","SNOMED","33183004","Post infectious thrombocytopenic purpura","1","20210127"),
    ("thrombocytopenia","SNOMED","267534000","Primary thrombocytopenia","1","20210127"),
    ("thrombocytopenia","SNOMED","154826009","Secondary thrombocytopenia","1","20210127"),
    ("thrombocytopenia","SNOMED","866152006","Thrombocytopenia due to 2019 novel coronavirus","1","20210127"),
    ("thrombocytopenia","SNOMED","82190001","Thrombocytopenia due to defective platelet production","1","20210127"),
    ("thrombocytopenia","SNOMED","78345002","Thrombocytopenia due to diminished platelet production","1","20210127"),
    ("thrombocytopenia","SNOMED","191323001","Thrombocytopenia due to extracorporeal circulation of blood","1","20210127"),
    ("thrombocytopenia","SNOMED","87902006","Thrombocytopenia due to non-immune destruction","1","20210127"),
    ("thrombocytopenia","SNOMED","302873008","Thrombocytopenic purpura","1","20210127"),
    ("thrombocytopenia","SNOMED","417626001","Thrombocytopenic purpura associated with metabolic disorder","1","20210127"),
    ("thrombocytopenia","SNOMED","402653004","Thrombocytopenic purpura due to defective platelet production","1","20210127"),
    ("thrombocytopenia","SNOMED","402654005","Thrombocytopenic purpura due to platelet consumption","1","20210127"),
    ("thrombocytopenia","SNOMED","78129009","Thrombotic thrombocytopenic purpura","1","20210127"),
    ("thrombocytopenia","SNOMED","441322009","Drug induced thrombotic thrombocytopenic purpura","1","20210127"),
    ("thrombocytopenia","SNOMED","19307009","Drug-induced immune thrombocytopenia","1","20210127"),
    ("thrombocytopenia","SNOMED","783251006","Hereditary thrombocytopenia with normal platelets","1","20210127"),
    ("thrombocytopenia","SNOMED","191322006","Thrombocytopenia caused by drugs","1","20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

# thrombotic thrombocytopenic purpura (TTP)
codelist_TTP = spark.createDataFrame(
  [
    ("TTP","ICD10","M31.1","Thrombotic microangiopathy","1","20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

# disseminated intravascular coagulation (DIC)
codelist_DIC = spark.createDataFrame(
  [
    ("DIC","ICD10","D65","Disseminated intravascular coagulation","1","20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)
 """

# COMMAND ----------

# MAGIC %md # 4 Combine

# COMMAND ----------

filtered_keys = [clist for clist in globals().keys() if (bool(re.match('^codelist_', clist))) & (bool(re.match('^codelist_match.*', clist)) == False)& (bool(re.match('^codelist_nonmatch.*', clist)) == False)]
print("Filtered keys:", filtered_keys)

# COMMAND ----------

# append (union) codelists defined above
# harmonise columns before appending
clistm = []
for indx, clist in enumerate(filtered_keys):
  print(f'{0 if indx<10 else ""}' + str(indx) + ' ' + clist)
  tmp = globals()[clist]
  print(f'{clist} type: {type(tmp)}')
  if(indx == 0):
    clistm = tmp
  else:
    # pre unionByName
    for col in [col for col in tmp.columns if col not in clistm.columns]:
      print('  M - adding column: ' + col)
      clistm = clistm.withColumn(col, f.lit(None))
    for col in [col for col in clistm.columns if col not in tmp.columns]:
      print('  C - adding column: ' + col)
      tmp = tmp.withColumn(col, f.lit(None))
    clistm = clistm.unionByName(tmp)
  
clistm = clistm\
  .orderBy('name', 'terminology', 'code')

# COMMAND ----------

# MAGIC %md # 5 Reformat

# COMMAND ----------

clistm = (
    clistm
    .withColumn("inclusion", f.coalesce(
        f.col("inclusion"),
        f.when(f.col("code_type") == "", None).otherwise(f.col("code_type"))))
    .withColumn("inclusion", f.when(f.col("inclusion").isNull(), 1).otherwise(f.col("inclusion")))
    .withColumn("covariate_only", f.when(f.col("covariate_only").isNull(), 0).otherwise(f.col("covariate_only")))
    .filter(f.col("inclusion") == 1) 
    .drop("code_type", "RecordDate", "DiabetesType", "subtype")
)


# COMMAND ----------

display(clistm)

# COMMAND ----------

# check
display(clistm.where((f.col('terminology') == 'ICD10') & (f.col('code').rlike('X$'))))

# COMMAND ----------

# check
display(clistm.where((f.col('terminology') == 'OPCS4') & (f.col('code').rlike('X$'))))

# COMMAND ----------

display(clistm.where((f.col('terminology') == 'ICD10') & (f.col('code').rlike('[\.\-\s]'))))

# COMMAND ----------

# remove trailing X's, decimal points, dashes, and spaces
clistmr = clistm\
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'X$', '')).otherwise(f.col('code')))\
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'[\.\-\s]', '')).otherwise(f.col('code')))\
  .withColumn('code', f.when(f.col('terminology') == 'OPCS4', f.regexp_replace('code', r'X$', '')).otherwise(f.col('code')))\
  .withColumn('code', f.when(f.col('terminology') == 'OPCS4', f.regexp_replace('code', r'[\.\-\s]', '')).otherwise(f.col('code')))


# COMMAND ----------

# MAGIC %md # 6 Check

# COMMAND ----------

# check
tmpt = tab(clistmr, 'name', 'terminology', var2_unstyled=1)

# COMMAND ----------

# check
display(clistmr)

# COMMAND ----------

# MAGIC %md # 7 Save

# COMMAND ----------

save_table(df=clistmr, out_name=f'{proj}_out_codelist_covariates', save_previous=False)