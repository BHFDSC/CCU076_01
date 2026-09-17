# Databricks notebook source
# MAGIC %md # CCU076_01-D02d-codelist_quality_assurance
# MAGIC
# MAGIC **Project** CCU076_01
# MAGIC
# MAGIC **Description** This notebook creates the codelist for the quality assurance, which includes codelists for pregnancy, prostate cancer, hormone replacement therapy (HRT) and the combined oral contraceptive pill (COCP).
# MAGIC
# MAGIC **Authors** Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Reviewers** ⚠ Alexia Sampri
# MAGIC
# MAGIC **Acknowledgements** Based on CCU002_07 and subsequently CCU003_05-D02a-codelist_quality_assurance
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`CCU076_01_out_codelist_quality_assurance`** : The SNOMED/BNF codes that will be used for quality assurance checks in the later quality assurance notebook. This includes pregnancy/birth/HRT/COCP codes for male sex checks and prostate cancer codes for female sex checks.

# COMMAND ----------

# MAGIC %md # 0. Setup

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

# DBTITLE 1,Common Functions
#%run "../../shds/common/functions"

# COMMAND ----------

# MAGIC %run "/Workspace/Shared/CCU076_01/Spencers_common_functions"

# COMMAND ----------

# MAGIC %md # 1. Parameters

# COMMAND ----------

# MAGIC %run "./CCU076_01-D01-parameters"

# COMMAND ----------

# widgets
dbutils.widgets.removeAll()
dbutils.widgets.text('1 project', proj)
dbutils.widgets.text('2 cohort', cohort)
dbutils.widgets.text('3 pipeline production date', pipeline_production_date)

# COMMAND ----------

# MAGIC %md # 2. Data

# COMMAND ----------

# HES APC
#hes_apc_archive = spark.table(f'{dsa}.hes_apc_all_years_archive')

# COMMAND ----------

# MAGIC %md # 3. Codelists

# COMMAND ----------

# MAGIC %md ## 3.1 Prostate cancer codelist

# COMMAND ----------

# DBTITLE 1,Declare Prostate Cancer SNOMED codes
# prostate_cancer (SNOMED codes only)
codelist_prostate_cancer = spark.createDataFrame(
  [
    ("prostate_cancer","SNOMED","126906006","Neoplasm of prostate","",""),
    ("prostate_cancer","SNOMED","81232004","Radical cystoprostatectomy","",""),
    ("prostate_cancer","SNOMED","176106009","Radical cystoprostatourethrectomy","",""),
    ("prostate_cancer","SNOMED","176261008","Radical prostatectomy without pelvic node excision","",""),
    ("prostate_cancer","SNOMED","176262001","Radical prostatectomy with pelvic node sampling","",""),
    ("prostate_cancer","SNOMED","176263006","Radical prostatectomy with pelvic lymphadenectomy","",""),
    ("prostate_cancer","SNOMED","369775001","Gleason Score 2-4: Well differentiated","",""),
    ("prostate_cancer","SNOMED","369777009","Gleason Score 8-10: Poorly differentiated","",""),
    ("prostate_cancer","SNOMED","385377005","Gleason grade finding for prostatic cancer (finding)","",""),
    ("prostate_cancer","SNOMED","394932008","Gleason prostate grade 5-7 (medium) (finding)","",""),
    ("prostate_cancer","SNOMED","399068003","Malignant tumor of prostate (disorder)","",""),
    ("prostate_cancer","SNOMED","428262008","History of malignant neoplasm of prostate (situation)","","")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

display(codelist_prostate_cancer)

# COMMAND ----------

# MAGIC %md ## 3.2 Pregnancy & birth codelist

# COMMAND ----------

# DBTITLE 1,Declare Pregnancy/Birth SNOMED codes
# pregnancy (SNOMED codes only)
# v1
tmp_pregnancy_v1 = spark.createDataFrame(
  [
    ("171057006","Pregnancy***REMOVED***alcohol***REMOVED***education***REMOVED***(procedure)"),
    ("72301000119103","Asthma***REMOVED***in***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("10742121000119104","Asthma***REMOVED***in***REMOVED***mother***REMOVED***complicating***REMOVED***childbirth***REMOVED***(disorder)"),
    ("10745291000119103","Malignant***REMOVED***neoplastic***REMOVED***disease***REMOVED***in***REMOVED***mother***REMOVED***complicating***REMOVED***childbirth***REMOVED***(disorder)"),
    ("10749871000119100","Malignant***REMOVED***neoplastic***REMOVED***disease***REMOVED***in***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("20753005","Hypertensive***REMOVED***heart***REMOVED***disease***REMOVED***complicating***REMOVED***AND/OR***REMOVED***reason***REMOVED***for***REMOVED***care***REMOVED***during***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("237227006","Congenital***REMOVED***heart***REMOVED***disease***REMOVED***in***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("169501005","Pregnant,***REMOVED***diaphragm***REMOVED***failure***REMOVED***(finding)"),
    ("169560008","Pregnant***REMOVED***-***REMOVED***urine***REMOVED***test***REMOVED***confirms***REMOVED***(finding)"),
    ("169561007","Pregnant***REMOVED***-***REMOVED***blood***REMOVED***test***REMOVED***confirms***REMOVED***(finding)"),
    ("169562000","Pregnant***REMOVED***-***REMOVED***vaginal***REMOVED***examination***REMOVED***confirms***REMOVED***(finding)"),
    ("169565003","Pregnant***REMOVED***-***REMOVED***planned***REMOVED***(finding)"),
    ("169566002","Pregnant***REMOVED***-***REMOVED***unplanned***REMOVED***-***REMOVED***wanted***REMOVED***(finding)"),
    ("413567003","Aplastic***REMOVED***anemia***REMOVED***associated***REMOVED***with***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("91948008","Asymptomatic***REMOVED***human***REMOVED***immunodeficiency***REMOVED***virus***REMOVED***infection***REMOVED***in***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("169488004","Contraceptive***REMOVED***intrauterine***REMOVED***device***REMOVED***failure***REMOVED***-***REMOVED***pregnant***REMOVED***(finding)"),
    ("169508004","Pregnant,***REMOVED***sheath***REMOVED***failure***REMOVED***(finding)"),
    ("169564004","Pregnant***REMOVED***-***REMOVED***on***REMOVED***abdominal***REMOVED***palpation***REMOVED***(finding)"),
    ("77386006","Pregnant***REMOVED***(finding)"),
    ("10746341000119109","Acquired***REMOVED***immune***REMOVED***deficiency***REMOVED***syndrome***REMOVED***complicating***REMOVED***childbirth***REMOVED***(disorder)"),
    ("10759351000119103","Sickle***REMOVED***cell***REMOVED***anemia***REMOVED***in***REMOVED***mother***REMOVED***complicating***REMOVED***childbirth***REMOVED***(disorder)"),
    ("10757401000119104","Pre-existing***REMOVED***hypertensive***REMOVED***heart***REMOVED***and***REMOVED***chronic***REMOVED***kidney***REMOVED***disease***REMOVED***in***REMOVED***mother***REMOVED***complicating***REMOVED***childbirth***REMOVED***(disorder)"),
    ("10757481000119107","Pre-existing***REMOVED***hypertensive***REMOVED***heart***REMOVED***and***REMOVED***chronic***REMOVED***kidney***REMOVED***disease***REMOVED***in***REMOVED***mother***REMOVED***complicating***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("10757441000119102","Pre-existing***REMOVED***hypertensive***REMOVED***heart***REMOVED***disease***REMOVED***in***REMOVED***mother***REMOVED***complicating***REMOVED***childbirth***REMOVED***(disorder)"),
    ("10759031000119106","Pre-existing***REMOVED***hypertensive***REMOVED***heart***REMOVED***disease***REMOVED***in***REMOVED***mother***REMOVED***complicating***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("1474004","Hypertensive***REMOVED***heart***REMOVED***AND***REMOVED***renal***REMOVED***disease***REMOVED***complicating***REMOVED***AND/OR***REMOVED***reason***REMOVED***for***REMOVED***care***REMOVED***during***REMOVED***childbirth***REMOVED***(disorder)"),
    ("199006004","Pre-existing***REMOVED***hypertensive***REMOVED***heart***REMOVED***disease***REMOVED***complicating***REMOVED***pregnancy,***REMOVED***childbirth***REMOVED***and***REMOVED***the***REMOVED***puerperium***REMOVED***(disorder)"),
    ("199007008","Pre-existing***REMOVED***hypertensive***REMOVED***heart***REMOVED***and***REMOVED***renal***REMOVED***disease***REMOVED***complicating***REMOVED***pregnancy,***REMOVED***childbirth***REMOVED***and***REMOVED***the***REMOVED***puerperium***REMOVED***(disorder)"),
    ("22966008","Hypertensive***REMOVED***heart***REMOVED***AND***REMOVED***renal***REMOVED***disease***REMOVED***complicating***REMOVED***AND/OR***REMOVED***reason***REMOVED***for***REMOVED***care***REMOVED***during***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("59733002","Hypertensive***REMOVED***heart***REMOVED***disease***REMOVED***complicating***REMOVED***AND/OR***REMOVED***reason***REMOVED***for***REMOVED***care***REMOVED***during***REMOVED***childbirth***REMOVED***(disorder)"),
    ("171054004","Pregnancy***REMOVED***diet***REMOVED***education***REMOVED***(procedure)"),
    ("106281000119103","Pre-existing***REMOVED***diabetes***REMOVED***mellitus***REMOVED***in***REMOVED***mother***REMOVED***complicating***REMOVED***childbirth***REMOVED***(disorder)"),
    ("10754881000119104","Diabetes***REMOVED***mellitus***REMOVED***in***REMOVED***mother***REMOVED***complicating***REMOVED***childbirth***REMOVED***(disorder)"),
    ("199225007","Diabetes***REMOVED***mellitus***REMOVED***during***REMOVED***pregnancy***REMOVED***-***REMOVED***baby***REMOVED***delivered***REMOVED***(disorder)"),
    ("237627000","Pregnancy***REMOVED***and***REMOVED***type***REMOVED***2***REMOVED***diabetes***REMOVED***mellitus***REMOVED***(disorder)"),
    ("609563008","Pre-existing***REMOVED***diabetes***REMOVED***mellitus***REMOVED***in***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("609566000","Pregnancy***REMOVED***and***REMOVED***type***REMOVED***1***REMOVED***diabetes***REMOVED***mellitus***REMOVED***(disorder)"),
    ("609567009","Pre-existing***REMOVED***type***REMOVED***2***REMOVED***diabetes***REMOVED***mellitus***REMOVED***in***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("199223000","Diabetes***REMOVED***mellitus***REMOVED***during***REMOVED***pregnancy,***REMOVED***childbirth***REMOVED***and***REMOVED***the***REMOVED***puerperium***REMOVED***(disorder)"),
    ("199227004","Diabetes***REMOVED***mellitus***REMOVED***during***REMOVED***pregnancy***REMOVED***-***REMOVED***baby***REMOVED***not***REMOVED***yet***REMOVED***delivered***REMOVED***(disorder)"),
    ("609564002","Pre-existing***REMOVED***type***REMOVED***1***REMOVED***diabetes***REMOVED***mellitus***REMOVED***in***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("76751001","Diabetes***REMOVED***mellitus***REMOVED***in***REMOVED***mother***REMOVED***complicating***REMOVED***pregnancy,***REMOVED***childbirth***REMOVED***AND/OR***REMOVED***puerperium***REMOVED***(disorder)"),
    ("526961000000105","Pregnancy***REMOVED***advice***REMOVED***for***REMOVED***patients***REMOVED***with***REMOVED***epilepsy***REMOVED***(procedure)"),
    ("527041000000108","Pregnancy***REMOVED***advice***REMOVED***for***REMOVED***patients***REMOVED***with***REMOVED***epilepsy***REMOVED***not***REMOVED***indicated***REMOVED***(situation)"),
    ("527131000000100","Pregnancy***REMOVED***advice***REMOVED***for***REMOVED***patients***REMOVED***with***REMOVED***epilepsy***REMOVED***declined***REMOVED***(situation)"),
    ("10753491000119101","Gestational***REMOVED***diabetes***REMOVED***mellitus***REMOVED***in***REMOVED***childbirth***REMOVED***(disorder)"),
    ("40801000119106","Gestational***REMOVED***diabetes***REMOVED***mellitus***REMOVED***complicating***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("10562009","Malignant***REMOVED***hypertension***REMOVED***complicating***REMOVED***AND/OR***REMOVED***reason***REMOVED***for***REMOVED***care***REMOVED***during***REMOVED***childbirth***REMOVED***(disorder)"),
    ("198944004","Benign***REMOVED***essential***REMOVED***hypertension***REMOVED***complicating***REMOVED***pregnancy,***REMOVED***childbirth***REMOVED***and***REMOVED***the***REMOVED***puerperium***REMOVED***-***REMOVED***delivered***REMOVED***(disorder)"),
    ("198945003","Benign***REMOVED***essential***REMOVED***hypertension***REMOVED***complicating***REMOVED***pregnancy,***REMOVED***childbirth***REMOVED***and***REMOVED***the***REMOVED***puerperium***REMOVED***-***REMOVED***delivered***REMOVED***with***REMOVED***postnatal***REMOVED***complication***REMOVED***(disorder)"),
    ("198946002","Benign***REMOVED***essential***REMOVED***hypertension***REMOVED***complicating***REMOVED***pregnancy,***REMOVED***childbirth***REMOVED***and***REMOVED***the***REMOVED***puerperium***REMOVED***-***REMOVED***not***REMOVED***delivered***REMOVED***(disorder)"),
    ("198949009","Renal***REMOVED***hypertension***REMOVED***complicating***REMOVED***pregnancy,***REMOVED***childbirth***REMOVED***and***REMOVED***the***REMOVED***puerperium***REMOVED***(disorder)"),
    ("198951008","Renal***REMOVED***hypertension***REMOVED***complicating***REMOVED***pregnancy,***REMOVED***childbirth***REMOVED***and***REMOVED***the***REMOVED***puerperium***REMOVED***-***REMOVED***delivered***REMOVED***(disorder)"),
    ("198954000","Renal***REMOVED***hypertension***REMOVED***complicating***REMOVED***pregnancy,***REMOVED***childbirth***REMOVED***and***REMOVED***the***REMOVED***puerperium***REMOVED***with***REMOVED***postnatal***REMOVED***complication***REMOVED***(disorder)"),
    ("199005000","Pre-existing***REMOVED***hypertension***REMOVED***complicating***REMOVED***pregnancy,***REMOVED***childbirth***REMOVED***and***REMOVED***puerperium***REMOVED***(disorder)"),
    ("23717007","Benign***REMOVED***essential***REMOVED***hypertension***REMOVED***complicating***REMOVED***AND/OR***REMOVED***reason***REMOVED***for***REMOVED***care***REMOVED***during***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("26078007","Hypertension***REMOVED***secondary***REMOVED***to***REMOVED***renal***REMOVED***disease***REMOVED***complicating***REMOVED***AND/OR***REMOVED***reason***REMOVED***for***REMOVED***care***REMOVED***during***REMOVED***childbirth***REMOVED***(disorder)"),
    ("29259002","Malignant***REMOVED***hypertension***REMOVED***complicating***REMOVED***AND/OR***REMOVED***reason***REMOVED***for***REMOVED***care***REMOVED***during***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("65402008","Pre-existing***REMOVED***hypertension***REMOVED***complicating***REMOVED***AND/OR***REMOVED***reason***REMOVED***for***REMOVED***care***REMOVED***during***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("8218002","Chronic***REMOVED***hypertension***REMOVED***complicating***REMOVED***AND/OR***REMOVED***reason***REMOVED***for***REMOVED***care***REMOVED***during***REMOVED***childbirth***REMOVED***(disorder)"),
    ("10752641000119102","Eclampsia***REMOVED***with***REMOVED***pre-existing***REMOVED***hypertension***REMOVED***in***REMOVED***childbirth***REMOVED***(disorder)"),
    ("118781000119108","Pre-existing***REMOVED***hypertensive***REMOVED***chronic***REMOVED***kidney***REMOVED***disease***REMOVED***in***REMOVED***mother***REMOVED***complicating***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("18416000","Essential***REMOVED***hypertension***REMOVED***complicating***REMOVED***AND/OR***REMOVED***reason***REMOVED***for***REMOVED***care***REMOVED***during***REMOVED***childbirth***REMOVED***(disorder)"),
    ("198942000","Benign***REMOVED***essential***REMOVED***hypertension***REMOVED***complicating***REMOVED***pregnancy,***REMOVED***childbirth***REMOVED***and***REMOVED***the***REMOVED***puerperium***REMOVED***(disorder)"),
    ("198947006","Benign***REMOVED***essential***REMOVED***hypertension***REMOVED***complicating***REMOVED***pregnancy,***REMOVED***childbirth***REMOVED***and***REMOVED***the***REMOVED***puerperium***REMOVED***with***REMOVED***postnatal***REMOVED***complication***REMOVED***(disorder)"),
    ("198952001","Renal***REMOVED***hypertension***REMOVED***complicating***REMOVED***pregnancy,***REMOVED***childbirth***REMOVED***and***REMOVED***the***REMOVED***puerperium***REMOVED***-***REMOVED***delivered***REMOVED***with***REMOVED***postnatal***REMOVED***complication***REMOVED***(disorder)"),
    ("198953006","Renal***REMOVED***hypertension***REMOVED***complicating***REMOVED***pregnancy,***REMOVED***childbirth***REMOVED***and***REMOVED***the***REMOVED***puerperium***REMOVED***-***REMOVED***not***REMOVED***delivered***REMOVED***(disorder)"),
    ("199008003","Pre-existing***REMOVED***secondary***REMOVED***hypertension***REMOVED***complicating***REMOVED***pregnancy,***REMOVED***childbirth***REMOVED***and***REMOVED***puerperium***REMOVED***(disorder)"),
    ("34694006","Pre-existing***REMOVED***hypertension***REMOVED***complicating***REMOVED***AND/OR***REMOVED***reason***REMOVED***for***REMOVED***care***REMOVED***during***REMOVED***childbirth***REMOVED***(disorder)"),
    ("37618003","Chronic***REMOVED***hypertension***REMOVED***complicating***REMOVED***AND/OR***REMOVED***reason***REMOVED***for***REMOVED***care***REMOVED***during***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("48552006","Hypertension***REMOVED***secondary***REMOVED***to***REMOVED***renal***REMOVED***disease***REMOVED***complicating***REMOVED***AND/OR***REMOVED***reason***REMOVED***for***REMOVED***care***REMOVED***during***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("71874008","Benign***REMOVED***essential***REMOVED***hypertension***REMOVED***complicating***REMOVED***AND/OR***REMOVED***reason***REMOVED***for***REMOVED***care***REMOVED***during***REMOVED***childbirth***REMOVED***(disorder)"),
    ("78808002","Essential***REMOVED***hypertension***REMOVED***complicating***REMOVED***AND/OR***REMOVED***reason***REMOVED***for***REMOVED***care***REMOVED***during***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("91923005","Acquired***REMOVED***immunodeficiency***REMOVED***syndrome***REMOVED***virus***REMOVED***infection***REMOVED***associated***REMOVED***with***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("10755671000119100","Human***REMOVED***immunodeficiency***REMOVED***virus***REMOVED***in***REMOVED***mother***REMOVED***complicating***REMOVED***childbirth***REMOVED***(disorder)"),
    ("721166000","Human***REMOVED***immunodeficiency***REMOVED***virus***REMOVED***complicating***REMOVED***pregnancy***REMOVED***childbirth***REMOVED***and***REMOVED***the***REMOVED***puerperium***REMOVED***(disorder)"),
    ("449369001","Stopped***REMOVED***smoking***REMOVED***before***REMOVED***pregnancy***REMOVED***(finding)"),
    ("449345000","Smoked***REMOVED***before***REMOVED***confirmation***REMOVED***of***REMOVED***pregnancy***REMOVED***(finding)"),
    ("449368009","Stopped***REMOVED***smoking***REMOVED***during***REMOVED***pregnancy***REMOVED***(finding)"),
    ("88144003","Removal***REMOVED***of***REMOVED***ectopic***REMOVED***interstitial***REMOVED***uterine***REMOVED***pregnancy***REMOVED***requiring***REMOVED***total***REMOVED***hysterectomy***REMOVED***(procedure)"),
    ("240154002","Idiopathic***REMOVED***osteoporosis***REMOVED***in***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("956951000000104","Pertussis***REMOVED***vaccination***REMOVED***in***REMOVED***pregnancy***REMOVED***(procedure)"),
    ("866641000000105","Pertussis***REMOVED***vaccination***REMOVED***in***REMOVED***pregnancy***REMOVED***declined***REMOVED***(situation)"),
    ("956971000000108","Pertussis***REMOVED***vaccination***REMOVED***in***REMOVED***pregnancy***REMOVED***given***REMOVED***by***REMOVED***other***REMOVED***healthcare***REMOVED***provider***REMOVED***(finding)"),
    ("169563005","Pregnant***REMOVED***-***REMOVED***on***REMOVED***history***REMOVED***(finding)"),
    ("10231000132102","In-vitro***REMOVED***fertilization***REMOVED***pregnancy***REMOVED***(finding)"),
    ("134781000119106","High***REMOVED***risk***REMOVED***pregnancy***REMOVED***due***REMOVED***to***REMOVED***recurrent***REMOVED***miscarriage***REMOVED***(finding)"),
    ("16356006","Multiple***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("237239003","Low***REMOVED***risk***REMOVED***pregnancy***REMOVED***(finding)"),
    ("276367008","Wanted***REMOVED***pregnancy***REMOVED***(finding)"),
    ("314204000","Early***REMOVED***stage***REMOVED***of***REMOVED***pregnancy***REMOVED***(finding)"),
    ("439311009","Intends***REMOVED***to***REMOVED***continue***REMOVED***pregnancy***REMOVED***(finding)"),
    ("713575004","Dizygotic***REMOVED***twin***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("80997009","Quintuplet***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("1109951000000101","Pregnancy***REMOVED***insufficiently***REMOVED***advanced***REMOVED***for***REMOVED***reliable***REMOVED***antenatal***REMOVED***screening***REMOVED***(finding)"),
    ("1109971000000105","Pregnancy***REMOVED***too***REMOVED***advanced***REMOVED***for***REMOVED***reliable***REMOVED***antenatal***REMOVED***screening***REMOVED***(finding)"),
    ("237238006","Pregnancy***REMOVED***with***REMOVED***uncertain***REMOVED***dates***REMOVED***(finding)"),
    ("444661007","High***REMOVED***risk***REMOVED***pregnancy***REMOVED***due***REMOVED***to***REMOVED***history***REMOVED***of***REMOVED***preterm***REMOVED***labor***REMOVED***(finding)"),
    ("459166009","Dichorionic***REMOVED***diamniotic***REMOVED***twin***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("459167000","Monochorionic***REMOVED***twin***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("459168005","Monochorionic***REMOVED***diamniotic***REMOVED***twin***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("459171002","Monochorionic***REMOVED***monoamniotic***REMOVED***twin***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("47200007","High***REMOVED***risk***REMOVED***pregnancy***REMOVED***(finding)"),
    ("60810003","Quadruplet***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("64254006","Triplet***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("65147003","Twin***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("713576003","Monozygotic***REMOVED***twin***REMOVED***pregnancy***REMOVED***(disorder)"),
    ("171055003","Pregnancy***REMOVED***smoking***REMOVED***education***REMOVED***(procedure)"),
    ("10809101000119109","Hypothyroidism***REMOVED***in***REMOVED***childbirth***REMOVED***(disorder)"),
    ("428165003","Hypothyroidism***REMOVED***in***REMOVED***pregnancy***REMOVED***(disorder)")
  ],
  ['code', 'term']  
)

tmp_pregnancy = (
  tmp_pregnancy_v1
  .withColumn('name', f.lit('pregnancy'))
  .withColumn('terminology', f.lit('SNOMED'))
  .select(['name', 'terminology', 'code', 'term'])
)


# save
codelist_pregnancy = tmp_pregnancy

display(codelist_pregnancy)

# COMMAND ----------

# MAGIC %md ## 3.3 HRT and Combined Oral Contraceptive Pill (COCP) codelist 

# COMMAND ----------

# DBTITLE 1,Declare COCP BNF codes
bnf_code_info = spark.table('dss_corporate.bnf_code_information')

# cocp
tmp_cocp = bnf_code_info\
  .where(f.substring(f.col('BNF_Presentation_Code'), 1, 6) == '070301')\
  .withColumn('name', f.lit('cocp'))

dss_cocp = tmp_cocp.drop_duplicates(['BNF_Presentation_Code'])

dss_cocp = (dss_cocp.select('BNF_Presentation_Code','BNF_Presentation','name')
            .withColumn('name', f.lit('cocp'))
            .withColumn('terminology', f.lit('BNF'))
            .withColumnRenamed('BNF_Presentation_Code', 'code')
            .withColumnRenamed('BNF_Presentation', 'term'))
            
display(dss_cocp)

# COMMAND ----------

# DBTITLE 1,Declare COCP BNF codes
# hrt
tmp_hrt =  bnf_code_info\
  .where(f.substring(f.col('BNF_Presentation_Code'), 1, 7) == '0604011')\
  .withColumn('name', f.lit('hrt'))

dss_hrt = tmp_hrt.drop_duplicates(['BNF_Presentation_Code'])

dss_hrt = (dss_hrt.select('BNF_Presentation_Code','BNF_Presentation','name')
            .withColumn('name', f.lit('hrt'))
            .withColumn('terminology', f.lit('BNF'))
            .withColumnRenamed('BNF_Presentation_Code', 'code')
            .withColumnRenamed('BNF_Presentation', 'term'))
            
display(dss_hrt)

# COMMAND ----------

# MAGIC %md ## 3.4 Append

# COMMAND ----------

# append (union) codelists defined above
codelist = (
  codelist_prostate_cancer
  .select('name', 'terminology', 'code', 'term')
  .unionByName(codelist_pregnancy)
  .unionByName(dss_cocp)
  .unionByName(dss_hrt)
  .orderBy('name', 'terminology', 'code')
)

display(codelist)

# COMMAND ----------

# MAGIC %md # 4. Check

# COMMAND ----------

# check 
tmpt = tab(codelist, 'name', 'terminology')

# COMMAND ----------

# MAGIC %md # 5. Save

# COMMAND ----------

save_table(df=codelist, out_name=f'{proj}_out_codelist_quality_assurance', save_previous=False)