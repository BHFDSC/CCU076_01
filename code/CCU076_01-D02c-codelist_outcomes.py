# Databricks notebook source
# MAGIC %md # CCU076_01-D02c-codelist_outcomes
# MAGIC
# MAGIC **Description** This notebook creates the outcome codelists. Does not yet make the arterial and venous composites, this is done in the outcomes notebook.
# MAGIC
# MAGIC **Authors** Isabel Walter, Alexia Sampri, Elena Raffetti.
# MAGIC
# MAGIC **Reviewers** 
# MAGIC
# MAGIC **Acknowledgements** Adapted from code from Elena Raffetti, Isabel Walter, Alexia Sampri, Tom Bolton and John Nolan. Based on CCU018-01)
# MAGIC
# MAGIC **Output** CCU076_01_out_codelist_outcomes

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import databricks.koalas as ks
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

# MAGIC %run "/Shared/SHDS/common/functions"

# COMMAND ----------

# MAGIC %md # 1 Parameters

# COMMAND ----------

# MAGIC %run ./CCU076_01-D01-parameters

# COMMAND ----------

# MAGIC %md # 2 Codelists

# COMMAND ----------

codelist_outcomes = spark.createDataFrame(pd.read_csv('/Workspace/Users/ijw36@cam.ac.uk/CCU076/CCU076_01/Codelists/codelist_outcomes.csv').fillna(""))
codelist_myocarditis = spark.createDataFrame(pd.read_csv('/Workspace/Users/ijw36@cam.ac.uk/CCU076/CCU076_01/Codelists/codelist_heart_disease.csv').fillna("")).filter(f.col("name") == "MC").drop('subtype')
codelist_pericarditis = spark.createDataFrame(pd.read_csv('/Workspace/Users/ijw36@cam.ac.uk/CCU076/CCU076_01/Codelists/codelist_heart_disease.csv').fillna("")).filter(f.col("name") == "PC").drop('subtype')

# COMMAND ----------

display(codelist_myocarditis)

# COMMAND ----------

display(codelist_pericarditis)

# COMMAND ----------

display(codelist_outcomes)

# COMMAND ----------

# MAGIC %md # 3 Combine

# COMMAND ----------

# append (union) codelists defined above
# harmonise columns before appending
"""clistm = []
for indx, clist in enumerate([clist for clist in globals().keys() if (bool(re.match('^codelist_.*', clist))) & (bool(re.match('^codelist_match.*', clist)) == False)]):
  print(f'{0 if indx<10 else ""}' + str(indx) + ' ' + clist)
  tmp = globals()[clist]
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
    clistm = clistm.unionByName(tmp) """
  
codelist_outcomes = codelist_outcomes.unionByName(codelist_myocarditis).unionByName(codelist_pericarditis)
clistm = codelist_outcomes\
  .orderBy('name', 'terminology', 'code')

# COMMAND ----------

# MAGIC %md # 4 Excluding codes based on inclusion and covariate_only variable
# MAGIC

# COMMAND ----------

clistm = clistm.filter(clistm.inclusion == "1")
clistm = clistm.filter(clistm.covariate_only == "0")

# COMMAND ----------

# MAGIC %md # 5 Reformat

# COMMAND ----------

# check
display(clistm)

# COMMAND ----------

# check
display(clistm.where((f.col('terminology') == 'ICD10') & (f.col('code').rlike('X$'))))

# COMMAND ----------

# check
#display(clistm.where((f.col('terminology') == 'OPCS4') & (f.col('code').rlike('X$'))))

# COMMAND ----------

display(clistm.where((f.col('terminology') == 'ICD10') & (f.col('code').rlike('[\.\-\s]'))))

# COMMAND ----------

# remove trailing X's, decimal points, dashes, and spaces
clistmr = clistm\
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'X$', '')).otherwise(f.col('code')))\
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'[\.\-\s]', '')).otherwise(f.col('code')))

""" .withColumn('code', f.when(f.col('terminology') == 'OPCS4', f.regexp_replac('code', r'X$', '')).otherwise(f.col('code')))
.withColumn('code', f.when(f.col('terminology') == 'OPCS4', f.regexp_replace('code', r'[\.\-\s]', '')).otherwise(f.col('code'))) """

# COMMAND ----------

# MAGIC %md # 7 Check

# COMMAND ----------

# check
tmpt = tab(clistmr, 'name', 'terminology', var2_unstyled=1)

# COMMAND ----------

# check
display(clistmr)

# COMMAND ----------

# output ICD10 codelist
tmp = clistmr\
  .where(f.col('terminology') == 'ICD10')
display(tmp)

# COMMAND ----------

# MAGIC %md # 8 Save

# COMMAND ----------

# save name
outName = f'{proj}_out_codelist_outcomes'.lower()

# save
save_table(df=clistmr, out_name=outName, save_previous=True)