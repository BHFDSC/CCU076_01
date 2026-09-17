#*******************************************************************************
#
# Project: CCU076
# Title:   Associations between air temperature and cardiovascular health outcomes
# Date:    13-09-2024
# Author:  Isabel Walter
# Purpose: Import split time series data from SDE, write to project folder, prepare 
#          final time series datasets. 
#
#*******************************************************************************


#### Preparation ####

rm(list = ls())

# Load libraries 
library(DBI)
library(dplyr)
library(dbplyr)
library(readr)
library(here)
library(forcats)

# Source function
fs <- c("retrieve_data")
for (f in fs) source(here::here("source", paste0(f, ".R")))

# Databricks connection 
con <- DBI::dbConnect(
  odbc::odbc(),
  dsn = 'databricks',
  HTTPPath = 'sql/protocolv1/o/847064027862604/1007-063147-3q3hqcj2',
  PWD =''
)

# Databricks database
db <- ""
catalog <- "dsa_391419_j3w9t"

options(mc.cores = 1)

#### Arterial event incl fatal events time series #####

# Tables to save
tbl <- c(#"ccu076_01_out_time_series_ae_fatal_1",
         #"ccu076_01_out_time_series_ae_fatal_2",
         #"ccu076_01_out_time_series_ae_fatal_3",
         "ccu076_01_out_time_series_ae_fatal_4",
         "ccu076_01_out_time_series_ae_fatal_5",
         "ccu076_01_out_time_series_ae_fatal_6",
         "ccu076_01_out_time_series_ae_fatal_7",
         "ccu076_01_out_time_series_ae_fatal_8",
         "ccu076_01_out_time_series_ae_fatal_9",
         "ccu076_01_out_time_series_ae_fatal_10",
         "ccu076_01_out_time_series_ae_fatal_11")

# Import and save the relevant datasets as rds objects
library(callr)

for (i in tbl) {
  callr::r(function(i, con_details, catalog, db) {
    con <- do.call(DBI::dbConnect, con_details)
    full_name <- paste(catalog, db, i, sep = ".")
    data <- DBI::dbGetQuery(con, paste0("SELECT * FROM ", full_name))
    readr::write_rds(data, here::here("data", paste0(i, ".rds")))
    DBI::dbDisconnect(con)
  }, args = list(i = i, con_details = list(drv = odbc::odbc(), dsn = "your_dsn"), catalog = catalog, db = db))
}

for (i in tbl) {
  retrieve_data(
    connection = con,
    catalog = catalog,
    schema= db,
    table = i
  )
  rm(list = setdiff(ls(), c("con", "db", "catalog", "i", "tbl")))
  gc()
}

  # Load datasets into list

dfs_to_combine = list() 

for (i in tbl) {
  df <- load_data(table = i)
  dfs_to_combine[[i]] <- df
}

# Make complete time series datasets 
ts <- combine_ts(dfs_to_combine)
rm(dfs_to_combine)

# Order ts
ts <- ts |>
  arrange(PERSON_ID, time_series_date)

# Save
save(ts, filename="time_series_ae_fatal")
rm(ts)

# #### Arterial event excl fatal events time series #####
# tbl <- c("ccu076_01_out_time_series_ae_nonfatal_1",
#          "ccu076_01_out_time_series_ae_nonfatal_2",
#          "ccu076_01_out_time_series_ae_nonfatal_3",
#          "ccu076_01_out_time_series_ae_nonfatal_4",
#          "ccu076_01_out_time_series_ae_nonfatal_5",
#          "ccu076_01_out_time_series_ae_nonfatal_6",
#          "ccu076_01_out_time_series_ae_nonfatal_7",
#          "ccu076_01_out_time_series_ae_nonfatal_8",
#          "ccu076_01_out_time_series_ae_nonfatal_9")
# 
# # Import and save the relevant datasets as rds objects
# 
# for (i in tbl) {
#   retrieve_data(
#     connection = con,
#     catalog = catalog,
#     schema= db,
#     table = i
#   )
# }
# 
# # Load datasets into list
# 
# dfs_to_combine = list() 
# 
# for (i in tbl) {
#   df <- load_data(table = i)
#   dfs_to_combine[[i]] <- df
# }
# 
# # Make complete time series datasets 
# ts <- combine_ts(dfs_to_combine)
# rm(dfs_to_combine)
# 
# # Order ts
# ts <- ts |>
#   arrange(PERSON_ID, time_series_date)
# 
# # Save
# save(ts, filename="time_series_ae_nonfatal")
# rm(ts)


#### Venous event incl fatal events time series ####

tbl <- c("ccu076_01_out_time_series_ve_fatal_1",
         "ccu076_01_out_time_series_ve_fatal_2",
         "ccu076_01_out_time_series_ve_fatal_3",
         "ccu076_01_out_time_series_ve_fatal_4",
         "ccu076_01_out_time_series_ve_fatal_5")

# Import and save the relevant datasets as rds objects

for (i in tbl) {
  retrieve_data(
    connection = con,
    catalog = catalog,
    schema= db,
    table = i
  )
}

# Load datasets into list

dfs_to_combine = list() 

for (i in tbl) {
  df <- load_data(table = i)
  dfs_to_combine[[i]] <- df
}

# Make complete time series datasets 
ts <- combine_ts(dfs_to_combine)
rm(dfs_to_combine)

# Order ts
ts <- ts |>
  arrange(PERSON_ID, time_series_date)

save(length(unique(ts$PERSON_ID)), filename="n_distinct_ve_fatal")

# Save
save(ts, filename="time_series_ve_fatal")
rm(ts)
gc()
# #### Venous event excl fatal events time series ####
# 
# tbl <- c("ccu076_01_out_time_series_ve_nonfatal_1",
#          "ccu076_01_out_time_series_ve_nonfatal_2",
#          "ccu076_01_out_time_series_ve_nonfatal_3",
#          "ccu076_01_out_time_series_ve_nonfatal_4")
# 
# # Import and save the relevant datasets as rds objects
# 
# for (i in tbl) {
#   retrieve_data(
#     connection = con,
#     database= db,
#     table = i
#   )
#   gc()
# }
# 
# # Load datasets into list
# 
# dfs_to_combine = list() 
# 
# for (i in tbl) {
#   df <- load_data(table = i)
#   dfs_to_combine[[i]] <- df
# }
# 
# # Make complete time series datasets 
# ts <- combine_ts(dfs_to_combine)
# rm(dfs_to_combine)
# 
# # Order ts
# ts <- ts |>
#   arrange(PERSON_ID, time_series_date)
# 
# # Save
# save(ts, filename="time_series_ve_nonfatal")
# rm(ts)

#### MI incl fatal events time series ####

tbl <- c("ccu076_01_out_time_series_mi_fatal_1",
         "ccu076_01_out_time_series_mi_fatal_2",
         "ccu076_01_out_time_series_mi_fatal_3",
         "ccu076_01_out_time_series_mi_fatal_4",
         "ccu076_01_out_time_series_mi_fatal_5",
         "ccu076_01_out_time_series_mi_fatal_6")

# Import and save the relevant datasets as rds objects


for (i in tbl) {
  retrieve_data(
    connection = con,
    catalog = catalog,
    schema= db,
    table = i
  )
  gc()
}

# Load datasets into list

dfs_to_combine = list() 

for (i in tbl) {
  df <- load_data(table = i)
  dfs_to_combine[[i]] <- df
  gc()
}

# Make complete time series datasets 
ts <- combine_ts(dfs_to_combine)
rm(dfs_to_combine)

# Order ts
ts <- ts |>
  arrange(PERSON_ID, time_series_date)

save(length(unique(ts$PERSON_ID)), filename="n_distinct_mi_fatal")

# Save
save(ts, filename="time_series_mi_fatal")
rm(ts)

# #### MI excl fatal events time series ####
# 
# tbl <- c("ccu076_01_out_time_series_mi_nonfatal_1",
#          "ccu076_01_out_time_series_mi_nonfatal_2",
#          "ccu076_01_out_time_series_mi_nonfatal_3",
#          "ccu076_01_out_time_series_mi_nonfatal_4",
#          "ccu076_01_out_time_series_mi_nonfatal_5")
# 
# # Import and save the relevant datasets as rds objects
# 
# for (i in tbl) {
#   retrieve_data(
#     connection = con,
#     database= db,
#     table = i
#   )
# }
# 
# # Load datasets into list
# 
# dfs_to_combine = list() 
# 
# for (i in tbl) {
#   df <- load_data(table = i)
#   dfs_to_combine[[i]] <- df
# }
# 
# # Make complete time series datasets 
# ts <- combine_ts(dfs_to_combine)
# rm(dfs_to_combine)
# 
# # Order ts
# ts <- ts |>
#   arrange(PERSON_ID, time_series_date)
# 
# # Save
# save(ts, filename="time_series_mi_nonfatal")
# rm(ts)

#### Stroke incl fatal events time series ####

tbl <- c("ccu076_01_out_time_series_stroke_fatal_1",
         "ccu076_01_out_time_series_stroke_fatal_2",
         "ccu076_01_out_time_series_stroke_fatal_3",
         "ccu076_01_out_time_series_stroke_fatal_4",
         "ccu076_01_out_time_series_stroke_fatal_5")

# Import and save the relevant datasets as rds objects

for (i in tbl) {
  retrieve_data(
    connection = con,
    catalog = catalog,
    schema= db,
    table = i
  )
  gc()
}

# Load datasets into list

dfs_to_combine = list() 

for (i in tbl) {
  df <- load_data(table = i)
  dfs_to_combine[[i]] <- df
}

# Make complete time series datasets 
ts <- combine_ts(dfs_to_combine)
rm(dfs_to_combine)

# Order ts
ts <- ts |>
  arrange(PERSON_ID, time_series_date)

# Save
save(ts, filename="time_series_stroke_fatal")
rm(ts)

#### Stroke excl fatal events time series ####

# tbl <- c("ccu076_01_out_time_series_stroke_nonfatal_1",
#          "ccu076_01_out_time_series_stroke_nonfatal_2",
#          "ccu076_01_out_time_series_stroke_nonfatal_3",
#          "ccu076_01_out_time_series_stroke_nonfatal_4")
# 
# # Import and save the relevant datasets as rds objects
# 
# for (i in tbl) {
#   retrieve_data(
#     connection = con,
#     database= db,
#     table = i
#   )
# }
# 
# # Load datasets into list
# 
# dfs_to_combine = list() 
# 
# for (i in tbl) {
#   df <- load_data(table = i)
#   dfs_to_combine[[i]] <- df
# }
# 
# # Make complete time series datasets 
# ts <- combine_ts(dfs_to_combine)
# rm(dfs_to_combine)
# 
# # Order ts
# ts <- ts |>
#   arrange(PERSON_ID, time_series_date)
# 
# # Save
# save(ts, filename="time_series_stroke_nonfatal")
# rm(ts)
# 
# #### Covid hospitalisation incl fatal events time series ####
# 
# tbl <- c("ccu076_01_out_time_series_covid_fatal_1",
#          "ccu076_01_out_time_series_covid_fatal_2",
#          "ccu076_01_out_time_series_covid_fatal_3",
#          "ccu076_01_out_time_series_covid_fatal_4",
#          "ccu076_01_out_time_series_covid_fatal_5",
#          "ccu076_01_out_time_series_covid_fatal_6")
# 
# # Import and save the relevant datasets as rds objects
# 
# for (i in tbl) {
#   retrieve_data(
#     connection = con,
#     database= db,
#     table = i
#   )
# }
# 
# # Load datasets into list
# 
# dfs_to_combine = list() 
# 
# for (i in tbl) {
#   df <- load_data(table = i)
#   dfs_to_combine[[i]] <- df
# }
# 
# # Make complete time series datasets 
# ts <- combine_ts(dfs_to_combine)
# rm(dfs_to_combine)
# 
# # Order ts
# ts <- ts |>
#   arrange(PERSON_ID, time_series_date)
# 
# # Save
# save(ts, filename="time_series_covid_fatal")
# rm(ts)


#### Time series censored at death ####

# # Tables to save
# tbl <- c("ccu076_01_out_time_series_cens_ae_fatal_1",
#          "ccu076_01_out_time_series_cens_ae_fatal_2",
#          "ccu076_01_out_time_series_cens_ae_fatal_3",
#          "ccu076_01_out_time_series_cens_ae_fatal_4",
#          "ccu076_01_out_time_series_cens_ae_fatal_5",
#          "ccu076_01_out_time_series_cens_ae_fatal_6",
#          "ccu076_01_out_time_series_cens_ae_fatal_7",
#          "ccu076_01_out_time_series_cens_ae_fatal_8",
#          "ccu076_01_out_time_series_cens_ae_fatal_9",
#          "ccu076_01_out_time_series_cens_ae_fatal_10",
#          "ccu076_01_out_time_series_cens_ae_fatal_11")
# 
# # Import and save the relevant datasets as rds objects
# 
# for (i in tbl) {
#   retrieve_data(
#     connection = con,
#     database= db,
#     table = i
#   )
# }
# 
# # Load datasets into list
# 
# dfs_to_combine = list() 
# 
# for (i in tbl) {
#   df <- load_data(table = i)
#   dfs_to_combine[[i]] <- df
# }
# 
# # Make complete time series datasets 
# ts <- combine_ts(dfs_to_combine)
# rm(dfs_to_combine)
# 
# # Order ts
# ts <- ts |>
#   arrange(PERSON_ID, time_series_date)
# 
# # Save
# save(ts, filename="time_series_cens_ae_fatal")
# rm(ts)
# 
# tbl <- c("ccu076_01_out_time_series_cens_ve_fatal_1",
#          "ccu076_01_out_time_series_cens_ve_fatal_2",
#          "ccu076_01_out_time_series_cens_ve_fatal_3",
#          "ccu076_01_out_time_series_cens_ve_fatal_4")
# 
# # Import and save the relevant datasets as rds objects
# 
# for (i in tbl) {
#   retrieve_data(
#     connection = con,
#     database= db,
#     table = i
#   )
# }
# 
# # Load datasets into list
# 
# dfs_to_combine = list() 
# 
# for (i in tbl) {
#   df <- load_data(table = i)
#   dfs_to_combine[[i]] <- df
# }
# 
# # Make complete time series datasets 
# ts <- combine_ts(dfs_to_combine)
# rm(dfs_to_combine)
# 
# # Order ts
# ts <- ts |>
#   arrange(PERSON_ID, time_series_date)
# 
# # Save
# save(ts, filename="time_series_cens_ve_fatal")
# rm(ts)
# 
# tbl <- c("ccu076_01_out_time_series_cens_mi_fatal_1",
#          "ccu076_01_out_time_series_cens_mi_fatal_2",
#          "ccu076_01_out_time_series_cens_mi_fatal_3",
#          "ccu076_01_out_time_series_cens_mi_fatal_4",
#          "ccu076_01_out_time_series_cens_mi_fatal_5",
#          "ccu076_01_out_time_series_cens_mi_fatal_6")
# 
# # Import and save the relevant datasets as rds objects
# 
# for (i in tbl) {
#   retrieve_data(
#     connection = con,
#     database= db,
#     table = i
#   )
# }
# 
# # Load datasets into list
# 
# dfs_to_combine = list() 
# 
# for (i in tbl) {
#   df <- load_data(table = i)
#   dfs_to_combine[[i]] <- df
# }
# 
# # Make complete time series datasets 
# ts <- combine_ts(dfs_to_combine)
# rm(dfs_to_combine)
# 
# # Order ts
# ts <- ts |>
#   arrange(PERSON_ID, time_series_date)
# 
# # Save
# save(ts, filename="time_series_cens_mi_fatal")
# rm(ts)
# 
# tbl <- c("ccu076_01_out_time_series_cens_stroke_fatal_1",
#          "ccu076_01_out_time_series_cens_stroke_fatal_2",
#          "ccu076_01_out_time_series_cens_stroke_fatal_3",
#          "ccu076_01_out_time_series_cens_stroke_fatal_4",
#          "ccu076_01_out_time_series_cens_stroke_fatal_5")

# Import and save the relevant datasets as rds objects
# 
# for (i in tbl) {
#   retrieve_data(
#     connection = con,
#     database= db,
#     table = i
#   )
# }
# 
# # Load datasets into list
# 
# dfs_to_combine = list() 
# 
# for (i in tbl) {
#   df <- load_data(table = i)
#   dfs_to_combine[[i]] <- df
# }
# 
# # Make complete time series datasets 
# ts <- combine_ts(dfs_to_combine)
# rm(dfs_to_combine)
# 
# # Order ts
# ts <- ts |>
#   arrange(PERSON_ID, time_series_date)
# 
# # Save
# save(ts, filename="time_series_cens_stroke_fatal")
# rm(ts)
# 
# tbl <- c("ccu076_01_out_time_series_cens_covid_fatal_1",
#          "ccu076_01_out_time_series_cens_covid_fatal_2",
#          "ccu076_01_out_time_series_cens_covid_fatal_3",
#          "ccu076_01_out_time_series_cens_covid_fatal_4",
#          "ccu076_01_out_time_series_cens_covid_fatal_5",
#          "ccu076_01_out_time_series_cens_covid_fatal_6")
# 
# # Import and save the relevant datasets as rds objects
# 
# for (i in tbl) {
#   retrieve_data(
#     connection = con,
#     database= db,
#     table = i
#   )
# }
# 
# # Load datasets into list
# 
# dfs_to_combine = list() 
# 
# for (i in tbl) {
#   df <- load_data(table = i)
#   dfs_to_combine[[i]] <- df
# }
# 
# # Make complete time series datasets 
# ts <- combine_ts(dfs_to_combine)
# rm(dfs_to_combine)
# 
# # Order ts
# ts <- ts |>
#   arrange(PERSON_ID, time_series_date)
# 
# # Save
# save(ts, filename="time_series_cens_covid_fatal")
# rm(ts)

