#*******************************************************************************
#
# Project: CCU076
# Title:   Associations between air temperature and cardiovascular health outcomes
# Date:    13-09-2024
# Author:  Isabel Walter
# Purpose: Prepare final time series datasets. 
#
#*******************************************************************************

rm(list = ls())

# Load libraries 
library(dplyr)
library(readr)
library(here)

# Source function
fs <- c("retrieve_data")
for (f in fs) source(here::here("source", paste0(f, ".R")))
#### Arterial event incl fatal events time series #####

# Tables to save
tbl <- c("ccu076_01_out_time_series_ae_fatal_1",
  "ccu076_01_out_time_series_ae_fatal_2",
  "ccu076_01_out_time_series_ae_fatal_3",
  "ccu076_01_out_time_series_ae_fatal_4",
  "ccu076_01_out_time_series_ae_fatal_5",
  "ccu076_01_out_time_series_ae_fatal_6",
  "ccu076_01_out_time_series_ae_fatal_7",
  "ccu076_01_out_time_series_ae_fatal_8",
  "ccu076_01_out_time_series_ae_fatal_9",
  "ccu076_01_out_time_series_ae_fatal_10",
  "ccu076_01_out_time_series_ae_fatal_11")

# Load datasets into list

dfs_to_combine = list() 

for (i in tbl) {
  df <- load_data(table = i)
  dfs_to_combine[[i]] <- df
  rm(df)
  gc()
}

# Make complete time series datasets 
ts <- combine_ts(dfs_to_combine)
rm(dfs_to_combine)
gc()

# Order ts
ts <- ts |>
  arrange(PERSON_ID, time_series_date)

# Print number of distinct individuals
num_individuals <- dplyr::n_distinct(ts$PERSON_ID)
message("Number of distinct individuals in AE ts: ", num_individuals)

# Save
save(ts, filename="time_series_ae_fatal")
rm(ts)
gc()


#### Venous event incl fatal events time series ####

tbl <- c("ccu076_01_out_time_series_ve_fatal_1",
         "ccu076_01_out_time_series_ve_fatal_2",
         "ccu076_01_out_time_series_ve_fatal_3",
         "ccu076_01_out_time_series_ve_fatal_4",
         "ccu076_01_out_time_series_ve_fatal_5")


dfs_to_combine = list() 

for (i in tbl) {
  df <- load_data(table = i)
  dfs_to_combine[[i]] <- df
  rm(df)
  gc()
}

# Make complete time series datasets 
ts <- combine_ts(dfs_to_combine)
rm(dfs_to_combine)
gc()

# Order ts
ts <- ts |>
  arrange(PERSON_ID, time_series_date)

# Print number of distinct individuals
num_individuals <- dplyr::n_distinct(ts$PERSON_ID)
message("Number of distinct individuals in VE ts: ", num_individuals)

# Save
save(ts, filename="time_series_ve_fatal")
rm(ts)
gc()

#### MI incl fatal events time series ####

tbl <- c("ccu076_01_out_time_series_mi_fatal_1",
         "ccu076_01_out_time_series_mi_fatal_2",
         "ccu076_01_out_time_series_mi_fatal_3",
         "ccu076_01_out_time_series_mi_fatal_4",
         "ccu076_01_out_time_series_mi_fatal_5",
         "ccu076_01_out_time_series_mi_fatal_6")


dfs_to_combine = list() 

for (i in tbl) {
  df <- load_data(table = i)
  dfs_to_combine[[i]] <- df
  rm(df)
  gc()
}

# Make complete time series datasets 
ts <- combine_ts(dfs_to_combine)
rm(dfs_to_combine)
gc()

# Order ts
ts <- ts |>
  arrange(PERSON_ID, time_series_date)

# Print number of distinct individuals
num_individuals <- dplyr::n_distinct(ts$PERSON_ID)
message("Number of distinct individuals in MI ts: ", num_individuals)

# Save
save(ts, filename="time_series_mi_fatal")
rm(ts)
gc()


#### Stroke incl fatal events time series ####

tbl <- c("ccu076_01_out_time_series_stroke_fatal_1",
         "ccu076_01_out_time_series_stroke_fatal_2",
         "ccu076_01_out_time_series_stroke_fatal_3",
         "ccu076_01_out_time_series_stroke_fatal_4",
         "ccu076_01_out_time_series_stroke_fatal_5")


dfs_to_combine = list() 

for (i in tbl) {
  df <- load_data(table = i)
  dfs_to_combine[[i]] <- df
  rm(df)
  gc()
}

# Make complete time series datasets 
ts <- combine_ts(dfs_to_combine)
rm(dfs_to_combine)
gc()

# Order ts
ts <- ts |>
  arrange(PERSON_ID, time_series_date)

# Print number of distinct individuals
num_individuals <- dplyr::n_distinct(ts$PERSON_ID)
message("Number of distinct individuals in stroke ts: ", num_individuals)

# Save
save(ts, filename="time_series_stroke_fatal")
rm(ts)
gc()


#### Censored datasets ####

#### Arterial events incl fatal events time series ####

# Tables to save
tbl <- c("ccu076_01_out_time_series_cens_ae_fatal_1",
         "ccu076_01_out_time_series_cens_ae_fatal_2",
         "ccu076_01_out_time_series_cens_ae_fatal_3",
         "ccu076_01_out_time_series_cens_ae_fatal_4",
         "ccu076_01_out_time_series_cens_ae_fatal_5",
         "ccu076_01_out_time_series_cens_ae_fatal_6",
         "ccu076_01_out_time_series_cens_ae_fatal_7",
         "ccu076_01_out_time_series_cens_ae_fatal_8",
         "ccu076_01_out_time_series_cens_ae_fatal_9",
         "ccu076_01_out_time_series_cens_ae_fatal_10",
         "ccu076_01_out_time_series_cens_ae_fatal_11")

# Load datasets into list

dfs_to_combine = list() 

for (i in tbl) {
  df <- load_data(table = i)
  dfs_to_combine[[i]] <- df
  rm(df)
  gc()
}

# Make complete time series datasets 
ts <- combine_ts(dfs_to_combine)
rm(dfs_to_combine)
gc()

# Order ts
ts <- ts |>
  arrange(PERSON_ID, time_series_date)

# Print number of distinct individuals
num_individuals <- dplyr::n_distinct(ts$PERSON_ID)
message("Number of distinct individuals in AE ts: ", num_individuals)

# Save
save(ts, filename="time_series_cens_ae_fatal")
rm(ts)
gc()


#### Venous event incl fatal events time series ####

tbl <- c("ccu076_01_out_time_series_cens_ve_fatal_1",
         "ccu076_01_out_time_series_cens_ve_fatal_2",
         "ccu076_01_out_time_series_cens_ve_fatal_3",
         "ccu076_01_out_time_series_cens_ve_fatal_4",
         "ccu076_01_out_time_series_cens_ve_fatal_5")


dfs_to_combine = list() 

for (i in tbl) {
  df <- load_data(table = i)
  dfs_to_combine[[i]] <- df
  rm(df)
  gc()
}

# Make complete time series datasets 
ts <- combine_ts(dfs_to_combine)
rm(dfs_to_combine)
gc()

# Order ts
ts <- ts |>
  arrange(PERSON_ID, time_series_date)

# Print number of distinct individuals
num_individuals <- dplyr::n_distinct(ts$PERSON_ID)
message("Number of distinct individuals in VE ts: ", num_individuals)

# Save
save(ts, filename="time_series_cens_ve_fatal")
rm(ts)
gc()

#### MI incl fatal events time series ####

tbl <- c("ccu076_01_out_time_series_cens_mi_fatal_1",
         "ccu076_01_out_time_series_cens_mi_fatal_2",
         "ccu076_01_out_time_series_cens_mi_fatal_3",
         "ccu076_01_out_time_series_cens_mi_fatal_4",
         "ccu076_01_out_time_series_cens_mi_fatal_5",
         "ccu076_01_out_time_series_cens_mi_fatal_6")


dfs_to_combine = list() 

for (i in tbl) {
  df <- load_data(table = i)
  dfs_to_combine[[i]] <- df
  rm(df)
  gc()
}

# Make complete time series datasets 
ts <- combine_ts(dfs_to_combine)
rm(dfs_to_combine)
gc()

# Order ts
ts <- ts |>
  arrange(PERSON_ID, time_series_date)

# Print number of distinct individuals
num_individuals <- dplyr::n_distinct(ts$PERSON_ID)
message("Number of distinct individuals in MI ts: ", num_individuals)

# Save
save(ts, filename="time_series_cens_mi_fatal")
rm(ts)
gc()


#### Stroke incl fatal events time series ####

tbl <- c("ccu076_01_out_time_series_cens_stroke_fatal_1",
         "ccu076_01_out_time_series_cens_stroke_fatal_2",
         "ccu076_01_out_time_series_cens_stroke_fatal_3",
         "ccu076_01_out_time_series_cens_stroke_fatal_4",
         "ccu076_01_out_time_series_cens_stroke_fatal_5")


dfs_to_combine = list() 

for (i in tbl) {
  df <- load_data(table = i)
  dfs_to_combine[[i]] <- df
  rm(df)
  gc()
}

# Make complete time series datasets 
ts <- combine_ts(dfs_to_combine)
rm(dfs_to_combine)
gc()

# Order ts
ts <- ts |>
  arrange(PERSON_ID, time_series_date)

# Print number of distinct individuals
num_individuals <- dplyr::n_distinct(ts$PERSON_ID)
message("Number of distinct individuals in stroke ts: ", num_individuals)

# Save
save(ts, filename="time_series_cens_stroke_fatal")
rm(ts)
gc()

