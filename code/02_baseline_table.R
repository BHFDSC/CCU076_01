#*******************************************************************************
#
# Project: CCU076
# Title:   Associations between air temperature and cardiovascular health outcomes
# Date:    13-09-2024
# Author:  Isabel Walter
# Purpose: Prepare baseline table, outcomes demographics table, flowchart
#
#*******************************************************************************


#### Preparation ####

# Load libraries 
library(dplyr)
library(readr)
library(here)
library(stringr)
library(purrr)
library(lubridate)

# Source function
fs <- c("summarise_categorical", "summarise_continuous", "retrieve_data")

for (f in fs) source(here::here("source", paste0(f, ".R")))

# Load data
cohort <- load_data("cohort_final")

#### Make baseline table ####
# 
# # Define categorical variables
# categorical_vars <- c("age_group", "sex", "ethnicity", "deprivation_index_quintiles", "region", "rural_urban_class",
#                       "death", "exp_covid_diagnosis", "exp_covid_hosp", "exp_covid_vacc",
#                       "cov_smoking_status", "cov_obesity_combined_flag",
#                       "cov_depression_flag", "cov_heart_disease_flag" ,"cov_hypertension_flag",
#                       "cov_diabetes_flag", "cov_ckd_flag", "cov_comorb_comp_flag", "cov_cvd_comorb_flag",
#                       "cov_stroke_flag", "out_arterial_event_flag",  "out_ADISS_flag", "out_MI_flag",
#                       "out_stroke_IS_flag", "out_stroke_NOS_flag", "out_RI_flag", "out_AT_flag",
#                       "out_venous_event_flag", "out_VT_flag","out_PE_flag", "out_ICVT_flag")#, "out_MC_flag")
# 
# #Make summary data for overall group
# cat <- summarise_categorical(.data = cohort, .col = categorical_vars, .header="Total")
# age <- summarise_continuous(.data = cohort, .col = study_start_age, .header="Total")
# 
# baseline_total <- reduce(list(age, cat), rbind)
# 
# # Make summary data for covid vs no covid group
# cat_covid <- summarise_categorical(.data = cohort, .col = categorical_vars[categorical_vars != "exp_covid_diagnosis"], .group_by = exp_covid_diagnosis)
# age_covid <- summarise_continuous(.data = cohort, .col = study_start_age, .group_by = exp_covid_diagnosis)
# 
# baseline_covid <- reduce(list(age_covid, cat_covid), rbind) |> select(-`P-value`)
# 
# # Make summary data for history of CVD vs no history of CVD group
# cohort <- cohort |>
#   mutate(cov_cvd_flag = case_when(cov_heart_disease_flag == "Heart disease" | cov_hypertension_flag == "Hypertension" | cov_stroke_flag == "Stroke" ~ 1,
#                                   TRUE ~ 0),
#          cov_cvd_flag = factor(cov_cvd_flag, levels = c(0,1), labels = c("No CVD disease history", "CVD disease history")))
# 
# cat_cvd <- summarise_categorical(.data = cohort, .col = categorical_vars, .group_by = cov_cvd_flag)
# age_cvd <- summarise_continuous(.data = cohort, .col = study_start_age, .group_by = cov_cvd_flag)
# 
# baseline_cvd <- reduce(list(age_cvd, cat_cvd), rbind) |> select(-`P-value`)
# 
# # Combine
# baseline <- reduce(list(baseline_total, baseline_covid, baseline_cvd), left_join, by="Characteristic")
# 
# # Adjust characteristic names
# baseline_final <- baseline |>
#   mutate(across(everything(), ~ ifelse(is.na(.), "", .))) |>
#   mutate(Characteristic = gsub("_at"," Arterial thrombosis", Characteristic)) |>
#   mutate(Characteristic = gsub("_", " ", Characteristic)) |>
#   mutate(Characteristic = gsub("fu days", "Follow-up days", Characteristic)) |>
#   mutate(Characteristic = gsub("study start age", "Age", Characteristic)) |>
#   mutate(Characteristic = gsub("covid|Covid", "COVID-19", Characteristic)) |>
#   mutate(Characteristic = gsub("exp |out |flag|cov |comp","", Characteristic)) |>
#   mutate(Characteristic = gsub("ADISS","Arterial dissection or ruptured aneurysm", Characteristic)) |>
#   mutate(Characteristic = gsub("MI ","Myocardial infarction", Characteristic)) |>
#   mutate(Characteristic = gsub("RI ","Retinal infarction", Characteristic)) |>
#   #mutate(Characteristic = gsub("stroke IS","Stroke IS", Characteristic)) |>
#   #mutate(Characteristic = gsub("stroke nos ","Stroke NOS", Characteristic)) |>
#   mutate(Characteristic = gsub("ICVT","Intracranial venous thrombosis", Characteristic)) |>
#   mutate(Characteristic = gsub("VT ","Venous thrombosis", Characteristic)) |>
#   mutate(Characteristic = gsub("PE ","Pulmonary embolism", Characteristic)) |>
#   mutate(Characteristic = str_replace(Characteristic, "^([a-z])", toupper))
# 
# # Save for output
# output(baseline_final, "baseline_table")


#### Make demographics table stratified for outcomes ####

# Load time series summary datasets
tbl <- c("ccu076_01_out_time_series_ae_nonfatal_individual_level_summary",
         "ccu076_01_out_time_series_ae_fatal_individual_level_summary",
         "ccu076_01_out_time_series_ve_nonfatal_individual_level_summary",
         "ccu076_01_out_time_series_ve_fatal_individual_level_summary",
         "ccu076_01_out_time_series_mi_nonfatal_individual_level_summary",
         "ccu076_01_out_time_series_mi_fatal_individual_level_summary",
         "ccu076_01_out_time_series_stroke_nonfatal_individual_level_summary",
         "ccu076_01_out_time_series_stroke_fatal_individual_level_summary",
         "ccu076_01_out_time_series_covid_nonfatal_individual_level_summary",
         "ccu076_01_out_time_series_covid_fatal_individual_level_summary",
         "ccu076_01_out_time_series_summary")

for (i in tbl) {
  df <- load_data(table = i)
  name <- sub("^[^_]*_[^_]*_[^_]*_", "", i)
  assign(name, df)
}


# Cohort add time_series flags 

# Function to create flags and convert them to factors correctly
create_flags <- function(cohort, ts_data, ts_flag_name, tmean_flag_name, pm2p5_flag_name) {
  
  lbl <- case_when(
    grepl("ve", ts_flag_name) & grepl("nonfatal", ts_flag_name) ~ "Non-fatal Venous Thrombotic Event",
    grepl("ve", ts_flag_name) & grepl("fatal", ts_flag_name) ~ "Venous Thrombotic Event",
    grepl("mi", ts_flag_name) & grepl("nonfatal", ts_flag_name) ~ "Non-fatal Acute Myocardial Infarction",
    grepl("mi", ts_flag_name) & grepl("fatal", ts_flag_name) ~ "Acute Myocardial Infarction",
    grepl("stroke", ts_flag_name) & grepl("nonfatal", ts_flag_name) ~ "Non-fatal Ischaemic Stroke",
    grepl("stroke", ts_flag_name) & grepl("fatal", ts_flag_name) ~ "Ischaemic Stroke",
    grepl("ae", ts_flag_name) & grepl("nonfatal", ts_flag_name) ~ "Non-fatal Arterial Thrombotic Event",
    grepl("ae", ts_flag_name) & grepl("fatal", ts_flag_name) ~ "Arterial Thrombotic Event",
    grepl("covid", ts_flag_name) & grepl("nonfatal", ts_flag_name) ~ "Non-fatal COVID19 diagnosis",
    grepl("covid", ts_flag_name) & grepl("fatal", ts_flag_name) ~ "COVID19 diagnosis",
    TRUE ~ "Other"  # Default case if no match
    )
  
  cohort <- cohort |>
    left_join(
      ts_data |>
        select(PERSON_ID, missing_tmean, missing_pm2p5) |>
        mutate(
          !!ts_flag_name := 1

        ) |>
        select(-missing_tmean, -missing_pm2p5),
      by = "PERSON_ID"
    ) |>
    mutate(
      !!ts_flag_name := factor(ifelse(is.na(get(ts_flag_name)), 0, get(ts_flag_name)), levels = c(0, 1), labels = c(paste0("No ",lbl),  lbl))
     )
}

# Apply function for each time series dataset and create flags per time series dataset
cohort <- create_flags(cohort, time_series_ve_fatal_individual_level_summary, "ts_ve_fatal_flag")
cohort <- create_flags(cohort, time_series_ve_nonfatal_individual_level_summary, "ts_ve_nonfatal_flag")
cohort <- create_flags(cohort, time_series_ae_fatal_individual_level_summary, "ts_ae_fatal_flag")
cohort <- create_flags(cohort, time_series_ae_nonfatal_individual_level_summary, "ts_ae_nonfatal_flag")
cohort <- create_flags(cohort, time_series_mi_fatal_individual_level_summary, "ts_mi_fatal_flag")
cohort <- create_flags(cohort, time_series_mi_nonfatal_individual_level_summary, "ts_mi_nonfatal_flag")
cohort <- create_flags(cohort, time_series_stroke_fatal_individual_level_summary, "ts_stroke_fatal_flag")
cohort <- create_flags(cohort, time_series_stroke_nonfatal_individual_level_summary, "ts_stroke_nonfatal_flag")
cohort <- create_flags(cohort, time_series_covid_fatal_individual_level_summary, "ts_covid_fatal_flag")
cohort <- create_flags(cohort, time_series_covid_nonfatal_individual_level_summary, "ts_covid_nonfatal_flag")

# Define categorical variables
categorical_vars <- c("age_group", "sex", "ethnicity", "deprivation_index_quintiles", "region",
                      "death", "exp_covid_diagnosis", "exp_covid_hosp", "exp_covid_vacc",
                      "rural_urban_class", "cov_smoking_status", "cov_obesity_combined_flag", "cov_hypertension_flag",
                      "cov_diabetes_flag", "cov_ckd_flag","cov_depression_flag", "cov_heart_disease_flag",
                      "cov_comorb_comp_flag", "cov_cvd_comorb_flag")

# Make summary data for overall group
cat_outcomes <- summarise_categorical(.data = cohort, .col = categorical_vars, .header="Total")
age_outcomes <- summarise_continuous(.data = cohort, .col = study_start_age, .header="Total")

outcomes_total <- reduce(list(age_outcomes, cat_outcomes), rbind)

# Make summary data for venous event vs no venous event group
age_ve_fatal <- summarise_continuous(.data = cohort, .col = study_start_age, .group_by = ts_ve_fatal_flag)
cat_ve_fatal <- summarise_categorical(.data = cohort, .col = categorical_vars, .group_by =  ts_ve_fatal_flag)
total_ve_fatal <- reduce(list(age_ve_fatal, cat_ve_fatal), rbind) |> select(-`P-value`)

age_ve_nonfatal <- summarise_continuous(.data = cohort, .col = study_start_age, .group_by = ts_ve_nonfatal_flag)
cat_ve_nonfatal <- summarise_categorical(.data = cohort, .col = categorical_vars, .group_by =  ts_ve_nonfatal_flag)
total_ve_nonfatal <- reduce(list(age_ve_nonfatal, cat_ve_nonfatal), rbind) |> select(-`P-value`)

# Make summary data for arterial event vs no arterial event group
age_ae_fatal <- summarise_continuous(.data = cohort, .col = study_start_age, .group_by = ts_ae_fatal_flag)
cat_ae_fatal <- summarise_categorical(.data = cohort, .col = categorical_vars, .group_by =  ts_ae_fatal_flag)
total_ae_fatal <- reduce(list(age_ae_fatal, cat_ae_fatal), rbind) |> select(-`P-value`)

age_ae_nonfatal <- summarise_continuous(.data = cohort, .col = study_start_age, .group_by = ts_ae_nonfatal_flag)
cat_ae_nonfatal <- summarise_categorical(.data = cohort, .col = categorical_vars, .group_by =  ts_ae_nonfatal_flag)
total_ae_nonfatal <- reduce(list(age_ae_nonfatal, cat_ae_nonfatal), rbind) |> select(-`P-value`)


# Make summary data for MI vs no MI group
age_mi_fatal <- summarise_continuous(.data = cohort, .col = study_start_age, .group_by = ts_mi_fatal_flag)
cat_mi_fatal <- summarise_categorical(.data = cohort, .col = categorical_vars, .group_by =  ts_mi_fatal_flag)
total_mi_fatal <- reduce(list(age_mi_fatal, cat_mi_fatal), rbind) |> select(-`P-value`)

age_mi_nonfatal <- summarise_continuous(.data = cohort, .col = study_start_age, .group_by = ts_mi_nonfatal_flag)
cat_mi_nonfatal <- summarise_categorical(.data = cohort, .col = categorical_vars, .group_by =  ts_mi_nonfatal_flag)
total_mi_nonfatal <- reduce(list(age_mi_nonfatal, cat_mi_nonfatal), rbind) |> select(-`P-value`)

# Make summary data for ischaemic stroke vs no ischaemic stroke group
age_stroke_fatal <- summarise_continuous(.data = cohort, .col = study_start_age, .group_by = ts_stroke_fatal_flag)
cat_stroke_fatal <- summarise_categorical(.data = cohort, .col = categorical_vars, .group_by =  ts_stroke_fatal_flag)
total_stroke_fatal <- reduce(list(age_stroke_fatal, cat_stroke_fatal), rbind) |> select(-`P-value`)

age_stroke_nonfatal <- summarise_continuous(.data = cohort, .col = study_start_age, .group_by = ts_stroke_nonfatal_flag)
cat_stroke_nonfatal <- summarise_categorical(.data = cohort, .col = categorical_vars, .group_by =  ts_stroke_nonfatal_flag)
total_stroke_nonfatal <- reduce(list(age_stroke_nonfatal, cat_stroke_nonfatal), rbind) |> select(-`P-value`)


# Make summary data for covid vs no covid group
age_covid_fatal <- summarise_continuous(.data = cohort, .col = study_start_age, .group_by = ts_covid_fatal_flag)
cat_covid_fatal <- summarise_categorical(.data = cohort, .col = categorical_vars, .group_by =  ts_covid_fatal_flag)
total_covid_fatal <- reduce(list(age_covid_fatal, cat_covid_fatal), rbind) |> select(-`P-value`)

age_covid_nonfatal <- summarise_continuous(.data = cohort, .col = study_start_age, .group_by = ts_covid_nonfatal_flag)
cat_covid_nonfatal <- summarise_categorical(.data = cohort, .col = categorical_vars, .group_by =  ts_covid_nonfatal_flag)
total_covid_nonfatal <- reduce(list(age_covid_nonfatal, cat_covid_nonfatal), rbind) |> select(-`P-value`)

# Combine
out_demo <- reduce(list(outcomes_total, total_ve_fatal, total_ve_nonfatal, 
                        total_ae_fatal, total_ae_nonfatal, total_mi_fatal,
                        total_mi_nonfatal, total_stroke_fatal, total_stroke_nonfatal,
                        total_covid_fatal, total_covid_nonfatal), left_join, by="Characteristic")

# Adjust characteristic names
outcomes_demographics <- out_demo |>
  mutate(across(everything(), ~ ifelse(is.na(.), "", .))) |>
  mutate(Characteristic = gsub("_", " ", Characteristic)) |>
  mutate(Characteristic = gsub("out |flag|cov |comp","", Characteristic)) |>
  mutate(Characteristic = str_replace(Characteristic, "^([a-z])", toupper)) 

# Add summary total numbers 

# Save for output 
output(outcomes_demographics, "demographics_by_outcome")

#### Make flowchart data ####

# Load data
# fc_df <- load_data("ccu076_01_tmp_inc_exc_flow_cases")
# 
# fc_df <- fc_df |>
#   select(-var, -n, -n_id) |>
#   mutate(indx = as.integer(indx),
#          n_id_distinct = as.integer(n_id_distinct)) |> 
#   arrange(indx)
# 
# fc_df$n_id_distinct <- round(fc_df$n_id_distinct / 5) * 5
# 
# # Save data
# output(fc_df, "flowchart_data")
