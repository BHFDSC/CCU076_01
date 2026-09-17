#*******************************************************************************
#
# Project: CCU076
# Title:   Associations between air temperature and cardiovascular health outcomes
# Date:    13-09-2024
# Author:  Isabel Walter
# Purpose: Import cohort and environmental data from SDE, write to project folder, prepare final cohort dataset. 
#
#*******************************************************************************

#### Preparation ####

rm(list = ls())

# Load libraries 
library(DBI)
library(dplyr)
library(tidyr)
library(readr)
library(here)
library(forcats)
library(lubridate)

# Source function
fs <- c("retrieve_data", "env_percentiles")
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

# Tables to save
tbl <- c("ccu076_01_out_cohort",
         "ccu076_01_out_covariates",
         "ccu076_01_out_exposures_covid",
         "ccu076_01_out_exposures_vacc",
         "ccu076_01_out_outcomes_all_with_washout",
         "ccu076_01_tmp_inc_exc_flow_cases",
         "ccu076_01_out_incidence_rates",
         "ccu076_01_out_incidence_rates_region",
         "ccu076_01_out_incidence_rates_ethnicity",
         "ccu076_01_out_incidence_rates_sex",
         "ccu076_01_out_incidence_rates_additional_subgroups",
         "ccu076_01_out_time_series_ae_nonfatal_individual_level_summary",
         "ccu076_01_out_time_series_ae_nonfatal_2week_individual_level_summary",
         "ccu076_01_out_time_series_ae_nonfatal_month_individual_level_summary",
         "ccu076_01_out_time_series_ae_fatal_individual_level_summary",
         "ccu076_01_out_time_series_ve_nonfatal_individual_level_summary",
         "ccu076_01_out_time_series_ve_nonfatal_2week_individual_level_summary",
         "ccu076_01_out_time_series_ve_nonfatal_month_individual_level_summary",
         "ccu076_01_out_time_series_ve_fatal_individual_level_summary",
         "ccu076_01_out_time_series_mi_nonfatal_individual_level_summary",
         "ccu076_01_out_time_series_mi_nonfatal_2week_individual_level_summary",
         "ccu076_01_out_time_series_mi_nonfatal_month_individual_level_summary",
         "ccu076_01_out_time_series_mi_fatal_individual_level_summary",
         "ccu076_01_out_time_series_stroke_nonfatal_individual_level_summary",
         "ccu076_01_out_time_series_stroke_nonfatal_2week_individual_level_summary",
         "ccu076_01_out_time_series_stroke_nonfatal_month_individual_level_summary",
         "ccu076_01_out_time_series_stroke_fatal_individual_level_summary",
         "ccu076_01_out_time_series_covid_nonfatal_individual_level_summary",
         "ccu076_01_out_time_series_covid_nonfatal_2week_individual_level_summary",
         "ccu076_01_out_time_series_covid_nonfatal_month_individual_level_summary",
         "ccu076_01_out_time_series_covid_fatal_individual_level_summary",
         "ccu076_01_out_time_series_summary")
         #"ccu076_01_tmp_env",
         #"hds_cur_lsoa_region_lookup")

#### Import and save all relevant datasets as rds objects ####

for (i in tbl) {
  retrieve_data(
    connection = con,
    catalog = catalog,
    schema= db,
    table = i
  )
}

#### Load datasets ####

for (i in tbl) {
  df <- load_data(table = i)
  name <- sub("^[^_]*_[^_]*_[^_]*_", "", i)
  assign(name, df)
}

### Save long outcome dataset and remove from data memory ###

outcomes_long <- outcomes_all_with_washout
save(table = outcomes_long, filename="outcomes_long")

#### Prepare cohort for baseline table ####

# Cohort 

# binary age group (create here with ifelse statements bcs for some reason case_when is not working)
age_group_bin <- ifelse(
  is.na(cohort$study_start_age), "Missing age group bin",  # Handle missing values first
  ifelse(
    cohort$study_start_age >= 65, "Age 65 years and older",  # Handle 65+ years
    ifelse(
      cohort$study_start_age >= 18 & cohort$study_start_age < 65, "Age 18-64 years",  # Handle 18-64 years
      NA  # Handle anything outside the specified ranges
    )
  )
)

# Convert to a factor with desired levels
age_group_bin <- factor(age_group_bin, 
                        levels = c("Age 18-64 years", "Age 65 years and older", "Missing age group bin"))

cohort_tmp0 <- cohort |> 
  select(PERSON_ID, date_of_birth, study_start_date, study_end_date, date_of_death, study_start_age, sex, ethnicity_5_group, lsoa, region, deprivation_index_quintiles, rural_urban_class) |>
  mutate(death = factor(ifelse(is.na(date_of_death), 0, 1), labels = c("Alive at study end", "Death during follow-up")),
         ethnicity = factor(case_when(is.na(ethnicity_5_group) ~ "Missing ethnicity",
                                      ethnicity_5_group == "White" ~ "White",
                                      ethnicity_5_group == "Asian or Asian British" ~ "Asian or Asian British",
                                      ethnicity_5_group == "Black, Black British, Caribbean or African" ~ "Black, Black British, Caribbean or African",
                                      ethnicity_5_group == "Mixed or multiple ethnic groups"  ~ "Mixed or multiple ethnic groups",
                                      ethnicity_5_group == "Other ethnic group" ~ "Other ethnic group"),
                            levels = c("White", "Asian or Asian British" , "Black, Black British, Caribbean or African", "Mixed or multiple ethnic groups", "Other ethnic group", "Missing ethnicity")),
         sex = factor(sex,  levels=c("F", "M"), labels=c("Female", "Male")),
         age_group = factor(case_when(is.na(study_start_age) ~ "Missing age group",
                                      study_start_age >= 18 & study_start_age < 65 ~ "Age 18-64 years",
                                      study_start_age >= 65 & study_start_age < 75 ~ "Age 65-74 years",
                                      study_start_age >= 75 & study_start_age < 85 ~ "Age 75-84 years",
                                      study_start_age >= 85 ~ "Age 85 years or older"), 
                            levels = c("Age 18-64 years","Age 65-74 years", "Age 75-84 years", 
                                       "Age 85 years or older", 
                                       "Missing age group")),
         region = factor(case_when(is.na(region) ~ "Missing region",
                                   region == "London" ~ "London",
                                   region == "West Midlands" ~ "West Midlands",
                                   region == "East Midlands" ~ "East Midlands",
                                   region == "North West" ~ "North West",
                                   region == "North East" ~ "North East",
                                   region == "South West" ~ "South West",
                                   region == "South East" ~ "South East",
                                   region == "East of England" ~ "East of England",
                                   region == "Yorkshire and The Humber" ~ "Yorkshire and The Humber"),
                         levels = c("London","West Midlands", "East Midlands", "North West","North East",
                                     "South West", "South East", "East of England", "Yorkshire and The Humber", "Missing region")),
         region_4groups = factor(case_when(is.na(region) ~ "Missing region 4 groups",
                                   region == "London" ~ "London",
                                   region == "West Midlands" | region == "East Midlands" | region == "East of England" ~ "Midlands and East",
                                   region == "North West" | region == "North East" | region == "Yorkshire and The Humber" ~ "North",
                                   region == "South West" | region == "South East" ~ "South"),
                         levels = c("London","Midlands and East", "North", "South", "Missing region")),
         rural_urban_class = factor(case_when(is.na(rural_urban_class) ~ "Missing class",
                                              rural_urban_class == "Urban" ~ "Urban",
                                              rural_urban_class == "Rural" ~ "Rural"),
                                    levels = c("Urban", "Rural", "Missing class")),
         deprivation_index_quintiles = factor(deprivation_index_quintiles, ordered=TRUE),
         deprivation_bin = factor(case_when(is.na(deprivation_index_quintiles) ~ "Missing deprivation index",
                                            deprivation_index_quintiles == 1 | deprivation_index_quintiles == 2 
                                            | deprivation_index_quintiles == 3 ~ "Low deprivation index (1-3)",
                                            deprivation_index_quintiles == 4 | deprivation_index_quintiles == 5  ~ "High deprivation index (4-5)"), 
                                  levels = c("Low deprivation index (1-3)", "High deprivation index (4-5)", "Missing deprivation index"))) |>
  select(PERSON_ID, date_of_birth, study_start_date, study_end_date, death, date_of_death, study_start_age, age_group, sex, ethnicity, rural_urban_class, region, region_4groups, deprivation_index_quintiles, deprivation_bin)

cohort_tmp0$age_group_bin <- age_group_bin
cohort_tmp0$age_group <- droplevels(cohort_tmp0$age_group)
cohort_tmp0$age_group_bin <- droplevels(cohort_tmp0$age_group_bin)
cohort_tmp0$rural_urban_class <- droplevels(cohort_tmp0$rural_urban_class)
cohort_tmp0$region <- droplevels(cohort_tmp0$region)
cohort_tmp0$region_4groups <- droplevels(cohort_tmp0$region_4groups)
cohort_tmp0$deprivation_bin <- droplevels(cohort_tmp0$deprivation_bin)

# Add covariates
cov_label_map <- list(
  cov_obesity_combined_flag = c("No excess weight" = 0, "Excess weight" = 1),
  cov_comorb_comp_flag = c("No comorbidities" = 0, "Comorbidities" = 1),
  cov_depression_flag = c("No depression" = 0, "Depression" = 1),
  cov_heart_disease_flag = c("No heart disease" = 0, "Heart disease" = 1),
  cov_hypertension_flag = c("No hypertension" = 0, "Hypertension" = 1),
  cov_stroke_flag = c("No stroke" = 0, "Stroke" = 1),
  cov_diabetes_flag = c("No diabetes" = 0, "Diabetes" = 1),
  cov_ckd_flag = c("No chronic kidney disease" = 0, "Chronic kidney disease" = 1)
)

cohort_tmp1 <- covariates |> 
  select(PERSON_ID, cov_smoking_status, ends_with("flag")) |>
  select(-cov_bmi_obesity_flag) |>
  right_join(cohort_tmp0, by="PERSON_ID") |>
  mutate(cov_smoking_status = factor(case_when(is.na(cov_smoking_status) ~ "Missing smoking status",
                                    cov_smoking_status == "smoking_current" ~ "Current smoker", 
                                    cov_smoking_status == "smoking_never" ~ "Never smoked",
                                    cov_smoking_status == "smoking_ex" ~ "Ex-smoker"), 
                                    levels=c("Never smoked", "Ex-smoker", "Current smoker", "Missing smoking status")),
         across(ends_with("flag"), 
                ~ {
                  column_name <- cur_column()
                  labels <- cov_label_map[[column_name]]
                  # Replace NA with 0 and convert to factor with labels
                  factor(replace(., is.na(.), 0), levels = c(0,1), labels = names(labels))
                }
         ),
         cov_cvd_comorb_flag = case_when(
           cov_ckd_flag == "Chronic kidney disease" ~ "CVD risk factor",
           cov_hypertension_flag == "Hypertension" ~ "CVD risk factor",
           cov_diabetes_flag == "Diabetes" ~ "CVD risk factor",
           TRUE ~ "No CVD risk factor"
         ),
         cov_cvd_comorb_flag = factor(cov_cvd_comorb_flag, levels = c("No CVD risk factor", "CVD risk factor"))
         )

                            

# Create label mapping for outcomes
label_map <- c(
  "ADISS" = "Arterial dissection or ruptured aneurysm",
  "APP" = "Appendicitis",
  "arterial_event" = "Arterial thrombotic event",
  "venous_event" = "Venous thrombotic event",
  "AT" = "Other arterial thrombosis",
  "ICVT" = "Intracranial venous thrombosis",
  "MI" = "Myocardial infarction",
  "PE" = "Pulmonary embolism",
  "RI" = "Retinal infarction",
  "stroke_IS" = "Ischaemic stroke",
  "stroke_NOS" = "Stroke NOS",
  "VT" = "Venous thrombosis",
  "MC" = "Myocarditis"
)

# Select earliest event per PERSON_ID and outcome
first_events <- outcomes_long %>%
  group_by(PERSON_ID, name) %>%
  arrange(DATE) %>%           # Earliest first
  slice(1) %>%
  ungroup()

# Create *_flag columns (factor with labels)
flags <- first_events %>%
  mutate(value = 1) %>%
  pivot_wider(
    id_cols = PERSON_ID,
    names_from = name,
    values_from = value,
    values_fill = 0,
    names_prefix = "out_",
    names_sep = "_"
  ) %>%
  rename_with(~ paste0(., "_flag"), starts_with("out_")) %>%
  mutate(across(
    ends_with("_flag"),
    ~ factor(.x, levels = c(0, 1),
             labels = c(paste0("No ", label_map[gsub("out_|_flag", "", cur_column())]),
                        label_map[gsub("out_|_flag", "", cur_column())])
    )
  ))

# Create *_date columns
dates <- first_events %>%
  select(PERSON_ID, name, DATE) %>%
  pivot_wider(
    id_cols = PERSON_ID,
    names_from = name,
    values_from = DATE,
    names_prefix = "out_",
    names_sep = "_"
  ) %>%
  rename_with(~ paste0(., "_date"), starts_with("out_"))

# Merge into final wide dataset
final_data <- flags %>%
  full_join(dates, by = "PERSON_ID")

# Reorder columns so *_date follows *_flag
ordered_names <- names(flags)[-1]  # all *_flag columns (excluding PERSON_ID)
date_names <- gsub("_flag", "_date", ordered_names)

# Final column order: PERSON_ID, out_XXX_flag, out_XXX_date, ...
final_col_order <- c("PERSON_ID", as.vector(rbind(ordered_names, date_names)))

# Reorder final_data
final_data <- final_data[, final_col_order]

outcomes_wide <- final_data

# Merge with cohort and fill NAs in *_flag with "No <label>"
cohort_tmp2 <- outcomes_wide %>%
  select(PERSON_ID, ends_with("date"), ends_with("flag")) %>%
  right_join(cohort_tmp1, by = "PERSON_ID") %>%
  mutate(across(
    ends_with("_flag"),
    ~ if (is.factor(.)) {
      lvls <- levels(.)
      replace(., is.na(.), lvls[1])
    } else {
      .
    }
  ))


# Make and add COVID diagnosis, COVID hospitalisation and COVID vaccination variables

# Create summary data with one row per person_id
df_covid_summ <- exposures_covid |>
  filter(!is.na(DATE)) |> #drop anyone without covid
  group_by(PERSON_ID) |>
  summarize(
    exp_covid_diagnosis = 1,
    exp_covid_hosp = factor(ifelse(any(covid_phenotype == "02_Covid_admission_primary_position"), 1, 0), levels=c(0,1), labels=c("No COVID hospitalisation", "COVID hospitalisation")))

cohort_tmp3 <- cohort_tmp2 |>
  left_join(df_covid_summ, by="PERSON_ID") |>
  mutate(exp_covid_diagnosis =  factor(ifelse(is.na(exp_covid_diagnosis), 0, exp_covid_diagnosis), levels = c(0,1), labels = c("No COVID diagnosis", "COVID diagnosis")),
         exp_covid_hosp = forcats::fct_explicit_na(exp_covid_hosp, na_level="No COVID hospitalisation"))

# Vaccination categorical variable

cohort_tmp4 <- exposures_vacc |> 
  select(PERSON_ID, vaccination_count) |>  
  right_join(cohort_tmp3, by="PERSON_ID") |> 
  mutate(exp_covid_vacc = factor(case_when(vaccination_count == 0 ~ 0, 
                                       vaccination_count == 1 ~ 1,
                                       vaccination_count >= 2 ~ 2),
                             labels=c("No vaccine", "1 vaccine", "2 or more vaccines"))) |>
 select(-vaccination_count)

# Create flag per outcome that indicates if someone had COVID19 within 6 months before first event
cohort_selected <- cohort_tmp4 |>
  select(PERSON_ID, out_MI_date, out_arterial_event_date, out_venous_event_date, out_stroke_IS_date)#, out_MC_date)

# Merging cohort with covid data
merged_data <- exposures_covid |>
  filter(!is.na(DATE)) |> #drop anyone without covid
  select(PERSON_ID, DATE) |>
  left_join(cohort_selected, by = "PERSON_ID") |>
  mutate(DATE = as.Date(DATE))  # Ensure DATE is in Date format

# Make COVID19 diagnosis indicators
merged_data <- merged_data |>
  mutate(
    out_MI_COVID19 = ifelse(is.na(out_MI_date), 0, as.numeric(DATE >= (as.Date(out_MI_date) - 180) & DATE <= as.Date(out_MI_date))),
    out_arterial_event_COVID19 = ifelse(is.na(out_arterial_event_date), 0, as.numeric(DATE >= (as.Date(out_arterial_event_date) - 180) & DATE <= as.Date(out_arterial_event_date))),
    out_venous_event_COVID19 = ifelse(is.na(out_venous_event_date), 0, as.numeric(DATE >= (as.Date(out_venous_event_date) - 180) & DATE <= as.Date(out_venous_event_date))),
    out_stroke_IS_COVID19 = ifelse(is.na(out_stroke_IS_date), 0, as.numeric(DATE >= (as.Date(out_stroke_IS_date) - 180) & DATE <= as.Date(out_stroke_IS_date)))
  )

# Collapsing data to one row per PERSON_ID, ensuring any COVID-19 within 6 months is recorded
covid_flags <- merged_data |>
  group_by(PERSON_ID) |>
  summarise(
    out_MI_COVID19 = max(out_MI_COVID19, na.rm = TRUE),
    out_arterial_event_COVID19 = max(out_arterial_event_COVID19, na.rm = TRUE),
    out_venous_event_COVID19 = max(out_venous_event_COVID19, na.rm = TRUE),
    out_stroke_IS_COVID19 = max(out_stroke_IS_COVID19, na.rm = TRUE)
  )


# Ensuring all cohort members are included, setting missing values to 0
cohort_tmp5 <- cohort_tmp4 |>
  left_join(covid_flags, by = "PERSON_ID") |>
  mutate(
    out_MI_COVID19 = replace_na(out_MI_COVID19, 0) |>
      factor(levels = c(0,1), labels = c("no COVID19 within six months before outcome", "COVID19 within six months before myocardial infarction")),
    out_arterial_event_COVID19 = replace_na(out_arterial_event_COVID19, 0) |>
      factor(levels = c(0,1), labels = c("no COVID19 within six months before outcome", "COVID19 within six months before arterial event")),
    out_venous_event_COVID19 = replace_na(out_venous_event_COVID19, 0) |>
      factor(levels = c(0,1), labels = c("no COVID19 within six months before outcome", "COVID19 within six months before venous event")),
    out_stroke_IS_COVID19 = replace_na(out_stroke_IS_COVID19, 0) |>
      factor(levels = c(0,1), labels = c("no COVID19 within six months before outcome", "COVID19 within six months before ischaemic stroke"))
  )

cohort_final <- cohort_tmp5

#### Get the percentiles of mean daily air temperature and daily air pollution for the study period in England ####

region_data <- region_lookup |>
  select('lsoa_code','region_name') |> 
  rename('lsoa' = 'lsoa_code', 
         'region' = 'region_name')
  
env_study <- env |>
  filter(date >= "2020-01-01" & date <= "2022-12-31" ) |>
  rename('lsoa' = 'LSOA11CD') |>
  left_join(region_data, by='lsoa') 

env_distribution <- calculate_distribution(env_study)

tmean_days_region <- env_study |>
  mutate(temp_rounded = round(tmean)) |> 
  filter(grepl("^E", lsoa)) |>
  group_by(region, temp_rounded) |>                        
  summarize(days_count = n_distinct(date), .groups = 'drop') |>
  filter(!is.na(region))

tmean_days_england <- env_study |>
  mutate(temp_rounded = round(tmean)) |>  
  filter(grepl("^E", lsoa)) |>
  group_by(temp_rounded) |>                        
  summarize(days_count = n_distinct(date), .groups = 'drop') |>
  mutate(region = "England")

tmean_days_england <- tmean_days_england |> select(colnames(tmean_days_region))

tmean_days <- rbind(tmean_days_england, tmean_days_region)


#### Save ####
save(env_distribution, filename="tmean_pm2p5_distribution")
output(env_distribution, "tmean_pm2p5_distribution")
output(tmean_days, "tmean_days")
save(table = cohort_final, filename="cohort_final")

              
keep <- c("cohort_final")
rm(list = setdiff(ls(), keep))              
gc()
