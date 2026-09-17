#*******************************************************************************
#
# Project: CCU076
# Title:   Associations between air temperature and cardiovascular health outcomes
# Date:    09-07-2025
# Author:  Isabel Walter
# Purpose: Combine all first stage model coefficients for output.
#
#*******************************************************************************

#### Preparation ####

# Load libraries
library(dplyr) ; library(stringr)

setwd("/db-mnt/databricks/rstudio_collab/CCU018/CCU076")

# Source function
fs <- c("retrieve_data")
for (f in fs) source(paste0("/db-mnt/databricks/rstudio_collab/CCU018/CCU076/source/", f, ".R"))


# Get list of target .rds files in the 'data' folder
# files <- list.files(
#   path = "data",
#   pattern = "crossreduce.*150000.*\\.rds$",
#   full.names = TRUE
# )

# Get list of target .rds files in the 'data' folder, filtering by size pattern and date
files <- list.files(
  path = "data",
  pattern = "^crossreduce.*(275000).*\\.rds$",
  full.names = TRUE
)

# Exclude any file with 'covid_fatal' in the name
files <- files[!grepl("covid_fatal", files)]

# Keep only files modified on or after 12 october 2025
files <- files[file.info(files)$mtime >= as.POSIXct("2025-10-12")]

# Remove old subgrouping models
files <- files[!grepl("subgroup_new", files)]

# Remove crossbasis parameter testing files, they are exported seperately
files <- files[!grepl("cb_param", files)]

# Mapping for subgroup values to variable names
subgroup_map <- list(
  # sex
  "Male" = "sex",
  "Female" = "sex",

  # deprivation_bin
  "Low deprivation index (1-3)" = "deprivation_bin",
  "High deprivation index (4-5)" = "deprivation_bin",
  "Missing deprivation index" = "deprivation_bin",
  
  # ethnicity
  "White" = "ethnicity",
  "Asian or Asian British" = "ethnicity",
  "Black, Black British, Caribbean or African" = "ethnicity",
  "Mixed or multiple ethnic groups" = "ethnicity",
  "Other ethnic group" = "ethnicity",
  "Missing ethnicity" = "ethnicity",
  
  # comorbidities
  "Comorbidities" = "cov_comorb_comp_flag",
  "No comorbidities" = "cov_comorb_comp_flag",
  
  # obesity
  "Excess weight" = "cov_obesity_combined_flag",
  "No excess weight" = "cov_obesity_combined_flag",
  
  # region
  "London" = "region",
  "West Midlands" = "region",
  "East Midlands" = "region",
  "North West" = "region",
  "North East" = "region",
  "South West" = "region",
  "South East" = "region",
  "East of England" = "region",
  "Yorkshire and The Humber" = "region",
  
  # Urban rural class
  "Urban" = "rural_urban_class",
  "Rural" = "rural_urban_class",
  
  # Smoking status,
  "Never smoked" = "smoking_status",
  "Current smoker" = "smoking_status",
  "Ex-smoker"= "smoking_status",
  "Missing smoking status" = "smoking_status",
  
  # Covid status
  "outcome with covid" = "covid_status",
  "outcome without covid" = "covid_status"
  
)

# Initialize list to store loaded and processed data
all_data <- list()

# Loop over each file
for (i in seq_along(files)) {
  file <- files[i]
  file_prefix <- str_remove(basename(file), "\\.rds$")
  file_name <- basename(file)
  
  df <- readr::read_rds(paste0("/db-mnt/databricks/rstudio_collab/CCU018/CCU076/data/",file_prefix, ".rds"))
  df$interaction_chisq <- ifelse(!is.null(df$interaction_chisq), as.character(df$interaction_chisq), "not applicable")
  df <- df |>
    mutate(
      across(everything(), as.character),
      across(c(size, nevents), as.numeric)
    )
  
  
  if (str_detect(file_name, "interaction_")) {
    analysis_var <- str_extract(file_name, "(?<=interaction_).+?(?=\\.rds)")
    subgroup_value <- df$subgroup
    if (str_detect(file_name, "lag_sensitivity")) {
      lag <- str_extract(file_name, "\\d{1,2}d_lag")
      lag_analysis <- paste0(lag, "_sensitivity")
      analysis_var <- paste0(lag_analysis, "_", analysis_var)
    }
    df$analysis <- analysis_var
    df$subgroup <- subgroup_value
  } else {
    analysis_str <- stringr::str_extract(
      file_name,
      "(?<=batchsize_(275000)_).+?(?=\\.rds$)"
    )
    
    # Remove leading underscore if it exists
    analysis_str <- stringr::str_remove(analysis_str, "^_")
    
    # remove "new_" anywhere in the extracted text
    analysis_str <- stringr::str_replace_all(analysis_str, "_new", "")
    
    if (str_detect(analysis_str, "lag_sensitivity")) {
      lag <- str_extract(file_name, "\\d{1,2}d_lag")
      analysis <- paste0(lag, "_sensitivity")
    } else {
      analysis <- analysis_str
    }
    
    df$analysis <- analysis
    df$subgroup <- "not applicable"
  }
  
  all_data[[length(all_data) + 1]] <- df
}


# Combine all data frames into one
final_data <- bind_rows(all_data)


# Round to nearest 5
final_data$nevents <- round(final_data$nevents / 5) * 5
final_data$size <- round(final_data$size / 5) * 5

# Suppress values <10 (but keep 0)
final_data$nevents <- ifelse(final_data$nevents > 0 & final_data$nevents < 10, "<10", as.character(final_data$nevents))
final_data$size <- ifelse(final_data$size > 0 & final_data$size < 10, "<10", as.character(final_data$size))

write.csv(final_data, file = "/db-mnt/databricks/rstudio_collab/CCU018/CCU076/output/first_stage_models_venous_events_covid6mo.csv", row.names = FALSE)

#output(final_data, filename="first_stage_models")
