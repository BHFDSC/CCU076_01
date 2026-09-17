#*******************************************************************************
#
# Project: CCU076
# Title:   Associations between air temperature and cardiovascular health outcomes
# Date:    16-06-2025
# Author:  Isabel Walter 
# Purpose: Compare different spline parameterisations of the venous event crossbasis. 
#
#*******************************************************************************


# Libraries
library(here)
library(dplyr)

# Source function
fs <- c("retrieve_data")
for (f in fs) source(here::here("source", paste0(f, ".R")))

# List only files ending with 'B.rds' (e.g., withB.rds or noB.rds)
files <- list.files(here("data"), pattern = "^crossreduce_cb_param_ve_fatal21d_lagbatchsize250000_temp_knots_.*_lag_knots_\\d\\.rds$", full.names = FALSE)

# Remove ".rds" from filenames for compatibility with load_data()
file_ids <- sub("\\.rds$", "", files)

# Initialize list to store results
results <- list()

# Loop through files
for (file in file_ids) {
  df <- load_data(file)  # Load using your custom function
  
  # Check for lowercase 'aic' column
  if ("aic" %in% names(df)) {
    mean_aic <- mean(df$aic, na.rm = TRUE)
    results[[file]] <- mean_aic
  }
}

# Convert to data frame and sort
aic_df <- data.frame(
  filename = names(results),
  mean_aic = unlist(results),
  row.names = NULL
) %>%
  arrange(mean_aic)

# Output results
output(aic_df, filename = "crossbasis_parameterisation")
