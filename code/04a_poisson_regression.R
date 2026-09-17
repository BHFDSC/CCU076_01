#*******************************************************************************
#
# Project: CCU076
# Title:   Associations between air temperature and cardiovascular health outcomes
# Date:    16-06-2025
# Author:  Isabel Walter 
# Purpose: Run poisson regression for the association between air temperature 
#          and CVD outcomes and COVID19, unblock code sections for specific sensitivity analyses, change
#          cts() input for subgroup and lag day analyses. For sensitivity analyses change output name.
#
#*******************************************************************************


# Load libraries
libs <- c("dplyr", "dlnm", "gnm", "pbs", "lubridate", "mgcv", "splines", "fixest", "data.table")

# Load libraries in a loop
for (lib in libs) {
  if (!requireNamespace(lib, quietly = TRUE)) {
    install.packages(lib, dependencies = TRUE)
  }
  library(lib, character.only = TRUE)
}

# Source function
fs <- c("retrieve_data", "time_series_month", "vechMat_replacement", "crossbasis_cpp")
for (f in fs) source(here::here("source", paste0(f, ".R")))


# Function that runs poisson regression and saves model
run_model <- function(filename, lagdays, ts, subgroup = NULL){
  
  # Make outcome indicator and environmental exposure numeric
  ts$outcome_ind <- as.numeric(ts$outcome_ind)
  ts$tmean <- as.numeric(ts$tmean)
  
  # Set crossbasis parameters 
  print("Set crossbasis parameters")
  
  # Argvar
  varfun <- "ns"
  distribution <- load_data("tmean_pm2p5_distribution")
  
  region_list <- c("London", "West Midlands", "East Midlands",
                   "North West", "North East", "South West", "South East",
                   "East of England", "Yorkshire and The Humber")
  
  # Default column name
  temp_col <- "tmean_20_22"
  
  # In case of the regional subgroup analysis use regional environmental distribution 
  if (!is.null(subgroup) && subgroup %in% region_list) {
    regional_col <- paste0("tmean_20_22_", subgroup)
    
    if (regional_col %in% names(distribution)) {
      temp_col <- regional_col
      message(paste("Using region-specific temperature percentiles:", temp_col))
    } else {
      warning(paste("Column", regional_col, "not found in distribution. Falling back to general tmean_20_22"))
    }
  }
  
  temp_knots <- distribution[[temp_col]][match(c("10.0%", "75.0%", "90.0%"), distribution$percentile)]
  temp_b_knots <- distribution[[temp_col]][match(c("1.0%", "99.0%"), distribution$percentile)]
  cen <-  distribution[[temp_col]][match(c("50.0%"), distribution$percentile)]
  rm(distribution)
  
  # Arglag
  lagfun <- "ns"

  print("create batch numbers for each individual")
  # Split up time series in batches for computational feasibility
  
  # Step 1: Batch the time series (ts) as in your original code
  batch_size <- 275000
  
  # Get unique individuals and shuffle them
  set.seed(32)  # For reproducibility
  unique_persons <- ts |>
    distinct(PERSON_ID) |>
    sample_frac(1)  # Shuffle for randomness
  
  # Calculate the number of batches needed
  num_batches <- ceiling(nrow(unique_persons) / batch_size)
  
  # Assign batch numbers evenly
  unique_persons <- unique_persons |>
    mutate(batch = rep(1:num_batches, length.out = n()))
  
  # Merge batch assignment back to the full dataset
  ts <- ts |>
    left_join(unique_persons, by = "PERSON_ID")
  
  print("Save batches of time series")
  # Save each batch separately
  for (batch_number in 1:num_batches) {
    ts_batch <- ts[ts$batch == batch_number, ]
    save(ts_batch, filename = paste0("batch_original_", batch_number))
  }
  
  # Clean up full ts from memory
  rm(ts)
  gc()
  
  # Initialise an empty list to store parameter data frames for each batch
  parlist <- list()
  
  
  print("Loop through each batched time series dataset")
  # Loop through each batched time series dataset
  for (batch_number in 1:num_batches) {
  
    print(paste0("batch_number: ", batch_number))
    
    # Load batch
    ts_subset <- load_data(paste0("batch_original_", batch_number))
    
    
    
    # Create crossbasis on subset
    print(paste0("create crossbasis of batch_number: ", batch_number))
    
    nk <- ifelse(lagdays > 7, 3, 2)
    
    environment(crossbasis_cpp) <- asNamespace("dlnm")
    
    # Parameterise the CB of temperature
    argvar <- list(fun=varfun, knots=temp_knots, Boundary.knots=temp_b_knots)

    arglag <- list(fun=lagfun, knots=logknots(lagdays,nk=nk))
    
    cb <- crossbasis_cpp(x=ts_subset$tmean, 
                                lag=lagdays, 
                                argvar=argvar,
                                arglag=arglag,
                                group=ts_subset$PERSON_ID)
    
    # Limit crossbasis, and time series to study period
    # Ensure both columns are Date type
    ts_subset$time_series_date <- as.Date(ts_subset$time_series_date)
    ts_subset$study_start_date <- as.Date(ts_subset$study_start_date)
    
    # Create logical index of valid rows (on or after study start date)
    valid_rows <- ts_subset$time_series_date >= ts_subset$study_start_date
    
    # Subset the dataset and all model components
    ts_subset   <- ts_subset[valid_rows, ]
    cb_subset   <- cb[valid_rows, ]
    
    # Get crossbasis reference attributes
    cb_attr <- c("range", "group", "class","df", "argvar", "arglag", "lag")
    
    for (i in cb_attr){
      attr(cb_subset, i) <- attributes(cb)[[i]] 
    }
    
    # Create term to stratify for month, year, and person id
    ts_subset$group <- interaction(ts_subset$PERSON_ID, ts_subset$month, ts_subset$year, sep=":")
    ts_subset <- ts_subset |> select(-month, -year)
    
    print(paste0("Fit poisson model ", batch_number))
    
    #  Identify groups that contain at least one outcome == 1
    groups_with_event <- unique(ts_subset$group[ts_subset$outcome_ind == 1])
    
    keep_rows <- ts_subset$group %in% groups_with_event
    
    # Subset both ts_subset and cb_subset using the same filter
    ts_subset <- ts_subset[keep_rows, ]
    cb_subset <- cb_subset[keep_rows, , drop = FALSE] 
    
    for (i in cb_attr){
      attr(cb_subset, i) <- attributes(cb)[[i]] 
    }
    
    rm(cb)
    
    model <- gnm(
      outcome_ind ~ cb_subset + factor(dayofweek), 
      data=ts_subset, 
      family=poisson,
      eliminate=group
     )
    
    print(paste0("Cross-reduce model ", batch_number))
    
    redpred <- crossreduce(cb_subset, model, cen = cen)
    
    # Check crosspred output
    cp <- crosspred(cb_subset, model,cen=cen, by=1, cumul=TRUE) 
    
    # Quick plot example
    png(here::here("figures/", paste0("tmean", lagdays,"d_exp_response_", filename, "_batchsize_", batch_size, "_batch_", batch_number, "_sensitivity_restricted_study_period_may", ".png")), width = 2000, height = 1500, res=300)
    plot(cp, "overall", xlab="Temperature", ylab="IRR of event", col=3,
         main=paste0("Temperature 0-", lagdays, "d lag: overall exposure-response"), ylim=c(0.5,3))
    dev.off()
    
    print(paste0("Store batch parameters ", batch_number))
    
    # Store parameters (coef + vectorized vcov)
    ncoef <- length(coef(redpred))
    par <- c(coef(redpred), vechMat_replacement(vcov(redpred)))
    names(par) <- c(paste0("coef", seq(ncoef)),
                    paste0("vcov", seq(ncoef * (ncoef + 1) / 2)))
    
    # Store batch information
    batch_par <- data.frame(
        outcome = filename,
        subgroup = ifelse(is.null(subgroup), "not applicable", subgroup),
        batch_number = batch_number,
        size = round(length(unique(ts_subset$PERSON_ID)) / 5) * 5,
        nevents=sum(ts_subset$outcome_ind,na.rm=T),
        aic = AIC(model),
        conv=model$converged,
        disp=sum(residuals(model,type="pearson")^2, na.rm=T)/model$df.residual,
        t(par),
        row.names = NULL
      )
      
    # Append to the list of parameters for each batch
    parlist[[batch_number]] <- batch_par
    
    print(paste0("Save model, time series, and crossbasis of revelant risk sets batch ", batch_number))
    
    # Save model, batched time series, and crossbasis of relevant risk sets
    save(model, filename = paste0("model_",filename, lagdays,"d_lag","_batchsize_",batch_size,"_sensitivity_restricted_study_period_may", "_batch_", batch_number))
    save(ts_subset, filename = paste0("ts_subset_",filename, lagdays,"d_lag","_batchsize_",batch_size,"_sensitivity_restricted_study_period_may", "_batch_", batch_number))
    save(cb_subset, filename = paste0("cb_subset_",filename, lagdays,"d_lag","_batchsize_",batch_size,"_sensitivity_restricted_study_period_may", "_batch_", batch_number))
  
    rm(ts_subset, cb_subset, model, redpred, valid_rows)
    
    # Delete batch file
    file.remove(here::here("data", paste0("batch_original_", batch_number, ".rds")))
    gc()
  }
  
  print("Combine all batch results in dataframe and save")
  # Combine all batch results into one data frame
  par_df <- do.call(rbind, parlist)
  
  # Save
  save(par_df, filename = paste0("crossreduce_",filename, lagdays,"d_lag","_batchsize_",batch_size,"_sensitivity_restricted_study_period_may"))
}


# Function that prepares data and then calls function to run poisson regression
cts <- function(filename, lagdays, stratification = NULL){
  
  print("load time series")
  
  # Load ts
  
  ts <- load_data(paste0("time_series_", filename))
  
  #cat("Number of distinct individuals in dataset:", length(unique(ts$PERSON_ID)), "\n")
  
  # ----------------------------------------------------------------------------
  # Code section: non fatal cases sensitivity analysis
  # ----------------------------------------------------------------------------
  
  # Filter to only non fatal cases
  # nonfatal_persons <- load_data(paste0("ccu076_01_out_time_series_", sub("_.*", "", filename), "_nonfatal_month_individual_level_summary"))
  # nonfatal_ids <- unique(nonfatal_persons$PERSON_ID)
  # ts <- ts[ts$PERSON_ID %in% nonfatal_ids, ]

  # ----------------------------------------------------------------------------
  # Code section end
  # ----------------------------------------------------------------------------
  
  # ----------------------------------------------------------------------------
  # Code section: fatal cases sensitivity analysis
  # ----------------------------------------------------------------------------
  
  # Filter to only fatal cases
  #nonfatal_persons <- load_data(paste0("ccu076_01_out_time_series_", sub("_.*", "", filename), "_nonfatal_individual_level_summary"))
  #nonfatal_ids <- unique(nonfatal_persons$PERSON_ID)
  #ts <- ts[!(ts$PERSON_ID %in% nonfatal_ids), ]
  
  #cat("Number of distinct individuals in fatal dataset:", length(unique(ts$PERSON_ID)), "\n")
  
  # ----------------------------------------------------------------------------
  # Code section end
  # ----------------------------------------------------------------------------
  
  # ----------------------------------------------------------------------------
  # Code section: changed start date sensitivity analysis
  # ----------------------------------------------------------------------------
  
  # Set study_start_date column for all rows
  ts <- ts |>
    mutate(
       study_start_date = as.Date("2020-05-01"),
       time_series_date = as.Date(time_series_date)
    )

  # Filter to time_series_date > (study_start_date - 21 days)
   filtered_ts <- ts |>
    filter(time_series_date > (study_start_date - days(21)))

  # Find PERSON_IDs with no outcome_ind == 1 after study_start_date
   persons_to_remove <- filtered_ts |>
    filter(time_series_date > study_start_date) |>
    group_by(PERSON_ID) |>
    summarise(has_outcome = any(outcome_ind == 1), .groups = 'drop') |>
    filter(!has_outcome) |>
    pull(PERSON_ID)

  # Remove those PERSON_IDs entirely
  ts <- filtered_ts |>
    filter(!(PERSON_ID %in% persons_to_remove))

  # Print how many persons were removed
  cat("Removed", length(persons_to_remove), "persons from dataset.\n")

  cat("Minimum time_series_date:", format(min(ts$time_series_date, na.rm = TRUE)), "\n")

  rm(filtered_ts, persons_to_remove)
  
  # ----------------------------------------------------------------------------
  # Code section end
  # ----------------------------------------------------------------------------
  
  # Ensure every person has at least 1 outcome event
  n_before <- dplyr::n_distinct(ts$PERSON_ID)
  
  ts <- ts |>
    dplyr::group_by(PERSON_ID) |>
    dplyr::filter(any(outcome_ind == 1)) |>
    dplyr::ungroup()
  
  cat("Removed", n_before - dplyr::n_distinct(ts$PERSON_ID), "individuals without outcome_ind == 1\n")
  
  rm(n_before)
  gc()
  
  # Make calendar month column
  
  ts <- time_series_month(ts)
  
  # Ensure dataset is sorted by 'person_id' and 'time_series_date'
  ts <- ts[order(ts$PERSON_ID, ts$time_series_date),]
  
  # ----------------------------------------------------------------------------
  # Code section: first outcome event sensitivity analysis
  # ----------------------------------------------------------------------------
  
  # filter to first ouctome event
  # Convert to data.table
  # ts <- as.data.table(ts)
  # 
  # # Modify outcome_ind to keep only the first event per person
  # ts[, outcome_ind := as.integer(seq_len(.N) == which(outcome_ind == 1)[1]), by = PERSON_ID]
  # 
  # # Convert back to data.frame
  # ts <- as.data.frame(ts)
  # 
  # # Count outcome events
  # n_outcomes <- sum(ts$outcome_ind == 1)
  # 
  # # Count distinct persons
  # n_persons <- length(unique(ts$PERSON_ID))
  # 
  # # Print results
  # cat("Number of outcome events: ", n_outcomes, "\n")
  # cat("Number of unique persons: ", n_persons, "\n")
  # 
  # # Stop if mismatch
  # if (n_outcomes != n_persons) {
  #   stop("Mismatch: Number of outcome_ind == 1 does not equal number of distinct persons.")
  # }
  # 
  # ts <- ts[order(ts$PERSON_ID, ts$time_series_date),]

  # ----------------------------------------------------------------------------
  # Code section end
  # ----------------------------------------------------------------------------
  
  
  ts <- ts |> select(PERSON_ID, study_start_date, time_series_date, outcome_ind, tmean, dayofweek, month, year)
  
  
  # Run modelling for subgroup if stratification analysis, else for full cohort
  
  if(!is.null(stratification)){
    
    cohort <- load_data("cohort_final")
    
    subset_list <- split(cohort[["PERSON_ID"]], cohort[[stratification]])
    rm(cohort)
    
    # Filter time series by person_id for each subgroup
    filtered_time_series_list <- lapply(names(subset_list), function(subgroup) {
      filter(ts, PERSON_ID %in% subset_list[[subgroup]])
    })
    
    # Name the list with the stratification groups for easier access
    names(filtered_time_series_list) <- names(subset_list)
    rm(subset_list, ts)
    
    # Find which groups are empty
    empty_groups <- names(filtered_time_series_list)[vapply(filtered_time_series_list, nrow, integer(1)) == 0]
    
    # If any empty groups, print them
    if (length(empty_groups) > 0) {
      message("Dropped groups (0 rows): ", paste(empty_groups, collapse = ", "))
    }
    
    # Remove empty ones
    filtered_time_series_list <- Filter(function(x) nrow(x) > 0, filtered_time_series_list)
    
    
    lapply(names(filtered_time_series_list), function(name) {
      run_model(filename=filename,
                lagdays=lagdays,
                ts = filtered_time_series_list[[name]],
                subgroup=name)
    })
    
    # If no subgrouping needed
  } else {
    run_model(filename=filename, 
              lagdays=lagdays, 
              ts=ts
    )
    
  }
  
}

setwd("/db-mnt/databricks/rstudio_collab/CCU018/CCU076")

# Example calls for subgroups and 21 lag days
# cts("ve_fatal", 21, "sex")
# cts("ve_fatal", 21, "age_group_bin")
# cts("ve_fatal", 21, "ethnicity")
# cts("ve_fatal", 21, "rural_urban_class")
# cts("ve_fatal", 21, "region")
# cts("ve_fatal", 21, "deprivation_bin")
# cts("ve_fatal", 21, "cov_comorb_comp_flag")
# cts("ve_fatal", 21, "cov_obesity_combined_flag")
# 
# cts("mi_fatal", 21, "sex")
# cts("mi_fatal", 21, "age_group_bin")
# cts("mi_fatal", 21, "ethnicity")
# cts("mi_fatal", 21, "rural_urban_class")
# cts("mi_fatal", 21, "region")
# cts("mi_fatal", 21, "deprivation_bin")
# cts("mi_fatal", 21, "cov_comorb_comp_flag")
# cts("mi_fatal", 21, "cov_obesity_combined_flag")

#cts("mi_fatal", 14, "sex")
#cts("mi_fatal", 14, "age_group_bin")
#cts("mi_fatal", 14, "ethnicity")
#cts("mi_fatal", 14, "rural_urban_class")
#cts("mi_fatal", 14, "region")
#cts("mi_fatal", 14, "deprivation_bin")
#cts("mi_fatal", 14, "cov_comorb_comp_flag")
#cts("mi_fatal", 14, "cov_obesity_combined_flag")
# 
# cts("stroke_fatal", 21, "sex")
# cts("stroke_fatal", 21, "age_group_bin")
# cts("stroke_fatal", 21, "ethnicity")
# cts("stroke_fatal", 21, "rural_urban_class")
# cts("stroke_fatal", 21, "region")
# cts("stroke_fatal", 21, "deprivation_bin")
# cts("stroke_fatal", 21, "cov_comorb_comp_flag")
# cts("stroke_fatal", 21, "cov_obesity_combined_flag")
# 
# cts("ae_fatal", 21, "sex")
# cts("ae_fatal", 21, "age_group_bin")
# cts("ae_fatal", 21, "ethnicity")
# cts("ae_fatal", 21, "rural_urban_class")
# cts("ae_fatal", 21, "region")
# cts("ae_fatal", 21, "deprivation_bin")
#cts("ae_fatal", 21, "cov_comorb_comp_flag")
#cts("ae_fatal", 21, "cov_obesity_combined_flag")

cts("ve_fatal", 21)
cts("mi_fatal", 21)
cts("stroke_fatal", 21)
cts("ae_fatal", 21)

# cts("ve_fatal", 21, "cov_smoking_status")
#cts("mi_fatal", 14, "cov_smoking_status")
# cts("stroke_fatal", 21, "cov_smoking_status")
# cts("ae_fatal", 21, "cov_smoking_status")
#cts("covid_fatal", 21, "cov_smoking_status")
# 
# cts("covid_fatal", 21, "sex")
# cts("covid_fatal", 21, "age_group_bin")
# cts("covid_fatal", 21, "ethnicity")
# cts("covid_fatal", 21, "rural_urban_class")
# cts("covid_fatal", 21, "region")
# cts("covid_fatal", 21, "deprivation_bin")
# cts("covid_fatal", 21, "cov_comorb_comp_flag")
# cts("covid_fatal", 21, "cov_obesity_combined_flag")

# cts("ve_fatal", 14)
# cts("mi_fatal", 14)
# cts("stroke_fatal", 14)
# cts("ae_fatal", 14)
# 
# cts("ve_fatal", 7)
# cts("mi_fatal", 7)
# cts("stroke_fatal", 7)
# cts("ae_fatal", 7)
  
# cts("ve_fatal", 4)
# cts("mi_fatal", 4)
# cts("stroke_fatal", 4)
# cts("ae_fatal", 4)
