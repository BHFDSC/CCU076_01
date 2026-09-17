#*******************************************************************************
#
# Project: CCU076
# Title:   Associations between air temperature and cardiovascular health outcomes
# Date:    16-06-2025
# Author:  Isabel Walter 
# Purpose: Run poisson regression for the association between air temperature 
#          and COVID19 and CVD outcomes, with different adjustments 
#             
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
run_model <- function(filename, lagdays, ts, subgroup = NULL, adjust = NULL){
  
  # Make outcome indicator and environmental exposure numeric
  ts$outcome_ind <- as.numeric(ts$outcome_ind)
  ts$COVID19_ind <- as.numeric(ts$COVID19_ind)
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
  
  # If air pollution adjustment; set study period to 2020-2021 (no air pollution data after)
  if (!is.null(adjust) && adjust == "air_pollution") {
    temp_col <- "tmean_20_21"
    message(paste("Using temperature percentiles for 2020-2021:", temp_col))
    
    pm2p5_col <- "pm2p5_20_21"
    pm2p5_knots <- distribution[[pm2p5_col]][match(c("10.0%", "75.0%", "90.0%"), distribution$percentile)]
    pm2p5_b_knots <-  distribution[[pm2p5_col]][match(c("1.0%", "99.0%"), distribution$percentile)]
    pm2p5_cen <-  distribution[[pm2p5_col]][match(c("50.0%"), distribution$percentile)]
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
    save(ts_batch, filename = paste0("adj_batch_", batch_number))
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
    ts_subset <- load_data(paste0("adj_batch_", batch_number))
    
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
    
    attributes_cb <- attributes(cb)
    
    # Limit crossbasis, and time series to study period
    # Ensure both columns are Date type
    ts_subset$time_series_date <- as.Date(ts_subset$time_series_date)
    ts_subset$study_start_date <- as.Date(ts_subset$study_start_date)
    
    # Create logical index of valid rows (on or after study start date)
    valid_rows <- ts_subset$time_series_date >= ts_subset$study_start_date
    
    # Subset cb
    cb_subset   <- cb[valid_rows, ]
    rm(cb)
    gc()
    
    # make air pollution adjusted model
    # if (!is.null(adjust) && adjust == "air_pollution") {
    #   pm2p5_argvar <- list(fun=varfun, knots=pm2p5_knots, Boundary.knots=pm2p5_b_knots)
    #   
    #   pm2p5_arglag <- list(fun=lagfun, knots=logknots(7,nk=3))
    #   
    #   pm2p5_cb <- crossbasis_cpp(x=ts_subset$pm2p5, 
    #                              lag=7, 
    #                              argvar=pm2p5_argvar,
    #                              arglag=pm2p5_arglag,
    #                              group=ts_subset$PERSON_ID)
    #   
    #   attributes_pm2p5_cb <- attributes(pm2p5_cb)
    #   
    #   pm2p5_cb_subset   <- pm2p5_cb[valid_rows, ]
    #   rm(pm2p5_cb)
    #   gc()
    # }
    # 
    # subset ts to valid rows within study period
    ts_subset   <- ts_subset[valid_rows, ]
    
    # Create term to stratify for month, year, and person id
    ts_subset$group <- interaction(ts_subset$PERSON_ID, ts_subset$month, ts_subset$year, sep=":")
    ts_subset <- ts_subset |> select(-month, -year)
    
    #  Identify groups that contain at least one outcome == 1
    groups_with_event <- unique(ts_subset$group[ts_subset$outcome_ind == 1])
    
    keep_rows <- ts_subset$group %in% groups_with_event
    
    # Subset both ts_subset and cb_subset using the same filter
    ts_subset <- ts_subset[keep_rows, ]
    cb_subset <- cb_subset[keep_rows, , drop = FALSE] 
    
    # Get crossbasis reference attributes
    cb_attr <- c("range", "group", "class","df", "argvar", "arglag", "lag")
    
    for (i in cb_attr){
      attr(cb_subset, i) <- attributes_cb[[i]] 
    }
    
    # filter pm2p5 cb for months with an outcome  
    # if (!is.null(adjust) && adjust == "air_pollution") {
    #   pm2p5_cb_subset <- pm2p5_cb_subset[keep_rows, , drop = FALSE] 
    #   
    #   for (i in cb_attr){
    #     attr(pm2p5_cb_subset, i) <- attributes_pm2p5_cb[[i]] 
    #   }
      
    #}
   
    print(paste0("Fit poisson model ", batch_number))
    
    formula_list <- list(
      "air_pollution" = "outcome_ind ~ cb_subset + dayofweek + pm2p5",
      "covid19" = "outcome_ind ~ cb_subset + dayofweek + COVID19_ind",
      "lockdown" = "outcome_ind ~ cb_subset + dayofweek + lockdown"
    )
    
    # Select the formula dynamically, or use default if adjust is NULL
    if (is.null(adjust)) {
      selected_formula <- "outcome_ind ~ cb_subset + dayofweek"
    } else {
      selected_formula <- formula_list[[adjust]]
    }
    
    
    print(paste("Model formula:", selected_formula))
    
    ts_subset$dayofweek <- as.factor(ts_subset$dayofweek)
    
    model <- gnm(
      as.formula(selected_formula), 
      data=ts_subset, 
      family=poisson,
      eliminate=group
    )
    
    print(paste0("Cross-reduce model ", batch_number))
    
    redpred <- crossreduce(cb_subset, model, cen = cen)
    
    # Check crosspred output
    cp <- crosspred(cb_subset, model,cen=cen, by=1, cumul=TRUE) 
    
    # Quick plot example
    #png(here::here("figures/", paste0("tmean", lagdays,"d_exp_response_", filename, "_batchsize_", batch_size, "_batch_", batch_number, "_adjusted_",adjust, "_cb_pm2p5", ".png")), width = 2000, height = 1500, res=300)
    png(here::here("figures/", paste0("tmean", lagdays,"d_exp_response_", filename, "_batchsize_", batch_size, "_batch_", batch_number, "_adjusted_for_",adjust, ".png")), width = 2000, height = 1500, res=300)
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
    
    rm(ts_subset, cb_subset, model, redpred, valid_rows)
    
    # Delete batch file
    file.remove(here::here("data", paste0("adj_batch_", batch_number, ".rds")))
    gc()
  }
  
  print("Combine all batch results in dataframe and save")
  # Combine all batch results into one data frame
  par_df <- do.call(rbind, parlist)
  
  # Save
  #save(par_df, filename = paste0("crossreduce_",filename, lagdays,"d_lag","_batchsize_",batch_size,"_adjusted_", adjust, "_cb_pm2p5"))
  save(par_df, filename = paste0("crossreduce_",filename, lagdays,"d_lag","_batchsize_",batch_size, "_adjusted_for_", adjust))
  
}


# Function that prepares data and then calls function to run poisson regression
cts <- function(filename, lagdays, stratification = NULL, adjust = NULL){
  
  print("load time series")
  
  # Load ts
  ts <- load_data(paste0("time_series_", filename))
  
  if ((!is.null(adjust) && adjust == "air_pollution") || is.null(adjust)){
    ts <- ts |>
      filter(time_series_date <= as.Date("2021-12-31"))
    print(paste("Air pollution analysis last date in ts:", max(ts$time_series_date)))
  }

  
  
  
  # if (!is.null(adjust) && adjust == "air_pollution") {
  #   ts <- ts |>
  #     filter(time_series_date <= as.Date("2021-12-31"))
  #   
  #   print(paste("Air pollution analysis last date in ts:", max(ts$time_series_date)))
  # }
  
  ts <- ts |>
    mutate(lockdown = case_when(
      (time_series_date >= as.Date("2020-03-23") & time_series_date <= as.Date("2020-05-10")) ~ 1,
      (time_series_date >= as.Date("2020-11-05") & time_series_date <= as.Date("2020-12-02")) ~ 1,
      (time_series_date >= as.Date("2021-01-06") & time_series_date <= as.Date("2021-03-08")) ~ 1,
      TRUE ~ 0  # Default value for non-lockdown days
    ))
  
  # ----------------------------------------------------------------------------
  # Code section: non fatal cases sensitivity analysis
  # ----------------------------------------------------------------------------
  
  # Filter to only non fatal cases
  #nonfatal_persons <- load_data(paste0("ccu076_01_out_time_series_", sub("_.*", "", filename), "_nonfatal_individual_level_summary"))
  #nonfatal_ids <- unique(nonfatal_persons$PERSON_ID)
  #ts <- ts[ts$PERSON_ID %in% nonfatal_ids, ]
  
  # ----------------------------------------------------------------------------
  # Code section end
  # ----------------------------------------------------------------------------
  
  # ----------------------------------------------------------------------------
  # Code section: changed start date sensitivity analysis
  # ----------------------------------------------------------------------------
  
  # Set study_start_date column for all rows
  #ts <- ts |>
  #  mutate(
  #    study_start_date = as.Date("2020-04-01"),
  #    time_series_date = as.Date(time_series_date)
  #  )
  
  # Filter to time_series_date > (study_start_date - 21 days)
  #filtered_ts <- ts |>
  #  filter(time_series_date > (study_start_date - days(21)))
  
  # Find PERSON_IDs with no outcome_ind == 1 after study_start_date
  #persons_to_remove <- filtered_ts |>
  #  filter(time_series_date > study_start_date) |>
  #  group_by(PERSON_ID) |>
  #  summarise(has_outcome = any(outcome_ind == 1), .groups = 'drop') |>
  #  filter(!has_outcome) |>
  #  pull(PERSON_ID)
  
  # Remove those PERSON_IDs entirely
  #ts <- filtered_ts |>
  #  filter(!(PERSON_ID %in% persons_to_remove))
  
  # Print how many persons were removed
  #cat("Removed", length(persons_to_remove), "persons from dataset.\n")
  
  #cat("Minimum time_series_date:", format(min(ts$time_series_date, na.rm = TRUE)), "\n")
  
  #rm(filtered_ts, persons_to_remove)
  
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
  cat(dplyr::n_distinct(ts$PERSON_ID), "individuals with an outcome_ind == 1\n")
  
  
  rm(n_before)
  gc()
  
  
  # Remove persons with any missing COVID19_ind 
  # Check the unique values of COVID19_ind
  print(paste("unique covid19 values:", unique(ts$COVID19_ind)))
  
  # Count NAs
  print(paste("total NAs:", sum(is.na(ts$COVID19_ind))))
  
  # Identify PERSON_IDs with any missing COVID19 value
  persons_with_missing <- ts %>%
    group_by(PERSON_ID) %>%
    filter(any(is.na(COVID19_ind))) %>%
    distinct(PERSON_ID)
  
  # Remove all rows for these PERSON_IDs
  ts <- ts %>%
    filter(!(PERSON_ID %in% persons_with_missing$PERSON_ID))
  
  # Print how many unique PERSON_IDs were removed
  cat("Number of PERSON_IDs removed due to missing COVID19:", nrow(persons_with_missing), "\n")
  
  rm(persons_with_missing)
  
  # Make calendar month column
  
  ts <- time_series_month(ts)
  
  # Ensure dataset is sorted by 'person_id' and 'time_series_date'
  ts <- ts[order(ts$PERSON_ID, ts$time_series_date),]
  
  # ----------------------------------------------------------------------------
  # Code section: first outcome event sensitivity analysis
  # ----------------------------------------------------------------------------
  
  # filter to first ouctome event
  # Convert to data.table
  #ts <- as.data.table(ts)
  
  # Modify outcome_ind to keep only the first event per person
  #ts[, outcome_ind := as.integer(seq_len(.N) == which(outcome_ind == 1)[1]), by = PERSON_ID]
  
  # Convert back to data.frame
  #ts <- as.data.frame(ts)
  
  # Count outcome events
  #n_outcomes <- sum(ts$outcome_ind == 1)
  
  # Count distinct persons
  #n_persons <- length(unique(ts$PERSON_ID))
  
  # Print results
  #cat("Number of outcome events: ", n_outcomes, "\n")
  #cat("Number of unique persons: ", n_persons, "\n")
  
  # Stop if mismatch
  #if (n_outcomes != n_persons) {
  #  stop("Mismatch: Number of outcome_ind == 1 does not equal number of distinct persons.")
  #}
  
  #ts <- ts[order(ts$PERSON_ID, ts$time_series_date),]
  
  # ----------------------------------------------------------------------------
  # Code section end
  # ----------------------------------------------------------------------------
  
  
  ts <- ts |> select(PERSON_ID, study_start_date, time_series_date, outcome_ind, COVID19_ind, tmean, pm2p5, dayofweek, month, year, lockdown)
  
  
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
    
    lapply(names(filtered_time_series_list), function(name) {
      run_model(filename=filename,
                lagdays=lagdays,
                ts = filtered_time_series_list[[name]],
                subgroup=name,
                adjust=adjust)
    })
    
    # If no subgrouping needed
  } else {
    run_model(filename=filename, 
              lagdays=lagdays, 
              ts=ts,
              adjust=adjust
    )
    
  }
  
}


# Example calls
setwd("/db-mnt/databricks/rstudio_collab/CCU018/CCU076")

# cts("ae_fatal", 21)
# cts("stroke_fatal", 21)
# cts("ve_fatal", 21)
# cts("mi_fatal", 21)

# cts("ae_fatal", 21, adjust="air_pollution")
# cts("stroke_fatal", 21, adjust="air_pollution")
# cts("ve_fatal", 21, adjust="air_pollution")
# cts("mi_fatal", 21, adjust="air_pollution")

cts("ae_fatal", 21, adjust = "covid19")
cts("ae_fatal", 21, adjust = "air_pollution")
cts("ae_fatal", 21, adjust = "lockdown")

cts("mi_fatal", 21, adjust = "covid19")
cts("mi_fatal", 21, adjust = "air_pollution")
cts("mi_fatal", 21, adjust = "lockdown")

cts("ve_fatal", 21, adjust = "covid19")
cts("ve_fatal", 21, adjust = "air_pollution")
cts("ve_fatal", 21, adjust = "lockdown")


cts("stroke_fatal", 21, adjust = "covid19")
cts("stroke_fatal", 21, adjust = "air_pollution")
cts("stroke_fatal", 21, adjust = "lockdown")

# cts("covid_fatal", 21, adjust = "air_pollution")
