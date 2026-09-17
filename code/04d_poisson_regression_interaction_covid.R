#*******************************************************************************
#
# Project: CCU076
# Title:   Associations between air temperature and cardiovascular health outcomes
# Date:   07-01-2026
# Author:  Isabel Walter 
# Purpose: Run poisson regression for the association between air temperature 
#          and CVD outcomes with formal testing of COVID19 effect modification
#*******************************************************************************


# Load libraries
libs <- c("dplyr", "tidyr", "dlnm", "gnm", "pbs", "lubridate", "mgcv", "splines", "fixest", "data.table")

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

# helper to make one row of information based on crossreduce output
make_row_from_redpred <- function(rp, model, subgroup_value, 
                                  stratification, filename, 
                                  batch_number, size=NULL, nevents=NULL) {
  ncoef <- length(coef(rp))
  par <- c(coef(rp), vechMat_replacement(vcov(rp)))
  names(par) <- c(
    paste0("coef", seq_len(ncoef)),
    paste0("vcov", seq_len(ncoef * (ncoef + 1) / 2))
  )
  
  data.frame(
    outcome       = filename,
    stratification = stratification,
    subgroup      = subgroup_value,
    batch_number  = batch_number,
    size          = size,
    nevents       = nevents,
    aic           = AIC(model),
    conv          = model$converged,
    disp          = sum(residuals(model, type = "pearson")^2, na.rm = TRUE) / model$df.residual,
    t(par),
    row.names = NULL,
    check.names = FALSE
  )
}


# Function that runs poisson regression and saves model
run_model <- function(filename, lagdays, ts, stratification){
  
  # Make outcome indicator and environmental exposure numeric
  ts$outcome_ind <- as.numeric(ts$outcome_ind)
  ts$tmean <- as.numeric(ts$tmean)
  ts$COVID19_ind <- factor(ts$COVID19_ind)
  
  # Set crossbasis parameters 
  print("Set crossbasis parameters")
  
  # Argvar
  varfun <- "ns"
  distribution <- load_data("tmean_pm2p5_distribution")
  
  # Default column name
  temp_col <- "tmean_20_22"
  
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
    save(ts_batch, filename = paste0("batch_", batch_number))
  }
  
  # Clean up full ts from memory
  rm(ts, ts_batch)
  gc()
  
  # Initialise an empty list to store parameter data frames for each batch
  parlist <- list()
  
  
  print("Loop through each batched time series dataset")
  # Loop through each batched time series dataset
  for (batch_number in 1:num_batches) {
    
    print(paste0("batch_number: ", batch_number))
    
    # Load batch
    ts_subset <- load_data(paste0("batch_", batch_number))
    
    
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
    
    print(paste0("Fit poisson model ", batch_number))
    
    if(stratification == "COVID19_ind"){
      size_ref <- "time_varying"
      size_int <- "time_varying"
      nevents_ref <- "time_varying"
      nevents_int <- "time_varying"
      model <- gnm(
      outcome_ind ~ cb_subset*COVID19_ind + factor(dayofweek), 
      data=ts_subset, 
      family=poisson,
      eliminate=group
    )
    }else if(stratification == "COVID19_6mo"){
      x <- as.numeric(as.character(ts_subset$COVID19_6mo))
      cat("COVID19_6mo unique:", paste(sort(unique(x)), collapse=", "), "\n")
      cat("COVID19_6mo NAs:", sum(is.na(x)), "\n")
      
     cb_subset_int_covid <- cb_subset * as.numeric(as.character(ts_subset$COVID19_6mo))
     size_ref <- round(
       length(
         unique(ts_subset$PERSON_ID[
           ts_subset[[stratification]] == 0
         ])
       ) / 5
     ) * 5
     
     size_int <- round(
       length(
         unique(ts_subset$PERSON_ID[
           ts_subset[[stratification]] == 1
         ])
       ) / 5
     ) * 5
     
     
     print(paste0("size_ref: ", size_ref))
     print(paste0("size_int: ", size_int))
     
     nevents_ref <- round(
       sum(ts_subset[[stratification]] == 0 &
             ts_subset$outcome_ind == 1,
           na.rm = TRUE) / 5
     ) * 5
     
     nevents_int <- round(
       sum(ts_subset[[stratification]] == 1 &
             ts_subset$outcome_ind == 1,
           na.rm = TRUE) / 5
     ) * 5
     
     ts_subset <- ts_subset %>% select(-stratification)
     gc()
     model <- gnm(
       outcome_ind ~ cb_subset + cb_subset_int_covid + factor(dayofweek), 
       data=ts_subset, 
       family=poisson,
       eliminate=group
     )
   }
    
    
    
    print(paste0("Remove interaction matrices ", batch_number))
    # capture the interaction column names 
    base_cols <- colnames(cb_subset)   # e.g., "v4.12", "v4.13", ...
    
    # main crossbasis coefficient names (reference / baseline term)
    cb_cols <- paste0("cb_subset", base_cols)
    
    # interaction coefficient names
    if (stratification == "COVID19_ind"){
      int_cols <- paste0("cb_subset", base_cols, ":COVID19_ind1")
      
    } else if(stratification == "COVID19_6mo"){
      int_cols <- paste0("cb_subset_int_covid", base_cols)
    }
    
    print(paste0("Cross-reduce reference model ", batch_number))
    
    # reference level (no interactions active)
    redpred_ref <- crossreduce(cb_subset, 
                               coef = coef(model)[cb_cols],
                               vcov = vcov(model)[cb_cols, cb_cols], 
                               cen = cen, 
                               model.link = "log")
    
    # crossreduce for COVID19_ind == 1
    V <- vcov(model)
    b <- coef(model)
    
    # ---- sanity checks (fail fast) ----
    missing_cb  <- setdiff(cb_cols,  names(b))
    missing_int <- setdiff(int_cols, names(b))
    
    if (length(missing_cb) > 0) {
      stop("These cb_cols were not found in coef(mod):\n", paste(missing_cb, collapse = ", "), call. = FALSE)
    }
    if (length(missing_int) > 0) {
      print(paste0("names(b): ",names(b)))
      stop("These int_cols were not found in coef(mod):\n", paste(missing_int, collapse = ", "), call. = FALSE)
    }
    
    # coefficients: base + interaction
    b_sum <- as.numeric(b[cb_cols]) + as.numeric(b[int_cols])
    
    # vcov for sum: Vb + Vi + Cov(b,i) + Cov(i,b)
    Vb <- V[cb_cols,  cb_cols,  drop = FALSE]
    Vi <- V[int_cols, int_cols, drop = FALSE]
    C  <- V[cb_cols,  int_cols, drop = FALSE]
    Vsum <- Vb + Vi + C + t(C)
    
    redpred_int <- crossreduce(cb_subset, 
                               coef = b_sum,
                               vcov = Vsum, 
                               cen = cen, 
                               model.link = "log")
    
    
    print(paste0("Store batch parameters ", batch_number))
    
    # reference row
    batch_par_ref <- make_row_from_redpred(redpred_ref, model, "No COVID19 diagnosis", 
                                           stratification, filename, batch_number, size_ref, nevents_ref)  
    
    # COVID19_ind == 1 
    batch_par_int <- make_row_from_redpred(redpred_int, model, "COVID19 diagnosis", 
                                           stratification, filename, batch_number, size_int, nevents_int)  
    
    # combine
    batch_par_all <- rbind(batch_par_ref, batch_par_int)
    
    # Append to the list of parameters for each batch
    
    print(paste0("Save model, time series, and crossbasis of revelant risk sets batch ", batch_number))
    
    # Save model, batched time series, and crossbasis of relevant risk sets
    save(model, filename = paste0("model_",filename, lagdays,"d_lag","_batchsize_",batch_size,"_interaction_",stratification, "_batch_", batch_number))
    
    if (stratification == "COVID19_6mo"){
      save(ts_subset, filename = paste0("ts_subset_",filename, lagdays,"d_lag","_batchsize_",batch_size,"_interaction_",stratification, "_batch_", batch_number))
      save(cb_subset, filename = paste0("cb_subset_",filename, lagdays,"d_lag","_batchsize_",batch_size,"_interaction_",stratification, "_batch_", batch_number))
    }
    
    rm(ts_subset, cb_subset, redpred_ref, redpred_int, valid_rows)
    gc()
    
    # Test of interaction
    if (stratification == "COVID19_6mo" & lagdays == 21){
      model_main <- paste0("model_", filename, lagdays, "d_lag_batchsize_", batch_size, 
                           "_sensitivity_first_event_batch_", batch_number)
      print(paste0("main model: ", model_main))
    }else if (stratification == "COVID19_6mo" & lagdays == 14){
      model_main <- paste0("model_", filename, lagdays, "d_lag_batchsize_", batch_size, 
                           "_lag_sensitivity_first_event_batch_", batch_number)
      print(paste0("main model: ", model_main))
    }else if (stratification == "COVID19_ind" & lagdays == 14){
      model_main <- paste0("model_", filename, lagdays, "d_lag_batchsize_", batch_size, 
                           "_lag_sensitivity_analysis__batch_", batch_number)
      print(paste0("main model: ", model_main))
    }else{
      model_main <- paste0("model_", filename, lagdays, "d_lag_batchsize_", batch_size, 
                         "_main_analysis_new_batch_", batch_number)
      print(paste0("main model: ", model_main))
    }
    
    model0 <- load_data(model_main)
    
    pval <- anova(model0, model, test="Chisq")$`Pr(>Chi)`[2]
    
    rm(model, model0, model_main)
    gc()
    
    batch_par_all$interaction_chisq <- pval
    
    parlist[[batch_number]] <- batch_par_all
    
    # Delete batch file
    file.remove(here::here("data", paste0("batch_", batch_number, ".rds")))
    gc()
  }
  
  print("Combine all batch results in dataframe and save")
  # Combine all batch results into one data frame
  par_df <- do.call(rbind, parlist)
  
  # Save
  save(par_df, filename = paste0("crossreduce_",filename, lagdays,"d_lag","_batchsize_",batch_size,"_interaction_", stratification))
}


# Function that prepares data and then calls function to run poisson regression
cts <- function(filename, lagdays, stratification = NULL){
  
  print("load time series")
  
  # Load ts
  ts <- load_data(paste0("time_series_", filename))
  
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
  
  # # filter to first ouctome event
  # # Convert to data.table
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
  # 
  # ----------------------------------------------------------------------------
  # Code section end
  # ----------------------------------------------------------------------------
  
  
  ts <- ts |> select(PERSON_ID, study_start_date, time_series_date, outcome_ind, tmean, COVID19_ind, dayofweek, month, year)
  
  if (stratification == "COVID19_6mo") {
    # Filter to first events only
    # Convert to data.table
    ts <- as.data.table(ts)
    
    # Modify outcome_ind to keep only the first event per person
    ts[, outcome_ind := as.integer(seq_len(.N) == which(outcome_ind == 1)[1]), by = PERSON_ID]
    
    # Convert back to data.frame
    ts <- as.data.frame(ts)
    
    # Count outcome events
    n_outcomes <- sum(ts$outcome_ind == 1)
    
    # Count distinct persons
    n_persons <- length(unique(ts$PERSON_ID))
    
    # Print results
    cat("Number of outcome events: ", n_outcomes, "\n")
    cat("Number of unique persons: ", n_persons, "\n")
    
    # Stop if mismatch
    if (n_outcomes != n_persons) {
      stop("Mismatch: Number of outcome_ind == 1 does not equal number of distinct persons.")
    }
    
    ts <- ts[order(ts$PERSON_ID, ts$time_series_date),]
    
    # Make COVID within 6 months flag
    # outcome date per person (robust even if outcome_ind==1 appears >1 time)
    outcomes <- ts %>%
      filter(outcome_ind == 1) %>%
      mutate(time_series_date = as.Date(time_series_date)) %>%
      group_by(PERSON_ID) %>%
      summarise(outcome_date = min(time_series_date), .groups = "drop")
    
    covid_exp <- load_data("ccu076_01_out_exposures_covid")
    
    # join COVID dates and compute within-6mo indicator, then collapse to 1 row/person
    covid_6mo_by_person <- outcomes %>%
      left_join(covid_exp %>% mutate(DATE = as.Date(DATE)), by = "PERSON_ID") %>%
      mutate(
        days_since_covid = as.numeric(outcome_date - DATE),
        covid_in_6mo = !is.na(days_since_covid) & days_since_covid >= 0 & days_since_covid <= 183
      ) %>%
      group_by(PERSON_ID) %>%
      summarise(
        COVID19_6mo = as.integer(any(covid_in_6mo, na.rm = TRUE)),
        .groups = "drop"
      )
    
    covid_6mo_by_person %>%
      count(COVID19_6mo) %>%
      mutate(
        label = if_else(COVID19_6mo == 1,
                        "COVID within 6 months",
                        "No COVID within 6 months")
      ) %>%
      select(label, n)
    
    print(paste0("rows ts: ", nrow(ts)))
    
    ts <- ts %>%
      left_join(covid_6mo_by_person, by = "PERSON_ID") %>%
      mutate(COVID19_6mo = tidyr::replace_na(COVID19_6mo, 0L))
    
    table(ts$COVID19_6mo, useNA = "ifany")
    str(ts$COVID19_6mo)
    
    print(paste0("rows ts with covid column: ", nrow(ts)))
    
    rm(outcomes, covid_6mo_by_person)
    
    run_model(filename=filename, 
              lagdays=lagdays, 
              ts=ts,
              stratification = "COVID19_6mo")
  }
  # Run model
  if (stratification == "COVID19_ind"){
    run_model(filename=filename, 
              lagdays=lagdays, 
              ts=ts,
              stratification = "COVID19_ind")
  }
}

setwd("/db-mnt/databricks/rstudio_collab/CCU018/CCU076")

#cts("ve_fatal", 21, "COVID19_ind")
#cts("ve_fatal", 21, "COVID19_6mo")

cts("mi_fatal", 14, "COVID19_ind")
cts("mi_fatal", 14, "COVID19_6mo")

cts("mi_fatal", 21, "COVID19_ind")
cts("mi_fatal", 21, "COVID19_6mo")

cts("stroke_fatal", 21, "COVID19_ind")
cts("stroke_fatal", 21, "COVID19_6mo")

cts("ae_fatal", 21, "COVID19_ind")
cts("ae_fatal", 21, "COVID19_6mo")

