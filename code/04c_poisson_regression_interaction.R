#*******************************************************************************
#
# Project: CCU076
# Title:   Associations between air temperature and cardiovascular health outcomes
# Date:   07-01-2026
# Author:  Isabel Walter 
# Purpose: Run poisson regression for the association between air temperature 
#          and CVD outcomes with formal testing of effect modification
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

# helper to make one row of information based on crossreduce output
make_row_from_redpred <- function(rp, model, subgroup_value, 
                                  stratification, filename, 
                                  batch_number, size, nevents) {
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
run_model <- function(filename, lagdays, ts, stratification, subgroup_map){
  
  # Make outcome indicator and environmental exposure numeric
  ts$outcome_ind <- as.numeric(ts$outcome_ind)
  ts$tmean <- as.numeric(ts$tmean)
  
  # Normalise subgroup_map to named character vector
  if (is.list(subgroup_map)) {
    subgroup_map <- unlist(subgroup_map, use.names = TRUE)
  }
  stopifnot(is.character(subgroup_map), !is.null(names(subgroup_map)))
  
  # Get subgroup labels for this stratification
  subgroups <- names(subgroup_map)[subgroup_map == stratification]
  
  subgroups <- names(subgroup_map)[subgroup_map == stratification]
  
  if (stratification == "ethnicity") {
    subgroups[subgroups %in% c(
      "Mixed or multiple ethnic groups",
      "Other ethnic group",
      "Missing ethnicity"
    )] <- "other"
    
    subgroups <- unique(subgroups)
  }
  
  ref_level  <- subgroups[[1]]
  dummy_cols <- subgroups[-1]  # interaction for non-reference only
  
  # check dummy columns exist in ts
  missing <- setdiff(dummy_cols, names(ts))
  if (length(missing) > 0) {
    stop("Missing dummy columns in ts: ", paste(missing, collapse = ", "), call. = FALSE)
  }
  
  int_names <- paste0("cb_subset_", make.names(dummy_cols))
  
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
     
    for (i in seq_along(dummy_cols)) {
      d <- dummy_cols[[i]]
      
      assign(int_names[[i]], cb_subset *  as.numeric(as.character(ts_subset[[d]])))
    }
    
    
    size_ref <- round(
      length(
        unique(ts_subset$PERSON_ID[
          ts_subset[[stratification]] == ref_level
        ])
      ) / 5
    ) * 5
    
    size_int <- sapply(dummy_cols, function(d) {
      round(
        length(
          unique(ts_subset$PERSON_ID[
            ts_subset[[d]] == 1
          ])
        ) / 5
      ) * 5
    })
    
    print(paste0("size_ref: ", size_ref))
    print(paste0("size_int: ", size_int))
    
    nevents_ref <- round(
      sum(ts_subset[[stratification]] == ref_level &
            ts_subset$outcome_ind == 1,
          na.rm = TRUE) / 5
    ) * 5
    
    nevents_int <- sapply(dummy_cols, function(d) {
      round(
        sum(ts_subset[[d]] == 1 & ts_subset$outcome_ind == 1, na.rm = TRUE) / 5
      ) * 5
    })
    
    ts_subset <- ts_subset %>% select(-all_of(dummy_cols), -stratification)
    gc()
    
    form <- as.formula(
      paste("outcome_ind ~ cb_subset +", paste(int_names, collapse = " + "), "+ factor(dayofweek)")
    )
    
    print(form)
    
    print(paste0("Fit poisson model ", batch_number))

    model <- gnm(
      form, 
      data=ts_subset, 
      family=poisson,
      eliminate=group
     )
    
  
    print(paste0("Remove interaction matrices ", batch_number))
    # capture the interaction column names 
    base_cols <- colnames(cb_subset)   # e.g., "v4.12", "v4.13", ...
    
    # main crossbasis coefficient names (reference / baseline term)
    cb_cols <- paste0("cb_subset", base_cols)
    
    # interaction coefficient names (one set per interaction object)
    int_cols_list <- lapply(int_names, function(nm) paste0(nm, base_cols))
    int_cols_all <- unlist(int_cols_list)
    
    # delete heavy interaction matrices to save memory
    rm(list = int_names)
    gc()
    
    print(paste0("Cross-reduce reference model ", batch_number))
    
    # reference level (no interactions active)
    redpred_ref <- crossreduce(cb_subset, 
                               coef = coef(model)[cb_cols],
                               vcov = vcov(model)[cb_cols, cb_cols], 
                               cen = cen, 
                               model.link = "log")
    
    # crossreduce for each dummy subgroup: base + that interaction
    redpred_list <- vector("list", length(dummy_cols))
    names(redpred_list) <- dummy_cols
    
    V <- vcov(model)
    b <- coef(model)
    
    # ---- sanity checks (fail fast) ----
    missing_cb  <- setdiff(cb_cols,  names(b))
    missing_int <- setdiff(int_cols_all, names(b))
    
    if (length(missing_cb) > 0) {
      stop("These cb_cols were not found in coef(mod):\n", paste(missing_cb, collapse = ", "), call. = FALSE)
    }
    if (length(missing_int) > 0) {
      stop("These int_cols were not found in coef(mod):\n", paste(missing_int, collapse = ", "), call. = FALSE)
    }
    
    for (i in seq_along(dummy_cols)) {
      
      print(paste0("Cross-reduce model ", dummy_cols[i], " batch ", batch_number))
      
      int_cols <- int_cols_list[[i]]
      
      print(paste0("cb_cols: ", cb_cols))
      
      print(paste0("int_cols: ", int_cols))
      
      # coefficients: base + interaction
      b_sum <- as.numeric(b[cb_cols]) + as.numeric(b[int_cols])
      
      # vcov for sum: Vb + Vi + Cov(b,i) + Cov(i,b)
      Vb <- V[cb_cols,  cb_cols,  drop = FALSE]
      Vi <- V[int_cols, int_cols, drop = FALSE]
      C  <- V[cb_cols,  int_cols, drop = FALSE]
      Vsum <- Vb + Vi + C + t(C)
      
      redpred_list[[i]] <- crossreduce(
        cb_subset,
        coef = b_sum,
        vcov = Vsum,
        cen = cen,
        model.link = "log"
      )
    }
    
    print(paste0("Store batch parameters ", batch_number))
    
    # reference row
    batch_par_ref <- make_row_from_redpred(redpred_ref, model, ref_level, 
                                           stratification, filename, batch_number, 
                                           size_ref, nevents_ref)  
    
    # one row per dummy crossreduce
    names(size_int)    <- dummy_cols
    names(nevents_int) <- dummy_cols
    batch_par_dummies <- do.call(
      rbind,
      lapply(dummy_cols, function(d) make_row_from_redpred(redpred_list[[d]], model, d, 
                                                           stratification, filename, batch_number, 
                                                           size_int[[d]], nevents_int[[d]]))
    )
    
    # combine
    batch_par_all <- rbind(batch_par_ref, batch_par_dummies)
      
    # Append to the list of parameters for each batch
    
    print(paste0("Save model, time series, and crossbasis of revelant risk sets batch ", batch_number))
    
    # Save model, batched time series, and crossbasis of relevant risk sets
    save(model, filename = paste0("model_",filename, lagdays,"d_lag","_batchsize_",batch_size,"_interaction_",stratification, "_batch_", batch_number))
    #save(ts_subset, filename = paste0("ts_subset_",filename, lagdays,"d_lag","_batchsize_",batch_size,"_interaction_",stratification, "_batch_", batch_number))
    #save(cb_subset, filename = paste0("cb_subset_",filename, lagdays,"d_lag","_batchsize_",batch_size,"_interaction_",stratification, "_batch_", batch_number))
  
    rm(ts_subset, cb_subset, redpred_ref, redpred_list, valid_rows, size_int, size_ref, nevents_int, nevents_ref)
    gc()
    
    # Test of interaction
    model_main <- paste0("model_", filename, lagdays, "d_lag_batchsize_", batch_size, 
                              "_main_analysis_new_batch_", batch_number)
    
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
  
  
  ts <- ts |> select(PERSON_ID, study_start_date, time_series_date, outcome_ind, tmean, dayofweek, month, year)
  
  
  # Run modelling for subgroup if stratification analysis, else for full cohort
  
  if(!is.null(stratification)){
    
    cohort <- load_data("cohort_final")
    
    ts <- add_strat_dummies(ts = ts,
                            cohort = cohort,
                            stratification = stratification,
                            subgroup_map = subgroup_map,
                            id_col = "PERSON_ID",
                            drop_strat_col = FALSE)
    
    rm(cohort)
    gc()
    
    run_model(filename=filename, 
              lagdays=lagdays, 
              ts=ts,
              stratification = stratification,
              subgroup_map = subgroup_map)
    
    # If no subgrouping 
  } else {
    print("No subgrouping variable provided.")
    
  }
  
}

setwd("/db-mnt/databricks/rstudio_collab/CCU018/CCU076")

subgroup_map <- list(
  # age groups
  "Age 18-64 years"        = "age_group_bin",
  "Age 65 years and older" = "age_group_bin",
  
  # sex
  "Female" = "sex",
  "Male"   = "sex",
  
  # deprivation_bin
  "Low deprivation index (1-3)"  = "deprivation_bin",
  "High deprivation index (4-5)" = "deprivation_bin",
  #"Missing deprivation index"    = "deprivation_bin",
  
  # ethnicity
  "White"                                   = "ethnicity",
  "Asian or Asian British"                  = "ethnicity",
  "Black, Black British, Caribbean or African" = "ethnicity",
  "Mixed or multiple ethnic groups"         = "ethnicity",
  "Other ethnic group"                      = "ethnicity",
  "Missing ethnicity"                       = "ethnicity",
  
  # comorbidities
  "No comorbidities"  = "cov_comorb_comp_flag",
  "Comorbidities"     = "cov_comorb_comp_flag",
  
  # obesity
  "No excess weight"  = "cov_obesity_combined_flag",
  "Excess weight"     = "cov_obesity_combined_flag",
  
  # region (names must match file suffixes)
  "London"                    = "region",
  "West Midlands"             = "region",
  "East Midlands"             = "region",
  "North West"                = "region",
  "North East"                = "region",
  "South West"                = "region",
  "South East"                = "region",
  "East of England"           = "region",
  "Yorkshire and The Humber"  = "region",
  
  # Urban-rural class
  "Urban" = "rural_urban_class",
  "Rural" = "rural_urban_class",
  
  # Smoking status
  "Never smoked"         = "cov_smoking_status",
  "Current smoker"       = "cov_smoking_status",
  "Ex-smoker"            = "cov_smoking_status",
  "Missing smoking status" = "cov_smoking_status"
)



add_strat_dummies <- function(ts,
                                  cohort,
                                  stratification,
                                  subgroup_map,
                                  id_col = "PERSON_ID",
                                  drop_strat_col = FALSE,
                                  dummy_as_factor = TRUE,
                                  dummy_labels = c("0", "1")) {
  stopifnot(is.character(stratification), length(stratification) == 1)
  
  # Accept your list structure
  if (is.list(subgroup_map)) subgroup_map <- unlist(subgroup_map, use.names = TRUE)
  stopifnot(is.character(subgroup_map), !is.null(names(subgroup_map)))
  
  expected_levels <- names(subgroup_map)[subgroup_map == stratification]
  if (length(expected_levels) == 0) {
    stop(sprintf("No subgroups found in subgroup_map for stratification '%s'.", stratification),
         call. = FALSE)
  }
  
  ref_level  <- expected_levels[[1]]
  dummy_cols <- expected_levels[-1]
  
  cohort_map <- cohort %>%
    select(all_of(c(id_col, stratification))) %>%
    distinct()
  
  out <- ts %>%
    left_join(cohort_map, by = setNames(id_col, id_col))
  
  # ---- STRICT VALIDATION (COMPARE LABELS, NOT STORAGE CLASS) ----
  observed_levels <- sort(unique(as.character(out[[stratification]])))
  
  if (any(is.na(observed_levels))) {
    stop(
      sprintf("NA values detected for '%s' after merge (missing PERSON_IDs or missing values in cohort).",
              stratification),
      call. = FALSE
    )
  }
  
  expected_sorted <- sort(unique(expected_levels))
  
  if (!setequal(observed_levels, expected_sorted)) {
    missing_in_ts <- setdiff(expected_sorted, observed_levels)
    extra_in_ts   <- setdiff(observed_levels, expected_sorted)
    
    msg <- paste0(
      "Stratification value mismatch for '", stratification, "'.\n",
      "Expected (from subgroup_map): ", paste(expected_sorted, collapse = ", "), "\n",
      "Observed (in ts after join):  ", paste(observed_levels, collapse = ", "), "\n"
    )
    if (length(missing_in_ts) > 0) msg <- paste0(msg, "Missing in ts: ", paste(missing_in_ts, collapse = ", "), "\n")
    if (length(extra_in_ts) > 0)   msg <- paste0(msg, "Unexpected in ts: ", paste(extra_in_ts, collapse = ", "), "\n")
    stop(msg, call. = FALSE)
  }
  
  # (Optional) enforce reference level ordering on the strat column itself
  # out[[stratification]] <- factor(as.character(out[[stratification]]), levels = expected_levels)
  
  # ---- CREATE DUMMIES ----
  strat_chr <- as.character(out[[stratification]])
  
  if (stratification == "ethnicity") {
    strat_chr[strat_chr %in% c(
      "Mixed or multiple ethnic groups",
      "Other ethnic group",
      "Missing ethnicity"
    )] <- "other"
  }
  
  if (stratification == "ethnicity") {
    dummy_cols <- setdiff(dummy_cols, c(
      "Mixed or multiple ethnic groups",
      "Other ethnic group",
      "Missing ethnicity"
    ))
    dummy_cols <- unique(c(dummy_cols, "other"))
  }
  
  for (lvl in dummy_cols) {
    v <- as.integer(strat_chr == lvl)  # 0/1 integer
    
    if (isTRUE(dummy_as_factor)) {
      out[[lvl]] <- factor(v, levels = c(0, 1), labels = dummy_labels)
    } else {
      out[[lvl]] <- v
    }
  }
  
  if (isTRUE(drop_strat_col)) out <- out %>% select(-all_of(stratification))
  
  attr(out, paste0(stratification, "_reference")) <- ref_level
  out
}

# Subgroup analyses venous thrombosis
#cts("ve_fatal", 21, "ethnicity")
#cts("ve_fatal", 21, "sex")
#cts("ve_fatal", 21, "age_group_bin")
#cts("ve_fatal", 21, "rural_urban_class")
#cts("ve_fatal", 21, "region")
#cts("ve_fatal", 21, "deprivation_bin")
#cts("ve_fatal", 21, "cov_comorb_comp_flag")
#cts("ve_fatal", 21, "cov_obesity_combined_flag")
#cts("ve_fatal", 21, "cov_smoking_status")

# Subgroup analyses myocardial infarction
# cts("mi_fatal", 21, "ethnicity")
# cts("mi_fatal", 21, "sex")
# cts("mi_fatal", 21, "age_group_bin")
# cts("mi_fatal", 21, "rural_urban_class")
# cts("mi_fatal", 21, "region")
# cts("mi_fatal", 21, "deprivation_bin")
# cts("mi_fatal", 21, "cov_comorb_comp_flag")
# cts("mi_fatal", 21, "cov_obesity_combined_flag")
# cts("mi_fatal", 21, "cov_smoking_status")

# Subgroup analyses stroke
# cts("stroke_fatal", 21, "ethnicity")
# cts("stroke_fatal", 21, "sex")
# cts("stroke_fatal", 21, "age_group_bin")
# cts("stroke_fatal", 21, "rural_urban_class")
# cts("stroke_fatal", 21, "region")
# cts("stroke_fatal", 21, "deprivation_bin")
# cts("stroke_fatal", 21, "cov_comorb_comp_flag")
# cts("stroke_fatal", 21, "cov_obesity_combined_flag")
# cts("stroke_fatal", 21, "cov_smoking_status")

# Subgroup analyses arterial thrombosis
# cts("ae_fatal", 21, "ethnicity")
# cts("ae_fatal", 21, "sex")
# cts("ae_fatal", 21, "age_group_bin")
# cts("ae_fatal", 21, "rural_urban_class")
#cts("ae_fatal", 21, "region")
#cts("ae_fatal", 21, "deprivation_bin")
#cts("ae_fatal", 21, "cov_comorb_comp_flag")
#cts("ae_fatal", 21, "cov_obesity_combined_flag")
#cts("ae_fatal", 21, "cov_smoking_status")
