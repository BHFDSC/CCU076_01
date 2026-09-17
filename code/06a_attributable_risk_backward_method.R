###############################################################################
# Project:  CCU076
# Title:    Associations between air temperature and cardiovascular health outcomes
# Date:     06-10-2025
# Author:   Isabel Walter
# Purpose:  Compute attributable numbers with attrdl() for main and
#           subgroup analyses, looping batches per outcome.  
#           Note: Adapt code for relevant outcome of interest.
###############################################################################

# Load libraries
libs <- c("dplyr", "dlnm", "gnm")

# Load libraries in a loop
for (lib in libs) {
  if (!requireNamespace(lib, quietly = TRUE)) {
    install.packages(lib, dependencies = TRUE)
  }
  library(lib, character.only = TRUE)
}

# Source function
fs <- c("retrieve_data", "attrdl")
for (f in fs) source(here::here("source", paste0(f, ".R")))


# ---- Mapping: subgroup VALUE -> subgroup VARIABLE ----
subgroup_map <- list(
  # age groups
  "age_binary_Age 65 years and older" = "age_group_bin",
  "age_binary_Age 18-64 years"        = "age_group_bin",
  
  # sex
  "Male"   = "sex",
  "Female" = "sex",
  
  # deprivation_bin
  "Low deprivation index (1-3)"  = "deprivation_bin",
  "High deprivation index (4-5)" = "deprivation_bin",
  "Missing deprivation index"    = "deprivation_bin",
  
  # ethnicity
  "White"                                   = "ethnicity",
  "Asian or Asian British"                  = "ethnicity",
  "Black, Black British, Caribbean or African" = "ethnicity",
  "Mixed or multiple ethnic groups"         = "ethnicity",
  "Other ethnic group"                      = "ethnicity",
  "Missing ethnicity"                       = "ethnicity",
  
  # comorbidities
  "Comorbidities"     = "cov_comorb_comp_flag",
  "No comorbidities"  = "cov_comorb_comp_flag",
  
  # obesity
  "Excess weight"     = "cov_obesity_combined_flag",
  "No excess weight"  = "cov_obesity_combined_flag",
  
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
  "Never smoked"         = "smoking_status",
  "Current smoker"       = "smoking_status",
  "Ex-smoker"            = "smoking_status",
  "Missing smoking status" = "smoking_status"
)

# -----------------------------
# Central temperature (cen)
# -----------------------------
get_cen <- function(subgroup_variable = NULL, subgroup_value = NULL) {
  distribution <- load_data("tmean_pm2p5_distribution")
  
  # Default column
  temp_col <- "tmean_20_22"
  
  # Region-specific override if available
  if (!is.null(subgroup_variable) && subgroup_variable == "region" &&
      !is.null(subgroup_value)) {
    regional_col <- paste0("tmean_20_22_", subgroup_value)
    if (regional_col %in% names(distribution)) {
      temp_col <- regional_col
      message("Using region-specific temperature percentiles: ", temp_col)
    } else {
      warning("Column ", regional_col, " not found in distribution. Using general column.")
    }
  }
  
  idx <- match("50.0%", distribution$percentile)
  distribution[[temp_col]][idx]
}

# -----------------------------
# Filename helpers
# -----------------------------
base_prefix <- function(outcome, tag) {
  # outcome in {"ve","mi","stroke","ae"}
  sprintf("%s_fatal21d_lag_batchsize_275000_%s", outcome, tag)
}

# Main analysis names
crossreduce_main <- function(outcome) sprintf("crossreduce_%s", base_prefix(outcome, "main_analysis_new"))
ts_main         <- function(outcome, b) sprintf("ts_subset_%s_batch_%d", base_prefix(outcome, "main_analysis_new"), b)
cb_main         <- function(outcome, b) sprintf("cb_subset_%s_batch_%d", base_prefix(outcome, "main_analysis_new"), b)
model_main      <- function(outcome, b) sprintf("model_%s_batch_%d", base_prefix(outcome, "main_analysis_new"), b)

# Subgroup analysis names (VALUE is in the tag)
crossreduce_sg <- function(outcome, value) sprintf("crossreduce_%s", base_prefix(outcome, paste0("subgroup_new_", value)))
ts_sg         <- function(outcome, value, b) sprintf("ts_subset_%s_batch_%d",  base_prefix(outcome, paste0("subgroup_new_", value)), b)
cb_sg         <- function(outcome, value, b) sprintf("cb_subset_%s_batch_%d",  base_prefix(outcome, paste0("subgroup_new_", value)), b)
model_sg      <- function(outcome, value, b) sprintf("model_%s_batch_%d",      base_prefix(outcome, paste0("subgroup_new_", value)), b)

# -----------------------------
# Utilities
# -----------------------------
safe_load <- function(name_no_ext) {
  print(paste0("Load ", name_no_ext))
  tryCatch(load_data(name_no_ext), error = function(e) NULL)
}

summarize_draws <- function(main_value, draws) {
  c(mean = main_value,
    ll   = as.numeric(quantile(draws, 0.025)),
    ul   = as.numeric(quantile(draws, 0.975)))
}

sum_batch_results_quantile_ci <- function(batch_list) {
  stopifnot(length(batch_list) >= 1)
  
  sum_one <- function(get_part) {
    # 1) sum point estimates across batches
    est_sum <- sum(vapply(batch_list, function(x) get_part(x)$est, numeric(1)))
    
    # 2) summarize each batch's draws, then sum the bounds
    per_batch_summ <- lapply(batch_list, function(x) {
      part <- get_part(x)
      summarize_draws(part$est, part$draws)
    })
    
    ll_sum <- sum(vapply(per_batch_summ, function(s) s[["ll"]], numeric(1)))
    ul_sum <- sum(vapply(per_batch_summ, function(s) s[["ul"]], numeric(1)))
    
    c(mean = est_sum, ll = ll_sum, ul = ul_sum)
  }
  
  list(
    an_all  = sum_one(function(x) x$an_all),
    an_hot  = sum_one(function(x) x$an_hot),
    an_cold = sum_one(function(x) x$an_cold)
  )
}

attrdl_all_ranges <- function(x, cases, basis, model, cen, nsim = 5000L) {
  
  print("Attrdl AN whole range")
  
  # Whole range (omit 'range')
  options(mc.cores = 10)
  
  an_all <- attrdl(x = x, basis = basis, cases = cases, model = model,
                   cen = cen, tot = TRUE,
                   sim = FALSE, dir = "back", type = "an")
  an_all_ci <- attrdl(x = x, basis = basis, cases = cases, model = model,
                    cen = cen, tot = TRUE, sim = TRUE, nsim = nsim, 
                    dir = "back", type = "an")
  
  gc()
  
  # print("Attrdl AF whole range")
  # 
  # options(mc.cores = 10)
  # af_all <- attrdl(x = x, basis = basis, cases = cases, model = model,
  #                  cen = cen, tot = TRUE,
  #                  sim = FALSE, dir = "back", type = "af")
  # af_all_ci <- attrdl(x = x, basis = basis, cases = cases, model = model,
  #                  cen = cen, tot = TRUE,
  #                  sim = TRUE, nsim = nsim, dir = "back", type = "af")
  # 
  # gc()
  
  print("Attrdl AN hot temperatures")
  rng_hot  <- c(cen, max(x, na.rm = TRUE))
  rng_cold <- c(min(x, na.rm = TRUE), cen)
  
  options(mc.cores = 10)
  an_hot <- attrdl(x = x, basis = basis, cases = cases, model = model,
                   cen = cen, tot = TRUE, range = rng_hot,
                   sim = FALSE, dir = "back", type = "an")
  an_hot_ci <- attrdl(x = x, basis = basis, cases = cases, model = model,
                   cen = cen, tot = TRUE, range = rng_hot,
                   sim = TRUE, nsim = nsim, dir = "back", type = "an")
  
  gc()
  
  # print("Attrdl AF hot temperatures")
  # options(mc.cores = 10)
  # af_hot <- attrdl(x = x, basis = basis, cases = cases, model = model,
  #                  cen = cen, tot = TRUE, range = rng_hot,
  #                  sim = FALSE, dir = "back", type = "af")
  # af_hot_ci <- attrdl(x = x, basis = basis, cases = cases, model = model,
  #                  cen = cen, tot = TRUE, range = rng_hot,
  #                  sim = TRUE, nsim = nsim, dir = "back", type = "af")
  # 
  # gc()
  
  options(mc.cores = 10)
  
  print("Attrdl AN cold temperatures")
  an_cold <- attrdl(x = x, basis = basis, cases = cases, model = model,
                   cen = cen, tot = TRUE, range = rng_cold,
                   sim = FALSE, dir = "back", type = "an")
  an_cold_ci <- attrdl(x = x, basis = basis, cases = cases, model = model,
                    cen = cen, tot = TRUE, range = rng_cold,
                    sim = TRUE, nsim = nsim, dir = "back", type = "an")
  
  gc()
  
  # print("Attrdl AF cold temperatures")
  # options(mc.cores = 10)
  # af_cold <- attrdl(x = x, basis = basis, cases = cases, model = model,
  #                   cen = cen, tot = TRUE, range = rng_cold,
  #                   sim = FALSE, dir = "back", type = "af")
  # af_cold_ci <- attrdl(x = x, basis = basis, cases = cases, model = model,
  #                   cen = cen, tot = TRUE, range = rng_cold,
  #                   sim = TRUE, nsim = nsim, dir = "back", type = "af")
  # 
  # gc()
  list(
    an_all  = list(est = an_all,  draws = an_all_ci),
    an_hot  = list(est = an_hot,  draws = an_hot_ci),
    an_cold = list(est = an_cold, draws = an_cold_ci)
  )
}

# -----------------------------
# MAIN analysis (no subgroup)
# -----------------------------
run_outcome_main <- function(outcome = c("ae"),#,"mi","stroke","ae")
                             nsim = 5000L) {
  
  print("Main analysis attributable risk")
  outcome <- match.arg(outcome)
  outcome_label <- paste0(outcome, "_fatal")
  cen <- get_cen()
  
  print("Load crossreduce")
  cr <- safe_load(crossreduce_main(outcome))
  if (is.null(cr)) {
    warning("No main crossreduce found for ", outcome_label)
    return(invisible(NULL))
  }
  n_batches <- nrow(cr)
  
  rows <- list()
  
  print("Load time series, crossbasis and model")
  for (b in seq_len(n_batches)) {
    print(paste0("Batch ", b))
    ts_subset <- safe_load(ts_main(outcome, b))
    print(paste0("rows ts subset: ", nrow(ts_subset)))
    cb_subset <- safe_load(cb_main(outcome, b))
    #print(attributes(cb_subset))
    print(paste0("rows cb subset: ", nrow(cb_subset)))
    model     <- safe_load(model_main(outcome, b))
    #print(class(model))
    #coef <- dlnm:::getcoef(model,class(model))
    #print(coef)
    if (is.null(ts_subset) || is.null(cb_subset) || is.null(model)) next
    
    res <- attrdl_all_ranges(
      x = ts_subset$tmean,
      cases = ts_subset$outcome_ind,
      basis = cb_subset,
      model = model,
      cen = cen,
      nsim = nsim
    )
    
    rows[[length(rows) + 1L]] <- list(
      outcome   = outcome_label,
      subgroup  = "not applicable", 
      batch_number     = b,
      res       = res                 # contains est + draws for all/hot/cold
    )
    
    print(paste0("Save batch ", b))
    tmp_name <- sprintf("attrdl_tmp_%s_main_analysis_new_batch_%d", outcome_label, b)
    save(rows[[length(rows)]], filename = tmp_name)
    
    #rows[[length(rows) + 1L]] <- row
    rm(ts_subset, cb_subset, model); gc()
  }
  
  if (!length(rows)) return(invisible(NULL))
  
  # extract batch results
  batch_res <- lapply(rows, `[[`, "res")
  
  # sum across batches, then summarize once
  summed_ci <- sum_batch_results_quantile_ci(batch_res)
  
  an_all_sum  <- summed_ci$an_all
  an_hot_sum  <- summed_ci$an_hot
  an_cold_sum <- summed_ci$an_cold
  
  df <- data.frame(
    outcome  = outcome_label,
    subgroup = "not applicable",
    
    an           = unname(an_all_sum["mean"]),
    an_ll_ci     = unname(an_all_sum["ll"]),
    an_ul_ci     = unname(an_all_sum["ul"]),
    
    an_hot       = unname(an_hot_sum["mean"]),
    an_hot_ll_ci = unname(an_hot_sum["ll"]),
    an_hot_ul_ci = unname(an_hot_sum["ul"]),
    
    an_cold       = unname(an_cold_sum["mean"]),
    an_cold_ll_ci = unname(an_cold_sum["ll"]),
    an_cold_ul_ci = unname(an_cold_sum["ul"]),
    
    row.names = NULL,
    stringsAsFactors = FALSE
  )
  
  print("Save all batches combined")
  save(df, filename = sprintf("attributable_risk_%s_main_analysis_new", outcome_label))
  invisible(df)
}

# -----------------------------
# SUBGROUP analyses
# loops over all available subgroup values for one outcome, runs attrdl() 
# batch-by-batch (all/hot/cold; AN with CIs), and saves progress at every level: 
# per-batch, per-value (all its batches), per-variable (all values), 
# and a master “all subgroups” file—printing clear progress messages.
# -----------------------------

run_outcome_subgroups <- function(outcome = c("ae"),#"mi","stroke","ae"), 
                                              nsim = 5000L) {
  outcome <- match.arg(outcome)
  outcome_label <- paste0(outcome, "_fatal")
  
  values <- names(subgroup_map)
  by_var <- list()
  
  master_name <- sprintf("attributable_risk_%s_all_subgroups", outcome_label)
  master_df <- tryCatch(safe_load(master_name), error = function(e) NULL)
  
  for (val in values) {
    cr <- safe_load(crossreduce_sg(outcome, val))
    if (is.null(cr)) {
      message("Skipping subgroup value (no files found): ", val)
      next
    }
    
    sg_var <- subgroup_map[[val]]
    cen    <- get_cen(subgroup_variable = sg_var, subgroup_value = val)
    n_batches <- nrow(cr)
    
    message("\n======================================================")
    message("Outcome: ", outcome_label)
    message("Subgroup variable: ", sg_var)
    message("Subgroup value:    ", val)
    message("Batches detected:  ", n_batches)
    message("cen (50th pct):    ", signif(cen, 6))
    message("======================================================")
    
    per_value_name <- sprintf("attributable_risk_%s_%s_%s", outcome_label, sg_var, val)
    per_value_df <- tryCatch(safe_load(per_value_name), error = function(e) NULL)
    done_batches <- if (!is.null(per_value_df)) unique(per_value_df$batch_number) else integer(0)
    
    for (b in seq_len(n_batches)) {
      if (b %in% done_batches) {
        message("Value: ", val, " | batch ", b, " already saved in ", per_value_name, " -> skipping.")
        next
      }
      
      message("Value: ", val, " | batch ", b, " — data loading...")
      ts_subset <- safe_load(ts_sg(outcome, val, b))
      cb_subset <- safe_load(cb_sg(outcome, val, b))
      model     <- safe_load(model_sg(outcome, val, b))
      
      if (is.null(ts_subset) || is.null(cb_subset) || is.null(model)) {
        warning("Value: ", val, " | batch ", b, " — missing ts/cb/model -> skipping this batch.")
        next
      }
      
      message("Value: ", val, " | batch ", b, " — attrdl calculations (all/hot/cold, an/af)...")
      res <- attrdl_all_ranges(
        x = ts_subset$tmean,
        cases = ts_subset$outcome_ind,
        basis = cb_subset,
        model = model,
        cen = cen,
        nsim = nsim
      )
      
      row <- data.frame(
        outcome      = outcome_label,
        subgroup     = val,          
        batch_number = b,
        an_est       = res$an_all$est,
        an_hot_est   = res$an_hot$est,
        an_cold_est  = res$an_cold$est, 
        stringsAsFactors = FALSE,
        row.names = NULL
      )
      
      # list-columns for draws
      row$an_draws      <- list(res$an_all$draws)
      row$an_hot_draws  <- list(res$an_hot$draws)
      row$an_cold_draws <- list(res$an_cold$draws)
      
      
      # per-batch temp save
      tmp_name <- sprintf("attrdl_tmp_%s_%s_%s_batch_%d", outcome_label, sg_var, val, b)
      message("Value: ", val, " | batch ", b, " — saving per-batch temp: ", tmp_name)
      save(row, filename = tmp_name)
      
      # append + save per-value after EACH batch
      if (is.null(per_value_df)) per_value_df <- row else per_value_df <- rbind(per_value_df, row)
      per_value_df <- per_value_df[order(per_value_df$batch_number), ]
      row.names(per_value_df) <- NULL
      message("Value: ", val, " | batch ", b, " — updating per-value file: ", per_value_name)
      save(per_value_df, filename = per_value_name)
      
      rm(ts_subset, cb_subset, model); gc()
    }
    
    # ---- if all batches are present, aggregate and write FINAL row (no batch col) ----
    done_batches <- unique(per_value_df$batch_number)
    
    if (setequal(done_batches, seq_len(n_batches))) {
      
      # 1) build the list that sum_batch_results() expects
      batch_res <- lapply(seq_len(nrow(per_value_df)), function(i) {
        list(
          an_all  = list(est = per_value_df$an_est[i],
                         draws = per_value_df$an_draws[[i]]),
          an_hot  = list(est = per_value_df$an_hot_est[i],
                         draws = per_value_df$an_hot_draws[[i]]),
          an_cold = list(est = per_value_df$an_cold_est[i],
                         draws = per_value_df$an_cold_draws[[i]]) 
          
        )
      })
      
      # 2) sum, calculated CIs
      summed_ci <- sum_batch_results_quantile_ci(batch_res)
      
      an_all_sum  <- summed_ci$an_all
      an_hot_sum  <- summed_ci$an_hot
      an_cold_sum <- summed_ci$an_cold
      
      
      final_row <- data.frame(
        outcome  = outcome_label,
        subgroup = val,
        
        an           = unname(an_all_sum["mean"]),
        an_ll_ci     = unname(an_all_sum["ll"]),
        an_ul_ci     = unname(an_all_sum["ul"]),
        
        an_hot       = unname(an_hot_sum["mean"]),
        an_hot_ll_ci = unname(an_hot_sum["ll"]),
        an_hot_ul_ci = unname(an_hot_sum["ul"]),
        
        an_cold       = unname(an_cold_sum["mean"]),
        an_cold_ll_ci = unname(an_cold_sum["ll"]),
        an_cold_ul_ci = unname(an_cold_sum["ul"]),
        
       
    
        stringsAsFactors = FALSE
      )
      
      # 4) update master_df (replace existing outcome+subgroup row)
      key <- !is.null(master_df) &&
        any(master_df$outcome == outcome_label & master_df$subgroup == val)
      
      if (is.null(master_df)) {
        master_df <- final_row
      } else {
        master_df <- master_df[!(master_df$outcome == outcome_label & master_df$subgroup == val), , drop = FALSE]
        master_df <- rbind(master_df, final_row)
      }
      row.names(master_df) <- NULL
      
      message("Subgroup complete: writing final row to master: ", master_name)
      save(master_df, filename = master_name)
    }
    
  }
  
  # Return (quietly) the objects in case you assign the result
  invisible(list(by_variable = by_var, master = master_df))
}


# -----------------------------
# Driver to run everything
# -----------------------------

# Code is currently adapted for the arterial event outcome 
run_all <- function(nsim = 5000L) {
  for (outcome in c("ae")) {
    try(run_outcome_main(outcome, nsim = nsim), silent = FALSE)
    try(run_outcome_subgroups(outcome, nsim = nsim), silent = FALSE)
  }
}

run_all()
