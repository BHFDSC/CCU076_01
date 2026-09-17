#*******************************************************************************
#
# Project:  CCU076_01
# Title:    Associations between air temperature and cardiovascular health outcomes
# Date:     23-07-2025
# Author:   Isabel Walter
# Purpose:  Create dataframes with cumulative IRRs per percentile.
#*******************************************************************************

# Load required libraries
library(tidyverse)
library(mixmeta)
library(splines)
library(here)
library(forcats)
library(readxl)
library(writexl)

# Load data
avgtmeansum <- read.csv(here("export/Oct_2025", "CCU076_01_tmean_pm2p5_distribution.csv"))

tmean_cols <- list(
  "North West" = "tmean_20_22_North.West",
  "Yorkshire and The Humber" = "tmean_20_22_Yorkshire.and.The.Humber",
  "North East" = "tmean_20_22_North.East",
  "East Midlands" = "tmean_20_22_East.Midlands",
  "West Midlands" = "tmean_20_22_West.Midlands",
  "East of England" = "tmean_20_22_East.of.England",
  "South East" = "tmean_20_22_South.East",
  "South West" = "tmean_20_22_South.West",
  "London" = "tmean_20_22_London"
)

# Define subgroup structure
subgroup_info <- list(
  "main_analysis" = NA,
  "sex" = c("Female", "Male"),
  "age_group_bin" = c("Age 18-64 years", "Age 65 years and older"),
  "ethnicity" = c("Asian or Asian British", "Black, Black British, Caribbean or African", "White"),
  "rural_urban_class" = c("Rural", "Urban"),
  "deprivation_bin" = c("Most deprived (quint. 1-3)", "Least deprived (quint. 4-5)"),
  "cov_comorb_comp_flag" = c("Comorbidities", "No comorbidities"),
  "cov_obesity_combined_flag" = c("Excess weight", "No excess weight"),
  "smoking_status" = c("Never smoked", "Current smoker", "Ex-smoker"),
  "COVID19_6mo" = c("COVID19 diagnosis within 6 mo before first event", "No COVID19 diagnosis within 6 mo before first event"),
  "region" = names(tmean_cols)
)
# OUTCOMES TO PROCESS
outcomes <- c("ve_fatal", "ae_fatal", "stroke_fatal", "mi_fatal")

for (outcome_name in outcomes) {
  
  # Load outcome-specific data
  df <- read_excel("export/Oct_2025/first_stage_models_with_interaction.xlsx") |>
    filter(outcome == outcome_name, subgroup != "Missing smoking status")
  
  # Recode subgroups
  df$subgroup <- recode(df$subgroup,
                        "Low deprivation index (1-3)" = "Most deprived (quint. 1-3)",
                        "High deprivation index (4-5)" = "Least deprived (quint. 4-5)"
  )
  
  # Reference temp distribution
  tmean_main <- avgtmeansum$tmean_20_22
  knots <- tmean_main[avgtmeansum$perc %in% paste0(c(10, 75, 90), ".0%")]
  b_knots <- tmean_main[avgtmeansum$perc %in% paste0(c(1, 99), ".0%")]
  bvar <- onebasis(tmean_main, fun = "ns", knots = knots, Bound = b_knots)
  cen <- tmean_main[avgtmeansum$perc == "50.0%"]
  
  # Start base table with percentiles
  irr_table <- data.frame(
    percentile = avgtmeansum$perc
  )
  
  # --- Overall model ---
  main_df <- df %>% filter(analysis == "main_analysis")
  coef <- as.matrix(main_df[, grep("^coef", names(main_df))])
  vcov_obj <- as.matrix(main_df[, grep("^vcov", names(main_df))])
  
  model <- mixmeta(coef ~ 1, vcov_obj, method = "fixed")
  cp <- crosspred(bvar, coef = coef(model), vcov = vcov(model),
                  at = tmean_main, model.link = "log", cen = cen)
  
  irr_table$lag0_21_overall <- cp$allRRfit
  irr_table$lag0_21_overall_lower <- cp$allRRlow
  irr_table$lag0_21_overall_upper <- cp$allRRhigh
  
  # --- Subgroup and region models ---
  for (analysis_name in names(subgroup_info)) {
    if (analysis_name == "main_analysis") next
    subgroups <- subgroup_info[[analysis_name]]
    
    if (analysis_name == "region") {
      for (region in subgroups) {
        tmean_col <- tmean_cols[[region]]
        tmean_region <- as.numeric(avgtmeansum[[tmean_col]])
        if (all(is.na(tmean_region))) next
        
        knots <- tmean_region[avgtmeansum$perc %in% paste0(c(10, 75, 90), ".0%")]
        b_knots <- tmean_region[avgtmeansum$perc %in% paste0(c(1, 99), ".0%")]
        bvar <- onebasis(tmean_region, fun = "ns", knots = knots, Bound = b_knots)
        cen <- tmean_region[avgtmeansum$perc == "50.0%"]
        
        df_region <- df %>% filter(analysis == "region", subgroup == region)
        if (nrow(df_region) == 0) next
        
        coef <- as.matrix(df_region[, grep("^coef", names(df_region))])
        vcov_obj <- as.matrix(df_region[, grep("^vcov", names(df_region))])
        
        model <- mixmeta(coef ~ 1, vcov_obj, method = "fixed")
        cp <- crosspred(bvar, coef = coef(model), vcov = vcov(model),
                        at = tmean_region, model.link = "log", cen = cen)
        
        base <- paste0("lag0_21_", region)
        irr_table[[base]] <- cp$allRRfit
        irr_table[[paste0(base, "_lower")]] <- cp$allRRlow
        irr_table[[paste0(base, "_upper")]] <- cp$allRRhigh
      }
    } else {
      # --- PER-SUBGROUP FIXED-EFFECTS (coef ~ 1) TABLE COLUMNS ---
      df_sub <- df %>% dplyr::filter(analysis == analysis_name)
      if (nrow(df_sub) == 0) next
      
      # keep intended order
      df_sub$subgroup <- factor(df_sub$subgroup, levels = subgroups)
      
      for (sg in subgroups) {
        # IMPORTANT: filter by outcome AND subgroup
        subdat <- df_sub %>%
          dplyr::filter(outcome == outcome_name, subgroup == sg)
        
        if (nrow(subdat) == 0) next
        
        coef_mat <- as.matrix(subdat[, grep("^coef", names(subdat))])
        vcov_mat <- as.matrix(subdat[, grep("^vcov", names(subdat))])
        
        # fixed-effects meta within this subgroup only
        model_sg <- mixmeta(coef_mat ~ 1, vcov_mat, data = subdat, method = "fixed")
        
        cp_sg <- crosspred(
          bvar,
          coef = coef(model_sg),
          vcov = vcov(model_sg),
          at   = tmean_main,
          model.link = "log",
          cen  = cen
        )
        
        base <- paste0("lag0_21_", sg)
        irr_table[[base]]                 <- cp_sg$allRRfit
        irr_table[[paste0(base, "_lower")]] <- cp_sg$allRRlow
        irr_table[[paste0(base, "_upper")]] <- cp_sg$allRRhigh
      }
    }
  }
  
  # Save as Excel
  out_file <- here("tables", paste0("cumulative_IRR_by_percentile_interaction_term_", outcome_name, ".xlsx"))
  dir.create(dirname(out_file), showWarnings = FALSE, recursive = TRUE)
  write_xlsx(irr_table, out_file)
  
  message("Saved: ", out_file)
  
}



##### IRR table sensitivity analyses ####

# ---------- Temperature distributions ----------
avgtmeansum <- read.csv(here("export/Oct_2025", "CCU076_01_tmean_pm2p5_distribution.csv"))

# England-wide
tmean_20_22 <- avgtmeansum$tmean_20_22
tmean_20_21 <- avgtmeansum$tmean_20_21

# Region columns (for 20–22)
tmean_cols <- list(
  "North West"               = "tmean_20_22_North.West",
  "Yorkshire and The Humber" = "tmean_20_22_Yorkshire.and.The.Humber",
  "North East"               = "tmean_20_22_North.East",
  "East Midlands"            = "tmean_20_22_East.Midlands",
  "West Midlands"            = "tmean_20_22_West.Midlands",
  "East of England"          = "tmean_20_22_East.of.England",
  "South East"               = "tmean_20_22_South.East",
  "South West"               = "tmean_20_22_South.West",
  "London"                   = "tmean_20_22_London"
)
regions <- names(tmean_cols)

base_analyses <- c(
  "14d_lag_sensitivity",
  "sensitivity_cens_at_death",
  "adjusted_for_covid19",
  "adjusted_for_lockdown",
  "sensitivity_fatal_1week",
  "sensitivity_fatal_2week",
  "sensitivity_fatal_month",
  "sensitivity_first_event",
  "sensitivity_nonfatal_1week",
  "sensitivity_nonfatal_2week",
  "sensitivity_nonfatal_month",
  "sensitivity_restricted_study_period_may",
  "4d_lag_sensitivity",
  "7d_lag_sensitivity"
)


# ---------- Subgroup structure ----------
subgroup_info <- list(
  "sex"                      = c("Female", "Male"),
  "age_group_bin"            = c("Age 18-64 years", "Age 65 years and older"),
  "ethnicity"                = c("Asian or Asian British",
                                 "Black, Black British, Caribbean or African",
                                 "White"),
  "rural_urban_class"        = c("Rural", "Urban"),
  "deprivation_bin"          = c("Most deprived (quint. 1-3)",
                                 "Least deprived (quint. 4-5)"),
  "cov_comorb_comp_flag"     = c("Comorbidities", "No comorbidities"),
  "cov_obesity_combined_flag"= c("Excess weight", "No excess weight"),
  "smoking_status"           = c("Never smoked", "Current smoker", "Ex-smoker"),
  "COVID19_6mo" = c("COVID19 diagnosis within 6 mo before first event", "No COVID19 diagnosis within 6 mo before first event"),
  "region"                   = names(tmean_cols)
)

# ---------- First-stage path & outcomes ----------
first_stage_path <- "export/Oct_2025/first_stage_models_with_interaction.xlsx"
outcomes <- c("ve_fatal", "ae_fatal", "stroke_fatal", "mi_fatal")

# ---------- Helpers ----------
make_basis <- function(tmean_vec, perc = avgtmeansum$perc) {
  knots   <- tmean_vec[perc %in% paste0(c(10, 75, 90), ".0%")]
  b_knots <- tmean_vec[perc %in% paste0(c(1, 99),  ".0%")]
  list(
    bvar = onebasis(tmean_vec, fun = "ns", knots = knots, Bound = b_knots),
    cen  = tmean_vec[perc == "50.0%"],
    at   = tmean_vec
  )
}

fit_with_basis <- function(df_sub, basis) {
  if (nrow(df_sub) == 0) return(NULL)
  coef_mat <- as.matrix(df_sub[, grep("^coef", names(df_sub)), drop = FALSE])
  vcov_mat <- as.matrix(df_sub[, grep("^vcov", names(df_sub)), drop = FALSE])
  model    <- mixmeta(coef_mat ~ 1, vcov_mat, data = df_sub, method = "fixed")
  cp <- crosspred(basis$bvar, coef = coef(model), vcov = vcov(model),
                  at = basis$at, model.link = "log", cen = basis$cen)
  list(fit = cp$allRRfit, low = cp$allRRlow, high = cp$allRRhigh)
}

safe_name <- function(x) gsub("[^A-Za-z0-9]+", "_", x)

# ---------- Main loop ----------
for (outcome_name in outcomes) {
  
  df <- read_excel(first_stage_path) |>
    filter(outcome == outcome_name, subgroup != "Missing smoking status") |>
    mutate(
      analysis = str_squish(analysis),
      subgroup = str_squish(subgroup),
      subgroup = recode(subgroup,
                        "Low deprivation index (1-3)"  = "Most deprived (quint. 1-3)",
                        "High deprivation index (4-5)" = "Least deprived (quint. 4-5)"
      )
    )
  
  irr_table <- tibble(percentile = avgtmeansum$perc)
  
  # ==============================
  # England-wide (20–22)
  # ==============================
  irr_table$tmean_20_22 <- tmean_20_22
  basis_main <- make_basis(tmean_20_22)
  
  # --- 1. Base and main sensitivity analyses ---
  for (an in base_analyses) {
    df_an <- df |> filter(analysis == an)
    res <- fit_with_basis(df_an, basis_main)
    if (is.null(res)) next
    irr_table[[paste0(an, "_fit")]]   <- res$fit
    irr_table[[paste0(an, "_lower")]] <- res$low
    irr_table[[paste0(an, "_upper")]] <- res$high
  }
  
  # --- 2. 14-day lag subgroup analyses (immediately after tmean_20_22 block) ---
  if (outcome_name == "mi_fatal") {
    for (sg_name in names(subgroup_info)) {
      
      if (sg_name == "region") next   # region handled later
      subgroup_levels <- subgroup_info[[sg_name]]
      an_name <- paste0("14d_lag_sensitivity_", sg_name)
      
      df_sub <- df |> filter(analysis == an_name)
      if (nrow(df_sub) == 0) next
      
      df_sub$subgroup <- factor(df_sub$subgroup, levels = subgroup_levels)
      
      for (sg in subgroup_levels) {
        subdat <- df_sub |> filter(subgroup == sg)
        if (nrow(subdat) == 0) next
        res <- fit_with_basis(subdat, basis_main)
        if (is.null(res)) next
        
        base <- paste0(an_name, "_", safe_name(sg))
        irr_table[[paste0(base, "_fit")]]   <- res$fit
        irr_table[[paste0(base, "_lower")]] <- res$low
        irr_table[[paste0(base, "_upper")]] <- res$high
      }
    }
  }
  
  # ==============================
  # Air pollution (20–21)
  # ==============================
  irr_table$tmean_20_21 <- tmean_20_21
  basis_air <- make_basis(tmean_20_21)
  for (an in c("adjusted_for_air_pollution", "air_pollution_period_unadjusted")) {
    df_an <- df |> filter(analysis == an)
    res <- fit_with_basis(df_an, basis_air)
    if (is.null(res)) next
    irr_table[[paste0(an, "_fit")]]   <- res$fit
    irr_table[[paste0(an, "_lower")]] <- res$low
    irr_table[[paste0(an, "_upper")]] <- res$high
  }
  
  # ==============================
  # Regional 14d analyses (after everything else)
  # ==============================
  if (outcome_name == "mi_fatal") {
    for (rg in regions) {
      rg_safe <- safe_name(rg)
      tcol <- tmean_cols[[rg]]
      tmean_region <- as.numeric(avgtmeansum[[tcol]])
      irr_table[[paste0("tmean_20_22_", rg_safe)]] <- tmean_region
      
      df_rg <- df |> filter(analysis == "14d_lag_sensitivity_region", subgroup == rg)
      if (nrow(df_rg) == 0) next
      
      region_basis <- make_basis(tmean_region)
      res <- fit_with_basis(df_rg, region_basis)
      if (is.null(res)) next
      
      base <- paste0("14d_lag_sensitivity_region_", rg_safe)
      irr_table[[paste0(base, "_fit")]]   <- res$fit
      irr_table[[paste0(base, "_lower")]] <- res$low
      irr_table[[paste0(base, "_upper")]] <- res$high
    }
  }
  
  # ==============================
  # Save results
  # ==============================
  out_file <- here("tables", paste0("cumulative_IRR_by_percentile_sens_interaction_term_", outcome_name, ".xlsx"))
  dir.create(dirname(out_file), showWarnings = FALSE, recursive = TRUE)
  write_xlsx(irr_table, out_file)
  message("Saved: ", out_file)
}
