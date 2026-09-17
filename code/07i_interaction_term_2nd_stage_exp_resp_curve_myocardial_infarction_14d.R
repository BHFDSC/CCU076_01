#*******************************************************************************
#
# Project:  CCU076_01
# Title:    Associations between air temperature and cardiovascular health outcomes
# Date:     30-10-2025
# Author:   Isabel Walter
# Purpose:  2nd stage modelling and exposure-response curves for mi_fatal only,
#           stratified subgroups, prefixed analyses, and 2x 4-panel outputs.
#
#*******************************************************************************

# ------------------------------
# Load libraries and data
# ------------------------------
library(mixmeta)
library(dlnm)
library(ggplot2)
library(dplyr)
library(here)
library(gridExtra)
library(RColorBrewer)
library(patchwork)  # for shared legend control
library(readxl)

df <- read_excel("export/Oct_2025/first_stage_models_with_interaction.xlsx")
avgtmeansum <- read.csv(here::here("export/Oct_2025", "CCU076_01_tmean_pm2p5_distribution.csv"))
tmean <- avgtmeansum$tmean_20_22

# ------------------------------
# 2nd stage model parameterisation
# ------------------------------
# Create basis and centering
knots <- tmean[avgtmeansum$perc %in% paste0(c(10,75,90), ".0%")]
b_knots <- tmean[avgtmeansum$perc %in% paste0(c(1,99), ".0%")]
bvar <- onebasis(tmean, fun = "ns", knots = knots, Bound = b_knots)
cen <- tmean[avgtmeansum$perc %in% paste0(50, ".0%")]

# ------------------------------
# Analysis config (PREFIXED analyses; mi_fatal only)
# ------------------------------
# Only these 8 stratifiers (drop age_group; keep age_group_bin)
stratifiers <- c(
  "sex", "age_group_bin", "ethnicity", "rural_urban_class",
  "deprivation_bin", "cov_comorb_comp_flag",
  "cov_obesity_combined_flag", "smoking_status"
)

# All analyses are stored with a prefix on df$analysis, e.g. "14d_lag_sensitivity_ethnicity"
analysis_prefix <- "14d_lag_sensitivity_"

# Human-friendly titles for panels (used as each plot's title)
stratifier_titles <- c(
  sex = "Stratified by sex",
  age_group_bin = "Stratified by age group",
  ethnicity = "Stratified by ethnic group",
  rural_urban_class = "Stratified by rural/urban class",
  deprivation_bin = "Stratified by deprivation index",
  cov_comorb_comp_flag = "Stratified by comorbidities",
  cov_obesity_combined_flag = "Stratified by excess weight",
  smoking_status = "Stratified by smoking status"
)

# Colors and linetypes
subgroup_colors <- list(
  sex = c("Female" = "#1b9e77", "Male" = "#d95f02"),
  age_group_bin = c("Age 18-64 years" = "#1b9e77", "Age 65 years and older" = "#d95f02"),
  ethnicity = c(
    "Asian or Asian British" = "#1b9e77",
    "Black, Black British, Caribbean or African" = "#d95f02",
    "Mixed or multiple ethnic groups" = "#7570b3",
    "White" = "#e7298a",
    "Missing ethnicity" = "#66a61e"
  ),
  rural_urban_class = c("Rural" = "#1b9e77", "Urban" = "#d95f02"),
  deprivation_bin = c("Most deprived (quint. 1-3)" = "#d95f02", "Least deprived (quint. 4-5)" = "#1b9e77"),
  cov_comorb_comp_flag = c("Comorbidities" = "#1b9e77", "No comorbidities" = "#d95f02"),
  cov_obesity_combined_flag = c("Excess weight" = "#1b9e77", "No excess weight" = "#d95f02"),
  smoking_status = c(
    "Never smoked" = "#1b9e77",
    "Current smoker" = "#d95f02",
    "Ex-smoker" = "#7570b3",
    "Missing smoking status" = "#e7298a"
  )
)

subgroup_linetypes <- lapply(subgroup_colors, function(x) {
  types <- c("solid", "dashed", "dotted", "dotdash", "longdash", "twodash")
  setNames(rep_len(types, length(x)), names(x))
})

# ------------------------------
# P value formatter (from second script)
# ------------------------------
fmt_pval <- function(p) {
  if (is.na(p)) return(NA_character_)
  p <- as.numeric(p)
  if (p < 0.001) return("p < 0.001")
  paste0("Interaction: p = ", sub("\\.?0+$", "", sprintf("%.3f", p)))
}

# ------------------------------
# Plotting function (UPDATED: add p-value annotation)
# ------------------------------
plot_cp_stratifier <- function(cp_list, strat_var, plot_title, pval = NA_real_) {
  plot_data <- bind_rows(cp_list, .id = "subgroup")
  plot_data$subgroup <- factor(plot_data$subgroup, levels = names(subgroup_colors[[strat_var]]))
  colors <- subgroup_colors[[strat_var]]
  
  # annotation position (based on your coord_cartesian limits)
  x_min <- -5; x_max <- 30
  y_min <-  0; y_max <-  3
  x_annot <- x_min + 0.03 * (x_max - x_min)
  y_annot <- y_min + 0.95 * (y_max - y_min)
  
  p_lab <- fmt_pval(pval)
  
  ggplot(plot_data, aes(x = exposure, y = fit, color = subgroup, fill = subgroup, linetype = subgroup)) +
    geom_ribbon(aes(ymin = lower, ymax = upper), alpha = 0.2, color = NA) +
    geom_line(size = 1) +
    geom_hline(yintercept = 1, linetype = "dashed", color = "grey") +
    geom_vline(xintercept = cen, size = 1.3, alpha = 0.3, color = "grey") +
    annotate("text", x = cen + 2, y = 3.5, label = "50th perc", color = "grey30", size = 3) +
    # p-value annotation (only if available)
    { if (!is.na(pval)) annotate("text",
                                 x = x_annot, y = y_annot, label = p_lab,
                                 hjust = 0, vjust = 1, size = 3.5, color = "black")
      else NULL } +
    labs(
      title = plot_title,
      x = "Temperature (°C)",
      y = "Incidence rate ratio (IRR)"
    ) +
    scale_color_manual(values = colors, drop = FALSE) +
    scale_fill_manual(values = colors, drop = FALSE) +
    scale_linetype_manual(values = subgroup_linetypes[[strat_var]], drop = FALSE) +
    scale_y_continuous(breaks = seq(0, 3, 1), expand = c(0, 0)) +
    scale_x_continuous(breaks = seq(-5, 30, by = 5), expand = c(0, 0)) +
    coord_cartesian(xlim = c(-5, 30), ylim = c(0, 3)) +
    theme_minimal() +
    theme(
      text = element_text(family = "Helvetica"),
      panel.grid = element_blank(),
      plot.title = element_text(size = 13, hjust = 0.5, face = "bold"),
      axis.title = element_text(size = 12),
      axis.text = element_text(size = 10),
      axis.line = element_line(size = 0.3, colour = "black"),
      axis.ticks = element_line(size = 0.3, colour = "black"),
      plot.margin = margin(10, 10, 10, 10),
      plot.background = element_rect(color = "white", fill = "white"),
      panel.border = element_rect(fill = NA, color = "black", size = 0.5),
      legend.position = "none"
    )
}

# ------------------------------
# Build each stratifier plot (mi_fatal only) + p-values
# ------------------------------
plots_by_strat <- list()

for (strat in stratifiers) {
  message("Processing stratifier: ", strat)
  
  analysis_name <- paste0(analysis_prefix, strat)
  
  # Filter df to this prefixed analysis and to mi_fatal only
  df_filtered <- df %>%
    filter(analysis == analysis_name, outcome == "mi_fatal")
  
  # Special-case trimming to 3 groups for ethnicity & smoking
  if (strat == "ethnicity") {
    keep_eth <- c("White", "Asian or Asian British", "Black, Black British, Caribbean or African")
    df_filtered <- df_filtered %>% filter(subgroup %in% keep_eth)
    subgroup_colors$ethnicity <- subgroup_colors$ethnicity[
      names(subgroup_colors$ethnicity) %in% keep_eth
    ]
  } else if (strat == "smoking_status") {
    keep_smoke <- c("Never smoked", "Current smoker", "Ex-smoker")
    df_filtered <- df_filtered %>% filter(subgroup %in% keep_smoke)
    subgroup_colors$smoking_status <- subgroup_colors$smoking_status[
      names(subgroup_colors$smoking_status) %in% keep_smoke
    ]
  }
  
  # Recode deprivation labels to match color map
  if (strat == "deprivation_bin") {
    df_filtered$subgroup <- dplyr::recode(
      df_filtered$subgroup,
      "Low deprivation index (1-3)" = "Most deprived (quint. 1-3)",
      "High deprivation index (4-5)" = "Least deprived (quint. 4-5)"
    )
  }
  
  # Skip if nothing left
  if (nrow(df_filtered) == 0) next
  
  # ---- pull interaction p-value like in the second script ----
  # (assumes column is called interaction_chisq in df)
  pval_int <- unique(df_filtered$interaction_chisq)
  pval_int <- pval_int[!is.na(pval_int)]
  pval_int <- if (length(pval_int) > 0) pval_int[1] else NA_real_
  
  # Determine available subgroups in the intended display order
  available_subgroups <- intersect(
    names(subgroup_colors[[strat]]),
    unique(df_filtered$subgroup)
  ) %>% factor(levels = names(subgroup_colors[[strat]])) %>% as.character()
  
  if (length(available_subgroups) == 0) next
  
  # Build cross-predictions list for each subgroup
  cp_list <- setNames(vector("list", length(available_subgroups)), available_subgroups)
  
  for (sg in available_subgroups) {
    subdat <- df_filtered %>% filter(subgroup == sg)
    if (nrow(subdat) == 0) next
    
    coef_mat <- as.matrix(subdat[, grep("^coef", names(subdat))])
    vcov_mat <- as.matrix(subdat[, grep("^vcov", names(subdat))])
    
    # Fixed-effects meta within this subgroup
    model_sg <- mixmeta(coef_mat ~ 1, vcov_mat, data = subdat, method = "fixed")
    
    # Cross-prediction on the same basis/grid
    cp_sg <- crosspred(
      bvar,
      coef = coef(model_sg),
      vcov = vcov(model_sg),
      at   = tmean,
      model.link = "log",
      cen  = cen
    )
    
    cp_list[[sg]] <- data.frame(
      exposure = cp_sg$predvar,
      fit      = cp_sg$allRRfit,
      lower    = cp_sg$allRRlow,
      upper    = cp_sg$allRRhigh
    )
  }
  
  # Title per-plot is the subgroup/stratifier name
  this_title <- stratifier_titles[[strat]]
  
  # UPDATED: pass pval into plotting function
  plots_by_strat[[strat]] <- plot_cp_stratifier(
    cp_list,
    strat_var  = strat,
    plot_title = this_title,
    pval       = pval_int
  )
}

# ------------------------------
# Assemble two 4-panel figures (8 total plots)
# ------------------------------
ordered_strats <- c(
  "sex", "age_group_bin", "ethnicity", "rural_urban_class",
  "deprivation_bin", "cov_comorb_comp_flag",
  "cov_obesity_combined_flag", "smoking_status"
)

ordered_strats <- ordered_strats[ordered_strats %in% names(plots_by_strat)]
group1 <- ordered_strats[1:min(4, length(ordered_strats))]
group2 <- ordered_strats[(length(group1) + 1):min(8, length(ordered_strats))]

panel_title <- "Acute myocardial infarction 14 day lag: Subgroup analyses"

# Panel 1
if (length(group1) > 0) {
  panel1 <- wrap_plots(
    lapply(plots_by_strat[group1], function(p) {
      p + theme(legend.position = "bottom", legend.title = element_blank())
    }),
    ncol = 2
  )
  panel1 <- panel1 + plot_annotation(
    title = panel_title,
    theme = theme(plot.title = element_text(size = 16, face = "bold", hjust = 0.5))
  )
  ggsave(
    filename = here::here("figures", paste0("panel1_mi_fatal_",analysis_prefix, "_subgroups_interaction.png")),
    plot = panel1, width = 12, height = 10, dpi = 300
  )
}

# Panel 2
if (length(group2) > 0) {
  panel2 <- wrap_plots(
    lapply(plots_by_strat[group2], function(p) {
      p + theme(legend.position = "bottom", legend.title = element_blank())
    }),
    ncol = 2
  )
  
  panel2 <- panel2 + plot_annotation(
    title = panel_title,
    theme = theme(plot.title = element_text(size = 16, face = "bold", hjust = 0.5))
  )
  ggsave(
    filename = here::here("figures", paste0("panel2_mi_fatal_", analysis_prefix, "_subgroups_interaction.png")),
    plot = panel2, width = 12, height = 10, dpi = 300
  )
}
