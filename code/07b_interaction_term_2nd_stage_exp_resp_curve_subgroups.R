#*******************************************************************************
#
# Project:  CCU076_01
# Title:    Associations between air temperature and cardiovascular health outcomes
# Date:     23-07-2025
# Author:   Isabel Walter
# Purpose:  2nd stage modelling and panel exposure response curve for stratified
#           analyses for venous thrombotic event, arterial thrombotic event, ischaemic 
#           stroke, myocardial infarction, based on models with interaction term.
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
# Define analysis, outcome, subgroup vectors
# ------------------------------

# Outcome label map

outcome_labels <- c(
  "ve_fatal" = "Venous thrombosis",
  "ae_fatal" = "Arterial thrombosis",
  "stroke_fatal" = "Ischaemic stroke",
  "mi_fatal" = "Acute myocardial infarction"
)

# Stratifying variables (from df$analysis)
stratifiers <- c(
  "sex", "age_group", "age_group_bin", "ethnicity", "rural_urban_class",
  "deprivation_bin", "cov_comorb_comp_flag", "cov_obesity_combined_flag",
  "smoking_status"
  
)

# Fixed subgroup color maps
subgroup_colors <- list(
  sex = c("Female" = "#1b9e77", "Male" = "#d95f02"),
  
  age_group = c(
    "Age 18-64 years" = "#1b9e77",
    "Age 65-74 years" = "#d95f02",
    "Age 75-84 years" = "#7570b3",
    "Age 85 years or older" = "#e7298a"
  ),
  
  age_group_bin = c(
    "Age 18-64 years" = "#1b9e77",
    "Age 65 years and older" = "#d95f02"
  ),
  
  ethnicity = c(
    "Asian or Asian British" = "#1b9e77",
    "Black, Black British, Caribbean or African" = "#d95f02",
    "other" = "#7570b3",
    "White" = "#e7298a"
  ),
  
  rural_urban_class = c("Rural" = "#1b9e77", "Urban" = "#d95f02"),
  
  deprivation_bin = c(
    "Most deprived (quint. 1-3)" = "#d95f02",
    "Least deprived (quint. 4-5)" = "#1b9e77"
  ),
  
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
# P value formatter 
# ------------------------------

fmt_pval <- function(p) {
  if (is.na(p)) return(NA_character_)
  p <- as.numeric(p)
  if (p < 0.001) return("p < 0.001")
  paste0("Interaction: p = ", sub("\\.?0+$", "", sprintf("%.3f", p)))
}

# ------------------------------
# Plotting function
# ------------------------------

# Plot function for one outcome
plot_cp_outcome <- function(cp_list, outcome_name, strat_var, pval = NA_real_) {
  plot_data <- bind_rows(cp_list, .id = "subgroup")
  plot_data$subgroup <- factor(plot_data$subgroup, levels = names(subgroup_colors[[strat_var]]))
  colors <- subgroup_colors[[strat_var]]
  
  # desired annotation position (based on your fixed coord_cartesian limits)
  x_min <- -5; x_max <- 30
  y_min <-  0; y_max <-  5
  x_annot <- x_min + 0.03 * (x_max - x_min)
  y_annot <- y_min + 0.95 * (y_max - y_min)
  
  p_lab <- fmt_pval(pval)
  
  ggplot(plot_data, aes(x = exposure, y = fit, color = subgroup, fill = subgroup, linetype = subgroup)) +
    geom_ribbon(aes(ymin = lower, ymax = upper), alpha = 0.2, color = NA) +
    geom_line(size = 1) +
    geom_hline(yintercept = 1, linetype = "dashed", color = "grey") +
    geom_vline(xintercept = cen, size = 1.5, alpha = 0.3, color = "grey") +
    annotate("text", x = cen + 2, y = 3.5, label = "50th perc", color = "grey", size = 3) +
    # p-value annotation
    { if (!is.na(pval)) annotate("text", x = x_annot, y = y_annot, label = p_lab,
                                 hjust = 0, vjust = 1, size = 3.5, color = "black")
      else NULL } +
    labs(
      title = paste0("Exposure-response curve: ", outcome_name),
      x = "Temperature (°C)",
      y = "Incidence rate ratio (IRR)"
    ) +
    scale_color_manual(values = colors, drop = FALSE) +
    scale_fill_manual(values = colors, drop = FALSE) +
    scale_linetype_manual(values = subgroup_linetypes[[strat_var]], drop = FALSE) +
    scale_y_continuous(breaks = seq(0, 5, 1), expand = c(0, 0)) +
    scale_x_continuous(breaks = seq(-5, 30, by = 5), expand = c(0, 0)) +
    coord_cartesian(xlim = c(-5, 30), ylim = c(0, 5)) +
    theme_minimal() +
    theme(
      text = element_text(family = "Helvetica"),
      panel.grid = element_blank(),
      plot.title = element_text(size = 14, hjust = 0.5, face = "bold"),
      axis.title = element_text(size = 13),
      axis.text = element_text(size = 11),
      axis.line = element_line(size = 0.3, colour = "black"),
      axis.ticks = element_line(size = 0.3, colour = "black"),
      plot.margin = margin(10, 10, 10, 10),
      plot.background = element_rect(color = "white", fill = "white"),
      panel.border = element_rect(fill = NA, color = "black", size = 0.5),
      legend.position = "none"
    )
}
# ------------------------------
# Making panels
# ------------------------------

# Main loop per stratifier
for (strat in stratifiers) {
  message("Processing: ", strat)
  
  # Special case: filter ethnicity to only 3 groups
  if (strat == "ethnicity") {
    df_filtered <- df %>%
      filter(analysis == strat, subgroup %in% c("White", "Asian or Asian British", "Black, Black British, Caribbean or African"))
    subgroup_colors$ethnicity <- subgroup_colors$ethnicity[
      names(subgroup_colors$ethnicity) %in% c("White", "Asian or Asian British", "Black, Black British, Caribbean or African")]
  } else if (strat == "smoking_status"){
    df_filtered <- df %>%
      filter(analysis == strat, subgroup %in% c("Never smoked", "Current smoker", "Ex-smoker"))
    subgroup_colors$smoking_status <- subgroup_colors$smoking_status[
      names(subgroup_colors$smoking_status) %in% c("Never smoked", "Current smoker", "Ex-smoker")]
  } else {
    df_filtered <- df %>% filter(analysis == strat)
  }
  
  if (strat == "deprivation_bin") {
    df_filtered$subgroup <- recode(df_filtered$subgroup,
                                   "Low deprivation index (1-3)" = "Most deprived (quint. 1-3)",
                                   "High deprivation index (4-5)" = "Least deprived (quint. 4-5)"
    )
  }
  
  # Skip if no data
  if (nrow(df_filtered) == 0) next
  
  panel_plots <- list()
  
  for (o in names(outcome_labels)) {
    tmeanpar <- df_filtered %>% filter(outcome == o)
    if (nrow(tmeanpar) == 0) next
    
    coef <- as.matrix(tmeanpar[, grep("coef", names(tmeanpar))])
    vcov <- as.matrix(tmeanpar[, grep("vcov", names(tmeanpar))])
    
    ## --- RUN A FIXED-EFFECTS META FOR EACH SUBGROUP SEPARATELY (coef ~ 1) ---
    available_subgroups <- intersect(names(subgroup_colors[[strat]]),
                                     unique(tmeanpar$subgroup)) %>%
      factor(levels = names(subgroup_colors[[strat]])) %>%
      as.character()
    if (length(available_subgroups) == 0) next
    
    # Build cp list: one crosspred per subgroup
    cp_list <- setNames(vector("list", length(available_subgroups)), available_subgroups)
    
    for (sg in available_subgroups) {
      # IMPORTANT: filter on outcome AND subgroup for the second-stage fit
      subdat <- tmeanpar %>%
        dplyr::filter(outcome == o, subgroup == sg)
      
      if (nrow(subdat) == 0) next
      
      # Extract first-stage coefficients and their (co)variances for this subgroup
      coef_mat <- as.matrix(subdat[, grep("^coef", names(subdat))])
      vcov_mat <- as.matrix(subdat[, grep("^vcov", names(subdat))])
      
      # Fixed-effects meta within this subgroup only
      model_sg <- mixmeta(coef_mat ~ 1, vcov_mat, data = subdat, method = "fixed")
      
      # Cross-prediction on your basis / grid (kept 'tmean' to match how 'bvar' was built)
      cp_sg <- crosspred(
        bvar,
        coef = coef(model_sg),
        vcov = vcov(model_sg),
        at   = tmean,
        model.link = "log",
        cen  = cen
      )
      
      # Store tidy frame for plotting
      cp_list[[sg]] <- data.frame(
        exposure = cp_sg$predvar,
        fit      = cp_sg$allRRfit,
        lower    = cp_sg$allRRlow,
        upper    = cp_sg$allRRhigh
      )
    }
    names(cp_list) <- available_subgroups
    
    pval_int <- unique(tmeanpar$interaction_chisq)
    pval_int <- pval_int[!is.na(pval_int)]
    pval_int <- if (length(pval_int) > 0) pval_int[1] else NA_real_
    
    # Create plot for this outcome
    panel_plots[[o]] <- plot_cp_outcome(cp_list, outcome_labels[o], strat, pval = pval_int)
    
  }
  
  # Combine 4 outcomes into one panel
  if (length(panel_plots) > 0) {
    new_order <- c("ve_fatal", "ae_fatal", "mi_fatal", "stroke_fatal")  # swap mi and stroke
    panel_ordered <- panel_plots[new_order]
    panel <- wrap_plots(panel_ordered, ncol = 2, guides = "collect") &
      theme(legend.position = "bottom",
            legend.title = element_blank())
    
    # Map of titles for each stratifier
    panel_titles <- c(
      sex = "Stratification by sex",
      age_group = "Stratification by age group",
      age_group_bin = "Stratification by binary age group",
      ethnicity = "Stratification by ethnic group",
      rural_urban_class = "Stratification by rural/urban class",
      deprivation_bin = "Stratification by deprivation index",
      cov_comorb_comp_flag = "Stratification by comorbidities",
      cov_obesity_combined_flag = "Stratification by excess weight",
      smoking_status = "Stratification by smoking status"
    )
    
    # Add the panel title
    panel <- panel + plot_annotation(
      title = panel_titles[strat],
      theme = theme(
        plot.title = element_text(
          size = 15,
          face = "bold",
          family = "Helvetica",
          hjust = 0.5
        )
      )
    )
    
    ggsave(
      filename = here::here("figures", paste0("panel_subgroup_interaction_", strat, ".png")),
      plot = panel,
      width = 12, height = 10, dpi = 300
    )
  }
}
