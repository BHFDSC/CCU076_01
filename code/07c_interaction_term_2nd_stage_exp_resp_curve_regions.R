#*******************************************************************************
#
# Project:  CCU076_01
# Title:    Associations between air temperature and cardiovascular health outcomes
# Date:     23-07-2025
# Author:   Isabel Walter
# Purpose:  2nd stage modelling and panel exposure response curve for regional
#           analyses for venous thrombotic event, arterial thrombotic event, ischaemic
#           stroke, myocardial infarction.
#
#*******************************************************************************

# ------------------------------
# Region-stratified outcome panels (region-specific tmean)
# ------------------------------
library(mixmeta)
library(dlnm)
library(ggplot2)
library(dplyr)
library(here)
library(RColorBrewer)
library(patchwork)
library(cowplot)
library(readxl)
library(stringr)

# Load data
df <- read_excel("export/Oct_2025/first_stage_models_with_interaction.xlsx")

df <- df |>
  filter(!(outcome == "mi_fatal" & str_detect(analysis, "14d_lag_sensitivity")))

avgtmeansum <- read.csv(here::here("export/Oct_2025", "CCU076_01_tmean_pm2p5_distribution.csv"))
tmean <- avgtmeansum$tmean_20_22

# Define region order
regions <- c(
  "North West", "Yorkshire and The Humber", "North East",
  "East Midlands", "West Midlands", "East of England",
  "South East", "South West", "London"
)

# Map region name to column name in avgtmeansum
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

# Outcome labels
outcome_labels <- c(
  "ve_fatal" = "Venous thrombosis",
  "ae_fatal" = "Arterial thrombosis",
  "stroke_fatal" = "Ischaemic stroke",
  "mi_fatal" = "Acute myocardial infarction"
)

# Colors
regional_col <- "#1b9e77"
national_col <- "grey50"

# National basis (fixed)
knots_nat <- tmean[avgtmeansum$perc %in% paste0(c(10,75,90), ".0%")]
b_knots_nat <- tmean[avgtmeansum$perc %in% paste0(c(1,99), ".0%")]
bvar_nat <- onebasis(tmean, fun="ns", knots=knots_nat, Bound=b_knots_nat)
cen_nat <- tmean[avgtmeansum$perc %in% paste0(50, ".0%")]

# ------------------------------
# P value formatter (same as your other script)
# ------------------------------
fmt_pval <- function(p) {
  if (is.na(p)) return(NA_character_)
  p <- as.numeric(p)
  if (p < 0.001) return("p < 0.001")
  paste0("Interaction: p = ", sub("\\.?0+$", "", sprintf("%.3f", p)))
}

# ------------------------------
# Plotting function per region (adds p-value annotation)
# ------------------------------
plot_region_panel <- function(cp_region, cp_national, region_name, cen_val, pval = NA_real_) {
  df_region <- data.frame(
    exposure = cp_region$predvar,
    fit = cp_region$allRRfit,
    lower = cp_region$allRRlow,
    upper = cp_region$allRRhigh,
    source = "Regional"
  )
  df_national <- data.frame(
    exposure = cp_national$predvar,
    fit = cp_national$allRRfit,
    lower = cp_national$allRRlow,
    upper = cp_national$allRRhigh,
    source = "National"
  )
  df_all <- rbind(df_region, df_national)
  df_all$source <- factor(df_all$source, levels = c("National", "Regional"))
  
  # annotation position (based on fixed coord_cartesian limits)
  x_min <- -5; x_max <- 30
  y_min <-  0; y_max <-  5
  x_annot <- x_min + 0.03 * (x_max - x_min)
  y_annot <- y_min + 0.95 * (y_max - y_min)
  
  p_lab <- fmt_pval(pval)
  
  ggplot(df_all, aes(x = exposure, y = fit, color = source, fill = source, linetype = source)) +
    geom_ribbon(aes(ymin = lower, ymax = upper), alpha = 0.2, color = NA) +
    geom_line(size = 1) +
    geom_hline(yintercept = 1, linetype = "dashed", color = "grey40") +
    geom_vline(xintercept = cen_val, size = 1.2, alpha = 0.3, color = "grey") +
    annotate("text", x = cen_val + 2.5, y = 3.5, label = "50th perc", color = "grey", size = 3) +
    # p-value annotation
    { if (!is.na(pval)) annotate("text", x = x_annot, y = y_annot, label = p_lab,
                                 hjust = 0, vjust = 1, size = 3.5, color = "black")
      else NULL } +
    labs(title = region_name, x = "Temperature (\u00b0C)", y = "IRR") +
    scale_color_manual(values = c("Regional" = regional_col, "National" = national_col)) +
    scale_fill_manual(values = c("Regional" = regional_col, "National" = national_col)) +
    scale_linetype_manual(values = c("Regional" = "solid", "National" = "dashed")) +
    scale_y_continuous(breaks = seq(0, 5, 1), expand = c(0, 0)) +
    scale_x_continuous(breaks = seq(-5, 30, by = 5), expand = c(0, 0)) +
    coord_cartesian(xlim = c(-5, 30), ylim = c(0, 5)) +
    theme_minimal() +
    theme(
      text = element_text(family = "Helvetica"),
      plot.title=element_text(size=14, hjust=0.5, face="bold"),
      axis.title=element_text(size=13),
      axis.text=element_text(size=11),
      panel.grid = element_blank(),
      axis.line = element_line(size = 0.3, colour = "black"),
      axis.ticks = element_line(size = 0.3, colour = "black"),
      panel.border = element_rect(fill = NA, color = "black", size = 0.5),
      plot.margin = margin(10, 10, 10, 10),
      plot.background = element_rect(fill = "white", color = "white")
    )
}

# ------------------------------
# Loop over outcomes
# ------------------------------
for (outcome in names(outcome_labels)) {
  message("Processing: ", outcome)
  
  # National model
  df_nat <- df %>% filter(analysis == "main_analysis", outcome == !!outcome)
  coef_nat <- as.matrix(df_nat[, grep("coef", names(df_nat))])
  vcov_nat <- as.matrix(df_nat[, grep("vcov", names(df_nat))])
  model_nat <- mixmeta(coef_nat ~ 1, vcov_nat, data = df_nat, method = "fixed")
  cp_nat <- crosspred(
    bvar_nat,
    coef = coef(model_nat),
    vcov = vcov(model_nat),
    model.link = "log",
    at = tmean,
    cen = cen_nat
  )
  
  region_plots <- list()
  
  for (reg in regions) {
    df_reg <- df %>% filter(analysis == "region", outcome == !!outcome, subgroup == reg)
    if (nrow(df_reg) == 0) next
    
    # Extract interaction p-value for THIS outcome + region (same pattern as other script)
    pval_int <- unique(df_reg$interaction_chisq)
    pval_int <- pval_int[!is.na(pval_int)]
    pval_int <- if (length(pval_int) > 0) pval_int[1] else NA_real_
    
    # Region-specific tmean and basis
    t_reg <- avgtmeansum[[tmean_cols[[reg]]]]
    knots_r <- t_reg[avgtmeansum$perc %in% paste0(c(10,75,90), ".0%")]
    b_knots_r <- t_reg[avgtmeansum$perc %in% paste0(c(1,99), ".0%")]
    cen_r <- t_reg[avgtmeansum$perc %in% paste0(50, ".0%")]
    bvar_r <- onebasis(t_reg, fun = "ns", knots = knots_r, Bound = b_knots_r)
    
    coef_r <- as.matrix(df_reg[, grep("coef", names(df_reg))])
    vcov_r <- as.matrix(df_reg[, grep("vcov", names(df_reg))])
    model_r <- mixmeta(coef_r ~ 1, vcov_r, data = df_reg, method = "ml")
    cp_r <- crosspred(
      bvar_r,
      coef = coef(model_r),
      vcov = vcov(model_r),
      model.link = "log",
      at = t_reg,
      cen = cen_r
    )
    
    region_plots[[reg]] <- plot_region_panel(cp_r, cp_nat, reg, cen_r, pval = pval_int)
  }
  
  valid_plots <- Filter(function(x) inherits(x, "gg"), region_plots)
  
  final_plot <- wrap_plots(valid_plots, ncol = 3, guides = "collect") &
    theme(
      legend.position = "bottom",
      legend.title = element_blank(),
      plot.title = element_text(size = 16, face = "bold", family = "Helvetica", hjust = 0.5)
    )
  
  final_plot <- final_plot + plot_annotation(
    title = paste0(outcome_labels[[outcome]], ": Region-stratified analysis")
  )
  
  ggsave(
    filename = here::here("figures", paste0("panel_regions_interaction_", outcome, ".png")),
    plot = final_plot,
    width = 14, height = 14, dpi = 300
  )
}
