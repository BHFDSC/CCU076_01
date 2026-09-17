#*******************************************************************************
# Project:  CCU076_01
# Title:    Associations between air temperature and cardiovascular health outcomes
# Date:     23-07-2025
# Author:   Isabel Walter
# Purpose:  2nd stage modelling and panel exposure response curve
#           stratified by COVID-19 status at outcome
#*******************************************************************************

# ------------------------------
# Libraries & data
# ------------------------------
library(mixmeta)
library(dlnm)
library(ggplot2)
library(dplyr)
library(here)
library(RColorBrewer)
library(cowplot)
library(readxl)
library(patchwork)
library(grid)   # for unit()

# Load data
df <- read_excel("export/Oct_2025/first_stage_models_with_interaction.xlsx")

# tmean distribution
avgtmeansum <- read.csv(here::here("export/Oct_2025", "CCU076_01_tmean_pm2p5_distribution.csv"))
tmean <- avgtmeansum$tmean_20_22

# ------------------------------
# 2nd-stage parameterisation
# ------------------------------
knots <- tmean[avgtmeansum$perc %in% paste0(c(10, 75, 90), ".0%")]
b_knots <- tmean[avgtmeansum$perc %in% paste0(c(1, 99), ".0%")]
bvar   <- onebasis(tmean, fun = "ns", knots = knots, Bound = b_knots)
cen    <- tmean[avgtmeansum$perc %in% paste0(50, ".0%")]

# ------------------------------
# Outcome labels
# ------------------------------
outcomes <- c("ve_fatal", "ae_fatal", "mi_fatal", "stroke_fatal")
outcome_labels <- c(
  "ve_fatal"     = "Venous thrombosis",
  "ae_fatal"     = "Arterial thrombosis",
  "mi_fatal"     = "Acute myocardial infarction",
  "stroke_fatal" = "Ischaemic stroke"
)

# ------------------------------
# P value formatter (same logic as your interaction script)
# ------------------------------
fmt_pval <- function(p) {
  if (is.na(p)) return(NA_character_)
  p <- as.numeric(p)
  if (p < 0.001) return("Interaction: p < 0.001")
  paste0("Interaction: p = ", sub("\\.?0+$", "", sprintf("%.3f", p)))
}

# Try to pull an interaction p-value from whatever column exists
get_pval <- function(df_sub) {
  # priority: match your interaction script
  if ("interaction_chisq" %in% names(df_sub)) {
    p <- unique(df_sub$interaction_chisq)
    p <- p[!is.na(p)]
    return(if (length(p) > 0) as.numeric(p[1]) else NA_real_)
  }
  # fallbacks if your file uses other names
  candidates <- c("interaction_p", "p_interaction", "p_value_interaction", "pval_interaction")
  hit <- intersect(candidates, names(df_sub))
  if (length(hit) > 0) {
    p <- unique(df_sub[[hit[1]]])
    p <- p[!is.na(p)]
    return(if (length(p) > 0) as.numeric(p[1]) else NA_real_)
  }
  NA_real_
}

# ------------------------------
# Helper: build crosspred DF
# ------------------------------
build_cp_df <- function(df_sub) {
  coef_cols <- grep("^coef", names(df_sub), value = TRUE)
  vcov_cols <- grep("^vcov", names(df_sub), value = TRUE)
  
  coef_mat <- as.matrix(df_sub[, coef_cols, drop = FALSE])
  vcov_mat <- as.matrix(df_sub[, vcov_cols, drop = FALSE])
  
  model <- mixmeta(coef_mat ~ 1, vcov_mat, data = df_sub, method = "fixed")
  
  cp <- crosspred(bvar, coef = coef(model), vcov = vcov(model),
                  model.link = "log", at = tmean, cen = cen)
  
  data.frame(
    exposure = cp$predvar,
    fit      = cp$allRRfit,
    lower    = cp$allRRlow,
    upper    = cp$allRRhigh,
    cen      = cp$cen,
    stringsAsFactors = FALSE
  )
}

# ------------------------------
# Small plot (legend hidden) + p-value annotation
# ------------------------------
make_outcome_plot <- function(dat_withcovid, dat_withoutcovid, outcome_name, pval = NA_real_) {
  
  # your required subgroup labels
  lab_yes <- "COVID19 diagnosis within 6 mo before first event"
  lab_no  <- "No COVID19 diagnosis within 6 mo before first event"
  
  dat_withcovid$Group    <- lab_yes
  dat_withoutcovid$Group <- lab_no
  plot_data <- dplyr::bind_rows(dat_withcovid, dat_withoutcovid)
  
  # annotation position (top-left) aligned with your fixed axis limits
  x_min <- -5; x_max <- 30
  y_min <-  0; y_max <-  5
  x_annot <- x_min + 0.03 * (x_max - x_min)
  y_annot <- y_min + 0.95 * (y_max - y_min)
  p_lab <- fmt_pval(pval)
  
  ggplot(
    plot_data,
    aes(x = exposure, y = fit, color = Group, fill = Group, linetype = Group)
  ) +
    geom_ribbon(
      aes(ymin = lower, ymax = upper),
      alpha = 0.18, linewidth = 0, show.legend = FALSE
    ) +
    geom_line(linewidth = 1) +
    geom_vline(xintercept = unique(plot_data$cen),
               linewidth = 1.5, alpha = 0.3, color = "grey") +
    annotate("text", x = unique(plot_data$cen) + 2, y = 3.5, label = "50th perc",
             color = "grey40", size = 3) +
    # p-value annotation (only if available)
    { if (!is.na(pval)) annotate("text", x = x_annot, y = y_annot, label = p_lab,
                                 hjust = 0, vjust = 1, size = 3.5, color = "black")
      else NULL } +
    labs(
      title = paste0("Exposure-response curve: ", outcome_name),
      x = "Temperature (°C)", y = "Incidence rate ratio (IRR)",
      color = NULL, fill = NULL, linetype = NULL
    ) +
    scale_y_continuous(breaks = seq(0, 5, 1), expand = c(0, 0)) +
    scale_x_continuous(breaks = seq(-5, 30, by = 5), expand = c(0, 0)) +
    coord_cartesian(xlim = c(-5, 30), ylim = c(0, 5)) +
    geom_hline(yintercept = 1, linetype = "dashed", color = "grey60") +
    scale_color_manual(
      values = c("COVID19 diagnosis within 6 mo before first event" = "#d95f02", 
                 "No COVID19 diagnosis within 6 mo before first event" = "#1b9e77")
    ) +
    scale_fill_manual(
      values = c("COVID19 diagnosis within 6 mo before first event" = "#d95f02", 
                 "No COVID19 diagnosis within 6 mo before first event" = "#1b9e77"),
      guide = "none"
    ) +
    scale_linetype_manual(
      values = c("COVID19 diagnosis within 6 mo before first event" = "longdash", 
                 "No COVID19 diagnosis within 6 mo before first event" = "solid")
    ) +
    guides(color = guide_legend(override.aes = list(fill = NA))) +
    theme_minimal() +
    theme(
      text = element_text(family = "Helvetica"),
      panel.grid = element_blank(),
      plot.title = element_text(size = 14, hjust = 0.5, face = "bold"),
      axis.title = element_text(size = 13),
      axis.text  = element_text(size = 11),
      axis.line  = element_line(size = 0.3, colour = "black"),
      axis.ticks = element_line(size = 0.3, colour = "black"),
      plot.margin = margin(10, 10, 10, 10),
      plot.background = element_rect(color = "white", fill = "white"),
      panel.border   = element_rect(fill = NA, color = "black", size = 0.5),
      legend.position = "none"
    )
}

# ------------------------------
# Build final 4-panel figure
# ------------------------------
plot_list <- list()

# your required subgroup values (used for filtering)
sg_yes <- "COVID19 diagnosis within 6 mo before first event"
sg_no  <- "No COVID19 diagnosis within 6 mo before first event"

for (o in names(outcome_labels)) {
  
  df_withcovid <- df %>%
    filter(analysis == "COVID19_6mo", outcome == o, subgroup == sg_yes)
  
  df_withoutcovid <- df %>%
    filter(analysis == "COVID19_6mo", outcome == o, subgroup == sg_no)
  
  if (nrow(df_withcovid) == 0 || nrow(df_withoutcovid) == 0) next
  
  dat_withcovid    <- build_cp_df(df_withcovid)
  dat_withoutcovid <- build_cp_df(df_withoutcovid)
  
  # grab p-value from whichever subgroup slice has it (should be identical if present)
  pval_o <- get_pval(bind_rows(df_withcovid, df_withoutcovid))
  
  plot_list[[o]] <- make_outcome_plot(dat_withcovid, dat_withoutcovid, outcome_labels[o], pval = pval_o)
}

if (length(plot_list) == 4) {
  panel_ordered <- plot_list[names(outcome_labels)]
  panel <- wrap_plots(panel_ordered, ncol = 2, guides = "collect") &
    theme(legend.position = "bottom", legend.title = element_blank(),
          legend.key.width = unit(1, "cm"))
  
  panel <- panel + plot_annotation(
    title = "Stratification by COVID-19 status at outcome event",
    theme = theme(plot.title = element_text(size = 15, face = "bold",
                                            family = "Helvetica", hjust = 0.5))
  )
  
  ggsave(
    filename = here::here("figures", "panel_tmean_COVID19_6mo_interaction_exp_resp.png"),
    plot = panel, width = 12, height = 10, dpi = 300
  )
}

# ------------------------------
# Single-outcome figure: mi_fatal with 14-day lag sensitivity
# ------------------------------

make_outcome_plot_tight <- function(dat_withcovid, dat_withoutcovid, title_text, pval = NA_real_) {
  
  lab_yes <- "COVID19 diagnosis within 6 mo before first event"
  lab_no  <- "No COVID19 diagnosis within 6 mo before first event"
  
  dat_withcovid$Group    <- lab_yes
  dat_withoutcovid$Group <- lab_no
  plot_data <- dplyr::bind_rows(dat_withcovid, dat_withoutcovid)
  
  # annotation position (top-left) aligned with your tight axis limits
  x_min <- -5; x_max <- 30
  y_min <-  0; y_max <-  3
  x_annot <- x_min + 0.03 * (x_max - x_min)
  y_annot <- y_min + 0.95 * (y_max - y_min)
  p_lab <- fmt_pval(pval)
  
  ggplot(
    plot_data,
    aes(x = exposure, y = fit, color = Group, fill = Group, linetype = Group)
  ) +
    geom_ribbon(aes(ymin = lower, ymax = upper), alpha = 0.18, linewidth = 0, show.legend = FALSE) +
    geom_line(linewidth = 1) +
    geom_vline(xintercept = unique(plot_data$cen), linewidth = 1.5, alpha = 0.3, color = "grey") +
    annotate("text",
             x = unique(plot_data$cen) + 3.5, y = 2.5, label = "50th perc",
             color = "grey40", size = 3, hjust = 1) +
    { if (!is.na(pval)) annotate("text", x = x_annot, y = y_annot, label = p_lab,
                                 hjust = 0, vjust = 1, size = 3.5, color = "black")
      else NULL } +
    labs(
      title = title_text,
      x = "Temperature (°C)", y = "Incidence rate ratio (IRR)",
      color = NULL, fill = NULL, linetype = NULL
    ) +
    scale_y_continuous(breaks = seq(0, 3, 1), expand = c(0, 0)) +
    scale_x_continuous(breaks = seq(-5, 30, by = 5), expand = c(0, 0)) +
    coord_cartesian(xlim = c(-5, 30), ylim = c(0, 3)) +
    geom_hline(yintercept = 1, linetype = "dashed", color = "grey60") +
    scale_color_manual(
      values = c("COVID19 diagnosis within 6 mo before first event" = "#d95f02", 
                 "No COVID19 diagnosis within 6 mo before first event" = "#1b9e77")
    ) +
    scale_fill_manual(
      values = c("COVID19 diagnosis within 6 mo before first event" = "#d95f02", 
                 "No COVID19 diagnosis within 6 mo before first event" = "#1b9e77"),
      guide = "none"
    ) +
    scale_linetype_manual(
      values = c("COVID19 diagnosis within 6 mo before first event" = "longdash", 
                 "No COVID19 diagnosis within 6 mo before first event" = "solid")
    ) +   guides(
      color    = guide_legend(nrow = 2, byrow = TRUE, override.aes = list(fill = NA, linewidth = 1.1)),
      linetype = guide_legend(nrow = 2, byrow = TRUE)
    ) +
    theme_minimal() +
    theme(
      text            = element_text(family = "Helvetica"),
      panel.grid      = element_blank(),
      axis.title      = element_text(size = 13),
      axis.text       = element_text(size = 11),
      axis.line       = element_line(size = 0.3, colour = "black"),
      axis.ticks      = element_line(size = 0.3, colour = "black"),
      panel.border    = element_rect(fill = NA, color = "black", size = 0.5),
      plot.background = element_rect(color = "white", fill = "white"),
      plot.title      = element_text(size = 13.5, face = "bold", hjust = 0.5, lineheight = 1.05),
      plot.margin     = margin(18, 16, 30, 16),
      legend.position = "bottom",
      legend.title    = element_blank(),
      legend.text     = element_text(size = 10),
      legend.key.width = grid::unit(1, "cm")
    )
}

# NOTE: I only changed subgroup labels here.
# If your lag-sensitivity analysis name also changed, update analysis == "...".
df_withcovid_14 <- df %>%
  dplyr::filter(
    analysis == "14d_lag_sensitivity_COVID19_6mo",  # <-- change if your column uses a new name
    outcome  == "mi_fatal",
    subgroup == sg_yes
  )

df_withoutcovid_14 <- df %>%
  dplyr::filter(
    analysis == "14d_lag_sensitivity_COVID19_6mo",  # <-- change if your column uses a new name
    outcome  == "mi_fatal",
    subgroup == sg_no
  )

if (nrow(df_withcovid_14) > 0 && nrow(df_withoutcovid_14) > 0) {
  dat_withcovid_14    <- build_cp_df(df_withcovid_14)
  dat_withoutcovid_14 <- build_cp_df(df_withoutcovid_14)
  
  pval_mi14 <- get_pval(bind_rows(df_withcovid_14, df_withoutcovid_14))
  
  p_mi_14 <- make_outcome_plot_tight(
    dat_withcovid_14,
    dat_withoutcovid_14,
    title_text = "Stratification by COVID-19 status at acute myocardial infarction:\n14 day lag period",
    pval = pval_mi14
  )
  
  ggsave(
    filename = here::here("figures", "mi_tmean_14d_lag_COVID19_6mo_interaction_exp_resp.png"),
    plot     = p_mi_14,
    width    = 6.5,
    height   = 5,
    dpi      = 300
  )
}

