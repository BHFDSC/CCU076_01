#*******************************************************************************
#
# Project:  CCU076_01
# Title:    Associations between air temperature and cardiovascular health outcomes
# Date:     23-07-2025
# Author:   Isabel Walter
# Purpose:  2nd stage modelling and panel exposure response curve for fatal vs
#           non-fatal analyses for venous thrombotic event, arterial thrombotic event, ischaemic 
#           stroke, myocardial infarction.
#
#*******************************************************************************

# ------------------------------ #
# Libraries & data               #
# ------------------------------ #

library(mixmeta)
library(dlnm)
library(ggplot2)
library(dplyr)
library(here)
library(RColorBrewer)
library(cowplot)
library(readxl)
library(patchwork)

# Load data
df <- read_excel("export/Oct_2025/first_stage_models.xlsx")

# tmean distribution
avgtmeansum <- read.csv(here::here("export/Oct_2025", "CCU076_01_tmean_pm2p5_distribution.csv"))
tmean <- avgtmeansum$tmean_20_22

# ------------------------------ #
# 2nd-stage parameterisation     #
# ------------------------------ #
# Define temperature percentiles for spline and centering
knots  <- tmean[avgtmeansum$perc %in% paste0(c(10,75,90), ".0%")]
b_knots <- tmean[avgtmeansum$perc %in% paste0(c(1,99), ".0%")]
bvar   <- onebasis(tmean, fun="ns", knots=knots, Bound=b_knots)
cen    <- tmean[avgtmeansum$perc %in% paste0(50, ".0%")]

# ------------------------------ #
# Outcome labels                 #
# ------------------------------ #
outcomes <- c("ve_fatal", "ae_fatal", "mi_fatal", "stroke_fatal")
outcome_labels <- c(
  "ve_fatal"     = "Venous thrombosis",
  "ae_fatal"     = "Arterial thrombosis",
  "mi_fatal"     = "Acute myocardial infarction",
  "stroke_fatal" = "Ischaemic stroke"
)

# ------------------------------ #
# Death-window analysis mapping  #
# ------------------------------ #
death_defs <- list(
  `7`  = list(
    fatal   = "sensitivity_fatal_1week",
    nonfat  = "sensitivity_nonfatal_1week",                 # (7d non-fatal label)
    title   = "Fatal vs non-fatal cases analysis (7 days)",
    slug    = "7days"
  ),
  `14` = list(
    fatal   = "sensitivity_fatal_2week",
    nonfat  = "sensitivity_nonfatal_2week",     
    title   = "Fatal vs non-fatal cases analysis (14 days)",
    slug    = "14days"
  ),
  `30` = list(
    fatal   = "sensitivity_fatal_month",
    nonfat  = "sensitivity_nonfatal_month",     
    title   = "Fatal vs non-fatal cases analysis (30 days)",
    slug    = "30days"
  )
)

# ------------------------------ #
# Helper: build crosspred DF     #
# ------------------------------ #
build_cp_df <- function(df_sub, who = "stratum") {
  # Select and order coef/vcov columns
  coef_cols <- grep("^coef", names(df_sub), value = TRUE)
  vcov_cols <- grep("^vcov", names(df_sub), value = TRUE)
  
  coef_mat <- as.matrix(df_sub[, coef_cols, drop = FALSE])
  vcov_mat <- as.matrix(df_sub[, vcov_cols, drop = FALSE])
  
  model <- mixmeta(coef_mat ~ 1, vcov_mat, data = df_sub, method = "fixed")
  
  # Cross-prediction
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

# ------------------------------ #
# Small plot (legend hidden)     #
# ------------------------------ #

make_outcome_plot <- function(dat_fatal, dat_nonfatal, outcome_name, days_txt) {
  lab_no   <- paste0("No death within ", days_txt, " of event")
  lab_yes  <- paste0("Death within ", days_txt, " of event")
  
  dat_fatal$Group    <- lab_yes
  dat_nonfatal$Group <- lab_no
  plot_data <- dplyr::bind_rows(dat_nonfatal, dat_fatal)
  
  ggplot(
    plot_data,
    aes(x = exposure, y = fit, color = Group, fill = Group, linetype = Group)
  ) +
    geom_ribbon(
      aes(ymin = lower, ymax = upper),
      alpha = 0.18, linewidth = 0, show.legend = FALSE
    ) +
    geom_line(linewidth = 1) +
    geom_vline(xintercept = unique(plot_data$cen), linewidth = 1.5, alpha = 0.3, color = "grey") +
    annotate("text", x = unique(plot_data$cen) + 2, y = 3.5, label = "50th perc",
             color = "grey40", size = 3) +
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
      values = setNames(c("#1b9e77", "#d95f02"), c(lab_no, lab_yes)),
      drop = FALSE
    ) +
    scale_fill_manual(
      values = setNames(c("#1b9e77", "#d95f02"), c(lab_no, lab_yes)),
      drop = FALSE, guide = "none"
    ) +
    # << New: consistent linetypes across all panels >>
    scale_linetype_manual(
      values = setNames(c("solid", "longdash"), c(lab_no, lab_yes)),
      drop = FALSE
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
      plot.margin = margin(10,10,10,10),
      plot.background = element_rect(color = "white", fill = "white"),
      panel.border   = element_rect(fill = NA, color = "black", size = 0.5),
      legend.position = "none"
    )
}


# ------------------------------ #
# Build panels (shared legend)   #
# ------------------------------ #
for (k in names(death_defs)) {
  fatal_code   <- death_defs[[k]]$fatal
  nonfat_code  <- death_defs[[k]]$nonfat
  panel_title  <- death_defs[[k]]$title
  slug         <- death_defs[[k]]$slug
  
  days_txt <- paste0(k, " days")  # e.g. "7 days", "14 days", "30 days"
  
  plot_list <- list()
  
  for (o in names(outcome_labels)) {
    df_fatal   <- df %>% dplyr::filter(analysis == fatal_code,  outcome == o)
    df_nonfat  <- df %>% dplyr::filter(analysis == nonfat_code, outcome == o)
    if (nrow(df_fatal) == 0 || nrow(df_nonfat) == 0) next
    
    dat_fatal  <- build_cp_df(df_fatal)
    dat_nonfat <- build_cp_df(df_nonfat)
    if (is.null(dat_fatal) || is.null(dat_nonfat)) next
    
    plot_list[[o]] <- make_outcome_plot(dat_fatal, dat_nonfat, outcome_labels[o], days_txt)
  }
  
  if (length(plot_list) == 4) {
    panel_ordered <- plot_list[names(outcome_labels)]
    panel <- wrap_plots(panel_ordered, ncol = 2, guides = "collect") &
      theme(legend.position = "bottom", legend.title = element_blank(),
            legend.key.width = unit(1, "cm"))
    
    panel <- panel + plot_annotation(
      title = panel_title,
      theme = theme(plot.title = element_text(size = 15, face = "bold",
                                              family = "Helvetica", hjust = 0.5))
    )
    
    ggsave(
      filename = here::here("figures",
                            paste0("panel_tmean_fatal_vs_nonfatal_", slug, "_exp_resp.png")),
      plot = panel, width = 12, height = 10, dpi = 300
    )
  }
}
