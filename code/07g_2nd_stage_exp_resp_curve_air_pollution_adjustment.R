#*******************************************************************************
# Project:  CCU076_01
# Title:    Associations between air temperature and cardiovascular health outcomes
# Date:     23-07-2025
# Author:   Isabel Walter
# Purpose:  2nd stage modelling and panel exposure response curve for
#           air pollution analyses (unadjusted vs adjusted)
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

# Load data
df <- read_excel("export/Oct_2025/first_stage_models.xlsx")

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
# Small plot (legend hidden)
# ------------------------------
make_outcome_plot <- function(dat_unadj, dat_adj, outcome_name) {
  dat_unadj$Group <- "Unadjusted for PM2.5"
  dat_adj$Group   <- "Adjusted for PM2.5"
  plot_data <- dplyr::bind_rows(dat_unadj, dat_adj)
  
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
      values = c("Unadjusted for PM2.5" = "#1b9e77",
                 "Adjusted for PM2.5"   = "#d95f02")
    ) +
    scale_fill_manual(
      values = c("Unadjusted for PM2.5" = "#1b9e77",
                 "Adjusted for PM2.5"   = "#d95f02"),
      guide = "none"
    ) +
    scale_linetype_manual(
      values = c("Unadjusted for PM2.5" = "solid",
                 "Adjusted for PM2.5"   = "longdash")
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

for (o in names(outcome_labels)) {
  df_unadj <- df %>% dplyr::filter(analysis == "air_pollution_period_unadjusted", outcome == o)
  df_adj   <- df %>% dplyr::filter(analysis == "adjusted_for_air_pollution", outcome == o)
  
  if (nrow(df_unadj) == 0 || nrow(df_adj) == 0) next
  
  dat_unadj <- build_cp_df(df_unadj)
  dat_adj   <- build_cp_df(df_adj)
  
  plot_list[[o]] <- make_outcome_plot(dat_unadj, dat_adj, outcome_labels[o])
}

if (length(plot_list) == 4) {
  panel_ordered <- plot_list[names(outcome_labels)]
  panel <- wrap_plots(panel_ordered, ncol = 2, guides = "collect") & 
    theme(legend.position = "bottom", legend.title = element_blank(),
          legend.key.width = unit(1, "cm"))
  
  panel <- panel + plot_annotation(
    title = "Adjustment for PM2.5 (Jan 2020-Dec 2021)",
    theme = theme(plot.title = element_text(size = 15, face = "bold",
                                            family = "Helvetica", hjust = 0.5))
  )
  
  ggsave(
    filename = here::here("figures", "panel_tmean_unadj_vs_adj_PM25_exp_resp.png"),
    plot = panel, width = 12, height = 10, dpi = 300
  )
}
