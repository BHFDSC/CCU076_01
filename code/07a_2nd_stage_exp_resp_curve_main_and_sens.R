#*******************************************************************************
#
# Project:  CCU076_01
# Title:    Associations between air temperature and cardiovascular health outcomes
# Date:     23-07-2025
# Author:   Isabel Walter
# Purpose:  2nd stage modelling and 4 panel exposure response curve venous thrombotic
#           event, arterial thrombotic event, ischaemic stroke, myocardial infarction.
#
#*******************************************************************************

rm(list = ls())

# ------------------------------
# Load libraries and data
# ------------------------------
x <- c("mixmeta", "dlnm", "scales", "dplyr", "ggplot2", "RColorBrewer", "here", "gridExtra",
       "readxl")
lapply(x, require, character.only = TRUE)

# first stage models 
df <- read_excel("export/Oct_2025/first_stage_models_with_interaction.xlsx")

# tmean distribution
avgtmeansum <- read.csv(here::here("export/Oct_2025", "CCU076_01_tmean_pm2p5_distribution.csv"))
tmean <- avgtmeansum$tmean_20_22

# ------------------------------
# 2nd stage model parameterisation
# ------------------------------

# Define temperature percentiles for spline and centering
knots <- tmean[avgtmeansum$perc %in% paste0(c(10,75,90), ".0%")]
b_knots <- tmean[avgtmeansum$perc %in% paste0(c(1,99), ".0%")]
bvar <- onebasis(tmean, fun="ns", knots=knots, Bound=b_knots)
cen <- tmean[avgtmeansum$perc %in% paste0(50, ".0%")]

# ------------------------------
# Define analysis and outcome vectors
# ------------------------------

analyses <- c("main_analysis", "sensitivity_first_event", "sensitivity_cens_at_death", 
              "sensitivity_restricted_study_period_may", "14d_lag_sensitivity", "7d_lag_sensitivity",
              "4d_lag_sensitivity", "adjusted_for_covid19", "adjusted_for_lockdown"
              )
outcomes <- c("ve_fatal", "ae_fatal", "mi_fatal", "stroke_fatal")

outcome_labels <- c(
  "ve_fatal" = "Venous thrombosis",
  "ae_fatal" = "Arterial thrombosis",
  "stroke_fatal" = "Ischaemic stroke",
  "mi_fatal" = "Acute myocardial infarction"
)

analysis_titles <- c(
  "main_analysis" = "Main analysis",
  "sensitivity_first_event" = "First events only",
  "sensitivity_cens_at_death" = "Censoring at death",
  "sensitivity_restricted_study_period_may" = "Restricted study period",
  "14d_lag_sensitivity" = "14-day lag period",
  "7d_lag_sensitivity" = "7-day lag period",
  "4d_lag_sensitivity" = "4-day lag period",
  "adjusted_for_covid19" = "Adjusted for COVID-19 diagnosis",
  "adjusted_for_lockdown" = "Adjusted for COVID-19 lockdown"
)

outcome_colors <- c(
  "ve_fatal" = "#1b9e77",
  "ae_fatal" = "#d95f02",
  "mi_fatal" = "#7570b3",
  "stroke_fatal" = "#e7298a"
)

# ------------------------------
# Plotting function
# ------------------------------


# Define function for generating ggplot object (no saving yet)
plot_cp <- function(cp, outcome_name, outcome_code) {
  color <- outcome_colors[outcome_code]
  
  ggplot(data.frame(exposure=cp$predvar, fit=cp$allRRfit, lower=cp$allRRlow, upper=cp$allRRhigh),
         aes(x=exposure, y=fit)) +
    geom_ribbon(aes(ymin=lower, ymax=upper), alpha=0.2, fill=color) +
    geom_line(linewidth=1, color=color) +
    geom_vline(xintercept=cp$cen, linewidth=2, alpha=0.3, color="grey") +
    annotate("text", x=cp$cen + 2, y=3.5, label="50th perc", color="grey", size=3) +
    labs(
      title=paste("Exposure-response curve:", outcome_name),
      x="Temperature (°C)",
      y="Incidence rate ratio (IRR)"
    ) +
    scale_y_continuous(breaks=seq(0, 5, 1), expand=c(0, 0)) +
    scale_x_continuous(breaks = seq(-5, 30, by = 5), expand = c(0, 0)) +
    coord_cartesian(xlim = c(-5, 30), ylim= c(0,5)) +
    geom_hline(yintercept=1, linetype="dashed", color="grey") +
    theme_minimal() +
    theme(
      text=element_text(family="Helvetica"),
      plot.title=element_text(size=14, hjust=0.5, face="bold"),
      axis.title=element_text(size=13),
      axis.text=element_text(size=11),
      panel.grid=element_blank(),
      axis.line=element_line(size=0.3, colour="black"),
      axis.ticks=element_line(size=0.3, colour="black"),
      plot.margin=margin(10,10,10,10),
      plot.background=element_rect(color="white", fill="white"),
      panel.border=element_rect(fill=NA, color="black", size=0.5)
    )
}

# ------------------------------
# Making panels
# ------------------------------

# Loop over analyses
for (a in analyses) {
  
  plot_list <- list()  # collect plots for the panel
  
  for (o in outcomes) {
    
    # Subset data
    tmeanpar <- df %>% filter(analysis == a, outcome == o)
    
    # Skip if no data for this combo
    if (nrow(tmeanpar) == 0) next
    
    coef <- as.matrix(tmeanpar[, grep("coef", names(tmeanpar))])
    vcov <- as.matrix(tmeanpar[, grep("vcov", names(tmeanpar))])
    
    # Meta-regression
    model <- mixmeta(coef ~ 1, vcov, data = tmeanpar, method = "fixed")
    
    # Crosspred
    cp <- crosspred(bvar, coef = coef(model), vcov = vcov(model),
                    model.link = "log", at = tmean, cen = cen)
    
    # Create plot and store
    plot_list[[o]] <- plot_cp(cp, outcome_name = outcome_labels[o], outcome_code=o)
  }
  
  # If we have 4 plots, arrange and save
  if (length(plot_list) == 4) {
    fig <- grid.arrange(grobs = plot_list, 
                        ncol = 2, 
                        top = grid::textGrob(
                          analysis_titles[a],
                          gp = grid::gpar(fontsize = 15, fontface = "bold", fontfamily = "Helvetica")
                        ))
    ggsave(
      filename = here::here("figures", paste0("panel_tmean_", a, "_exp_resp.png")),
      plot = fig,
      width = 12, height = 10, dpi = 300,
      bg = "white"   
    )
  }
}

