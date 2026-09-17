#*******************************************************************************
#
# Project:  CCU076_01
# Title:    Associations between air temperature and cardiovascular health outcomes
# Date:     30-10-2025
# Author:   Isabel Walter 
# Purpose:  2nd stage modelling and forest plot for MI (fatal), 14-day lag sensitivity
#
#   ADAPTED to match July-2025 script style:
#   - Adds interaction p-values under headers (per analysis/stratifier)
#   - COVID variable renamed + moved earlier (after Overall)
#   
#
#*******************************************************************************

# Load required libraries
library(tidyverse)
library(mixmeta)
library(splines)
library(here)
library(forcats)
library(readxl)
library(dlnm)
library(stringr)

# ------------------------------
# Load data and adapt 'analysis' labels for 14d lag sensitivity
# ------------------------------
outcome_focus <- "mi_fatal"

raw <- read_excel("export/Oct_2025/first_stage_models_with_interaction.xlsx") |>
  filter(
    outcome == outcome_focus,
    str_detect(analysis, "^14d_lag_sensitivity($|_)")
  ) |>
  mutate(
    analysis = case_when(
      analysis == "14d_lag_sensitivity" ~ "main_analysis",
      str_detect(analysis, "^14d_lag_sensitivity_") ~ str_replace(analysis, "^14d_lag_sensitivity_", ""),
      TRUE ~ analysis
    )
  )

df <- raw

# Harmonize subgroup labels
df$subgroup <- recode(df$subgroup,
                      "Low deprivation index (1-3)"  = "Most deprived (quint. 1-3)",
                      "High deprivation index (4-5)" = "Least deprived (quint. 4-5)"
)

# ---- COVID: rename subgroup labels to match July-2025 style ----
# (adjust these source strings if your MI file uses slightly different wording)
df$subgroup <- recode(
  df$subgroup,
  "COVID-19 within 6 mo before outcome"        = "COVID19 in last 6mo",
  "No COVID-19 within 6 mo before outcome"     = "No COVID19 in last 6mo",
  "COVID19 diagnosis within 6 mo before first event"    = "COVID19 in last 6mo",
  "No COVID19 diagnosis within 6 mo before first event" = "No COVID19 in last 6mo"
)

avgtmeansum <- read.csv(here("export/Oct_2025", "CCU076_01_tmean_pm2p5_distribution.csv"))

# Percentiles of interest
hot_percentiles  <- c("75.0%", "95.0%", "99.0%")
cold_percentiles <- c("25.0%", "5.0%", "1.0%")
all_percentiles  <- c(cold_percentiles, hot_percentiles)

# Temperature column names for regions
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

# ------------------------------
# Define subgroup structure (COVID moved earlier + variable renamed)
# ------------------------------
subgroup_info <- list(
  "main_analysis" = NA,
  "COVID19_6mo" = c("COVID19 in last 6mo", "No COVID19 in last 6mo"),  # <-- renamed + moved
  "sex"                 = c("Female", "Male"),
  "age_group_bin"       = c("Age 18-64 years", "Age 65 years and older"),
  "ethnicity"           = c("Asian or Asian British", "Black, Black British, Caribbean or African", "White"),
  "rural_urban_class"   = c("Rural", "Urban"),
  "deprivation_bin"     = c("Most deprived (quint. 1-3)", "Least deprived (quint. 4-5)"),
  "cov_comorb_comp_flag"= c("Comorbidities", "No comorbidities"),
  "cov_obesity_combined_flag" = c("Excess weight", "No excess weight"),
  "smoking_status"      = c("Never smoked", "Current smoker", "Ex-smoker"),
  "region"              = names(tmean_cols)
)

# ------------------------------
# Interaction p-value helpers (like July-2025 script)
# ------------------------------
pval_col <- "interaction_chisq"  # change if your column is named differently

fmt_pval <- function(p) {
  if (is.na(p)) return(NA_character_)
  p <- as.numeric(p)
  if (p < 0.001) return("p < 0.001")
  sub("\\.?0+$", "", sprintf("p = %.3f", p))
}

# one p-value per analysis/stratifier (not per subgroup)
pvals_by_analysis <- df %>%
  group_by(analysis) %>%
  summarise(
    p_int = {
      vv <- unique(.data[[pval_col]])
      vv <- vv[!is.na(vv)]
      if (length(vv) > 0) vv[1] else NA_real_
    },
    .groups = "drop"
  )

get_p_for <- function(analysis_name) {
  out <- pvals_by_analysis %>% filter(analysis == analysis_name) %>% pull(p_int)
  if (length(out) == 0) return(NA_real_)
  out[1]
}

p_annot <- function(x, y, analysis_name, alpha_sig = 0.05) {
  p <- get_p_for(analysis_name)
  if (is.na(p)) return(NULL)
  p <- as.numeric(p)
  
  lab <- fmt_pval(p)
  ff  <- ifelse(!is.na(p) && p < alpha_sig, "bold", "plain")
  
  annotate("text", x = x, y = y, label = lab,
           fontface = ff, size = 2.4, family = "Helvetica", color = "black")
}

# ------------------------------
# Helper to process subgroup analyses (non-region)
# ------------------------------
process_subgroup <- function(analysis_name, subgroups, tmean, analysis_label) {
  tmean <- as.numeric(tmean)
  knots   <- tmean[avgtmeansum$perc %in% paste0(c(10, 75, 90), ".0%")]
  b_knots <- tmean[avgtmeansum$perc %in% paste0(c(1, 99),  ".0%")]
  bvar <- onebasis(tmean, fun = "ns", knots = knots, Bound = b_knots)
  cen  <- tmean[avgtmeansum$perc == "50.0%"]
  
  cp_list <- setNames(vector("list", length(subgroups)), subgroups)
  
  for (sg in subgroups) {
    subdat <- df %>%
      dplyr::filter(analysis == analysis_name,
                    outcome  == outcome_focus,
                    subgroup == sg)
    
    if (nrow(subdat) == 0) next
    
    coef_mat <- as.matrix(subdat[, grep("^coef", names(subdat))])
    vcov_mat <- as.matrix(subdat[, grep("^vcov", names(subdat))])
    
    model_sg <- mixmeta(coef_mat ~ 1, vcov_mat, data = subdat, method = "fixed")
    
    cp_sg <- crosspred(
      bvar,
      coef = coef(model_sg),
      vcov = vcov(model_sg),
      at   = tmean,
      model.link = "log",
      cen  = cen
    )
    
    cp_list[[sg]] <- data.frame(
      tmean_celcius = names(cp_sg$allRRfit),
      percentile    = avgtmeansum$perc,
      irr           = cp_sg$allRRfit,
      lower         = cp_sg$allRRlow,
      upper         = cp_sg$allRRhigh,
      subgroup      = sg,
      row.names     = NULL
    )
  }
  
  bind_rows(cp_list) %>%
    dplyr::filter(percentile %in% all_percentiles) %>%
    dplyr::mutate(
      temperature_type = ifelse(percentile %in% hot_percentiles, "hot", "cold"),
      analysis = analysis_label
    ) %>%
    dplyr::select(analysis, subgroup, temperature_type, percentile, irr, lower, upper)
}

# ------------------------------
# Collect all results
# ------------------------------
all_results <- list()

# --- MAIN ANALYSIS (national) ---
tmean_main <- avgtmeansum$tmean_20_22
knots   <- tmean_main[avgtmeansum$perc %in% paste0(c(10, 75, 90), ".0%")]
b_knots <- tmean_main[avgtmeansum$perc %in% paste0(c(1, 99),  ".0%")]
bvar <- onebasis(tmean_main, fun = "ns", knots = knots, Bound = b_knots)
cen  <- tmean_main[avgtmeansum$perc == "50.0%"]

main_df <- df %>% filter(analysis == "main_analysis")
coef <- as.matrix(main_df[, grep("^coef", names(main_df))])
vcov_obj <- as.matrix(main_df[, grep("^vcov", names(main_df))])

model <- mixmeta(coef ~ 1, vcov_obj, method = "fixed")
cp <- crosspred(bvar, coef = coef(model), vcov = vcov(model),
                at = tmean_main, model.link = "log", cen = cen)

main_df_plot <- data.frame(
  tmean_celcius = names(cp$allRRfit),
  percentile = avgtmeansum$perc,
  irr = cp$allRRfit,
  lower = cp$allRRlow,
  upper = cp$allRRhigh,
  row.names = NULL
) %>%
  filter(percentile %in% all_percentiles) %>%
  mutate(
    temperature_type = ifelse(percentile %in% hot_percentiles, "hot", "cold"),
    subgroup = "Overall",
    analysis = "main_analysis"
  ) %>%
  select(analysis, subgroup, temperature_type, percentile, irr, lower, upper)

all_results[[1]] <- main_df_plot

# --- SUBGROUP & REGION ANALYSIS ---
index <- 2
for (analysis_name in names(subgroup_info)) {
  if (analysis_name == "main_analysis") next
  subgroups <- subgroup_info[[analysis_name]]
  
  if (analysis_name == "region") {
    for (region in subgroups) {
      tmean_col <- tmean_cols[[region]]
      tmean_region <- as.numeric(avgtmeansum[[tmean_col]])
      if (all(is.na(tmean_region))) next
      
      knots   <- tmean_region[avgtmeansum$perc %in% paste0(c(10, 75, 90), ".0%")]
      b_knots <- tmean_region[avgtmeansum$perc %in% paste0(c(1, 99),  ".0%")]
      bvar_r <- onebasis(tmean_region, fun = "ns", knots = knots, Bound = b_knots)
      cen_r  <- tmean_region[avgtmeansum$perc == "50.0%"]
      
      df_region <- df %>% filter(analysis == "region", subgroup == region)
      if (nrow(df_region) == 0) next
      
      coef_r <- as.matrix(df_region[, grep("^coef", names(df_region))])
      vcov_r <- as.matrix(df_region[, grep("^vcov", names(df_region))])
      
      model_r <- mixmeta(coef_r ~ 1, vcov_r, method = "fixed")
      cp_r <- crosspred(bvar_r, coef = coef(model_r), vcov = vcov(model_r),
                        at = tmean_region, model.link = "log", cen = cen_r)
      
      region_df <- data.frame(
        tmean_celcius = names(cp_r$allRRfit),
        percentile = avgtmeansum$perc,
        irr = cp_r$allRRfit,
        lower = cp_r$allRRlow,
        upper = cp_r$allRRhigh,
        row.names = NULL
      ) %>%
        filter(percentile %in% all_percentiles) %>%
        mutate(
          temperature_type = ifelse(percentile %in% hot_percentiles, "hot", "cold"),
          subgroup = region,
          analysis = "region"
        ) %>%
        select(analysis, subgroup, temperature_type, percentile, irr, lower, upper)
      
      all_results[[index]] <- region_df
      index <- index + 1
    }
  } else {
    subgroup_df <- process_subgroup(analysis_name, subgroups, tmean_main, analysis_label = analysis_name)
    all_results[[index]] <- subgroup_df
    index <- index + 1
  }
}

# ------------------------------
# Combine + plot
# ------------------------------
plot_df <- bind_rows(all_results) %>%
  mutate(
    subgroup = fct_inorder(subgroup),
    temperature_type = factor(temperature_type, levels = c("hot", "cold"),
                              labels = c("Heat", "Cold"))
  )

plot_df$percentile <- factor(
  plot_df$percentile,
  levels = c("1.0%", "5.0%", "25.0%", "75.0%", "95.0%", "99.0%"),
  labels = c("1st percentile", "5th percentile", "25th percentile",
             "75th percentile", "95th percentile", "99th percentile")
)

custom_colors <- c(
  "1st percentile" = "#08306B",
  "5th percentile" = "#4292C6",
  "25th percentile" = "#DEEBF7",
  "75th percentile" = "#FEE0D2",
  "95th percentile" = "#FC9272",
  "99th percentile" = "#CB181D"
)

custom_shapes <- c(
  "1st percentile" = 17,
  "5th percentile" = 16,
  "25th percentile" = 15,
  "75th percentile" = 15,
  "95th percentile" = 16,
  "99th percentile" = 17
)

# ---- IMPORTANT: update vertical line positions for new order (COVID moved earlier) ----
# New block layout:
# Overall(1) | COVID(2) | Sex(2) | Age(2) | Ethnicity(3) | Rural(2) | IMD(2) | Comorb(2) | Obesity(2) | Smoking(3) | Region(9)
# cumulative ends: 1,3,5,7,10,12,14,16,18,21,30
vlines <- c(1.45, 3.45, 5.45, 7.45, 10.55, 12.55, 14.55, 16.55, 18.55, 21.45)

# shading (keep your original approach)
subgroup_levels <- levels(plot_df$subgroup)
all_positions   <- seq_along(subgroup_levels)
shade_positions <- all_positions[(which(subgroup_levels == "Female")) %% 2 == all_positions %% 2]

# ---- PLOT (keep y-scale from MI script: 0.4–2) ----
y_header <- 2.0   # within upper limit (2)
y_pval   <- 1.80  # just under header

p <- ggplot(plot_df, aes(
  x = subgroup, y = irr, ymin = lower, ymax = upper,
  color = percentile, shape = percentile, group = percentile
)) +
  geom_vline(data = data.frame(x = shade_positions), aes(xintercept = x),
             color = "#1b9e77", size = 16, alpha = 0.05) +
  geom_vline(xintercept = vlines, color = "grey50", linetype = "dashed") +
  geom_hline(yintercept = 1, color = "grey30", linetype = "solid") +
  geom_pointrange(position = position_dodge(width = 0.8), size = 0.8) +
  facet_wrap(~temperature_type, ncol = 1, scales = "free_y",
             labeller = as_labeller(c(
               "Heat" = "Hot mean daily temperature percentiles",
               "Cold" = "Cold mean daily temperature percentiles"
             ))) +
  scale_x_discrete(labels = function(x) ifelse(grepl("^spacer_", x), "", x)) +
  scale_color_manual(values = custom_colors, na.translate = FALSE) +
  scale_shape_manual(values = custom_shapes, na.translate = FALSE) +
  
  # KEEP MI y-axis scale exactly
  scale_y_log10(
    breaks = c(0.3, 1, 1.5, 2),
    limits = c(0.3, 2),
    labels = scales::label_number(accuracy = 0.1)
  ) +
  
  labs(
    x = NULL,
    y = "Incidence rate ratio (IRR)",
    title = "Incidence rate ratios for acute myocardial infarction across temperature extremes\n14-day lag period"
  ) +
  
  # --- HEADERS (COVID moved earlier; keep x positions aligned to group midpoints) ---
  annotate("text", x = 0.98, y = y_header, label = "Overall",
           fontface = "bold", size = 2.5, family = "Helvetica") +
  annotate("text", x = 2.5,  y = y_header, label = "COVID-19",
           fontface = "bold", size = 2.5, family = "Helvetica") +
  annotate("text", x = 4.5,  y = y_header, label = "Sex",
           fontface = "bold", size = 2.5, family = "Helvetica") +
  annotate("text", x = 6.5,  y = y_header, label = "Age group",
           fontface = "bold", size = 2.5, family = "Helvetica") +
  annotate("text", x = 9,    y = y_header, label = "Ethnic group",
           fontface = "bold", size = 2.5, family = "Helvetica") +
  annotate("text", x = 11.55, y = y_header, label = "Urban/rural area",
           fontface = "bold", size = 2.5, family = "Helvetica") +
  annotate("text", x = 13.5, y = y_header, label = "IMD",
           fontface = "bold", size = 2.5, family = "Helvetica") +
  annotate("text", x = 15.5, y = y_header, label = "Comorbidities",
           fontface = "bold", size = 2.5, family = "Helvetica") +
  annotate("text", x = 17.5, y = y_header, label = "Excess weight",
           fontface = "bold", size = 2.5, family = "Helvetica") +
  annotate("text", x = 20,   y = y_header, label = "Smoking status",
           fontface = "bold", size = 2.5, family = "Helvetica") +
  annotate("text", x = 26,   y = y_header, label = "Region",
           fontface = "bold", size = 2.5, family = "Helvetica") +
  
  # --- P-VALUES UNDER HEADERS (no "Overall") ---
  p_annot(x = 2.5,  y = y_pval, analysis_name = "COVID19_6mo") +
  p_annot(x = 4.5,  y = y_pval, analysis_name = "sex") +
  p_annot(x = 6.5,  y = y_pval, analysis_name = "age_group_bin") +
  p_annot(x = 9,    y = y_pval, analysis_name = "ethnicity") +
  p_annot(x = 11.55, y = y_pval, analysis_name = "rural_urban_class") +
  p_annot(x = 13.5, y = y_pval, analysis_name = "deprivation_bin") +
  p_annot(x = 15.5, y = y_pval, analysis_name = "cov_comorb_comp_flag") +
  p_annot(x = 17.5, y = y_pval, analysis_name = "cov_obesity_combined_flag") +
  p_annot(x = 20,   y = y_pval, analysis_name = "smoking_status") +
  p_annot(x = 26,   y = y_pval, analysis_name = "region") +
  
  theme_minimal(base_family = "Helvetica", base_size = 12) +
  theme(
    plot.title = element_text(size = 16, hjust = 0.5, face = "bold"),
    axis.text = element_text(size = 11),
    strip.text = element_text(size = 11, face = "bold", margin = margin(b = 10)),
    strip.placement = "outside",
    panel.grid.major.y = element_blank(),
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_blank(),
    panel.border = element_rect(fill = NA, color = "black", size = 0.5),
    legend.title = element_blank(),
    legend.position = "right",
    legend.key.height = unit(1.5, "lines"),
    legend.key.width = unit(1.5, "lines"),
    legend.text = element_text(size = 10),
    axis.text.x = element_text(angle = 45, hjust = 1),
    plot.background = element_rect(fill = "white", color = "white"),
    plot.margin = margin(10, 10, 10, 10)
  ) +
  guides(
    color = guide_legend(override.aes = list(size = 1.5)),
    shape = guide_legend(override.aes = list(size = 1.5))
  )

# ---- SAVE ----
ggsave(
  filename = here::here("figures", "panel_forest_plot_IRR_mi_fatal_14d_lag_sensitivity_with_interaction.png"),
  plot = p,
  width = 15, height = 8, dpi = 300
)

