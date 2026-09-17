#*******************************************************************************
#
# Project:  CCU076_01
# Title:    Associations between air temperature and cardiovascular health outcomes
# Date:     28-07-2025
# Author:   Isabel Walter
# Purpose:  Final forest plot for venous thrombotic event and AMI attributable risk.
#
#*******************************************************************************

# Load libraries
library(tidyverse)
library(here)
library(forcats)

# ------------------------------
# Venous events
# ------------------------------

# Load data
df <- read_csv(here("export/Oct_2025", "CCU076_01_attributable_risk_venous_events.csv"))

# Define subgroup orders
ethnicity_order <- c(
  "Asian or Asian British",
  "Black, Black British, Caribbean or African",
  "White"
)

region_order <- c(
  "North West",
  "Yorkshire and The Humber",
  "North East",
  "East Midlands",
  "West Midlands",
  "East of England",
  "South East",
  "South West",
  "London"
)

# Prepare and filter data
df_filtered <- df %>%
  filter(outcome == "ve_fatal", effect %in% c("heat", "cold")) %>%
  mutate(
    subgroup = if_else(subgroup == "overall", "Overall", subgroup),
    subgroup = if_else(subgroup == "age_binary_Age 18-64 years", "Age 18-64 years", subgroup),
    subgroup = if_else(subgroup == "age_binary_Age 65 years and older", "Age 65 years and older", subgroup),
    subgroup = if_else(subgroup == "Low deprivation index (1-3)", "Most deprived (quint. 1-3)", subgroup),
    subgroup = if_else(subgroup == "High deprivation index (4-5)", "Least deprived (quint. 4-5)", subgroup),
    effect = case_when(
      effect == "heat" ~ "Heat",
      effect == "cold" ~ "Cold"
    ),
    effect = factor(effect, levels = c("Heat", "Cold"))
  ) %>%
  filter(!subgroup %in% c(
    "Mixed or multiple ethnic groups",
    "Other ethnic group",
    "Missing smoking status",
    "outcome_with_covid",
    "outcome_without_covid",
    "Missing ethnicity"
  )) %>%
  mutate(
    subgroup = factor(
      subgroup,
      levels = c(
        "Overall",
        "Female", "Male",
        "Age 18-64 years", "Age 65 years and older",
        sort(ethnicity_order),
        "Rural", "Urban",
        "Most deprived (quint. 1-3)", "Least deprived (quint. 4-5)",
        "Comorbidities", "No comorbidities",
        "Excess weight", "No excess weight",
        "Never smoked", "Current smoker", "Ex-smoker",
        region_order
      )
    )
  )

# Determine shading positions (alternating, starting from 2nd level)
shading_positions <- seq(2, length(levels(df_filtered$subgroup)), by = 2)

# Build plot
p <- ggplot(df_filtered, aes(x = subgroup, y = rate_per_100k)) +
  # Alternating shading using thick vertical lines
  geom_vline(
    xintercept = shading_positions,
    color = "#1b9e77", size = 18, alpha = 0.05
  ) +
  
  # Confidence intervals and points (NO significance highlighting)
  geom_pointrange(
    aes(ymin = rate_CI_low, ymax = rate_CI_high, color = effect),
    position = position_dodge(width = 0.6),
    size = 0.9,
    shape = 16
  ) +
  
  # Value labels (ALL points)
  geom_text(
    aes(label = sprintf("%.1f", rate_per_100k), color = effect),
    vjust = 0.4,
    hjust = -0.4,
    size = 3,
    position = position_dodge(width = 0.6),
    show.legend = FALSE
  ) +
  
  # Vertical dashed lines separating groups
  geom_vline(
    xintercept = c(1.45, 3.45, 5.45, 8.55, 10.55, 12.55, 14.55, 16.55, 19.45),
    linetype = "dashed", color = "grey50"
  ) +
  
  # Group labels inside the plot
  annotate("text", x = 1, y = 20, label = "Overall",
           size = 2.5, family = "Helvetica", fontface = "bold") +
  annotate("text", x = 2.5, y = 20, label = "Sex",
           size = 2.5, family = "Helvetica", fontface = "bold") +
  annotate("text", x = 4.5, y = 20, label = "Age group",
           size = 2.5, family = "Helvetica", fontface = "bold") +
  annotate("text", x = 7, y = 20, label = "Ethnic group",
           size = 2.5, family = "Helvetica", fontface = "bold") +
  annotate("text", x = 9.5, y = 20, label = "Urban/rural area",
           size = 2.5, family = "Helvetica", fontface = "bold") +
  annotate("text", x = 11.5, y = 20, label = "IMD",
           size = 2.5, family = "Helvetica", fontface = "bold") +
  annotate("text", x = 13.5, y = 20, label = "Comorbidities",
           size = 2.5, family = "Helvetica", fontface = "bold") +
  annotate("text", x = 15.5, y = 20, label = "Excess weight",
           size = 2.5, family = "Helvetica", fontface = "bold") +
  annotate("text", x = 18, y = 20, label = "Smoking status",
           size = 2.5, family = "Helvetica", fontface = "bold") +
  annotate("text", x = 24, y = 20, label = "Region",
           size = 2.5, family = "Helvetica", fontface = "bold") +
  
  # Facet by temperature type
  facet_wrap(
    ~effect, ncol = 1, scales = "free_y",
    labeller = labeller(effect = c(
      "Heat" = "Heat-attributable rate (temperatures > 50th percentile)",
      "Cold" = "Cold-attributable rate (temperatures < 50th percentile)"
    ))
  ) +
  
  # Horizontal reference line (solid)
  geom_hline(yintercept = 0, color = "grey30", linetype = "solid") +
  
  # Custom colors
  scale_color_manual(values = c("Heat" = "#FC9272", "Cold" = "#4292C6")) +
  
  # Axis labels and theme
  scale_y_continuous(expand = expansion(mult = c(0, 0.05))) +
  labs(
    x = NULL,
    y = "Annual rate per 100,000 person-years",
    title = "Venous events attributable to air temperature"
  ) +
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
    legend.position = "none",
    axis.text.x = element_text(angle = 45, hjust = 1),
    plot.background = element_rect(fill = "white", color = "white"),
    plot.margin = margin(10, 10, 10, 10)
  )

# Save plot
ggsave(
  filename = here("figures", "panel_forest_plot_AR_venous_events.png"),
  plot = p,
  width = 15, height = 8, dpi = 300
)

# ------------------------------
# AMI 14 d lag period
# ------------------------------

# Load data
df <- read_csv(here("export/Oct_2025", "CCU076_01_attributable_risk_acute_myocardial_infarction_14d_lag.csv"))

# Define subgroup orders
ethnicity_order <- c(
  "Asian or Asian British",
  "Black, Black British, Caribbean or African",
  "White"
)

region_order <- c(
  "North West",
  "Yorkshire and The Humber",
  "North East",
  "East Midlands",
  "West Midlands",
  "East of England",
  "South East",
  "South West",
  "London"
)

# Prepare and filter data
df_filtered <- df %>%
  filter(outcome == "mi_fatal", effect %in% c("heat", "cold")) %>%
  mutate(
    subgroup = if_else(subgroup == "overall", "Overall", subgroup),
    subgroup = if_else(subgroup == "age_binary_Age 18-64 years", "Age 18-64 years", subgroup),
    subgroup = if_else(subgroup == "age_binary_Age 65 years and older", "Age 65 years and older", subgroup),
    subgroup = if_else(subgroup == "Low deprivation index (1-3)", "Most deprived (quint. 1-3)", subgroup),
    subgroup = if_else(subgroup == "High deprivation index (4-5)", "Least deprived (quint. 4-5)", subgroup),
    effect = case_when(
      effect == "heat" ~ "Heat",
      effect == "cold" ~ "Cold"
    ),
    effect = factor(effect, levels = c("Heat", "Cold"))
  ) %>%
  filter(!subgroup %in% c(
    "Mixed or multiple ethnic groups",
    "Other ethnic group",
    "Missing smoking status",
    "outcome_with_covid",
    "outcome_without_covid",
    "Missing ethnicity"
  )) %>%
  mutate(
    subgroup = factor(
      subgroup,
      levels = c(
        "Overall",
        "Female", "Male",
        "Age 18-64 years", "Age 65 years and older",
        sort(ethnicity_order),
        "Rural", "Urban",
        "Most deprived (quint. 1-3)", "Least deprived (quint. 4-5)",
        "Comorbidities", "No comorbidities",
        "Excess weight", "No excess weight",
        "Never smoked", "Current smoker", "Ex-smoker",
        region_order
      )
    )
  )

# Determine shading positions (alternating, starting from 2nd level)
shading_positions <- seq(2, length(levels(df_filtered$subgroup)), by = 2)

# Build plot
p <- ggplot(df_filtered, aes(x = subgroup, y = rate_per_100k)) +
  # Alternating shading using thick vertical lines
  geom_vline(
    xintercept = shading_positions,
    color = "#1b9e77", size = 18, alpha = 0.05
  ) +
  
  # Confidence intervals and points (NO significance highlighting)
  geom_pointrange(
    aes(ymin = rate_CI_low, ymax = rate_CI_high, color = effect),
    position = position_dodge(width = 0.6),
    size = 0.9,
    shape = 16
  ) +
  
  # Value labels (ALL points)
  geom_text(
    aes(label = sprintf("%.1f", rate_per_100k), color = effect),
    vjust = 0.4,
    hjust = -0.4,
    size = 3,
    position = position_dodge(width = 0.6),
    show.legend = FALSE
  ) +
  
  # Vertical dashed lines separating groups
  geom_vline(
    xintercept = c(1.45, 3.45, 5.45, 8.55, 10.55, 12.55, 14.55, 16.55, 19.45),
    linetype = "dashed", color = "grey50"
  ) +
  
  # Group labels inside the plot
  annotate("text", x = 1, y = 20, label = "Overall",
           size = 2.5, family = "Helvetica", fontface = "bold") +
  annotate("text", x = 2.5, y = 20, label = "Sex",
           size = 2.5, family = "Helvetica", fontface = "bold") +
  annotate("text", x = 4.5, y = 20, label = "Age group",
           size = 2.5, family = "Helvetica", fontface = "bold") +
  annotate("text", x = 7, y = 20, label = "Ethnic group",
           size = 2.5, family = "Helvetica", fontface = "bold") +
  annotate("text", x = 9.5, y = 20, label = "Urban/rural area",
           size = 2.5, family = "Helvetica", fontface = "bold") +
  annotate("text", x = 11.5, y = 20, label = "IMD",
           size = 2.5, family = "Helvetica", fontface = "bold") +
  annotate("text", x = 13.5, y = 20, label = "Comorbidities",
           size = 2.5, family = "Helvetica", fontface = "bold") +
  annotate("text", x = 15.5, y = 20, label = "Excess weight",
           size = 2.5, family = "Helvetica", fontface = "bold") +
  annotate("text", x = 18, y = 20, label = "Smoking status",
           size = 2.5, family = "Helvetica", fontface = "bold") +
  annotate("text", x = 24, y = 20, label = "Region",
           size = 2.5, family = "Helvetica", fontface = "bold") +
  
  # Facet by temperature type
  facet_wrap(
    ~effect, ncol = 1, scales = "free_y",
    labeller = labeller(effect = c(
      "Heat" = "Heat-attributable rate (temperatures > 50th percentile)",
      "Cold" = "Cold-attributable rate (temperatures < 50th percentile)"
    ))
  ) +
  
  # Horizontal reference line (solid)
  geom_hline(yintercept = 0, color = "grey30", linetype = "solid") +
  
  # Custom colors
  scale_color_manual(values = c("Heat" = "#FC9272", "Cold" = "#4292C6")) +
  
  # Axis labels and theme
  scale_y_continuous(expand = expansion(mult = c(0, 0.05))) +
  labs(
    x = NULL,
    y = "Annual rate per 100,000 person-years",
    title = "Acute myocardial infarction attributable to air temperature"
  ) +
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
    legend.position = "none",
    axis.text.x = element_text(angle = 45, hjust = 1),
    plot.background = element_rect(fill = "white", color = "white"),
    plot.margin = margin(10, 10, 10, 10)
  )

# Save plot
ggsave(
  filename = here("figures", "panel_forest_plot_AR_myocardial_infarction_14d.png"),
  plot = p,
  width = 15, height = 8, dpi = 300
)
