#*******************************************************************************
#
# Project:  CCU076_01
# Title:    Associations between air temperature and cardiovascular health outcomes
# Author:   Isabel Walter
# Date:     28-07-2025
# Purpose:  Plot regional choropleths across England for visual comparison.
#
#*******************************************************************************

# Load required libraries
library(tidyverse)
library(sf)
library(patchwork)
library(ggtext)
library(here)

# --- FILE PATHS ---
shp_path <- here("data","NUTS_RG_01M_2021_4326.shp")
risk_data <- read_csv("export/Oct_2025/CCU076_01_attributable_risk_venous_events.csv")
avgtmeansum <- read.csv(here("export/Oct_2025", "CCU076_01_tmean_pm2p5_distribution.csv"))
incidence <-  read_csv("export/Oct_2025/incidence_rates_region_rounded.csv")
incidence_ve <- incidence |>
  filter(name == "venous_event",
         region %in% c("North West", "Yorkshire and The Humber", "North East",
                         "East Midlands", "West Midlands", "East of England",
                         "South East", "South West", "London")) |>
  mutate(annual_incidence = rowMeans(across(c(incidence_per_100k_2020, 
                                              incidence_per_100k_2021, 
                                              incidence_per_100k_2022)), 
                                     na.rm = TRUE)) |>
  select(region, annual_incidence)

names(incidence_ve) <- c("region", "annual_incidence")


# --- READ SHAPEFILE ---
nuts1 <- st_read(shp_path)

# Filter only the 9 English NUTS1 regions
england_nuts1 <- nuts1 %>%
  filter(LEVL_CODE == 1, CNTR_CODE == "UK", NUTS_NAME %in% c(
    "North West (England)", "Yorkshire and the Humber", "North East (England)",
    "East Midlands (England)", "West Midlands (England)", "East of England",
    "South East (England)", "South West (England)", "London"
  )) %>%
  mutate(region = case_when(
    NUTS_NAME == "North West (England)" ~ "North West",
    NUTS_NAME == "Yorkshire and the Humber" ~ "Yorkshire and The Humber",
    NUTS_NAME == "North East (England)" ~ "North East",
    NUTS_NAME == "East Midlands (England)" ~ "East Midlands",
    NUTS_NAME == "West Midlands (England)" ~ "West Midlands",
    NUTS_NAME == "East of England" ~ "East of England",
    NUTS_NAME == "South East (England)" ~ "South East",
    NUTS_NAME == "South West (England)" ~ "South West",
    NUTS_NAME == "London" ~ "London",
    TRUE ~ NA_character_
  ))

# --- Extract rates from attributable risk data ---
region_order <- levels(factor(england_nuts1$region))

risk_subset <- risk_data %>%
  filter(outcome == "ve_fatal",
         effect %in% c("cold", "heat"),
         subgroup %in% region_order) %>%
  mutate(significant = rate_CI_low > 0 | rate_CI_high < 0) %>%
  select(region = subgroup, effect, rate_per_100k, significant) %>%
  pivot_wider(
    names_from = effect,
    values_from = c(rate_per_100k, significant),
    names_glue = "{effect}_{.value}"
  )

# --- Extract 50th percentile temp ---
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

t_row <- avgtmeansum %>% filter(percentile == "50.0%")

temperature_df <- tibble(
  region = names(tmean_cols),
  temp = map_dbl(tmean_cols, ~ t_row[[.x]])
)

# --- Join all data ---
region_data <- england_nuts1 %>%
  left_join(temperature_df, by = "region") %>%
  left_join(incidence_ve, by = "region") %>%
  left_join(risk_subset, by = "region")

# Define shared theme
map_theme <- theme_minimal(base_family = "Helvetica") +
  theme(
    plot.title = element_text(size = 12, family = "Helvetica", face = "bold", hjust = 0.5),
    legend.title = element_text(face = "plain", size = 9, family = "Helvetica"),
    legend.title.position = "top",
    legend.text = element_text(size = 8, family = "Helvetica"),
    legend.position = "bottom",
    legend.key.width = unit(1.5, "cm"),
    legend.key.height = unit(0.4, "cm"),
    axis.text = element_blank(),
    axis.title = element_blank(),
    panel.grid = element_blank(),
    plot.margin = margin(2, 2, 2, 2, unit = "mm") 
  )

# Median temperature plot
p_temp <- ggplot(region_data) +
  geom_sf(aes(fill = temp), color = "white") +
  scale_fill_gradient(low = "#e0f3db", high = "#43a2ca") +
  labs(fill = "Median temperature during study period in °C") +
  map_theme +
  ggtitle("Median temperature")

# Background incidence rate
p_inc <- ggplot(region_data) +
  geom_sf(aes(fill = annual_incidence), color = "white") +
  scale_fill_gradient(low = "#f2e5ff", high = "#6a00b9") +
  labs(fill = "Annual incidence rate per 100,000 person-years") +
  map_theme +
  ggtitle("Venous event incidence")

# Cold-attributable rate
p_cold <- ggplot(region_data) +
  geom_sf(aes(fill = cold_rate_per_100k), color = "white") +
  geom_text(
    data = region_data %>% filter(cold_significant == TRUE),
    aes(geometry = geometry, label = "*"),
    stat = "sf_coordinates",
    size = 5,
    family = "Helvetica",
    color = "black"
  ) +
  scale_fill_gradient(low = "#deebf7", high = "#08306B") +
  labs(fill = "Annual incidence rate per 100,000 person-years") +
  map_theme +
  ggtitle("Cold-attributable rate")

# Heat-attributable rate
p_heat <- ggplot(region_data) +
  geom_sf(aes(fill = heat_rate_per_100k), color = "white") +
  geom_text(
    data = region_data %>% filter(heat_significant == TRUE),
    aes(geometry = geometry, label = "*"),
    stat = "sf_coordinates",
    size = 5,
    family = "Helvetica",
    color = "black"
  ) +
  scale_fill_gradient(low = "#fee0d2", high = "#CB181D") +
  labs(fill = "Annual incidence rate per 100,000 person-years") +
  map_theme +
  ggtitle("Heat-attributable rate")

# Combine all plots
final_plot <- (p_temp | p_inc) /
  (p_cold | p_heat) +
  plot_layout(heights = c(1, 1.1)) +  # slightly more height for bottom row
  plot_annotation(
    title = "Venous event regional attributable risk",
    theme = theme(
      plot.title = element_text(family = "Helvetica", size = 16, face = "bold", hjust = 0.5)
    )
  )
# Save to file
ggsave(
  filename = here("figures", "panel_attributable_risk_regional_map.png"),
  plot = final_plot,
  width = 8,
  height = 12,
  dpi = 300
)
