#*******************************************************************************
#
# Project:  CCU076_01
# Title:    Associations between air temperature and cardiovascular health outcomes
# Author:   Isabel Walter
# Date:     28-07-2025
# Purpose:  Plot overall temperature distribution and regional choropleths across England 
#           for visual comparison of temperature distribution.
#
#*******************************************************************************
library(tidyverse)
library(ggplot2)
library(dplyr)
library(sf)
library(patchwork)
library(here)


## --- Load and prepare data ---

# Percentiles file (has columns: percentile, tmean_20_22, ...)
dist <- read.csv(here("export/Oct_2025","CCU076_01_tmean_pm2p5_distribution.csv"))

dist_plot <- dist %>%
  mutate(
    percentile_value = as.numeric(sub("%", "", percentile)),
    tmean_20_22      = as.numeric(tmean_20_22)
  ) %>%
  filter(!is.na(tmean_20_22), !is.na(percentile_value)) %>%
  arrange(tmean_20_22)

# Days-per-temperature file (has columns: region, temp_rounded, days_count)
days <- read.csv(here("export/Oct_2025","CCU076_01_tmean_days.csv"))

days_eng <- days %>%
  filter(region == "England") %>%
  mutate(
    temp_rounded = as.numeric(temp_rounded),
    days_count   = as.numeric(days_count)
  ) %>%
  filter(!is.na(temp_rounded), !is.na(days_count)) %>%
  arrange(temp_rounded)

# Colours
curve_color <- "#d95f02"
bars_color  <- "#1b9e77"

# --- Percentile markers to draw ---
target_pcts <- c(1, 5, 25, 50, 75, 95, 99)

# Get the temperature at those percentiles (handles cases like "1.0%" etc.)
pct_lines <- dist_plot %>%
  mutate(
    pct_int = as.integer(round(percentile_value)),      # 1,5,25,...
    label   = paste0("p", pct_int)
  ) %>%
  filter(pct_int %in% target_pcts) %>%
  group_by(pct_int) %>%
  slice_min(order_by = abs(percentile_value - pct_int), n = 1) %>%  # closest row if needed
  ungroup()

## --- Scaling so 100% aligns with 700 on the left axis ---
scale_top <- 700  # desired top value on both axes

p <- ggplot() +
  # Bars: number of days per rounded temperature
  geom_col(
    data = days_eng,
    aes(x = temp_rounded, y = days_count,
        fill = "Number of days"),          # <--- mapped for legend
    alpha = 0.6
  ) +
  
  # Percentile vertical lines
  geom_vline(
    data = pct_lines,
    aes(xintercept = tmean_20_22),
    colour = "grey60",
    linewidth = 0.4
  ) +
  
  # Labels above lines (p1, p5, ...)
  geom_text(
    data = pct_lines,
    aes(x = tmean_20_22, y = scale_top, label = label),
    colour = "grey40",
    vjust  = -0.6,
    size   = 3,
    family = "Helvetica"
  ) +
  
  # Line: percentile curve, rescaled to match 0–700 range
  geom_line(
    data = dist_plot,
    aes(x = tmean_20_22,
        y = (percentile_value / 100) * scale_top,
        colour = "Percentile curve"),      # <--- mapped for legend
    linewidth = 1
  ) +
  
  # Manual legend scales
  scale_fill_manual(
    name   = "",
    values = c("Number of days" = bars_color)
  ) +
  scale_colour_manual(
    name   = "",
    values = c("Percentile curve" = curve_color)
  ) +
  
  # Axes and titles
  labs(
    title = "Daily mean temperature distribution and percentiles\nEngland, 2020–2022",
    x     = "Temperature (°C)",
    y     = "Number of days"
  ) +
  scale_x_continuous(
    breaks = seq(-5, 30, by = 5),
    limits = c(-5, 30),
    expand = c(0, 0)
  ) +
  scale_y_continuous(
    breaks = seq(0, scale_top, by = 100),
    expand = c(0, 0),
    sec.axis = sec_axis(
      ~ . / scale_top * 100,
      name   = "Percentile (%)",
      breaks = seq(0, 100, by = 25)
    )
  ) +
  coord_cartesian(ylim = c(0, scale_top), clip = "off") +
  
  theme_minimal() +
  theme(
    text            = element_text(family = "Helvetica"),
    plot.title      = element_text(size = 14, hjust = 0.5, face = "bold",
                                   margin = margin(b = 20)),
    axis.title      = element_text(size = 13),
    axis.text       = element_text(size = 11),
    panel.grid      = element_blank(),
    axis.line       = element_line(size = 0.3, colour = "black"),
    axis.ticks      = element_line(size = 0.3, colour = "black"),
    plot.margin     = margin(10, 10, 10, 10),
    plot.background = element_rect(color = "white", fill = "white"),
    panel.border    = element_rect(fill = NA, color = "black", size = 0.5),
    legend.position = "bottom"                
  )

ggsave(
  filename = here("figures", "tmean_distribution_study_period_england.png"),
  plot  = p,
  width = 6,
  height = 4.5,
  dpi   = 300
)


# Load required libraries


library(tidyverse)
library(sf)
library(patchwork)
library(here)

# --- FILE PATHS ---
shp_path <- here("data","NUTS_RG_01M_2021_4326.shp")
avgtmeansum <- read.csv(here("export/Oct_2025", "CCU076_01_tmean_pm2p5_distribution.csv"))

# --- READ SHAPEFILE ---
nuts1 <- st_read(shp_path)

# Filter only the 9 English NUTS1 regions
england_nuts1 <- nuts1 %>%
  filter(
    LEVL_CODE == 1, CNTR_CODE == "UK",
    NUTS_NAME %in% c(
      "North West (England)", "Yorkshire and the Humber", "North East (England)",
      "East Midlands (England)", "West Midlands (England)", "East of England",
      "South East (England)", "South West (England)", "London"
    )
  ) %>%
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

# --- Define temperature columns per region ---
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

# --- Extract 5th, 50th, 95th percentile temps ---
get_percentile_df <- function(percentile_label) {
  row <- avgtmeansum %>% filter(percentile == percentile_label)
  tibble(
    region = names(tmean_cols),
    temp = purrr::map_dbl(tmean_cols, ~ row[[.x]])
  )
}

temp_5  <- get_percentile_df("5.0%")  %>% mutate(percentile = "5th percentile")
temp_50 <- get_percentile_df("50.0%") %>% mutate(percentile = "Median")
temp_95 <- get_percentile_df("95.0%") %>% mutate(percentile = "95th percentile")

temperature_df <- bind_rows(temp_5, temp_50, temp_95)

# --- Join with shapefile ---
region_data <- england_nuts1 %>%
  left_join(temperature_df, by = "region")

# --- Shared map theme ---
map_theme <- theme_minimal(base_family = "Helvetica") +
  theme(
    plot.title = element_text(size = 12, family = "Helvetica",
                              face = "bold", hjust = 0.5),
    legend.title = element_text(face = "plain", size = 9, family = "Helvetica"),
    legend.title.position = "top",
    legend.text = element_text(size = 8, family = "Helvetica"),
    legend.position = "bottom",
    legend.key.width = unit(1.5, "cm"),
    legend.key.height = unit(0.4, "cm"),
    axis.text = element_blank(),
    axis.title = element_blank(),
    panel.grid = element_blank(),
    #plot.margin = margin(2, 2, 2, 2, unit = "mm"),
    panel.background = element_rect(fill = "white", colour = NA),
    # <<< border around each A/B/C plot (map + legend) >>>
    plot.background  = element_rect(fill = "white", colour = "grey", size = 0.5)
  )


# --- Custom plot function with colour scale by percentile ---
plot_temp_map <- function(df, perc_label, tag_letter) {
  data <- df %>% filter(percentile == perc_label)
  
  # text label e.g. "A  5th percentile"
  panel_label <- paste0(tag_letter, "  ", perc_label)
  
  p <- ggplot() +
    geom_sf(data = data, aes(fill = temp), colour = "grey40", size = 0.3) +   # internal borders
    geom_sf(data = england_nuts1, fill = NA, colour = "grey20", size = 0.4) + # outer border
    labs(fill = "Temperature (°C)") +
    map_theme +
    
    # --- Add inline label to top-left corner of the map area ---
    annotate(
      "text",
      x = -Inf, y = Inf,                   # top-left in panel coordinates
      label = panel_label,
      hjust = -0.1, vjust = 1.3,           # fine-tune horizontal / vertical position
      family = "Helvetica",
      fontface = "bold",
      size = 3.5
    )
  
  # colour scales
  if (perc_label == "5th percentile") {
    p <- p + scale_fill_gradient(
      low = "#deebf7", high = "#08306B",
      breaks = scales::pretty_breaks(n = 4),
      labels = scales::label_number(accuracy = 0.1)
    )
  } else if (perc_label == "Median") {
    p <- p + scale_fill_gradient(
      low = "#e0f3db", high = "#43a2ca",
      breaks = scales::pretty_breaks(n = 4),
      labels = scales::label_number(accuracy = 0.1)
    )
  } else if (perc_label == "95th percentile") {
    p <- p + scale_fill_gradient(
      low = "#fee0d2", high = "#CB181D",
      breaks = scales::pretty_breaks(n = 4),
      labels = scales::label_number(accuracy = 0.1)
    )
  }
  
  p
}
# --- Individual maps with inline A/B/C titles ---
p5  <- plot_temp_map(region_data, "5th percentile", "A.")
p50 <- plot_temp_map(region_data, "Median", "B.")
p95 <- plot_temp_map(region_data, "95th percentile", "C.")

# --- Combine into panel ---
final_plot <- (p5 | p50 | p95) +
  theme(legend.position = "bottom")  # each plot keeps its own legend

# --- Save ---
ggsave(
  filename = here("figures", "panel_temperature_distribution_regional_map_borders.png"),
  plot = final_plot,
  width = 12,
  height = 6,
  dpi = 300
)

