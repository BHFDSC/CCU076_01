###############################################################################
# Project:  CCU076
# Title:    Associations between air temperature and cardiovascular health outcomes
# Date:     06-10-2025
# Author:   Isabel Walter
# Purpose:  Standardise attributable numbers and prepare export.
#           
###############################################################################

# Load libraries
library(dplyr) ; library(stringr) ; library(readr)

# Source function
fs <- c("retrieve_data")
for (f in fs) source(here::here("source", paste0(f, ".R")))

# Impact analysis files
df_out_main <- load_data("attributable_risk_ae_fatal_main_analysis_new")
rownames(df_out_main)<- NULL
df_out_subgroups <- load_data("attributable_risk_ae_fatal_all_subgroups")
df_out <- rbind(df_out_main, df_out_subgroups)
df_out <- df_out[!duplicated(df_out[c("subgroup")]), ]

#if mi_fatal 14d dataset:
#df_out$subgroup[df_out$subgroup == "Age 65 years and older"] <- "age_binary_Age 65 years and older"
#df_out$subgroup[df_out$subgroup == "Age 18-64 years"] <- "age_binary_Age 18-64 years"

df_out_avg <- df_out %>%
  # keep only outcome, subgroup, batch_number, and all "an*" columns
  select(outcome, subgroup, starts_with("an")) %>%
  # ensure the "an*" columns are numeric in case they came in as character
  mutate(across(starts_with("an"), ~ suppressWarnings(as.numeric(.)))) %>%
  # drop all AF columns (if any slipped in)
  select(-starts_with("af"))


# start from df_out_avg (one row per outcome + subgroup, averaged over batches)
df_out_long <- bind_rows(
  df_out_avg %>%
    mutate(subgroup = if_else(subgroup == "not applicable", "overall", subgroup),
           effect = "cold") %>%
    transmute(outcome, subgroup, effect,
              excess_events = an_cold,
              CI_low        = an_cold_ll_ci,
              CI_high       = an_cold_ul_ci),
  
  df_out_avg %>%
    mutate(subgroup = if_else(subgroup == "not applicable", "overall", subgroup),
           effect = "heat") %>%
    transmute(outcome, subgroup, effect,
              excess_events = an_hot,
              CI_low        = an_hot_ll_ci,
              CI_high       = an_hot_ul_ci),
  
  df_out_avg %>%
    mutate(subgroup = if_else(subgroup == "not applicable", "overall", subgroup),
           effect = "total") %>%
    transmute(outcome, subgroup, effect,
              excess_events = an,
              CI_low        = an_ll_ci,
              CI_high       = an_ul_ci)
) %>%
  arrange(subgroup, factor(effect, levels = c("cold","heat","total")))


# incidence rate files
incidence_rates <- load_data("ccu076_01_out_incidence_rates") 
incidence_rates$subgroup <- NA
incidence_rates_ethn <- load_data("ccu076_01_out_incidence_rates_ethnicity")
incidence_rates_region <- load_data("ccu076_01_out_incidence_rates_region")
names(incidence_rates_region)[22] <- "subgroup"
incidence_rates_sex <- load_data("ccu076_01_out_incidence_rates_sex")
incidence_rates_subgroups <- load_data("ccu076_01_out_incidence_rates_additional_subgroups")

norm_inc <- function(df) {
  # find the outcome & subgroup columns
  nm_outcome  <- "name"  
  nm_subgroup <- "subgroup"
  
  df <- df %>%
    select(!ends_with("2020_2021"))
  
  py_cols  <- grep("^person_years_20(20|21|22)$", names(df), value = TRUE)
  inc_cols <- grep("^incidence_per_100k_20(20|21|22)$", names(df), value = TRUE)
  
  df %>%
    rename(outcome_name = !!nm_outcome, subgroup = !!nm_subgroup) %>%
    mutate(
      subgroup = case_when(
        is.na(subgroup) ~ "overall",
        TRUE ~ subgroup
      ),
      person_years = if (length(py_cols)) rowMeans(pick(all_of(py_cols)), na.rm = TRUE) else NA_real_,
      background_incidence = if (length(inc_cols)) rowMeans(pick(all_of(inc_cols)), na.rm = TRUE) else NA_real_
    ) %>%
    select(outcome_name, subgroup, person_years, background_incidence) %>%
    distinct()
}

inc_all <- bind_rows(
  norm_inc(incidence_rates),
  norm_inc(incidence_rates_ethn),
  norm_inc(incidence_rates_region),
  norm_inc(incidence_rates_sex),
  norm_inc(incidence_rates_subgroups)
)

## 1) Harmonise inc_all to match attributable risk datasets -----------------------------

inc_all <- inc_all %>%
  mutate(
    across(c(outcome_name, subgroup), ~ str_squish(as.character(.))),
    # outcomes: rename to match df_out_long$outcome
    outcome_name = recode(outcome_name,
                          "venous_event"   = "ve_fatal",
                          "arterial_event" = "ae_fatal",
                          "stroke_IS"      = "stroke_fatal",
                          "MI"             = "mi_fatal",
                          .default = outcome_name),
    # subgroups: only the ones that differ
    subgroup = recode(subgroup,
                      "M" = "Male",
                      "F" = "Female",
                      "Binary age: Age 65 years or older" = "age_binary_Age 65 years and older",
                      "Binary age: Age 18–64 years"       = "age_binary_Age 18-64 years",
                      "High deprivation index (4–5)"= "High deprivation index (4-5)",
                      "Low deprivation index (1–3)" = "Low deprivation index (1-3)",
                      .default = subgroup)
  ) %>%
  distinct(outcome_name, subgroup, person_years, background_incidence)

years <- 3  # 2020, 2021, 2022

df_out_final <- df_out_long %>%
  # join on harmonised names
  left_join(inc_all, by = c("outcome" = "outcome_name", "subgroup" = "subgroup")) %>%
  mutate(
    # annualise excess events and CIs
    annual_excess_events = excess_events / years,
    annual_CI_low        = CI_low        / years,
    annual_CI_high       = CI_high       / years,
    
    # excess incidence rates per 100k (guard against missing denominators)
    rate_per_100k = if_else(!is.na(person_years) & person_years > 0,
                            annual_excess_events / person_years * 1e5, NA_real_),
    rate_CI_low   = if_else(!is.na(person_years) & person_years > 0,
                            annual_CI_low / person_years * 1e5, NA_real_),
    rate_CI_high  = if_else(!is.na(person_years) & person_years > 0,
                            annual_CI_high / person_years * 1e5, NA_real_),
    
    # % attributable (guard against missing background incidence)
    percent_attributable = if_else(!is.na(background_incidence) & background_incidence > 0,
                                   (rate_per_100k / background_incidence) * 100, NA_real_),
    percent_CI_low       = if_else(!is.na(background_incidence) & background_incidence > 0,
                                   (rate_CI_low   / background_incidence) * 100, NA_real_),
    percent_CI_high      = if_else(!is.na(background_incidence) & background_incidence > 0,
                                   (rate_CI_high  / background_incidence) * 100, NA_real_)
  ) %>%
  select(
    outcome, subgroup, effect,
    excess_events, CI_low, CI_high,
    annual_excess_events, annual_CI_low, annual_CI_high,
    rate_per_100k, rate_CI_low, rate_CI_high,
    percent_attributable, percent_CI_low, percent_CI_high,
    person_years, background_incidence
  )

columns_to_round <- c(
  "person_years"
)

for (col in columns_to_round) {
  df_out_final[[col]] <- as.integer(
    round(as.double(df_out_final[[col]]) / 5) * 5
  )
}

output(df_out_final, "CCU076_01_attributable_risk_arterial_event")

