#*******************************************************************************
#
# Project: CCU076
# Title:   Associations between air temperature and cardiovascular health outcomes
# Date:    16-06-2025
# Author:  Isabel Walter 
# Purpose: Run different spline parameterisations of the venous event crossbasis. 
#
#*******************************************************************************

# Load libraries
libs <- c("dplyr", "dlnm", "gnm", "pbs", "lubridate", "mgcv", "splines")

# Load libraries in a loop
for (lib in libs) {
  if (!requireNamespace(lib, quietly = TRUE)) {
    install.packages(lib, dependencies = TRUE)
  }
  library(lib, character.only = TRUE)
}

# Source function
fs <- c("retrieve_data", "time_series_month", "vechMat_replacement", "crossbasis_cpp")
for (f in fs) source(here::here("source", paste0(f, ".R")))


# Function that runs poisson regression and saves model
run_model <- function(filename, lagdays, ts, subgroup = NULL){
  
  # Spline settings
  temp_knot_sets <- list(
    c("10.0%", "75.0%", "90.0%")#, still need to do 4 lag knots of this one
    #c("5.0%", "50.0%", "95.0%"),
    #c("5.0%", "35.0%", "65.0%", "95.0%"),
    #c("10.0%", "40.0%", "70.0%", "90.0%"),
    #c("25.0%", "75.0%"),
    #c("10.0%", "90.0%")
  )
  
  boundary_knots <- c("1.0%", "99.0%")
  
  lag_knots_options <- c(4) #2,3,4)
  
  #vardegrees <- c(2, 3)
  #week_dfs <- c(4, 5) # found that 5 performs consistently better than 4 so go for 5
  #year_dfs <- c(2,3) # found that 2 or 3 did not change AIC, so go for 2
  
  distribution <- load_data("tmean_pm2p5_distribution")
  cen <- distribution$tmean_20_22[distribution$percentile == "50.0%"]
  
  print("Creating strata for eliminate argument")
  # Create term to stratify for month and person id
  ts$group <- interaction(ts$PERSON_ID, ts$month, ts$year, sep=":")
  ts <- ts |> select(-month, -year)
  
  print("create batch numbers for each individual")
  # Split up time series, crossbasis, splines in batches for computational feasibility
  
  # Step 1: Batch the time series (ts) as in your original code
  batch_size <- 275000
  
  # Get unique individuals and shuffle them
  set.seed(123)  # For reproducibility
  unique_persons <- ts |>
    distinct(PERSON_ID) |>
    sample_frac(1)  # Shuffle for randomness
  
  # Calculate the number of batches needed
  num_batches <- ceiling(nrow(unique_persons) / batch_size)
  
  # Assign batch numbers evenly
  unique_persons <- unique_persons |>
    mutate(batch = rep(1:num_batches, length.out = n()))
  
  # Merge batch assignment back to the full dataset
  ts <- ts |>
    left_join(unique_persons, by = "PERSON_ID")
  
  # Loop over all combinations
  for (temp_knots_labels in temp_knot_sets) {
    for (lag_knots in lag_knots_options) {
     # for (week_df in week_dfs) {
      #  for (year_df in year_dfs) {
       #   for (include_boundary in c(TRUE, FALSE)) {
            
            # Resolve knot values
            temp_knots <- distribution$tmean_20_22[
              match(temp_knots_labels, distribution$percentile)
            ]
            
            temp_b_knots <- distribution$tmean_20_22[
              match(boundary_knots, distribution$percentile)
            ]
            
            include_boundary <- TRUE
            
            # Build argvar
            if (include_boundary) {
              varfun <- "ns"
              argvar <- list(fun = varfun,
                             knots = temp_knots,
                             Boundary.knots = temp_b_knots)
            } else {
              varfun <- "bs"
              argvar <- list(fun = varfun,
                             knots = temp_knots,
                             degree = vardegree)
            }
            
            # Print parameters
            cat("\n==============================\n")
            cat("Temp knots  :", paste(temp_knots_labels, collapse = ", "), "\n")
            cat("N lag knots  :", paste(lag_knots, collapse = ", "), "\n")
            #cat("vardegree   :", vardegree, "\n")
            #cat("week_df     :", week_df, "\n")
            #cat("year_df     :", year_df, "\n")
            #cat("Boundary    :", ifelse(include_boundary, "YES", "NO"), "\n")
            cat("==============================\n")
            
            # Time splines
            #print("Creating spline terms")
            #splweek <- ns(ts$weekofyear, df = week_df)
            #splyear <- ns(ts$year, df = year_df)
            
            # Lag setup
            lagfun <- "ns"
            nk <- lag_knots
            arglag <- list(fun = lagfun, knots = logknots(lagdays, nk = nk))
            
            # Generate filename
            knot_label <- paste(gsub("\\.", "", gsub("%", "", temp_knots_labels)), collapse = "_")
            #bflag <- ifelse(include_boundary, "withB", "noB")
            
            # Conditional degree tag cause not used when method is ns
            #deg_tag <- if (!include_boundary) paste0("deg", vardegree, "_") else ""
            
            # Construct final file_id
            file_id <- paste0(
              "_temp_knots_", knot_label,
              "_lag_knots_", lag_knots
            )
            
            # Make outcome indicator and environmental exposure numeric
            ts$outcome_ind <- as.numeric(ts$outcome_ind)
            ts$tmean <- as.numeric(ts$tmean)
            
            print("Save batches of ts and splines")
            # Save each batch separately
            for (batch_number in 1:num_batches) {
              ts_batch <- ts[ts$batch == batch_number, ]
              #splyear_batch <- splyear[ts$batch == batch_number, ]
              #splweek_batch <- splweek[ts$batch == batch_number, ]
              save(ts_batch, filename = paste0("spline_test_batch_", batch_number))
              #save(splyear_batch, filename = paste0("splyear_batch_", batch_number))
              #save(splweek_batch, filename = paste0("splweek_batch_", batch_number))
              }
  
          # Clean up full ts from memory
          #rm(splyear, splweek)
          gc()
          
          # Initialize an empty list to store parameter data frames for each batch
          parlist <- list()
          
          
          print("Loop through each batched time series dataset")
          # Loop through each batched time series dataset
          for (batch_number in 1:num_batches) {
            
            print(paste0("batch_number: ", batch_number))
            
            # Load batch
            ts_subset <- load_data(paste0("spline_test_batch_", batch_number))
            
            # Create crossbasis on subset
            print(paste0("create crossbasis of batch_number: ", batch_number))
            
            environment(crossbasis_cpp) <- asNamespace("dlnm")
            
            start_time <- Sys.time()
            cb <- crossbasis_cpp(x=ts_subset$tmean, 
                                        lag=lagdays, 
                                        argvar=argvar,
                                        arglag=arglag,
                                        group=ts_subset$PERSON_ID)
            
            # Limit crossbasis, and time series to study period
            # Ensure both columns are Date type
            ts_subset$time_series_date <- as.Date(ts_subset$time_series_date)
            ts_subset$study_start_date <- as.Date(ts_subset$study_start_date)
            
            # Create logical index of valid rows (on or after study start date)
            valid_rows <- ts_subset$time_series_date >= ts_subset$study_start_date
            
            # Subset the dataset and all model components
            ts_subset   <- ts_subset[valid_rows, ]
            cb_subset   <- cb[valid_rows, ]
            
            # Get crossbasis reference attributes
            cb_attr <- c("range", "group", "class","df", "argvar", "arglag", "lag")
            
            for (i in cb_attr){
              attr(cb_subset, i) <- attributes(cb)[[i]] 
            }
            
            
            print(paste0("Fit poisson model ", batch_number))
            
            #  Identify groups that contain at least one outcome == 1
            groups_with_event <- unique(ts_subset$group[ts_subset$outcome_ind == 1])
            
            keep_rows <- ts_subset$group %in% groups_with_event
            
            # Subset both ts_subset and cb_subset using the same filter
            ts_subset <- ts_subset[keep_rows, ]
            cb_subset <- cb_subset[keep_rows, , drop = FALSE] 
            
            for (i in cb_attr){
              attr(cb_subset, i) <- attributes(cb)[[i]] 
            }
            
            rm(cb)
            group_subset <- ts_subset$group
            
            # Fit the GNM Poisson model
            model <- gnm(
              outcome_ind ~ cb_subset + factor(dayofweek), 
              data=ts_subset, 
              family=poisson,
              eliminate=group_subset
            )
            
            print(paste0("Cross-reduce model ", batch_number))
            # Cross-reduce the model
            redpred <- crossreduce(cb_subset, model, cen = cen)
            
            print(paste0("Store batch parameters ", batch_number))
            # Store parameters (coef + vectorized vcov)
            ncoef <- length(coef(redpred))
            par <- c(coef(redpred), vechMat_replacement(vcov(redpred)))
            names(par) <- c(paste0("coef", seq(ncoef)),
                            paste0("vcov", seq(ncoef * (ncoef + 1) / 2)))
            
            # Store batch information
            batch_par <- data.frame(
              outcome = filename,
              subgroup = ifelse(is.null(subgroup), "not applicable", subgroup),
              batch_number = batch_number,
              size = round(length(unique(ts_subset$PERSON_ID)) / 5) * 5,
              nevents=sum(ts_subset$outcome_ind,na.rm=T),
              aic = AIC(model),
              conv=model$converged,
              disp=sum(residuals(model,type="pearson")^2, na.rm=T)/model$df.residual,
              t(par),
              row.names = NULL
            )
            
            # Append to the list of parameters for each batch
            parlist[[batch_number]] <- batch_par
            
            rm(ts_subset, cb_subset, 
               group_subset, model, redpred)
            
            # Delete batch file
            file.remove(here::here("data", paste0("spline_test_batch_", batch_number, ".rds")))
            gc()
          }
          
          print("Combine all batch results in dataframe and save")
          # Combine all batch results into one data frame
          par_df <- do.call(rbind, parlist)
          
          # Save
          save(par_df, filename = paste0("crossreduce_cb_param_",filename, lagdays,"d_lag",
                                         "batchsize",batch_size, file_id))
         # }
        #}
      #}
    }
  }
}


# Function that prepares data and then calls function to run poisson regression
cts <- function(filename, lagdays, stratification = NULL){
  
  print("load time series")
  
  # Load ts
  ts <- load_data(paste0("time_series_", filename))
  
  ts <- time_series_month(ts)
  
  # Ensure dataset is sorted by 'person_id' and 'time_series_date'
  ts <- ts[order(ts$PERSON_ID, ts$time_series_date),]
  
  ts <- ts |> select(PERSON_ID, outcome_ind, time_series_date,study_start_date, tmean, dayofweek, month, year)
  
  run_model(filename=filename, 
              lagdays=lagdays, 
              ts=ts
    )
  
}

cts("ve_fatal", 21)
