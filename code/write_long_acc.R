# Load required libraries
library(move2)      # For accessing Movebank data
library(data.table) # For efficient data manipulation
library(lubridate)  # For working with date and time
library(arrow)      # For writing parquet files
library(progressr)  

# Step 1: Define parameters for data retrieval
# These parameters are kept from the original script for context.
# date_start <- as.POSIXct("2024-09-01 00:00:00", tz = "UTC")
# date_end <- as.POSIXct("2024-11-01 23:59:59", tz = "UTC")
study_id <- 3445611111

# Step 2: Download or read acceleration data
# The original download call is preserved but commented out.
# acc_data <- movebank_download_study(
#   study_id = study_id,
#   sensor_type_id = "acceleration",
#   individual_id  = 4212243165,
#   timestamp_start = date_start,
#   timestamp_end = date_end
# )

# Read data using data.table's fread for speed and convert to data.table
#acc_data <- fread("data/Baboons MBRP Mpala Kenya_acc_20240701_20250601.csv")
acc_data <- fread("/mnt/EAS_shared/baboon/working/data/raw/2025/acc/Baboons MBRP Mpala Kenya_acc_20240701_20250929.csv")
setnames(acc_data, gsub("-", "_", names(acc_data), fixed = TRUE))

setwd("/mnt/EAS_shared/baboon/working/data/processed/2025/acc")

# Step 2a: Adding group_id from metadata using a data.table join
# Extract metadata and convert to data.table
metadata <- setDT(movebank_download_deployment(study_id))
# Perform a left join to add group_id and sex to the main table
#acc_data[metadata, on = .(individual_local_identifier), `:=`(group_id = i.group_id, sex = i.sex)]

# Step 3: Clean and process the data using data.table
# Rename columns for clarity using setnames
setnames(acc_data, 
         old = c("individual_local_identifier","tag_local_identifier", "eobs:accelerations_raw"), 
         new = c("tag","collar", "eobs_accelerations_raw_str"))

# Select necessary columns and ensure timestamp is in POSIXct format
acc_data <- acc_data[, .(tag, collar,timestamp, eobs_accelerations_raw_str)]
acc_data[, timestamp := as.POSIXct(timestamp, tz = "UTC")]

# Filter out rows with empty acceleration data
acc_data <- acc_data[eobs_accelerations_raw_str != ""]

# Step 4: Parse accelerations into X, Y, Z components
# Use tstrsplit for a fast and direct split of the raw string into columns
# Create a directory for the output files
output_dir <- "acc_v0"
dir.create(output_dir, showWarnings = FALSE)

# Get unique tags to iterate over
unique_tags <- unique(acc_data$tag)

# Read calibration data upfront
acc_calib <- read.csv('data/acc_calib.csv')
acc_calib$tag <- as.factor(acc_calib$tag)
acc_calib$x0 <- as.numeric(acc_calib$x0)
acc_calib$y0 <- as.numeric(acc_calib$y0)
acc_calib$z0 <- as.numeric(acc_calib$z0)
acc_calib$Sx <- as.numeric(acc_calib$Sx)
acc_calib$Sy <- as.numeric(acc_calib$Sy)
acc_calib$Sz <- as.numeric(acc_calib$Sz)

# Initialize the progress bar
total_tags <- length(unique_tags)
pb <- txtProgressBar(min = 0, max = total_tags, style = 3)

# Loop over each unique tag
for (i in seq_along(unique_tags)) {
  current_tag <- unique_tags[i]
  
  # Filter data for the current individual
  individual_acc_data <- acc_data[tag == current_tag]
  current_collar <- individual_acc_data$collar[1]
  # --- Start of processing for a single individual ---
  
  ## remove high res data, more than a single burst per minute or long bursts
  # Create a temporary minute column for grouping, but keep it separate
  individual_acc_data[, minute_temp := format(timestamp, "%Y-%m-%d %H:%M")]
  
  # Select first row per minute while ensuring original timestamp is preserved
  individual_acc_data <- individual_acc_data[order(timestamp), .SD[1], by = minute_temp]
  
  # Remove the temporary minute column
  individual_acc_data[, minute_temp := NULL]
  
  # Use tstrsplit for a fast and direct split of the raw string into columns
  acc_cols <- tstrsplit(individual_acc_data$eobs_accelerations_raw_str, 
                        " ", type.convert = TRUE)[1:120]
  col_count <- length(acc_cols)
  
  # Create a new data.table `d2` with the parsed acceleration data
  d2 <- as.data.table(acc_cols)
  
  # If there's no acceleration data for this individual, skip to the next
  if (col_count == 0) {
    setTxtProgressBar(pb, i) # Update progress bar
    next
  }
  
  col_count <- ncol(d2)
  # Dynamically name the new columns (x1, y1, z1, x2, y2, z2, ...)
  xyz_names <- paste0(rep(c("x", "y", "z"), length.out = col_count), 
                      rep(1:(col_count / 3), each = 3)) # 
  setnames(d2, new = xyz_names)
  
  # Add timestamp and tag for joining and identification
  d2[, `:=`(burst_timestamp = individual_acc_data$timestamp)]
  
  # Reshape data from wide to long format using data.table's melt
  individual_long_data <- melt(d2, 
                               id.vars = c("burst_timestamp"),
                               measure.vars = patterns(X = "^x", Y = "^y", Z = "^z"),
                               variable.name = "index",
                               value.name = c("X", "Y", "Z"))
  
  individual_long_data[, timestamp := burst_timestamp + (as.numeric(index) - 1) * 0.05]
  
  # Apply calibration
  # Find the calibration values for the current tag
  calib_row <- acc_calib[acc_calib$tag == current_collar, ]
  
  # if (nrow(calib_row) > 0) {
  #   # Apply calibration formulas using the calibration values
  #   individual_long_data[, `:=`(
  #     X = (X - calib_row$x0) * calib_row$Sx * 9.81,
  #     Y = (Y - calib_row$y0) * calib_row$Sy * -9.81,
  #     Z = (Z - calib_row$z0) * calib_row$Sz * 9.81
  #   )]
  # } else {
  #   # If no calibration data is available, use raw values
  #   individual_long_data[, `:=`(
  #     X = X,
  #     Y = Y,
  #     Z = Z
  #   )]
  # }
  
  setorder(individual_long_data, timestamp, X, Y, Z, burst_timestamp, index)
  
  # --- End of processing for a single individual ---
  
  # Save the processed data for the current individual to a Parquet file
  output_filename <- file.path(output_dir, paste0(current_tag,"_",current_collar,".parquet"))
  write_parquet(individual_long_data, output_filename)
  
  # Update the progress bar
  setTxtProgressBar(pb, i)
}

# Close the progress bar
close(pb)

# ############################# VEDBA #################################
# acc_raw <- read_parquet("data/acc_v0/24AA12_6P8Q.parquet")
# setDT(acc_raw)   # ensures acc_raw is a proper data.table
# set(acc_raw, j = "grp", value = paste(acc_raw$timestamp, acc_raw$tag, sep = "_"))
# 
# 
# # Your dy_acc function
# dy_acc <- function(vect, win_size = 10){
#   pad_size <- win_size/2 - 0.5
#   padded <- unlist(c(rep(NA, pad_size), vect, rep(NA, pad_size)))
#   acc_vec <- rep(NA, length = length(vect))
#   
#   for(i in 1:length(vect)){
#     win <- padded[i:(i+(2*pad_size))]
#     m_ave <- mean(win, na.rm = TRUE)
#     acc_comp <- vect[i] - m_ave
#     acc_vec[i] <- acc_comp 
#   }
#   
#   return(unlist(acc_vec))
# }
# 
# # Apply the function to X, Y, and Z columns for each grp
# acc_raw[, vectorial_sum := {
#   # These are now temporary variables that exist only for this calculation
#   dax <- dy_acc(X)
#   day <- dy_acc(Y)
#   daz <- dy_acc(Z)
#   
#   # The final line is the value assigned to 'vectorial_sum'
#   sqrt(dax^2 + day^2 + daz^2)
# }, by = grp]
# 
# 
# acc_min <- acc_raw[, .(
#   tag = tag[1],
#   timestamp = timestamp[1],
#   vedba = mean(vectorial_sum, na.rm = TRUE)
#   ), by = grp]
# 
# acc_min$logVedba <- log(acc_min$vedba)
# acc_min[, c("grp", "vedba") := NULL]
#####################################################################
#Calculate VeDBA from standardized ACC data
# acc_to_vedba <- function(df, rolling_mean_width, group_col = NULL) {
#   df <- copy(df)  # <-- prevents modifying the original data.table
#   
#   present_axes <- c("X", "Y", "Z")
#   by_expr <- if (!is.null(group_col)) group_col else NULL
#   
#   # STATIC
#   df[, (paste0(present_axes, "_static")) := lapply(present_axes, function(axis) {
#     frollmean(get(axis), n = rolling_mean_width, align = "center", na.rm = TRUE)
#   }), by = by_expr]
#   
#   # DYNAMIC
#   df[, (paste0(present_axes, "_dynamic")) := Map(
#     function(raw, stc) raw - stc,
#     lapply(present_axes, function(axis) get(axis)),
#     .SD
#   ), by = by_expr, .SDcols = paste0(present_axes, "_static")]
#   
#   # VeDBA
#   df[, VeDBA := sqrt(Reduce(`+`, lapply(.SD, function(d) d^2))),
#      by = by_expr,
#      .SDcols = paste0(present_axes, "_dynamic")]
#   
#   # fracNA
#   df[, fracNA := frollmean(is.na(get("X")), n = rolling_mean_width, align = 'center', na.rm = TRUE),
#      by = by_expr]
#   
#   # Cleanup
#   df[, c(paste0(present_axes, "_static"), paste0(present_axes, "_dynamic")) := NULL]
#   
#   return(df)
# }
# 
# ved <- acc_to_vedba(acc_raw, rolling_mean_width = 10, group_col = "grp")
# 
# 
# ##################
# 
# # Load necessary libraries
# library(data.table)
# library(future)
# library(future.apply)
# 
# # --- Optimized Function ---
# # This version uses the highly optimized data.table::frollmean instead of a for-loop.
# # It calculates a centered moving average, which is what your original function did.
# # NOTE: Your function requires an ODD window size (e.g., 7, 9, 11) to create a
# # perfectly centered window. I've set it to 7 as an example.
# dy_acc_fast <- function(vect, win_size = 7) {
#   # frollmean is much faster than a manual for-loop
#   moving_avg <- frollmean(vect, n = win_size, align = "center", na.rm = TRUE)
#   
#   # The core calculation remains the same: value - moving average
#   acc_comp <- vect - moving_avg
#   
#   return(acc_comp)
# }
# 
# # Read calibration data upfront
# acc_calib <- read.csv('data/acc_calib.csv')
# acc_calib$tag <- as.factor(acc_calib$tag)
# acc_calib$x0 <- as.numeric(acc_calib$x0)
# acc_calib$y0 <- as.numeric(acc_calib$y0)
# acc_calib$z0 <- as.numeric(acc_calib$z0)
# acc_calib$Sx <- as.numeric(acc_calib$Sx)
# acc_calib$Sy <- as.numeric(acc_calib$Sy)
# acc_calib$Sz <- as.numeric(acc_calib$Sz)
# 
# # --- Parallel Execution ---
# 
# # 1. Set a parallel processing plan
# # This will use all available cores on your machine minus one
# plan(multisession, workers = availableCores() - 1)
# 
# # 2. Get the unique list of groups to process
# unique_groups <- unique(acc_raw$grp)
# 
# # 3. Split the data.table into a list of smaller tables, one for each group
# # This is necessary for distributing the work across cores
# list_of_dts <- split(acc_raw, by = "grp", keep.by = FALSE)
# 
# # 4. Run the calculation in parallel using future_lapply
# # This applies the function to each data.table in the list concurrently
# results_list <- future_lapply(list_of_dts, function(dt) {
#   # Perform the calculation on the small data.table for a single group
#   # This calculates the vectorial sum without creating intermediate columns
#   dt[, vectorial_sum := {
#     dax <- dy_acc_fast(X)
#     day <- dy_acc_fast(Y)
#     daz <- dy_acc_fast(Z)
#     sqrt(dax^2 + day^2 + daz^2)
#   }]
#   return(dt)
# })
# 
# # 5. Combine the results from all parallel jobs back into one data.table
# acc_raw_with_vedba <- rbindlist(results_list, idcol = "grp")
# 
# # 6. Shut down the parallel workers when you are done
# plan(sequential)
# 
# # Now `acc_raw_with_vedba` is your final table with the `vectorial_sum` column
# # You can view the head to check the result
# head(acc_raw_with_vedba)

