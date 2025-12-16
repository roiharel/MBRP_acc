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

tag_ids <- c(10337, 14545 , 12640) # tag ids lilac
# Define start and end for your noon-to-noon filter
start_time <- as.POSIXct("2025-07-27 00:00:00")
end_time   <- as.POSIXct("2025-08-02 00:00:00")

# Filter by collar/tag and noon-to-noon timestamp
acc_data_f <- acc_data[
  collar %in% tag_ids & timestamp >= start_time & timestamp < end_time
]

# Step 4: Parse accelerations into X, Y, Z components
# Use tstrsplit for a fast and direct split of the raw string into columns
# Create a directory for the output files
output_dir <- "acc_v1_cont"
dir.create(output_dir, showWarnings = FALSE)

# Get unique tags to iterate over
unique_tags <- unique(acc_data$tag)

# Read calibration data upfront
acc_calib <- read.csv('acc_calib.csv')
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
                        " ", type.convert = TRUE)
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
  
  if (nrow(calib_row) > 0) {
    # Apply calibration formulas using the calibration values
    individual_long_data[, `:=`(
      X = (X - calib_row$x0) * calib_row$Sx * 9.81,
      Y = (Y - calib_row$y0) * calib_row$Sy * -9.81,
      Z = (Z - calib_row$z0) * calib_row$Sz * 9.81
    )]
  } else {
    # If no calibration data is available, use raw values
    individual_long_data[, `:=`(
      X = X,
      Y = Y,
      Z = Z
    )]
  }
  
  setorder(individual_long_data, timestamp, X, Y, Z, burst_timestamp, index)
  
  individual_long_data <- individual_long_data[, .(
    Timestamp = timestamp,
    X, Y, Z
  )]
  
  # --- End of processing for a single individual ---
  
  # Save the processed data for the current individual to a Parquet file
  output_filename <- file.path(output_dir, paste0(current_tag,"_",current_collar,".parquet"))
  write_parquet(individual_long_data, output_filename)
  
  # Update the progress bar
  setTxtProgressBar(pb, i)
}

# Close the progress bar
close(pb)
