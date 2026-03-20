# Required libraries
library(suncalc)  # For getSunlightTimes
library(stringr)  # For str_split_fixed
library(tibble)  # 
library(arrow)
# Define constants if not already defined elsewhere
mpala_lat <- -0.292  # Example latitude - replace with your actual value
mpala_lon <- 36.899  # Example longitude - replace with your actual value
frag_block <- 3      # Define fragmentation block size

# Create results directory if it doesn't exist
if (!dir.exists("results")) {
  dir.create("results")
}

# List all files in the data/inactivity directory
inactivity_files <- list.files(path = "data/inactivity/", 
                               pattern = ".*", 
                               full.names = TRUE,
                               recursive = TRUE)

study_nights <- {
  all_nights <- lapply(inactivity_files, function(f) read_parquet(f)[, c("night", "night_date")])
  all_nights <- do.call(rbind, all_nights)
  min_n <- min(all_nights$night)
  max_n <- max(all_nights$night)
  min_d <- min(as.Date(all_nights$night_date))
  max_d <- max(as.Date(all_nights$night_date))
  tibble(night = min_n:max_n, night_date = seq(min_d, max_d-1, by = "1 day"))
}

tag_names <- sub("\\.[^.]*$", "",basename(inactivity_files))

# Initialize an empty dataframe to store all sleep_per results
all_sleep_per <- data.frame()

# Initialize empty vectors for sleep and wake durations across all files
all_sleep_durs <- c()
all_wake_durs <- c()

# Process each file - for each individual...

for (ind in  1:length(inactivity_files)) { #
  tag_id <- tag_names[ind]
  cat("Processing file:", tag_id, "\n")
  # Read the inactivity data
  id_dat <- read_parquet(inactivity_files[ind])
  
  # Ensure date columns are properly formatted
  id_dat$night_date <- as.Date(id_dat$night_date)
  id_dat$local_timestamp <- as.POSIXct(id_dat$local_timestamp)
  
  # Setup the analysis structures
  study_nights <- min(id_dat$night):max(id_dat$night)
  study_night_dates <- seq.Date(min(id_dat$night_date), max(id_dat$night_date), by = "day")
  if (length(study_nights)>10){
  # Initialize sleep_per dataframe
  sleep_per <- data.frame(
    tag = rep(tag_id, each = length(study_nights)),
    night = rep(study_nights, times = 1),
    night_date = rep(study_night_dates, times = 1),
    total_pot_sleep = NA, 
    total_sleep_bouts = NA,
    onset = as.POSIXct(NA),
    waking = as.POSIXct(NA),
    SPT = NA,
    WASO = NA,
    TST = NA,
    sleep_eff = NA,
    wake_bouts = NA,
    frag_wake_bouts = NA,
    summed_VeDBA = NA,
    night_VeDBA_corr = NA,
    ave_vedba = NA,
    dark_pot_sleep = NA,
    dark_ave_vedba = NA,
    max_time_diff = NA,
    n_bursts = NA
  )
  
  # Initialize vectors for durations
  sleep_durs <- c()
  wake_durs <- c()
  
  # Create a vector of the nights for which this individual has data
  nights <- unique(id_dat$night_date)
  
  # For each night on which this individual has data
  for (n in 1:length(nights)) {
    night <- nights[n]
    # print(paste(tag_id, night))
    
    # Subset this individual's data to just that night
    night_dat <- id_dat[id_dat$night_date == night, ]
    # Should already be in order, but just in case
    night_dat <- night_dat[order(night_dat$local_timestamp), ]
    
    sleep_per[sleep_per$tag == tag_id & sleep_per$night_date == night, ]$n_bursts <- nrow(night_dat)
    
    sleep_per[sleep_per$tag == tag_id & sleep_per$night_date == night, ]$max_time_diff <- max(diff(as.numeric(night_dat$timestamp)))/60
    
    sleep_per[sleep_per$tag == tag_id & sleep_per$night_date == night, ]$total_pot_sleep <- sum(night_dat$pot_sleep)
    
    sleep_per[sleep_per$tag == tag_id & sleep_per$night_date == night, ]$total_sleep_bouts <- sum(night_dat$sleep_bouts)
    
    sleep_per[sleep_per$tag == tag_id & sleep_per$night_date == night, ]$ave_vedba <- mean(night_dat$logvedba)
    
    # Get the sun times for the specified location and date
    sun_times <- getSunlightTimes(date = night, lat = mpala_lat, lon = mpala_lon, 
                                  keep = c("nightEnd", "night"))
    # Extract dark_start and dark_end
    dark_start <- str_split_fixed(sun_times$night, " ", 2)[,2] 
    dark_end <- str_split_fixed(sun_times$nightEnd, " ", 2)[,2]
    
    #sleep_per[sleep_per$tag == tag_id & sleep_per$night_date == night, ]$dark_pot_sleep <- 
    #  sum(night_dat$pot_sleep[night_dat$local_time > dark_start | night_dat$local_time < dark_end])
    
    #sleep_per[sleep_per$tag == tag_id & sleep_per$night_date == night, ]$dark_ave_vedba <- 
    #  mean(night_dat$logvedba[night_dat$local_time > dark_start | night_dat$local_time < dark_end])
    
    SPT_dat <- night_dat[night_dat$sleep_per == 1, ]
    
    if (nrow(SPT_dat) > 0) {
      onset <- min(SPT_dat$local_timestamp)
      
      sleep_per[sleep_per$tag == tag_id & sleep_per$night_date == night, ]$onset <- 
        as.POSIXct(onset, origin = "1970-01-01")
      
      waking <- max(SPT_dat$local_timestamp) 
      
      sleep_per[sleep_per$tag == tag_id & sleep_per$night_date == night, ]$waking <- 
        as.POSIXct(waking, origin = "1970-01-01")
      
      SPT <- as.numeric(waking - onset, units = 'mins') + 1
      
      sleep_per[sleep_per$tag == tag_id & sleep_per$night_date == night, ]$SPT <- SPT
      
      WASO <- sum(SPT_dat$sleep_bouts == 0)
      
      sleep_per[sleep_per$tag == tag_id & sleep_per$night_date == night, ]$WASO <- WASO
      
      TST <- sum(SPT_dat$sleep_bouts == 1)
      
      sleep_per[sleep_per$tag == tag_id & sleep_per$night_date == night, ]$TST <- TST
      
      sleep_per[sleep_per$tag == tag_id & sleep_per$night_date == night, ]$sleep_eff <- 
        TST / nrow(SPT_dat)
      
      sleep_per[sleep_per$tag == tag_id & sleep_per$night_date == night, ]$summed_VeDBA <- 
        sum(SPT_dat$logvedba)
      
      sleep_per[sleep_per$tag == tag_id & sleep_per$night_date == night, ]$night_VeDBA_corr <- 
        sum(SPT_dat$logvedba) / SPT
      
      temp <- rle(SPT_dat$sleep_bouts)
      
      runs <- as.numeric(rep(temp$lengths >= frag_block, times = temp$lengths))
      
      frag_wake_bouts <- as.numeric(SPT_dat$sleep_bouts == 0 & runs == 1)
      
      diffs <- diff(c(1, frag_wake_bouts))
      
      sleep_per[sleep_per$tag == tag_id & sleep_per$night_date == night, ]$frag_wake_bouts <- 
        sum(diffs == 1)
      
      # Find the distinct sleep bouts (i.e. epochs of sleep separated by waking)
      diffs <- diff(c(0, SPT_dat$sleep_bouts))
      
      # Save the number of distinct wake bouts
      sleep_per[sleep_per$tag == tag_id & sleep_per$night_date == night, ]$wake_bouts <- 
        sum(diffs == -1)
      
      # Find durations of sleep and wake bouts
      temp <- rle(SPT_dat$sleep_bouts)
      
      # Add the duration of sleep bouts to the sleep bout duration vector
      sleep_durs <- c(sleep_durs, temp$lengths[temp$values == 1])
      # Add the duration of wake bouts to the wake bout duration vector
      wake_durs <- c(wake_durs, temp$lengths[temp$values == 0])
    }
  }
  
  # Append this file's results to the overall results
  all_sleep_per <- rbind(all_sleep_per, sleep_per)
  all_sleep_durs <- c(all_sleep_durs, sleep_durs)
  all_wake_durs <- c(all_wake_durs, wake_durs)
  }
}

# Save the combined results
write.csv(all_sleep_per, file = "plots/results/combined_sleep_analysis.csv", row.names = FALSE)

# Also save the duration data
sleep_duration_data <- data.frame(
  type = c(rep("sleep", length(all_sleep_durs)), rep("wake", length(all_wake_durs))),
  duration = c(all_sleep_durs, all_wake_durs)
)
write.csv(sleep_duration_data, file = "results/sleep_wake_durations.csv", row.names = FALSE)

# Print summary
cat("Analysis complete!\n")
cat("Processed", length(inactivity_files), "files\n")
cat("Total sleep records:", nrow(all_sleep_per), "\n")
cat("Results saved to 'results/combined_sleep_analysis.csv' and 'results/sleep_wake_durations.csv'\n")
