# Current Date and Time (UTC): 2025-09-01 15:48:19
# User: roiharel

library(tictoc) # Make sure tictoc library is loaded
library(arrow) 
library(stringr)
library(dplyr)
library(zoo)
library(lubridate)
library(mixtools)
library(suncalc)
################# Determining sleep periods with modification of Van Hees et al. 2018 method ###################
################# Functions #########################

## function for normalizing a vector
normalize_func <- function( x ) return( (x - mean( x, na.rm = T ) )/ sd( x, na.rm = T ) )

calculate_threshold <- function(logvedba_vector, thres_value = 0.1) {
  # Remove NA values
  logvedba_vector <- logvedba_vector[!is.na(logvedba_vector)]
  
  # Check if the data is valid
  if (length(logvedba_vector) < 2) {
    stop("Insufficient data to calculate threshold.")
  }
  
  # Fit a Gaussian mixture model with 2 components
  fit <- tryCatch({
    normalmixEM(logvedba_vector, k = 2, arbmean = TRUE, arbvar = TRUE)
  }, error = function(e) {
    stop("Error fitting model: ", e$message)
  })
  
  # Identify the leftmost distribution
  sorted_indices <- order(fit$mu)
  leftmost_index <- sorted_indices[1]
  
  # Calculate the posterior probabilities for the leftmost component
  posterior_probs_leftmost <- fit$posterior[, leftmost_index]
  
  # Calculate the leftmost point where the probability of being from the left distribution is under specified value
  threshold <- min(logvedba_vector[
    logvedba_vector > max(logvedba_vector[posterior_probs_leftmost > thres_value]) &
      posterior_probs_leftmost < thres_value])
  
  return(threshold)
}

################## Read in the VeDBA data files from directory ###################

# Start timing the entire process
tic("Total Processing Time")

# Get list of all VeDBA files in the directory
input_dir <- "/mnt/EAS_shared/baboon/working/data/processed/2025/acc/vedba"
output_dir <- "/mnt/EAS_shared/baboon/working/data/processed/2025/acc/inactivity"

# Get list of all VeDBA files in the directory
vedba_files <- list.files(input_dir, pattern=".parquet", full.names=TRUE)
print(paste("Found", length(vedba_files), "VeDBA files to process"))

# Create an empty dataframe to store all the processed data
inactivity <- data.frame()
thrs <- data.frame(animal = character(), thresh = numeric())

# Process each file individually
for(vedba_file in vedba_files) {
  # Start timing this file's processing
  tic(paste("Processing file:", basename(vedba_file)))
    
  ####  splitname <- strsplit(basename(filename),split='_')[[1]]
  animal <- sub(".parquet", "", basename(vedba_file))
  # Read the current VeDBA file
  print(paste("Processing:", vedba_file))
  d1 <- read_parquet(vedba_file)
  
  ## turn the data table into a dataframe
  d1 <- as.data.frame(d1)
  
  d1$local_timestamp <- d1$timestamp
  ## make a column for the time of the burst
  d1$time <- str_split_fixed(d1$timestamp, " ", 2)[,2]
  
  ## make a column for local time
  d1$local_time <- str_split_fixed(d1$local_timestamp, " ", 2)[,2]
  
  ## Remove duplicates
  d1 <- d1[!duplicated(d1[, c('local_timestamp')]), ]
  
  ## assign each minute of data to a given night. A night lasts from noon to noon. First, apply a time shift so that each night is a unit, and not each day
  time_shift <- d1$local_timestamp - 12*60*60
  
  ## save the date of the first night of the study (the date of the night is always the date of the evening at the beginning of that night)
  start_date <- as.Date(min(d1$local_timestamp)- 12*60*60)
  
  ## assign night as number of nights from the start of the study, with all data before the first noon representing night 1
  d1$night <- as.numeric(as.Date(time_shift) - start_date + 1)
  
  d1$night_date <- as.Date(d1$local_timestamp - 12*60*60)
  
  ## save a variable denoting the total number of minutes in the day
  mins_in_day <- 60*24 # there are 14 hours between 17:00:00 and 07:00:00 
  
  mins_thresh_to_include <- 16*60 ## this is the maximum total number of minutes of data that can be missing from a day and still have that day included in the analysis
  
  time_gap <- 30*60 ## this is the maximum allowable time gap between two accelerometer bursts (in seconds)
  
  mov_window <- 9 ## this is the size of the moving window (in minutes) used in calculating the rolling median of the average VeDBA
  
  block_size <- 20 ## duration in minutes of the blocks of continuous inactivity that will be considered sleep
  
  gap_size <- 45 ## maximum duration between sleep blocks that will be merged
  
  percentile_for_no_mult <- 0.2 # this is the percentile threshold of the log VeDBA within the 18:00 to 06:30 period used to classify activity vs. inactivity
  
  waso_block <- 3 ## this is the number of consecutive minutes of inactivity needed to classify a period as sleep
  
  frag_block <- 2 ## this is the number of minutes of waking that need to be consecutive to be considered a wake bout during the night
  
  # Define the coordinates for Mpala Research Centre
  mpala_lat <- 0.2827
  mpala_lon <- 36.8986
  
  ## make a copy of d1. We will fill in this new dataframe with information about if the baboon was asleep in each epoch
  file_dat <- d1[d1$local_time > "12:00:00" | d1$local_time < "12:00:00", ]
  
  file_dat$sleep_per <- NA ## binary indicating whether a row belongs to the sleep period window
  file_dat$pot_sleep <- NA ## binary indicating whether the row is below the VeDBA threshold, making it a potential sleep bout
  file_dat$sleep_bouts <- NA ## binary indicating whether the row is considered sleep, based on the waso or nap requirements
  file_dat$n_bursts <- NA ## the number of bursts collected in a given noon-to-noon period
  file_dat$max_time_diff <- NA ## the maximum difference between consecutive fixes in a given noon-to-noon period
  
  # Extract the single tag from this file (no need for loop as each file has only one tag)
  #tag <- unique(file_dat$tag)[1]
  print(paste("Processing tag:", animal))
  
  # Get all nights for this tag
  nights <- unique(file_dat$night_date)
  
  # Create an empty vector to fill with the rolling log VeDBAs from each night
  full_roll <- c()
  
  # For each night on which this individual has data
  for(night in nights) {
    # Subset the individual's data to this night
    night_dat <- file_dat[file_dat$night_date == night, ]
    
    # Take the rolling median of the log VeDBA
    roll_logvedba <- rollmedian(night_dat$logvedba, mov_window, fill = NA, align = 'center')
    #roll_logvedba <- night_dat$logvedba
    # Add the rolling medians to the vector of the individuals rolling medians for the whole study period
    full_roll <- c(full_roll, roll_logvedba)
  }
  
  ## determine the threshold activity vs. inactivity threshold
  thresh <- tryCatch({
    calculate_threshold(full_roll, 0.05)
  }, error = function(e) {
    # Fallback to percentile if threshold calculation fails
    quantile(full_roll, percentile_for_no_mult, na.rm = TRUE)
  })
  
  thrs <- rbind(thrs, data.frame(animal = animal, thresh = thresh, stringsAsFactors = FALSE))
  
  # p <- ggplot(data.frame(full_roll), aes(x = full_roll)) +
  #   geom_density(fill = "steelblue", alpha = 0.6) +
  #   geom_vline(xintercept = thresh, color = "red", linetype = "dashed", size = 1) +
  #   annotate("text", x = thresh, y = Inf, label = "3", vjust = 1.5, color = "red", size = 4) +
  #   theme_minimal()
  # 
  # filename <- paste0(animal, "_", thresh, ".jpg")
  # ggsave(filename, plot = p, width = 6, height = 4, dpi = 300)
  
  
  # Process each night for this animal
  for(night in nights) {
    print(paste(animal, format(as.Date(night), "%Y-%m-%d")))
    
    ## subset this individual's data to just that night
    night_dat <- file_dat[file_dat$night_date == night, ]
    
    ## create empty columns for the sleep period, potential sleep bouts, and sleep bout binary variables
    night_dat$sleep_per <- NA
    night_dat$pot_sleep <- NA
    night_dat$sleep_bouts <- NA
    
    ## save a column of the total number of bursts for that day
    n_bursts <- nrow(night_dat)
    
    ## sort the timestamps, and book end them with the beginning and end of the night
    sorted_times <- c(
      as.POSIXct(paste(as.Date(night, origin = "1970-01-01", tz = 'UTC'), '18:00:00'), tz = 'UTC'), 
      sort(night_dat$local_timestamp), 
      as.POSIXct(paste(as.Date((night + 1), origin = "1970-01-01", tz = 'UTC'), '06:30:00'), tz = 'UTC')
    )
    
    ## find the time difference in seconds between each burst
    time_diffs <- as.numeric(diff(sorted_times, units = 'secs'))
    
    ### find blocks of continuous inactivity
    ## take the rolling median of the log VeDBA and save it as a column
    roll_logvedba <- rollmedian(night_dat$logvedba, mov_window, fill = NA, align = 'center')
    
    ## find the run length encoding of periods above and below the threshold
    temp <- rle(as.numeric(roll_logvedba < thresh))
    
    ## mark the rows that are part of runs
    sleep_per_runs <- as.numeric(rep(temp$lengths > block_size, times = temp$lengths))
    
    ## mark the rows corresponding to sleep bouts
    sleep_per_sleep_bouts <- as.numeric(roll_logvedba < thresh & sleep_per_runs == 1)
    
    
    goldenHour <- getSunlightTimes( date = as.Date(min(night_dat$timestamp)), 
                                    lon = mpala_lon, lat = mpala_lat )[, c( 'goldenHour')]
    goldenHourEnd <- getSunlightTimes( date = as.Date(min(night_dat$timestamp + hours(24))), 
                                       lon = mpala_lon, lat = mpala_lat )[, c( 'goldenHourEnd')]
    
    
    sleep_per_sleep_bouts_trimmed <- sleep_per_sleep_bouts 
    sleep_per_sleep_bouts_trimmed[night_dat$timestamp < goldenHour | night_dat$timestamp > goldenHourEnd] <- 0 
    ## find when sleep bouts start and end
    diffs <- diff(c(0, sleep_per_sleep_bouts_trimmed))
    starts <- which(diffs == 1)[-1]
    ends <- which(diffs == -1)
    
    
    ## if there are any sleep bouts...
    if(length(which(diffs == 1)) != 0 && n_bursts > mins_thresh_to_include) {
      ## FIRST AND LAST SLEEP SEGMENTS TO DETERMINE SPT
      onset <- which(diffs == 1)[1] ####################### WAS CHANGED ##################### START OF FIRST SEGMENT
      wake <- tail(ends,1) ######################## WAS ends
      
      #   
      #   ## find the duration of the gaps between each sleep bout
      #   gaps <- as.numeric(night_dat$local_timestamp[starts] - night_dat$local_timestamp[ends[1:length(starts)]], units = 'mins')
      #   
      #   ## sleep bouts separated by gaps that are shorter than that specified by gap_size will be merged
      #   inds_to_remove <- which(gaps < gap_size)
      #   
      #   ## if there are NO gaps between sleep bouts that are to be removed...
      #   if(length(inds_to_remove) == 0) {
      #     ## set sleep onset index to be the start of sleep bouts
      #     onset <- which(diffs == 1)
      #     
      #     ## set waking index to be the end of sleep bouts
      #     wake <- ends
      #   } else { ## if there ARE gaps between sleep bouts that are to be removed...
      #     ## set sleep onset index to be the start of sleep bouts that do not correspond to the gaps to be removed
      #     onset <- which(diffs == 1)[-(inds_to_remove + 1)]
      #     
      #     ## set waking index to be the end of sleep bouts that do not correspond to the gaps to be removed
      #     wake <- ends[-inds_to_remove]
      #   }
      
      ## determine which sleep period is the longest
      per_ind <- which.max(as.numeric(night_dat$local_timestamp[wake] - night_dat$local_timestamp[onset], units = 'secs'))
      
      ## fill in the sleep period data frame with the sleep onset and waking time associated with the longest sleep period in the day
      night_dat$sleep_per <- as.numeric(
        night_dat$local_timestamp >= night_dat$local_timestamp[onset[per_ind]] & 
          night_dat$local_timestamp <= night_dat$local_timestamp[wake[per_ind]]
      )
      
    } else { ## if there aren't any sleep bouts, record all rows as a 0 in the sleep_per column
      night_dat$sleep_per <- 0
    }
    
    night_dat$pot_sleep <- as.numeric(night_dat$logvedba < thresh)
    
    ## find the run length encoding of periods above and below the threshold
    temp <- rle(as.numeric(night_dat$logvedba < thresh))
    
    ## mark the rows that are part of runs
    runs <- as.numeric(rep(temp$lengths >= waso_block, times = temp$lengths))
    
    ## mark the rows corresponding to sleep bouts
    night_dat$sleep_bouts <- as.numeric(night_dat$logvedba < thresh & runs == 1)
    
    ### put the night data back into file_dat
    file_dat[file_dat$night_date == night, ] <- night_dat
    
  }
  
  # Write individual file results to parquet as we go (this is optional but helps with resumability)
  tag_filename <- gsub("[^a-zA-Z0-9]", "_", animal)
  write_parquet(file_dat, file.path(output_dir, paste0(tag_filename, ".parquet")))
  
  # Append the processed file data to our growing dataset
  if(nrow(inactivity) == 0) {
    file_dat$tag <- animal
    inactivity <- file_dat
  } else {
    file_dat$tag <- animal
    inactivity <- rbind(inactivity, file_dat)
  }
  
  toc() # End timing for this file
}

# Save the resulting inactivity dataset
write_parquet(inactivity, "inactivity.parquet")
write.csv(thrs, "thresholds.csv")

# End timing the entire process
toc(log = TRUE)

# Print the timing log
tic.log(format = TRUE)
