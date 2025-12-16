# Load necessary packages
{
  library(parallel)
  library(doParallel)
  library(pbapply)  # For progress bar
  library(zoo)  # For rollmedian if not already loaded
  library(sjPlot)
  library(insight)
  library(httr)
  library(brms)
  library( zoo )
  library( hms )
  library( data.table )
  library( stringr )
  library( lubridate )
  library( lmerTest )
  library( plotrix )
  library( suncalc )
  library( LaplacesDemon )
  library( dplyr )
  library( purrr )
  library( HDInterval )
  library(multcomp)
  library( nlme )
  library(tidyr) 
  library(lmerTest)
  library( sp )
  library( stats )
  library( entropy )
  library( reshape2 )
  library( plyr )
  library(rstan)
  library( brms )
  library(fitdistrplus)
  library( gamm4 )
  library(glmmTMB)
  library( mgcv )
  library( rstudioapi )
  library(mixtools)
  library(arrow)
  
}

################# Functions #########################

## function for normalizing a vector
normalize_func <- function( x ) return( (x - mean( x, na.rm = T ) )/ sd( x, na.rm = T ) )

## function for plotting times from noon to noon. It will make correct 12:00 - 24:00 to be 00:00 - 12:00 and 00:00 - 12:00 to be 12:00 to 24:00
calculate_threshold <- function(log_vedba_vector, thres_value = 0.1) {
  # Remove NA values
  log_vedba_vector <- log_vedba_vector[!is.na(log_vedba_vector)]
  
  # Standardize the data
  #log_vedba_vector <- scale(log_vedba_vector)
  
  # Check if the data is valid
  if (length(log_vedba_vector) < 2) {
    stop("Insufficient data to calculate threshold.")
  }
  
  # Fit a Gaussian mixture model with 2 components
  fit <- tryCatch({
    normalmixEM(log_vedba_vector, k = 2, arbmean = TRUE, arbvar = TRUE)
  }, error = function(e) {
    stop("Error fitting model: ", e$message)
  })
  
  # Identify the leftmost distribution
  sorted_indices <- order(fit$mu)
  leftmost_index <- sorted_indices[1]
  
  # Calculate the posterior probabilities for the leftmost component
  posterior_probs_leftmost <- fit$posterior[, leftmost_index]
  
  # Calculate the leftmost point where the probability of being from the left distribution is under 25%
  threshold <- min(log_vedba_vector[
    log_vedba_vector > max(log_vedba_vector[posterior_probs_leftmost > thres_value]) &
      posterior_probs_leftmost < thres_value])
  
  return(threshold)
}

class_meth <- 'percentile_thresh' # I will generalize to allowing other classification methods. For now, it only works with percentile thresh
sep_thresh <- F  # this determines whether the log VeDBA threshold that makes the determination between inactive and active should be recalculated as the declared percentile of the raw log VeDBA, and not percentile of the rolling median of the log VeDBA, when determining whether each epoch is sleep or wake (the percentile of the rolling median of the log VeDBA is used in the determination of the sleep period). This parameter is only relevant if class_meth == 'percentile_thresh'

################# Determining sleep periods with modification of Van Hees et al. 2018 method ###################

################## Read in the d1 (accelerometer burst) data ###################

## d1 is a dataframe with a row for each minute for each baboon. Each row contains the raw (or interpolated) GPS acc burst, and several different measures calculated from bursts (like VeDBA)
#d1 <- readRDS("data/acc_vedba.RDS") 
d1 <- read_parquet("data/acc_vedba.parquet")

baboon_data<- readRDS("data/bdat.RDS")

## turn the data table into a dataframe
d1 <- as.data.frame( d1 )


d1$tag <- d1$individual_local_identifier

## turn timestamp - UTC - POSIX element and time into character
## some of the timestamps are not exactly on the minute because the burst occurred late. Round the timestamps to the nearest minute
d1$flr_local_timestamp <- floor_date( d1$local_timestamp, unit = 'min' )
#d1$timestamp <- as.POSIXct( d1$flr_local_timestamp, tz = 'UTC' )
d1$timestamp <- d1$flr_local_timestamp

## make a column for the time of the burst
d1$time <- str_split_fixed(d1$timestamp, " ", 2)[,2]

## make a column for local time
d1$local_time <- str_split_fixed(d1$local_timestamp, " ", 2)[,2]

## view the dataframe and it's summary
#head(d1)

dup_inds <- sort( c( which(  duplicated( d1[ , c( 'tag', 'timestamp' ) ] ) ) , which( duplicated( d1[ , c( 'tag', 'timestamp' ) ], fromLast = T ) ) ) )

d1[ dup_inds, ]

d1 <- d1[ !duplicated( d1[ , c( 'tag', 'local_timestamp' ) ] ), ]

## assign each minute of data to a given night. A night lasts from noon to noon. First, apply a time shift so that each night is a unit, and not each day
time_shift <- d1$local_timestamp - 12*60*60

## save the date of the first night of the study (the date of the night is always the date of the evening at the beginning of that night; so the first night of the study is 2012-07-31, although the data starts on 2012-08-01, because the data on that first morning is still technically part of the data for the previous night, as a night is noon to noon)
start_date <- as.Date(min(d1$local_timestamp)- 12*60*60)

## assign night as number of nights from the start of the study, with all data before the first noon representing night 1
d1$night <- as.numeric( as.Date(time_shift) - start_date + 1 )

d1$night_date <- as.Date( d1$local_timestamp - 12*60*60 )

## show how many baboon-nights there are
nrow( unique( d1[ , c( 'tag', 'night' ) ] ) )

## check where the night changes from one night to the next to see if it is at noon
d1[(diff(c( d1$night_date )) == 1),]

## save a variable denoting the total number of minutes in the day
mins_in_day <- 60*24 # there are 14 hours between 17:00:00 and 07:00:00 

missing_mins <- 45 ## this is the maximum total number of minutes of data that can be missing from a day and still have that day included in the analysis (for sleep period time and sleep based analyses; i.e. not ave_vedba)

time_gap <- 20*60 ## this is the maximum allowable time gap between two accelerometer bursts (in seconds) that can exist in a noon-to-noon period without removing this noon-to-noon period from the data

mov_window <- 9 ## this is the size of the moving window (in minutes) used in calculating the rolling median of the average VeDBA

block_size <- 30 ## duration in minutes of the blocks of continuous inactivity that will be considered sleep

gap_size <- 45 ## maximum duration between sleep blocks that will be merged

percentile_for_no_mult <- 0.90 # this is the percentile threshold of the log VeDBA within the 18:00 to 06:30 period used to classify activity vs. inactivity (without multiplying by a multiplier)

waso_block <- 3 ## this is the number of consecutive minutes of inactivity needed to classify a period as sleep. A waso_block of 1 means that anytime the value is below the threshold, the baboon in considered sleeping and anytime the value is above the threshold the baboon is considered awake

frag_block <- 2 ## this is the number of minutes of waking that need to be consecutive to be considered a wake bout during the night (other epochs of wake that do not meet this criterion will still be considered wake for WASO and wake_bouts, but not frag_wake_bouts)

# Define the coordinates for Mpala Research Centre
mpala_lat <- 0.2827
mpala_lon <- 36.8986

#dark_start <- '19:55:00' # the time at which evening astronomical twilight ends (or whatever time you want to use to consider the start of 'night')
#dark_end <- '05:23:00' # the time at which morning astronomical twilight starts (or whatever time you want to use to consider the end of 'night')

## shows the time (as well as one previous time and one later time) where a minute is skipped. This shows that throughout the data, a burst at every minute is represented
#sort( unique( d1$time ) ) [ which( diff( as_hms( sort( unique( d1$time ) ) ) ) != as_hms( '00:01:00' ) ) + -1:1 ] 

## again confirms that every minute is represented in the data except for one (can tell this by comparing this number to the minutes_in_day variable above)
length( unique(d1$time) )

## create a vector containing the names of each baboon
tag_names <- unique( d1$tag )

# Detect number of cores
n_cores <- 20 # detectCores() - 1  # leave one core free
cl <- makeCluster(n_cores)
registerDoParallel(cl)

# Make full_dat as before
full_dat <- d1[ d1$local_time > "12:00:00" | d1$local_time < "12:00:00", ]
full_dat$sleep_per <- NA
full_dat$pot_sleep <- NA
full_dat$sleep_bouts <- NA
full_dat$n_bursts <- NA
full_dat$max_time_diff <- NA

# Get tag names
tag_names <- unique(full_dat$tag)

# Export necessary variables/functions to each cluster node
clusterExport(cl, varlist = c("calculate_threshold", "mov_window", "block_size", "gap_size", "waso_block", "percentile_for_no_mult", "sep_thresh"))

# Begin parallel processing over tag names with progress bar
results <- foreach(tag = tag_names, .combine = rbind, .packages = c("zoo", "pbapply","mixtools")) %dopar% {
  
  id_dat <- full_dat[full_dat$tag == tag, ]
  nights <- unique(id_dat$night_date)
  full_roll <- c()
  
  for (night in nights) {
    night_dat <- id_dat[id_dat$night_date == night, ]
    roll_log_vedba <- rollmedian(night_dat$log_vedba, mov_window, fill = NA, align = 'center')
    full_roll <- c(full_roll, roll_log_vedba)
  }
  
  thresh <- calculate_threshold(full_roll, 0.7)
  if (sep_thresh) {
    unsmooth_thresh <- quantile(id_dat$log_vedba, percentile_for_no_mult, na.rm = TRUE)
  }
  
  # Store processed rows for this tag
  processed_rows <- list()
  
  for (night in nights) {
    print(tag, night)
    night_dat <- id_dat[id_dat$night_date == night, ]
    night_dat$sleep_per <- NA
    night_dat$pot_sleep <- NA
    night_dat$sleep_bouts <- NA
    night_dat$n_bursts <- nrow(night_dat)
    
    sorted_times <- c(as.POSIXct(paste(as.Date(night, origin = "1970-01-01", tz = 'UTC'), '12:00:00'), tz = 'UTC'),
                      sort(night_dat$local_timestamp),
                      as.POSIXct(paste(as.Date((night + 1), origin = "1970-01-01", tz = 'UTC'), '12:30:00'), tz = 'UTC'))
    
    time_diffs <- as.numeric(diff(sorted_times, units = 'secs'))
    night_dat$max_time_diff <- ifelse(length(time_diffs) > 0, max(time_diffs), NA)
    
    roll_log_vedba <- rollmedian(night_dat$log_vedba, mov_window, fill = NA, align = 'center')
    temp <- rle(as.numeric(roll_log_vedba < thresh))
    sleep_per_runs <- as.numeric(rep(temp$lengths > block_size, times = temp$lengths))
    sleep_per_sleep_bouts <- as.numeric(roll_log_vedba < thresh & sleep_per_runs == 1)
    
    diffs <- diff(c(0, sleep_per_sleep_bouts))
    starts <- which(diffs == 1)[-1]
    ends <- which(diffs == -1)
    
    if (length(starts) > 0) {
      gaps <- as.numeric(night_dat$local_timestamp[starts] - night_dat$local_timestamp[ends[1:length(starts)]], units = 'mins')
      inds_to_remove <- which(gaps < gap_size)
      
      onset <- if (length(inds_to_remove) == 0) which(diffs == 1) else which(diffs == 1)[-(inds_to_remove + 1)]
      wake <- if (length(inds_to_remove) == 0) ends else ends[-inds_to_remove]
      
      if (length(onset) > 0 && length(wake) > 0) {
        per_ind <- which.max(as.numeric(night_dat$local_timestamp[wake] - night_dat$local_timestamp[onset], units = 'secs'))
        night_dat$sleep_per <- as.numeric(night_dat$local_timestamp >= night_dat$local_timestamp[onset[per_ind]] &
                                            night_dat$local_timestamp <= night_dat$local_timestamp[wake[per_ind]])
      } else {
        night_dat$sleep_per <- 0
      }
    } else {
      night_dat$sleep_per <- 0
    }
    
    if (sep_thresh) {
      night_dat$pot_sleep <- as.numeric(night_dat$log_vedba < unsmooth_thresh)
      temp <- rle(as.numeric(night_dat$log_vedba < unsmooth_thresh))
      runs <- as.numeric(rep(temp$lengths >= waso_block, times = temp$lengths))
      sleep_bouts <- as.numeric(night_dat$log_vedba < unsmooth_thresh & runs == 1)
    } else {
      night_dat$pot_sleep <- as.numeric(night_dat$log_vedba < thresh)
      temp <- rle(as.numeric(night_dat$log_vedba < thresh))
      runs <- as.numeric(rep(temp$lengths >= waso_block, times = temp$lengths))
      sleep_bouts <- as.numeric(night_dat$log_vedba < thresh & runs == 1)
    }
    
    night_dat$sleep_bouts <- sleep_bouts
    
    # Collect processed night data
    processed_rows[[length(processed_rows) + 1]] <- night_dat
  }
  
  # Combine all nights into one data frame for this tag
  do.call(rbind, processed_rows)
}

# Stop the cluster
stopCluster(cl)

# Replace full_dat with the processed version
full_dat <- results



pre_clean_full <- full_dat

study_nights <- min( d1$night ):max( d1$night )

study_night_dates <- as.Date( min( d1$night_date ):max( d1$night_date ), origin = '1970-01-01' )


sleep_per <- data.frame( tag = rep( unique( d1$tag ), each = length( study_nights ) ), night = rep( study_nights, times = length( tag ) ), night_date = rep( study_night_dates, times = length( tag ) ), total_pot_sleep = NA, total_sleep_bouts = NA, onset = NA, waking = NA, SPT = NA, WASO = NA, TST = NA, sleep_eff = NA, wake_bouts = NA, frag_wake_bouts = NA, summed_VeDBA = NA, night_VeDBA_corr = NA, ave_vedba = NA, dark_pot_sleep = NA, dark_ave_vedba = NA, max_time_diff = NA, n_bursts= NA )


## create empty vectors for the durations of sleep and wake bouts. We will fill these in to see if the distributions of the durations of these bouts later
sleep_durs <- c()
wake_durs <- c() 


## for each individual...
for( tag in tag_names ){
  
  ## subset the data to just this individual's data
  id_dat <- full_dat[ full_dat$tag == tag, ]
  
  ## create a vector the nights for which this individual has data
  nights <- unique( id_dat$night_date )
  
  ## for each night on which this individual has data
  for( n in 1:length(nights) ){
    
    night <- nights[n]
    
    ## subset this individual's data to just that night
    night_dat <- id_dat[ id_dat$night_date == night, ]
    
    ## should already be in order, but just in case
    night_dat <- night_dat[ order( night_dat$local_timestamp ), ]
    
    sleep_per[ sleep_per$tag == tag & sleep_per$night_date == night, ]$n_bursts <- unique( night_dat$n_bursts )
    
    sleep_per[ sleep_per$tag == tag & sleep_per$night_date == night, ]$max_time_diff <- unique( night_dat$max_time_diff )
    
    sleep_per[ sleep_per$tag == tag & sleep_per$night_date == night, ]$total_pot_sleep <- sum( night_dat$pot_sleep )
    
    sleep_per[ sleep_per$tag == tag & sleep_per$night_date == night, ]$total_sleep_bouts <- sum( night_dat$sleep_bouts )
    
    sleep_per[ sleep_per$tag == tag & sleep_per$night_date == night, ]$ave_vedba <- mean( night_dat$log_vedba )
    
    # Get the sun times for the specified location and date
    sun_times <- getSunlightTimes(date = night, lat = mpala_lat, lon = mpala_lon, keep = c("nightEnd", "night"))
    # Extract dark_start and dark_end
    dark_start <- str_split_fixed(sun_times$night, " ", 2)[,2] 
    dark_end <- str_split_fixed(sun_times$nightEnd, " ", 2)[,2]
    
    sleep_per[ sleep_per$tag == tag & sleep_per$night_date == night, ]$dark_pot_sleep <- sum( night_dat$pot_sleep[ night_dat$local_time > dark_start | night_dat$local_time < dark_end ] )
    sleep_per[ sleep_per$tag == tag & sleep_per$night_date == night, ]$dark_ave_vedba <- mean( night_dat$log_vedba[ night_dat$local_time > dark_start | night_dat$local_time < dark_end ] )
    
    SPT_dat <- night_dat[ night_dat$sleep_per == 1, ]
    
    if( nrow( SPT_dat ) > 0 ){
      
      onset <- min( SPT_dat$local_timestamp )
      
      sleep_per[ sleep_per$tag == tag & sleep_per$night_date == night, ]$onset <- onset
      
      waking <- max( SPT_dat$local_timestamp ) 
      
      sleep_per[ sleep_per$tag == tag & sleep_per$night_date == night, ]$waking <- waking
      
      SPT <- as.numeric( waking - onset, units = 'mins' ) + 1
      
      sleep_per[ sleep_per$tag == tag & sleep_per$night_date == night, ]$SPT <- SPT
      
      WASO <- sum( SPT_dat$sleep_bouts == 0 )
      
      sleep_per[ sleep_per$tag == tag & sleep_per$night_date == night, ]$WASO <- WASO
      
      TST <- sum( SPT_dat$sleep_bouts == 1 )
      
      sleep_per[ sleep_per$tag == tag & sleep_per$night_date == night, ]$TST <- TST
      
      sleep_per[ sleep_per$tag == tag & sleep_per$night_date == night, ]$sleep_eff <- TST/ nrow( SPT_dat )
      
      sleep_per[ sleep_per$tag == tag & sleep_per$night_date == night, ]$summed_VeDBA <- sum( SPT_dat$log_vedba )
      
      sleep_per[ sleep_per$tag == tag & sleep_per$night_date == night, ]$night_VeDBA_corr <- sum( SPT_dat$log_vedba ) / SPT
      
      temp <- rle( SPT_dat$sleep_bouts )
      
      runs <- as.numeric( rep( temp$lengths >= frag_block, times = temp$lengths ) )
      
      frag_wake_bouts <- as.numeric( SPT_dat$sleep_bouts == 0 & runs == 1 )
      
      diffs <- diff( c( 1, frag_wake_bouts ) )
      
      sleep_per[ sleep_per$tag == tag & sleep_per$night_date == night, ]$frag_wake_bouts <- sum( diffs == 1 )
      
      ## find the distinct sleep bouts (i.e. epochs of sleep separated by waking)
      diffs <- diff( c( 0, SPT_dat$sleep_bouts ) )
      
      ## save the number of distinct wake bouts
      sleep_per[ sleep_per$tag == tag & sleep_per$night_date == night, ]$wake_bouts <- sum( diffs == -1 )
      
      ## find durations of sleep and wake bouts
      temp <- rle( SPT_dat$sleep_bouts )
      
      ## add the duration of sleep bouts to the sleep bout duration vector
      sleep_durs <- c( sleep_durs, temp$lengths[ temp$values == 1 ] )
      ## add the duration of wake bouts to the wake bout duration vector
      wake_durs <- c( wake_durs, temp$lengths[ temp$values == 0 ] )
      
      
    }
  }
}


sum( !is.na( sleep_per$SPT ) )

### check number of nights for which sleep period was calculated and inspect those for which no sleep period was calculated ###

sleep_per_nona <- sleep_per[ !is.na( sleep_per$SPT ), ]

nrow( sleep_per_nona )

left_out <- unique( d1[ , c( 'tag', 'night' ) ] )[ !paste( unique( d1[ , c( 'tag', 'night' ) ] )$tag, unique( d1[ , c( 'tag', 'night' ) ] )$night ) %in% paste( sleep_per_nona$tag, sleep_per_nona$night ), ]


for( i in 1:nrow( left_out ) ){
  tag_night_dat <- d1[ d1$tag == left_out$tag[ i ] & d1$night == left_out$night[ i ], ]
  plot( tag_night_dat$local_timestamp, tag_night_dat$log_vedba )
} 

nrow( left_out )

tag_night_dat <- d1[ d1$tag == d1$tag[1] & d1$night == 3, ]

plot( tag_night_dat$local_timestamp, tag_night_dat$log_vedba )


############# Cleaning the dataframes of data on nights with insufficient data ################


# how many baboon-nights did we have ACC data for

nrow( unique( d1[ , c( 'tag','night' ) ] ) ) # 649

nrow( unique( full_dat[ , c( 'tag','night' ) ] ) ) # 646

sum( !is.na( sleep_per$dark_ave_vedba ) ) # 646



## remove all these variable from the night, and from the days on the early side of the noon-to-noon period if the noon-to-noon period is missing a lot of data (because then we might not be able to reliably calculate the sleep VeDBA threshold, and a lot of epochs might be missing, which would skew TST and such)
nrow( unique( full_dat[ full_dat$n_bursts < ( mins_in_day - missing_mins ), c( 'tag', 'night') ] ) ) # 74 baboon-nights removed from this cleaning step
sum( sleep_per$n_bursts < ( mins_in_day - missing_mins ) & !is.na( sleep_per$n_bursts ) ) # confirmed 74 removed from this cleaning step


sleep_per[ sleep_per$n_bursts < ( mins_in_day - missing_mins ) & !is.na( sleep_per$n_bursts ), c( 'onset', 'waking', 'SPT', 'sleep_eff', 'TST', 'WASO', 'wake_bouts', 'summed_VeDBA', 'night_VeDBA_corr', 'ave_vedba', 'total_pot_sleep', 'total_sleep_bouts', 'dark_ave_vedba', 'dark_pot_sleep' ) ] <- NA

sleep_per_nona <- sleep_per[ !is.na( sleep_per$SPT ), ]

nrow( sleep_per_nona )

#### this next cleaning step below can be deleted because it doesn't actually remove anything 

## remove all these variable from the night, and from the days on the early side of the noon-to-noon period (only for those depending on SPT) if the noon-to-noon period has large gaps of missing data (because then we can't reliably calculate the SPT)
nrow( unique( full_dat[ full_dat$max_time_diff > time_gap, c( 'tag', 'night') ] ) ) # 0 baboon-nights removed from this cleaning step
sum( sleep_per$max_time_diff > time_gap & !is.na( sleep_per$max_time_diff ) ) # 0 baboon-nights removed from this cleaning step


sleep_per[ sleep_per$max_time_diff > time_gap & !is.na( sleep_per$max_time_diff ), c( 'onset', 'waking', 'SPT', 'sleep_eff', 'TST', 'WASO', 'wake_bouts', 'summed_VeDBA', 'ave_vedba', 'total_pot_sleep', 'total_sleep_bouts', 'dark_ave_vedba', 'dark_pot_sleep' ) ] <- NA

sleep_per_nona <- sleep_per[ !is.na( sleep_per$SPT ), ]

nrow( sleep_per_nona )

## remove data for sleep period and sleep bouts on days when there is a lot of missing data, because we cannot reliably calculate the sleep VeDBA threshold and there may be a lot of missing epochs
full_dat[ full_dat$n_bursts < ( mins_in_day - missing_mins ), c( 'sleep_per' ) ] <- NA

## remove data for sleep period on days when there are large gaps of missing data, because we can't reliably calculate the SPT with gaps in the data
full_dat[ full_dat$max_time_diff > time_gap, 'sleep_per'  ] <- NA



## how many baboon-nights do we have ACC data for after cleaning

sum( !is.na( sleep_per$total_sleep_bouts ) ) # 572 baboon-nights




## reformat sleep timestamp
sleep_per$onset <- as.POSIXct( sleep_per$onset, origin = "1970-01-01 00:00:00", tz = "UTC" )

## reformat waking timestamp
sleep_per$waking <- as.POSIXct( sleep_per$waking, origin = "1970-01-01 00:00:00", tz = "UTC" )

## make columns for just the time part of the sleep onset and waking timestamps
sleep_per$onset_time <- as_hms( sleep_per$onset )
sleep_per$waking_time <- as_hms( sleep_per$waking )

write.csv( full_dat, paste0( 'data/full_dat_', class_meth, '_sep_thresh_', sep_thresh, '.csv' ), row.names = F )
write.csv( sleep_per, paste0( 'data/sleep_per_', class_meth, '_sep_thresh_', sep_thresh, '.csv' ), row.names = F )

