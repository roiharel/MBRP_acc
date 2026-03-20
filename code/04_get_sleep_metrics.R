# Required libraries
library(arrow)
library(dplyr)

#----PARAMS----
datadir <- '/mnt/EAS_shared/cross_sleep/working/Data/'  # base directory for VeDBA files

inactive_periods_path <- paste0(datadir, 'inactive_periods/inactive_periods_allspecies.RData') # adjust species if needed

# Load inactive periods data
inactive_periods <- load(inactive_periods_path) 

inactive_periods <- inactive_periods %>%
  # Remove rows with time_win == 3600
  filter(time_win > 3000) %>%
  # Modify filename for baboon rows
  mutate(filename = if_else(
    grepl("baboon", filename),
    sapply(strsplit(filename, "_"), function(x) paste(x[-3], collapse = "_")),
    filename
  ))

# Initialize new columns
inactive_periods$TST_hr <- NA
inactive_periods$sleep_efficiency <- NA
inactive_periods$n_awakenings_gt2min <- NA

# Loop through each row to compute metrics
for(i in 1:nrow(inactive_periods)) {
  row <- inactive_periods[i, ]
  
  # Load corresponding VeDBA data
  dat <- arrow::read_parquet(paste0(datadir, 'VeDBA/', row$filename))
  
  # Replace infinite values
  dat$logvedba[is.infinite(dat$logvedba)] <- row$logvedba_thresh - 1
  
  # Extract sleep period
  sleep_dat <- dat[row$start_row:row$end_row-1, ]
  
  # Estimate sampling interval
  dt <- median(diff(sleep_dat$timestamp))
  
  # Identify inactive rows
  inactive_rows <- which(sleep_dat$logvedba < row$logvedba_thresh)
  
  # Total Sleep Time (TST)
  TST_sec <- length(inactive_rows) * dt
  TST_hr <- TST_sec / 3600
  inactive_periods$TST_hr[i] <- TST_hr
  
  # Sleep Efficiency
  SPT_hr <- row$duration_hr
  inactive_periods$sleep_efficiency[i] <- TST_hr / SPT_hr
  
  # Awakenings > 2 minutes
  is_active <- sleep_dat$logvedba >= row$logvedba_thresh
  rle_active <- rle(is_active)
  durations <- rle_active$lengths[rle_active$values == TRUE]
  inactive_periods$n_awakenings_gt2min[i] <- sum(durations * dt > 120, na.rm = TRUE)
}

# Save updated data
savepath <- paste0(datadir, 'inactive_periods/inactive_periods_with_metrics.RData')
save(list = 'inactive_periods', file = savepath)
