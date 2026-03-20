library(move2)
library(dplyr)
library(tidyr)
library(lubridate)
library(arrow)
library(stringr)
library(progress)
library(progressr)
library(hms)

# Group Awake Events Detection - Proportional method with corrected time sorting.
# Group Awake Events Detection using a Proportional Threshold Method


study_id <- 3445611111 #1529016954 #  # Replace with your specific study ID
metadata <- movebank_download_deployment(study_id)

#awake_threshold = 0.9
#min_awake_individuals = 3


# # List all files in the directory
# inactivity_files <- list.files(
#   path = "data/inactivity/",
#   pattern = ".*",
#   full.names = TRUE,
#   recursive = TRUE
# )
# 
# # Read and merge all Parquet files, adding a 'tag' column from the filename
# inactivity <- do.call(rbind, lapply(inactivity_files, function(file) {
#   df <- read_parquet(file)
#   df$tag <- tools::file_path_sans_ext(basename(file)) 
#   return(df)
# }))

inactivity <- read_parquet("/mnt/EAS_shared/baboon/working/data/processed/2025/acc/inactivity/inactivity.parquet")
full_dat_meta <- inactivity %>%
  separate(tag, into = c("individual_local_identifier", "tag_local_identifier"), sep = "_(?=[^_]+$)") %>%
  left_join(metadata %>%
              select(individual_local_identifier, tag_local_identifier, group_id),
            by = "individual_local_identifier")


find_group_awake_events <- function(df, proportion_threshold = 0.6, min_duration_minutes = 30, nt_st = 21, nt_en = 4) {
  # Record start time for performance tracking
  analysis_start_time <- Sys.time()
  cat(sprintf("Analysis started at: %s\n", format(analysis_start_time)))
  
  # Ensure 'night_date' is a Date object
  if (!is.Date(df$night_date)) {
    cat("Info: 'night_date' column is not a Date object. Converting with lubridate::as_date().\n")
    df$night_date <- as_date(df$night_date)
  }
  
  # 1. Filter for night time
  df <- df %>%
    dplyr::mutate(
      hour = hour(as_hms(time))
    )
  df_night <- df %>% filter(hour >= nt_st | hour < nt_en)
  
  cat(sprintf("Found %d records during night time.\n", nrow(df_night)))
  
  if(nrow(df_night) == 0) {
    cat("No night data found.\n")
    return(data.frame())
  }
  
  # 2. Get unique group-night combinations and sort them
  group_nights <- df_night %>%
    select(night_date, group_id) %>%
    distinct() %>%
    arrange(group_id, night_date)
  
  cat(sprintf("Preparing to process %d unique group-night combinations.\n", nrow(group_nights)))
  
  # --- PROGRESS BAR SETUP ---
  # Initialize the progress bar
  #pb <- progress_bar$new(
  #  format = "Processing :group/:date [:bar] :percent | ETA: :eta",
  #  total = nrow(group_nights),
  #  clear = FALSE,
  #  width = 80
  #)
  
  all_events <- list()
  
  # 3. Process each group-night
  for(i in 1:nrow(group_nights)) {
    current_date <- group_nights$night_date[i]
    current_group <- group_nights$group_id[i]
    
    # --- PROGRESS BAR TICK ---
    # Update the progress bar with the current group and date
    #pb$tick(tokens = list(group = current_group, date = as.character(current_date)))
    
    next_day_date <- current_date + days(1)
    
    group_night_data <- df_night %>%
      filter(night_date == current_date, group_id == current_group)
    
    total_individuals <- n_distinct(group_night_data$individual_local_identifier)
    
    if (total_individuals == 0) {
      next
    }
    
    # 4. Calculate proportion of awake individuals
    awake_summary <- group_night_data %>%
      group_by(time, hour) %>%
      summarise(
        num_awake = sum(sleep_per == 0, na.rm = TRUE),
        .groups = 'drop'
      ) %>%
      mutate(
        proportion_awake = num_awake / total_individuals,
        is_group_awake = proportion_awake >= proportion_threshold
      )
    
    # 5. Sort times chronologically
    awake_summary <- awake_summary %>%
      mutate(
        sortable_datetime = if_else(
          hour < 5, 
          as_datetime(paste(next_day_date, time)),
          as_datetime(paste(current_date, time))
        )
      ) %>%
      arrange(sortable_datetime)
    
    if(nrow(awake_summary) < min_duration_minutes) {
      next
    }
    
    # 6. Find consecutive events
    rle_events <- rle(awake_summary$is_group_awake)
    awake_runs <- which(rle_events$values == TRUE & rle_events$lengths >= min_duration_minutes)
    
    if(length(awake_runs) > 0) {
      end_indices <- cumsum(rle_events$lengths)
      
      for(run_idx in awake_runs) {
        end_pos <- end_indices[run_idx]
        start_pos <- end_pos - rle_events$lengths[run_idx] + 1
        
        event_data <- awake_summary[start_pos:end_pos, ]
        event_individuals_data <- group_night_data %>%
          filter(time %in% event_data$time, sleep_per == 0)
        active_individuals_list <- unique(event_individuals_data$individual_local_identifier)
        
        all_events[[length(all_events) + 1]] <- data.frame(
          date = current_date,
          group_id = current_group,
          time_start = event_data$time[1],
          time_end = event_data$time[nrow(event_data)],
          duration_minutes = nrow(event_data),
          avg_num_awake = mean(event_data$num_awake),
          num_active_individuals = length(active_individuals_list),
          active_individuals = paste(sort(active_individuals_list), collapse = ", ")
        )
      }
    }
  }
  
  # 7. Combine results and report
  if (length(all_events) > 0) {
    result_df <- bind_rows(all_events)
    analysis_end_time <- Sys.time()
    analysis_duration <- difftime(analysis_end_time, analysis_start_time, units = "mins")
    
    cat(sprintf("\n\nAnalysis complete in %.1f minutes.\n", as.numeric(analysis_duration)))
    cat(sprintf("Found %d total group awake events.\n", nrow(result_df)))
    
    return(result_df)
  } else {
    cat("\n\nAnalysis complete. No group awake events were found.\n")
    return(data.frame())
  }
}
# --- How to Run the Function ---

# Step 1: Load your data
# df <- read.csv("your_data_file.csv")
#df <- full_dat_meta[full_dat_meta$group_id == "Chartreuse",]
df <- full_dat_meta

orders <- c("Ymd HMS", "Ymd")
datetime_object <- parse_date_time(df$timestamp, orders = orders, tz = "Africa/Nairobi")
df$time <- format(datetime_object, "%H:%M:%S")

# Step 3: Run the function
result <- find_group_awake_events(df, proportion_threshold = 0.9, 
                                  min_duration_minutes = 45, 
                                  nt_st = 21 , 
                                  nt_en = 4)

# Step 4: View and save results
print(head(result))
# write.csv(result, "group_awake_events_proportional.csv", row.names = FALSE)

chosen_res <- full_dat_meta %>%
  filter(
    night_date == result[49,]$date,
    group_id == result[49,]$group_id)

library(dplyr)

result <- result %>% mutate(date = as_date(date))

chosen_res <- full_dat_meta %>%
  semi_join(result[37, ], by = c("night_date" = "date", "group_id")) %>%
  mutate(activity = case_when(
    sleep_bouts == 1 ~ "Inactive",
    sleep_bouts == 0 ~ "Active"),
    timestamp = as.POSIXct(timestamp, format = "%Y-%m-%d %H:%M:%S"))


ggplot(chosen_res, aes(x = timestamp, y = individual_local_identifier, fill =  group_id)) +
  geom_tile(aes(alpha = activity)) +
  scale_alpha_manual(values = c("Inactive" = .2, "Active" = 0.5)) +
  scale_x_datetime(date_breaks = "4 hours", date_labels = "%H:%M") +
  theme_minimal() +
  theme(axis.text.y = element_blank(),
        axis.ticks.y = element_blank(),
        axis.text.x = element_text(angle = 45, hjust = 1),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank())
