# Load required libraries
library(move2)
library(dplyr)
library(lubridate)
library( stringr )
library( data.table )
library(foreach)
library(doParallel)
library(progress)
library(magrittr)
library(keyring)

acc_data_trim <- readRDS("data/acc_v1.RDS")

# Define the dy_acc function
dy_acc <- function(vect, win_size = 7){
  pad_size <- win_size/2 - 0.5
  padded <- unlist(c(rep(NA, pad_size), vect, rep(NA, pad_size)))
  acc_vec <- rep(NA, length = length(vect))
  
  for(i in 1:length(vect)){
    win <- padded[i:(i+(2*pad_size))]
    m_ave <- mean(win, na.rm = TRUE)
    acc_comp <- vect[i] - m_ave
    acc_vec[i] <- acc_comp 
  }
  
  return(unlist(acc_vec))
}

# Set up parallel backend
cl <- makeCluster(detectCores() - 1)
registerDoParallel(cl)
# 
res <- foreach(i = 1:nrow(acc_data_trim), .combine = rbind) %dopar% {
  
  # Calculate the components
  x_component <- abs(dy_acc(acc_data_trim$x_cal[i, ]))
  y_component <- abs(dy_acc(acc_data_trim$y_cal[i, ]))
  z_component <- abs(dy_acc(acc_data_trim$z_cal[i, ]))
  
  
  # Calculate the vectorial sum
  vectorial_sum <- sqrt(x_component^2 + y_component^2 + z_component^2)
  ave_vedba_value <- mean(vectorial_sum, na.rm = TRUE)
  
  # Calculate the pitch angle in radians
  pitch <- atan2(x_component, sqrt(y_component^2 + z_component^2))
  ave_pitch <- mean(pitch, na.rm = TRUE)
  
  # Return a list with both ave_vedba and pitch_angle if needed
  c(ave_vedba = ave_vedba_value, pitch_angle = ave_pitch)
}

# Convert the results to a data frame for easier manipulation
res_df <- as.data.frame(res)
acc_data_trim$ave_vedba <- as.numeric(res_df$ave_vedba)
acc_data_trim$ave_pitch <- as.numeric(res_df$pitch_angle)

# Calculate the vectorial sum and mean for each row in parallel
# acc_data_trim$ave_vedba <- foreach(i = 1:nrow(acc_data_trim$x_cal), .combine = c) %dopar% {
# 
# #  pb$tick()
# 
#   x_component <- abs(dy_acc(acc_data_trim$x_cal[i, ]))
#   y_component <- abs(dy_acc(acc_data_trim$y_cal[i, ]))
#   z_component <- abs(dy_acc(acc_data_trim$z_cal[i, ]))
#   
#   vectorial_sum <- sqrt(x_component^2 + y_component^2 + z_component^2)
#   mean(vectorial_sum, na.rm = TRUE)
# }

# Stop the cluster
stopCluster(cl)

acc_data_trim$log_vedba <- log( acc_data_trim$ave_vedba )

# Assuming 'acc_data_trim' is your data frame
filtered_data <- acc_data_trim[, c("individual_local_identifier", 
                                   "local_timestamp", 
                                   "tag_local_identifier", 
                                   "log_vedba",
                                   "pitch")]

# filtered_data <- acc_data_trim[, c("individual_local_identifier", 
#                                    "local_timestamp", 
#                                    "tag_local_identifier", 
#                                    "group_id", 
#                                    "sex",
#                                    "log_vedba",
#                                    "pitch")]


saveRDS(acc_data_trim,"data/acc_vedba.RDS")
