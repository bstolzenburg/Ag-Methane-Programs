# Drift Adjustments ----

## Function to generate drift adjustment factor and apply drift for the flare
DriftAdjustment <- function(start_date,start_time,stop_date,stop_time){
  # Creating 3,6,12 hour time intervals for calculating flow averages
  hrs_3<- 3/24
  hrs_6 <- 6/24
  hrs_12 <- 12/24
  
  # Creating drift timeline variables
  start <- chron(dates. = start_date,times. = start_time)
  stop <- chron(dates. = stop_date,times. = stop_time)
  
  # Creating filtered dataset between start/stop times 
  processed_logs_drift <- processed_logs%>%
    filter(date_time >= start & date_time <= stop)
  
  # Getting timestamps for 3, 6 and 12 hours after drift ended
  drift_3hrs <- stop + hrs_3
  drift_6hrs <- stop + hrs_6
  drift_12hrs <- stop + hrs_12
  
  # Filtering processed logs according to the 3, 6, 12 hour timeframes
  ## 3 hour timeframe
  drift_3hr_df <- processed_logs%>%
    filter(date_time >= stop & date_time <= drift_3hrs)
  
  ## 6 hour timeframe
  drift_6hr_df <- processed_logs%>%
    filter(date_time >= stop & date_time <= drift_6hrs)
  
  ## 12 hour timeframe
  drift_12hr_df <- processed_logs%>%
    filter(date_time >= stop & date_time <= drift_12hrs)
  
  # Calculating average flows 
  ## Average flow during drift
  drift_avg <- mean(processed_logs_drift$flare_flow)
  
  ## Average flow 3 hours after
  after_3hr_avg <- mean(drift_3hr_df$flare_flow)
  
  
  ## Average flow 6 hours after
  after_6hr_avg <- mean(drift_6hr_df$flare_flow)
  
  
  ## Average flow 12 hours after
  after_12hr_avg <- mean(drift_12hr_df$flare_flow)
  
  
  
  # Calculating drift adjustment ratio
  drift_3hr_ratio = after_3hr_avg / drift_avg
  drift_6hr_ratio = after_6hr_avg / drift_avg
  drift_12hr_ratio = after_12hr_avg / drift_avg
  
  # Calculating average adjustment ratio
  ratios <- c(drift_3hr_ratio,drift_6hr_ratio,drift_12hr_ratio)
  drift_adjustment_factor <- mean(ratios)
  
  
  # Adjusting flow in processed_logs 
  processed_logs_adj <- processed_logs%>%
    mutate(flare_flow_adj = ifelse(date_time %in% processed_logs_drift$date_time,
                                   flare_flow * drift_adjustment_factor,
                                   flare_flow))%>%
    mutate(flare_flow_drift_applied = ifelse(date_time %in% processed_logs_drift$date_time,
                                             "Drift Applied",
                                             "No Drift Applied"))%>%
    mutate(drift_adj_factor = ifelse(date_time %in% processed_logs_drift$date_time,
                                     drift_adjustment_factor,
                                     NA))
  
  # Creating dataframe for drift adjustment factors
  da_factors <- data.frame(drift_start = start,
                           drift_stop = stop,
                           average_drift_flow = drift_avg,
                           average_3hr_flow = after_3hr_avg,
                           average_6hr_flow = after_6hr_avg,
                           average_12hr_flow = after_12hr_avg,
                           drift_factor = drift_adjustment_factor)
  
  # Creating list of return dataframes
  return_df <- list(processed_logs_adj,da_factors)
  
  return(return_df)
}

# Applying drift to flow
drift_adj_list <- DriftAdjustment(start_date = '',
                                  start_time = '',
                                  stop_date = '',
                                  stop_time = '')
# Getting drift adjusted processed_logs
processed_logs_adj <- drift_adj_list[[1]]

# Getting summary of drift adjustments used
drift_summary <- drift_adj_list[[2]]

rm(drift_adj_list)




# USE THIS FUNCTION FOR ANY OTHER DRIFT ADJUSTMENTS
## Function to generate drift adjustment factor when drift has already been applied above 
DriftAdjustment2 <- function(start_date,start_time,stop_date,stop_time){
  # Creating 3,6,12 hour time intervals for calculating flow averages
  hrs_3<- 3/24
  hrs_6 <- 6/24
  hrs_12 <- 12/24
  
  # Creating drift timeline variables
  start <- chron(dates. = start_date,times. = start_time)
  stop <- chron(dates. = stop_date,times. = stop_time)
  
  # Creating filtered dataset between start/stop times 
  processed_logs_drift <- processed_logs_adj%>%
    filter(date_time >= start & date_time <= stop)
  
  # Getting timestamps for 3, 6 and 12 hours after drift ended
  drift_3hrs <- stop + hrs_3
  drift_6hrs <- stop + hrs_6
  drift_12hrs <- stop + hrs_12
  
  # Filtering processed logs according to the 3, 6, 12 hour timeframes
  ## 3 hour timeframe
  drift_3hr_df <- processed_logs_adj%>%
    filter(date_time >= stop & date_time <= drift_3hrs)
  
  ## 6 hour timeframe
  drift_6hr_df <- processed_logs_adj%>%
    filter(date_time >= stop & date_time <= drift_6hrs)
  
  ## 12 hour timeframe
  drift_12hr_df <- processed_logs_adj%>%
    filter(date_time >= stop & date_time <= drift_12hrs)
  
  # Calculating average flows 
  ## Average flow during drift
  drift_avg <- mean(processed_logs_drift$flare_flow)
  
  ## Average flow 3 hours after
  after_3hr_avg <- mean(drift_3hr_df$flare_flow)
  
  ## Average flow 6 hours after
  after_6hr_avg <- mean(drift_6hr_df$flare_flow)
  
  ## Average flow 12 hours after
  after_12hr_avg <- mean(drift_12hr_df$flare_flow)
  
  
  # Calculating drift adjustment ratio
  drift_3hr_ratio = after_3hr_avg / drift_avg
  drift_6hr_ratio = after_6hr_avg / drift_avg
  drift_12hr_ratio = after_12hr_avg / drift_avg
  
  # Calculating average adjustment ratio
  drift_adjustment_factor <- mean(drift_3hr_ratio,drift_6hr_ratio,drift_12hr_ratio)
  
  # Adjusting flow in processed_logs 
  processed_logs_adj <- processed_logs_adj%>%
    mutate(flare_flow_adj = ifelse(date_time %in% processed_logs_drift$date_time,
                                   flare_flow * drift_adjustment_factor,
                                   flare_flow_adj))%>%
    mutate(flare_flow_drift_applied = ifelse(date_time %in% processed_logs_drift$date_time,
                                             "Drift Applied",
                                             flare_flow_drift_applied))%>%
    mutate(drift_adj_factor = ifelse(date_time %in% processed_logs_drift$date_time,
                                     drift_adjustment_factor,
                                     drift_adj_factor))
  
  # Creating dataframe for drift adjustment factors
  da_factors <- data.frame(drift_start = start,
                           drift_stop = stop,
                           average_drift_flow = drift_avg,
                           average_3hr_flow = after_3hr_avg,
                           average_6hr_flow = after_6hr_avg,
                           average_12hr_flow = after_12hr_avg,
                           drift_factor = drift_adjustment_factor)
  
  # Appending to existing drift_summary
  drift_summary <- rbind(drift_summary,da_factors)
  
  # Creating list of return df
  return_df <- list(processed_logs_adj,drift_summary)
  
  return(return_df)
}

# Applying second drift to flow
drift_adj_list <- DriftAdjustment2(start_date = '',
                                   start_time = '',
                                   stop_date = '',
                                   stop_time = '')


# Getting drift adjusted processed_logs
processed_logs_adj <- drift_adj_list[[1]]

# Getting summary of drift adjustments used
drift_summary <- drift_adj_list[[2]]

rm(drift_adj_list)

# Applying third drift to flow
drift_adj_list <- DriftAdjustment2(start_date = '',
                                   start_time = '',
                                   stop_date = '',
                                   stop_time = '')


# Getting drift adjusted processed_logs
processed_logs_adj <- drift_adj_list[[1]]

# Getting summary of drift adjustments used
drift_summary <- drift_adj_list[[2]]
