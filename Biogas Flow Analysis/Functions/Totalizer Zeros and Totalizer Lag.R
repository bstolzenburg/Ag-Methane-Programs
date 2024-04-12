# Code to correct totalizer zeros and totalizer lag
# Run code before determining flare operational flow

## Correcting totalizer values for engine when there are zeros ----

# Creating non-zero engine totalizer 
non_zero_engine <- processed_logs%>%
  select(date_time,G1_totalizer)%>%
  mutate(G1_totalizer_filled = case_when(G1_totalizer == 0 ~ NA,
                                         TRUE ~ G1_totalizer))

# Reversing order of dataframe
non_zero_engine <- map_df(non_zero_engine, rev)

# Filling in NA's with most recent totalizer value, and then putting back in correct order 
non_zero_engine <- non_zero_engine%>%
  mutate(G1_totalizer_filled = na.locf(G1_totalizer_filled))

# Put dataframe back in correct order 
non_zero_engine <- map_df(non_zero_engine, rev)

### Adjust flow for totalizer zeros -----
# Creating column for previous non-zero totalizer value
processed_logs$G1_totalizer_ZerosFilled <- non_zero_engine$G1_totalizer_filled

# Correcting flow 
processed_logs <- processed_logs%>%
  # Correcting flow variable
  mutate(G1_flow = case_when(G1_totalizer !=0 & lead(G1_totalizer)==0 ~ G1_totalizer - lead(G1_totalizer_ZerosFilled),
                             TRUE ~ G1_flow))%>%
  # Correcting data substitution label 
  mutate(G1_flow_valid = case_when(G1_totalizer !=0 & lead(G1_totalizer)==0 ~ 'Previous totalizer value is zero, subtracted from previous non-zero totalizer',
                                   TRUE ~ G1_flow_valid))



## Correcting totalizer values for flare when there are zeros ----

# Creating non-zero flare totalizer 
non_zero_flare <- processed_logs%>%
  select(date_time,flare_totalizer)%>%
  mutate(flare_totalizer_filled = case_when(flare_totalizer == 0 ~ NA,
                                            TRUE ~ flare_totalizer))

# Reversing order of dataframe
non_zero_flare <- map_df(non_zero_flare, rev)

# Filling in NA's with most recent totalizer value, and then putting back in correct order 
non_zero_flare <- non_zero_flare%>%
  mutate(flare_totalizer_filled = na.locf(flare_totalizer_filled))

# Put dataframe back in correct order 
non_zero_flare <- map_df(non_zero_flare, rev)

### Adjust flow for totalizer zeros -----
# Creating column for previous non-zero totalizer value
processed_logs$flare_totalizer_ZerosFilled <- non_zero_flare$flare_totalizer_filled

# Correcting flow 
processed_logs <- processed_logs%>%
  
  # Correcting flow variable
  mutate(flare_flow = case_when(flare_totalizer !=0 & lead(flare_totalizer)==0 ~ flare_totalizer - lead(flare_totalizer_ZerosFilled),
                                TRUE ~ flare_flow))%>%
  
  # Correcting data substitution label 
  mutate(flare_flow_valid = case_when(flare_totalizer !=0 & lead(flare_totalizer)==0 ~ 'Previous totalizer value is zero, subtracted from previous non-zero totalizer',
                                      TRUE ~ flare_flow_valid))





# Totalizer Lag Corrections -----

# Function to process for totalizer lag
TotalizerLag <- function(logs, totalizer){
  # Rearranging the logs in ascending order
  logs <- logs%>%
    arrange(date_time)
  
  # Reversing the order of the totalizer column
  totalizer <- rev(totalizer)
  
  # Creating empty vector to track the lag counter
  totalizer_lag <- vector()
  
  # Iterating through logs to identify and count totalizer lag
  for (i in 1:nrow(logs)){
    if (i != 1){
      if (totalizer[i] == totalizer[i-1]){
        totalizer_lag <- append(totalizer_lag,totalizer_lag[i-1]+1)
      }else{
        totalizer_lag <- append(totalizer_lag,0)
      }
    }else {
      totalizer_lag <- append(totalizer_lag,0)
    }
    
  }
  
  # Reversing the order of the lag counter
  totalizer_lag <- rev(totalizer_lag)
  
  return(totalizer_lag)
}

##Correcting totalizer lag for engine ----

# Creating totalizer lag column for engine
processed_logs$engine_lag <- TotalizerLag(processed_logs,processed_logs$G1_totalizer)

# Correcting totalizer lag for current timestamp so the average is calculated on the correct number of timestamps
processed_logs$engine_lag <- ifelse(processed_logs$engine_lag == 0 & lead(processed_logs$engine_lag) !=0,
                                    lead(processed_logs$engine_lag)+1,
                                    processed_logs$engine_lag)

## Max gap was < 7 days so data substitution is not required



## Correcting totalizer lag for flare ----

# Creating totalizer lag column for flare
processed_logs$flare_lag <- TotalizerLag(processed_logs,processed_logs$flare_totalizer_ZerosFilled) # Using flare_totalizer_ZerosFilled so any periods will zeros are appropriately checked for operational activity

# Correcting totalizer lag for current timestamp so the average is calculated on the correct number of timestamps
processed_logs$flare_lag <- ifelse(processed_logs$flare_lag == 0 & lead(processed_logs$flare_lag) !=0,
                                   lead(processed_logs$flare_lag)+1,
                                   processed_logs$flare_lag)

# Correcting for totalizer lag by calculating average flow during lag, with threshold for 24 hours (96 timestamps)
flare_lag<- processed_logs%>%
  select(date_time,flare_flow,flare_lag)%>%
  filter(flare_lag !=0)%>%
  mutate(average_flare_flow = case_when(flare_flow != 0 & flare_lag < 96 ~ flare_flow/flare_lag,
                                        TRUE ~ 0))

# Fill in zeros with NA so they can be filled down with average flow
flare_lag_filled <- flare_lag%>%
  mutate(time_diff = as.numeric(difftime(date_time,lead(date_time),units = c('mins'))))%>%
  mutate(average_flare_flow = case_when(average_flare_flow == 0 & lag(time_diff) > 15 ~ 0,
                                        average_flare_flow == 0 & lag(time_diff)== 15 & flare_lag <96 ~ NA, # Prevents timestamps with no flow from getting filled in from the previous value
                                        flare_lag > 96 ~ 0,
                                        TRUE ~ average_flare_flow))%>%
  select(-time_diff)%>%
  mutate(average_flare_flow = na.locf(average_flare_flow))




# Merging the flare_lag_filled with processed_logs
processed_logs_merged <- merge(processed_logs,flare_lag_filled[c('date_time','average_flare_flow')],
                               by = 'date_time',all.x = TRUE)

# Reverse order of processed_logs_merged
processed_logs_merged <- map_df(processed_logs_merged,rev)

# Replace processed_logs with processed_logs_merged
processed_logs <- processed_logs_merged

### Replace flare flow with calculated lag average where appropriate ----
processed_logs <- processed_logs%>%
  mutate(flare_flow = case_when(average_flare_flow !=0 & is.na(average_flare_flow)== FALSE ~ average_flare_flow,
                                TRUE ~ flare_flow))

## Adding data substitution labeling for flare lag substitution
processed_logs <- processed_logs%>%
  mutate(flare_flow_valid = case_when(average_flare_flow != 0 & flare_flow == average_flare_flow ~ 'Totalizer stuck for less than 24 hours, average flow substituted',
                                      average_flare_flow == 0 & flare_flow != average_flare_flow ~ 'Totalizer stuck for more than 24 hours, zero BDE assumed for totalizer',
                                      TRUE ~ flare_flow_valid))


rm(processed_logs_merged)