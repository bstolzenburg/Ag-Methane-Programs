# Data Substitution ----------------------------------------------


# Converting date_time to POSIXct
processed_logs$date_time <- as.POSIXct(processed_logs$date_time,tz = 'GMT')

# Function to substitute flow values
library(dplyr)

data_substitution <- function(df, start, end, date_time, flow_variable_text) {
  # Convert input datetimes into POSIXct
  start <- as.POSIXct(start, format = "%m/%d/%y %H:%M:%S", tz = 'GMT')
  end <- as.POSIXct(end, format = "%m/%d/%y %H:%M:%S", tz = 'GMT')
  
  # Quoting variables for use in dplyr
  date_time <- enquo(date_time)
  flow_variable <- sym(flow_variable_text)  # Convert text to symbol for tidy evaluation
  
  # Calculate 72 hour before/after period             ## MODIFY FOR DIFFERENT PERIODS
  start_period <- start - 72*60*60
  end_period <- end + 72*60*60
  
  # Filter df for periods before and after gap
  ci_data <- df %>%
    filter((!!date_time >= start_period & !!date_time < start) | (!!date_time > end & !!date_time <= end_period))
  
  # Getting flow vector from filtered df
  flow <- ci_data[[flow_variable_text]]
  
  # Calculate confidence interval for period 
  ci <- t.test(flow, conf.level = 0.95)$conf.int                  ## MODIFY FOR DIFFERENT CONFIDENCE LEVEL
  
  # Getting upper/lower ci
  ci_lower <- ci[1]
  ci_upper <- ci[2]
  
  # Creating separate flow variable names for upper/lower ci
  flow_lower <- paste0(flow_variable_text, '_lower_ci')
  flow_upper <- paste0(flow_variable_text, '_upper_ci')
  
  # Creating new flow variables for upper/lower ci using dynamic variable names
  flow_corrected_df <- df %>%
    mutate(!!flow_lower := case_when(
      !!date_time >= start & !!date_time <= end ~ ci_lower,
      TRUE ~ !!flow_variable
    )) %>%
    mutate(!!flow_upper := case_when(
      !!date_time >= start & !!date_time <= end ~ ci_upper,
      TRUE ~ !!flow_variable
    ))
  
  return(flow_corrected_df)
}




# Getting start and end date for gap(s)
start <- '10/19/22 9:15:00'              # Modify as needed
end <- '10/21/22 6:45:00'

# Calling function (repeat this and alter the flow column text if doing data substitution for multiple flow columns)
processed_logs <- data_substitution(processed_logs,
                                    start,
                                    end,
                                    date_time,
                                    'G2_flow') # Modify this as needed

# Example of second flow variable substitution
# processed_logs <- data_substitution(processed_logs,
#                                     start,
#                                     end,
#                                     date_time,
#                                     'G3_flow') 


# Uncomment to xchk

# start <- as.POSIXct('10/19/22 9:15:00', format = "%m/%d/%y %H:%M:%S", tz = 'GMT')
# end <- as.POSIXct('10/21/22 6:45:00', format = "%m/%d/%y %H:%M:%S", tz = 'GMT')
# 
# xchk <- processed_logs%>%
#   filter(date_time >= start & date_time <= end)