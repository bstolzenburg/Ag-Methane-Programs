## Padding Missing Data ----------------------------------------------------------------

# Convert date to correct format for excel 
processed_logs$Date <- mdy(processed_logs$Date)

## Filling in missing timestamps with empty rows (NA) in the 'processed_logs_filled' df

# Merge 'Date' and 'Time' columns to create a single timestamp column
processed_logs$date_time <- as.POSIXct(paste(processed_logs$Date, processed_logs$Time), 
                                       format = "%Y-%m-%d %H:%M:%S",tz = "GMT")



# Padding dataset for missing timestamps 
processed_logs_filled <- pad(processed_logs,by = "date_time")

# Arrange in descending order 
processed_logs_filled <- processed_logs_filled%>%
  arrange(desc(date_time))



## Creating Summary of Padded Data ------
# Identify the rows with NAs in the original data
missing_data <- processed_logs_filled %>%
  mutate(missing_flag = is.na(Date)) %>%
  arrange(date_time)

# Create a grouping variable to identify consecutive NAs
missing_data <- missing_data %>%
  mutate(group = cumsum(!missing_flag & lag(missing_flag, default = FALSE)) + 1) %>%
  filter(missing_flag) %>%
  group_by(group) %>%
  summarise(
    start_gap = min(date_time),
    end_gap = max(date_time),
    missing_timestamps = n()
  ) %>%
  mutate(missing_intervals = missing_timestamps * 15)

# Convert to the desired format
missing_summary <- missing_data %>%
  select(start_gap, end_gap, missing_timestamps, missing_intervals)

# Replacing NA's in the Date and Time columns in processed_logs_filled
processed_logs_filled$Date <- as.Date(processed_logs_filled$date_time)
processed_logs_filled$Time <- format(processed_logs_filled$date_time,format = "%H:%M:%S" )

rm(missing_data)