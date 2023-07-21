# Code template for filling in data gaps and filling with the most recent totalizer difference 

# Function to load project modules
LoadModules <- function(){
  library(plyr)
  library(readr)
  library(dplyr)
  library(tidyr)
  library(tidyverse)
  library(writexl)
  library(lubridate)
  library(openxlsx)
  library(chron)
  library(zoo)
  library(yaml)
  library(fs)
  library(stringr)
  library(padr)
}

# Loading modules
LoadModules()


## Padding Missing Data ----- 

## Filling in missing timestamps with empty rows (NA) in the 'processed_logs_filled' df

# Merge 'Date' and 'Time' columns to create a single timestamp column
processed_logs$date_time <- as.POSIXct(paste(processed_logs$Date, processed_logs$Time), 
                                       format = "%Y-%m-%d %H:%M:%S",tz = "GMT")



# Padding dataset for missing timestamps 
processed_logs_filled <- pad(processed_logs,by = "date_time")

# Arrange in descending order 
processed_logs_filled <- processed_logs_filled%>%
  arrange(desc(date_time))

# Replacing NA's in the Date and Time columns 
processed_logs_filled$Date <- as.Date(processed_logs_filled$date_time)
processed_logs_filled$Time <- format(processed_logs_filled$date_time,format = "%H:%M:%S" )






# Filling NAs with most recent feasible totalizer value

## Replace NAs with the most recent non-NA value in each column
processed_logs_filled$G1_flow <- na.locf(processed_logs_filled$G1_flow)
processed_logs_filled$G1_kwh <- na.locf(processed_logs_filled$G1_kwh)
processed_logs_filled$missing_timestamp <- na.locf(processed_logs_filled$missing_timestamp)

# Creating data substitution label
processed_logs_filled <- processed_logs_filled%>%
  mutate(flow_data_substitution = ifelse(missing_timestamp != 0,
                                         "Flow Data Gap Substituted",
                                         "No Substitution"))

# Function to recalculate the flow value for periods w/ missing timestamps by dividing the 
# most recent totalizer difference by the # of missing timestamps and applying it across the 
# range of missing rows

fill_flow_variables <- function(variables_list, df) {
  filled_df <- df
  
  for (var in variables_list) {
    filled_df <- filled_df %>%
      mutate_at(vars({{ var }}), ~ ifelse(missing_timestamp != 0,
                                          ./missing_timestamp,
                                          .))
  }
  
  return(filled_df)
}

# Define the list of variables to perform the operation on
variables_list <- c("G1_flow",'G1_kwh')                          #INPUT

# Call the function
processed_logs_filled <- fill_flow_variables(variables_list, processed_logs_filled)

# Replacing processed_logs with processed_logs_filled
processed_logs <- processed_logs_filled



## Creating gap_summary for filled timestamps --------

# Function to create gap summary for missing timestamps 
GapSummary <- function(logs,missing_timestamp){
  # Quoting variables 
  missing_timestamp <- enquo(missing_timestamp)
  
  # Filtering processed logs where there are missing timestamps
  gap_summary <- logs%>%
    
    # Adding row numbers so hyperlinks can be created
    mutate(row_numbers = seq.int(nrow(logs))+1)%>%
    
    # Filtering for invalid totalizer differences
    filter(!!missing_timestamp != 0)%>%
    
    # Creating hyperlink string to link to processed logs 
    mutate(link = makeHyperlinkString(sheet = 'Processed Logs',text = 'Link to Processed_Logs',row = row_numbers, col = 1))%>%
    
    # Selecting columns for gap summary 
    ## Adding in G1_diff and G1_kwh to provide justification for use of totalizer values across gaps
    select(Date,Time,G1_totalizer_diff,G1_flow,G1_kwh_totalizer_diff,G1_kwh,missing_timestamp,link)
  
  
  
  return(gap_summary)
  
}

# Creating flow gap substitution summary 
flow_gap_summary <- GapSummary(processed_logs,missing_timestamp)
