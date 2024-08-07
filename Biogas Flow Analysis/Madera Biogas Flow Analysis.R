# Biogas Flow Analysis Program 
# Bryan Stolzenburg (Ag Methane Advisors)
# 5.15.24

# Updates Log
## Added section to pad missing rows and get average flow data across gaps 
## Added section to fill in missing ch4 readings with rolling average
## Added to weighted CH4 calculation code to include flare flow during overhall in 5/2023
## Incorporate dynamic paths so this file can be called from other file locations using the 'source()' function
## Added filter_date to only process data that is in the current RP, to avoid new methodologies changing previously verified data

## 5.15.24 updates
## Updated totalizer_lag section to correctly distribute flow when totalizer readings were missed
## Updated logic for processing totalizer differences so first totalizer difference is maintained
## so it can be used for padding missing data. Instead check to see if previous totalizer value is zero. 
## Moved timestamp padding above totalizer lag so there are no conflicts with substitution
## Added na.locf for flare flow (May 2023 engine overhaul). 
## Filtered logs for filter_date and period_end



# Importing Modules ---------------

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
  library(here)
  library(conflicted)
}

# Loading modules
LoadModules()

# Resolving conflicts with packages 
## Filter
conflicts_prefer(dplyr::filter)
conflicts_prefer(dplyr::arrange)
conflicts_prefer(dplyr::mutate)
conflicts_prefer(dplyr::lag)
conflicts_prefer(dplyr::summarise)
conflicts_prefer(dplyr::summarize)

# Setting Filter Date 
filter_date <- chron(dates. = '12/20/2022',                   ### This should correspond to the cutoff of the previous verification, so any previously verified data remains fixed.
                     times. = '23:45:00')                     ### Leaving a period of ~ 10 days so the first timestamp can be calculated or if data substitution is needed

period_start <- as_datetime('2023-01-01 00:00:00',tz = 'GMT')

# period_end <- chron(dates. = '12/31/2023',
#                     times. = '23:45:00')     # OPTIONAL: End date for analysis. UNCOMMENT TO APPLY




# Setting Dynamic Working Environment ------------------------------------------
## Get user name
user_name <- Sys.getenv("USERNAME")

# Build base path
base_path <- base_path <- file.path("C:", "Users", user_name, winslash = "\\")

# Get target directory
target_dir <- file.path(base_path, "Patrick J Wood Dropbox",
                        "_operations",
                        "Clients",
                        "Philip Verwey Farms",
                        'Farm1Madera',
                        'RP 2023',
                        'Calcs',
                        'Flow',
                        'eLCFS')

# Setting working directory
setwd(target_dir)


# Reading in Gas Logs -------------------------------------------------------------------------------

### Reading In Merged Logs 

# Creating path variable for the 'Flow Data' folder
flow_data_path <- file.path(getwd(),'Flow Data')  

## Importing data and creating merged logs ----
merged_logs <- ldply(list.files(path = flow_data_path, full.names = TRUE),.fun = read.csv,check.names = FALSE,header = TRUE) 


## Getting Farm Configuration from YAML file ---------------------------------------

## Function to parse YAML file to get farm configuration
ParseYAML <- function(farm){
  # Adding '.' to farm_name
  farm_name <- paste(farm,'.',sep = '')
  
  # Creating path to Log_Columns.yml
  yml_path <- file.path(path_home(),'Patrick J Wood Dropbox','_operations','Software','Github','Ag-Methane-Programs','Biogas Flow Analysis','Log_Columns.yml')
  
  # Reading in Log_Columns.yml to a nested list
  yml_list <- read_yaml(yml_path)
  
  # Parsing yml_list to named vector
  yml_parsed <- unlist(yml_list)
  
  # Filter named vector for only farm_name
  yml_filtered <- yml_parsed[grepl(farm_name,names(yml_parsed))]
  
  # Getting names and values from filtered vector
  column_names <- names(yml_filtered)
  indexes <- unname(yml_filtered)
  
  # Getting rid of farm name in headers
  column_names <- gsub(farm_name,'',column_names)
  
  # Creating function output
  out <- list(column_names,indexes)
  
  return(out)
  
}

# Parsing YAML file for farm 
### Aurora Ridge | Chaput | Four Hills | Hanford | Madera

cfg <- ParseYAML('Madera')                                     ####### INPUT 

# Getting column headers
column_names <- cfg[[1]]

# Getting index values
indexes <- cfg[[2]]



# Cleaning Merged Logs ------------------------------------------------------------------------

## Fixing degree symbol in column headers ----

# Convert column names to character and then to UTF-8
colnames(merged_logs) <- as.character(colnames(merged_logs))
colnames(merged_logs) <- iconv(colnames(merged_logs), "latin1", "UTF-8")

## Saving original headers to be applied at end of program

# Saving the original column names as 'headers' variable
og_headers <- colnames(merged_logs)

# Get number of headers in original dataframe 
# -1 to exclude date_time column from original headers
max_header <- as.integer(length(og_headers))


## Formatting Dates and Times and sorting dataframe ----------------------------------------------------------------

# Convert all dates to same format
merged_logs$Date <- as.Date(parse_date_time(merged_logs$Date,c('mdy','ymd')))
merged_logs$Date <-format.Date(merged_logs$Date, '%m/%d/%Y' )



# Function to create date_time variable from separate date/time columns
DateTime<- function(logs){
  # Convert Time from character to Chron
  logs$Time <- chron(times. = logs$Time)
  
  # Merge Date and Time to create date_time column
  logs$date_time <- chron(dates. = as.character(logs$Date),times. = times(logs$Time))
  
  return(logs)
}

# Creating date_time variable
merged_logs <- DateTime(merged_logs)

# Removing any duplicate rows 
merged_logs <- merged_logs%>%
  distinct()


# Sorting by date_time in descending order by date_time
merged_logs <- merged_logs%>%
  arrange(desc(date_time))


# Filtering merged logs ---------------

# If period_end identified (uncommented section above) then applying filter to merged logs

if(exists('period_end')){
  # Filtering merged logs for after filter_date and before period_end
  merged_logs <- merged_logs%>%
    filter(date_time > filter_date & date_time <= period_end)
  
  message('\n Filtered merged logs between ',filter_date,' and ',period_end,'\n')
}else{
  # Filtering for only data applicable to current RP, leaving out data already verified 
  merged_logs <- merged_logs%>%
    filter(date_time > filter_date)
  
  message('Filtered merged_logs for only data after: ',filter_date,'\n')
  message('Operational Period Start: ',period_start,'\n')
}


# Cleaning Gas Log Column Headers ------------------------------------------------------------------------

# Function to clean column headers


# Function to clean up columns
CleanCols <- function(index,names){
  
  # Creating headers variable
  headers <- colnames(merged_logs)
  
  ## Creating dataframe of the headers, along with corresponding index
  col_index <- 1:length(headers)
  
  log_columns <- data.frame(col_index,headers)
  
  # Creating list of headers to be changed 
  indexes_to_change <- index
  
  # Creating dataframe of the headers to be changed
  change_headers <- data.frame(indexes_to_change)
  
  # Filtering for only the columns where the header must be changed
  log_columns<- log_columns%>% 
    filter(col_index %in% change_headers$indexes_to_change)
  
  # Creating datafrmae of new names (Input the new names in the order they appear in the console)
  new_names <- names 
  
  # Adding the new names to the log_columns dataframe 
  log_columns$new_name <- new_names 
  
  # Creating list of new header names 
  new_names <- headers
  
  # Changing the names of the headers in the new_names item dataframe
  for (x in log_columns$col_index){
    new_names[x]<- log_columns$new_name[log_columns$col_index == x]
  }
  
  # Changing header names of merged logs 
  colnames(merged_logs)<-new_names
  
  return(merged_logs)
  
}

# Changing column names of merged_logs to streamline calculations
merged_logs <- CleanCols(indexes,column_names)

# Cleaning up directory 
rm(cfg)




# Processing Gas Logs ----------------------------------------------------------------

# Create list of totalizers 
totalizer_list <- column_names[grepl('totalizer',column_names)]

## Processing merged logs-------------

TotalizerDiff <- function(merged_logs,totalizer_list){
  # Getting first timestamp
  time1 <- merged_logs$date_time[nrow(merged_logs)]
  
  # Creating processed_logs df from merged_logs
  processed_logs <- merged_logs
  
  # Iterating through list of totalizers and calculating totalizer differences
  for (totalizer in totalizer_list){
    # Creating name for difference column
    diff_name <-paste(totalizer,'diff',sep = '_')
    
    # Creating processed_logs df
    processed_logs <- processed_logs%>%
      mutate(!! diff_name := case_when(date_time == time1 ~ 0,
                                    TRUE ~ as.numeric(get(totalizer) - lead(get(totalizer)))))
  }
  
  return(processed_logs)
}

processed_logs <- TotalizerDiff(merged_logs,totalizer_list)




## Processing Totalizer Values ---------------------------------------------------------------
ProcessLogs<-function(logs,totalizer,total_diff,new_name,substitution_method){
  # Quoting variables in function 
  totalizer <- enquo(totalizer)
  total_diff <- enquo(total_diff)
  new_name <- quo_name(new_name)
  substitution_method <- quo_name(substitution_method)
  
  # Correcting for zeros in the totalizer columnn  
  logs<- logs%>%
    mutate(!!new_name := case_when(!!totalizer == 0 ~ 0,
                                   !!total_diff < 0 ~ 0,
                                   lead(!!totalizer) == 0 ~ 0,
                                   TRUE ~ as.numeric(!!total_diff)))
  
  # Creating data substitution label, any time there is a negative totalizer difference 
  logs <- logs%>%
    mutate(!!substitution_method := case_when(!!total_diff < 0 ~ 'Invalid Totalizer Difference, Flow Set to Zero',
                                              lead(!!totalizer)==0 ~ 'Invalid Totalizer Difference',
                                              TRUE ~ "Totalizer Feasible"))
                                              
  
  return(logs)
}




# Loop to run process flow for all values in totalizer_list
for (totalizer in totalizer_list){
  
  # Checking if kwh or flow totalizer 
  if(grepl('kwh',totalizer)==TRUE){
    
    # Creating name for difference column
    diff_name <-paste(totalizer,'diff',sep = '_')
    
    # Creating kwh variable name
    new_name <- gsub('_totalizer','',totalizer)
    
    
    # Creating substitution method variable 
    sub_method <- paste(new_name,'valid',sep = '_')
    
    
  }else{
    
    # Creating name for difference column
    diff_name <-paste(totalizer,'diff',sep = '_')
    
    # Creating flow variable name
    new_name <- gsub('totalizer','flow',totalizer)
    
    
    # Creating substitution method variable 
    sub_method <- paste(new_name,'valid',sep = '_')
    
  }
  
  # Calling ProcessLogs function creating new variable
  processed_logs <- ProcessLogs(processed_logs,get(totalizer),get(diff_name),new_name,sub_method)
  
  
}

rm(diff_name,new_name,sub_method,totalizer)




## Flare Operational Flow ------------------------------------------------------------------------------
# Processing operational/non-operational flare flow again based on new flare_flow numbers 

## Flare Operational Activity
FlareOperation <- function(logs,thermocouple,flare_flow){
  # Quoting variables 
  thermocouple <- enquo(thermocouple)
  flare_flow <- enquo(flare_flow)
  
  # Determining if flare was operational based on temperature
  logs<- logs%>% 
    mutate(flare_oper = case_when(!!thermocouple >= 120  ~ 'Operational',
                                  !!thermocouple < 120 ~ 'Non-operational',
                                  TRUE ~ 'Non-operational'))
  
  # Declaring flow as operational or non-operational based on F1_oper
  logs<- logs%>%
    mutate(flare_flow_op = case_when(flare_oper == 'Operational' ~ as.numeric(flare_flow),
                                     TRUE ~ 0))%>%
    mutate(flare_flow_nonop = case_when(flare_oper == 'Non-operational' ~ as.numeric(flare_flow),
                                        TRUE ~ 0))
  return(logs)
}
# Differentiating flare operation
processed_logs<-FlareOperation(processed_logs,flare_temp,flare_flow)







# Gap Analysis & Missing Timestamp Recognition --------------------------------------------

# Convert date to correct format for excel 
processed_logs$Date <- mdy(processed_logs$Date)

# Calculating the time difference based on date_time column
processed_logs<-processed_logs%>%
  mutate(time_diff = as.numeric(difftime(date_time,lead(date_time),units = c('mins'))))



# Creating a missing timestamp column (1 timestamp = 15 minutes)
processed_logs<-processed_logs%>%
  mutate(missing_timestamp = case_when(time_diff == 15 ~ 0,
                                       time_diff > 15 ~ time_diff/15,
                                       is.na(time_diff)==TRUE ~ 0))




## Padding Missing Data ----------------------------------------------------------------

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
processed_logs_filled$flare_flow <- na.locf(processed_logs_filled$flare_flow)
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
variables_list <- c("G1_flow",'G1_kwh','flare_flow')                          #INPUT

# Call the function
processed_logs_filled <- fill_flow_variables(variables_list, processed_logs_filled)

# Replacing processed_logs with processed_logs_filled
processed_logs <- processed_logs_filled


## Creating gap_summary for filled timestamps ------------------------

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
    select(Date,Time,G1_totalizer_diff,G1_flow,G1_kwh_totalizer_diff,G1_kwh,flare_totalizer,flare_flow,missing_timestamp,link)
  
  
  
  return(gap_summary)
  
}

# Creating flow gap substitution summary 
flow_gap_summary <- GapSummary(processed_logs,missing_timestamp)







# Totalizer Lag Corrections -----------------------------------------------------

# Function to create totalizer_lag column
create_totalizer_lag <- function(data,column_name){
  # Use run length encoding to detect periods of totalizer lag
  rle_values <- rle(data[[column_name]])
  
  # Create a vector that repeats the sequence of lengths for each group of constants
  lag_column <- rep(rle_values$lengths,times = rle_values$lengths)
  
  # Adding the vector as a new dataframe column
  data$totalizer_lag <- lag_column
  
  # Return modified dataframe
  return(data)
}

# Calling totalizer lag function to create lag column
processed_logs <- create_totalizer_lag(processed_logs,'G1_totalizer')


# Function to identify timestamps that need adjustment and distribute flow across those rows
adjust_lag <- function(data,totalizer,lag,flow,power,valid){
  # Quoting variables
  totalizer <- enquo(totalizer)
  lag <- enquo(lag)
  flow <- enquo(flow)
  power <- enquo(power)
  valid <- enquo(valid)
  
  # Logic to adjust totalizer_lag column to identify timestamps that need adjustment
  data <- data%>%
    mutate(totalizer_needs_adjustment = case_when(is.na(lag(!!totalizer))==TRUE & row_number() != 1 ~ 0,   # Not including timestamps before gaps occurred, or the first row
                                                  !!lag > 1 & lead(!!lag) != 1 & !!flow ==0 & !!power > 0 ~ 1,  # 1 = missed flow reading
                                                  !!flow > 0 & lead(!!lag) > 1 & !!power > 0 & lead(!!power) > 0 ~ 2,           # 2 = has reading that needs distributing
                                                  TRUE ~ 0))      # 0 = no adjustment needed
  
  # Logic to adjust flow for columns that need distributed flow 
  data <- data%>%
    mutate(!!flow := case_when(totalizer_needs_adjustment == 2 ~ !!flow/lead(!!lag),
                                     totalizer_needs_adjustment == 0 ~ !!flow,
                                     totalizer_needs_adjustment == 1 ~ lag(!!flow)/!!lag))
  
  # Logic to label periods where flow was distributed
  data <- data%>%
    mutate(!!valid := case_when(totalizer_needs_adjustment == 2 | totalizer_needs_adjustment == 1 ~ 'Totalizer stuck, flow distributed',
                               TRUE ~ !!valid))
  
  # Dropping columns from final dataframe
  # data <- data%>%
  #   select(- totalizer_needs_adjustment,-!!lag)
  
  return(data)
                
}

processed_logs <- adjust_lag(processed_logs,G1_totalizer,totalizer_lag,G1_flow,G1_power,G1_flow_valid)

## Creating Gap Summaries ---------------------------------------------------------------------------------

## Funciton to create a gap summary for engines
EngineSummary <- function(logs,device_flow_valid,device_kwh_valid){
  # Quoting variables 
  device_flow_valid <- enquo(device_flow_valid)
  device_kwh_valid <- enquo(device_kwh_valid)
  
  # Filtering processed logs where device flow/kwh was identifed as not valid
  gap_summary <- logs%>%
    
    # Adding row numbers so hyperlinks can be created
    mutate(row_numbers = seq.int(nrow(logs))+1)%>%
    
    # Filtering for invalid totalizer differences
    filter(!!device_flow_valid != 'Totalizer Feasible' | !!device_kwh_valid != 'Totalizer Feasible')%>%
    
    # Creating hyperlink string to link to processed logs 
    mutate(link = makeHyperlinkString(sheet = 'Processed Logs',text = 'Link to Processed_Logs',row = row_numbers, col = 1))%>%
    
    # Selecting columns for gap summary 
    select(Date,Time,!!device_flow_valid,!!device_kwh_valid,link)
  
  
  
  return(gap_summary)
  
}



## Function to create gap summary for flare 
FlareSummary <- function(logs,device_flow_valid){
  # Quoting variables 
  device_flow_valid <- enquo(device_flow_valid)
  
  # Filtering processed logs where device flow/kwh was identifed as not valid
  gap_summary <- logs%>%
    
    # Adding row numbers so hyperlinks can be created
    mutate(row_numbers = seq.int(nrow(logs))+1)%>%
    
    # Filtering for invalid totalizer differences
    filter(!!device_flow_valid != 'Totalizer Feasible')%>%
    
    # Creating hyperlink string to link to processed logs 
    mutate(link = makeHyperlinkString(sheet = 'Processed Logs',text = 'Link to Processed_Logs',row = row_numbers, col = 1))%>%
    
    # Selecting columns for gap summary 
    select(Date,Time,!!device_flow_valid,link)
  
  
  
  return(gap_summary)
  
}



### Creating gap summaries 

## Engine 
G1_gap_summary <- EngineSummary(processed_logs,G1_flow_valid,G1_kwh_valid)

## Flare 
flare_gap_summary <- FlareSummary(processed_logs,flare_flow_valid)



# Weighted CH4 % Calculations -----------------------------------

# Filling in missing CH4 values for empty columns
processed_logs$ch4_meter[processed_logs$ch4_meter ==0]<- NA

# Calculating rolling mean window of 7 days before the timestamp
roll_mean_window <- 168 * (60 / 15)  # Window size: 7 days (168 hours = 4 * (60 minutes / 15 minutes)

# Creating ch4_sub column to calculate rolling average for missing ch4_meter readings 
processed_logs <- processed_logs%>%
  
  # Create ch4_substitution label
  mutate(ch4_substitution = ifelse(is.na(ch4_meter),
                          "CH4 Reading Substituted",
                          "No Substitution"))%>%
  # Substitute NA CH4 readings
  mutate(ch4_sub = ifelse(is.na(ch4_meter),
                          rollmean(ch4_meter, k = roll_mean_window, align ='right',na.rm = TRUE),
                          ch4_meter))


## Creating Gap Summary for CH4 Substitution
Ch4_gap_summary <- processed_logs%>%
  # Adding row numbers so hyperlinks can be created
  mutate(row_numbers <<- seq.int(nrow(processed_logs))+1)%>%
  
  # Creating hyperlink string to link to processed logs "Date" column
  mutate(link = makeHyperlinkString(sheet = 'Processed Logs',
                                    text = 'Link to Processed_Logs',
                                    row = row_numbers,
                                    col = 1))%>%
  
  # Filtering for instances where ch4_substitution occurred
  filter(ch4_substitution != 'No Substitution')%>%
  
  # Selecting columns
  select(Date,Time,ch4_meter,ch4_sub,link)

## Filtering dataframe so only relevant months are included in weighted_ch4 summary
processed_logs <- processed_logs%>%
  filter(date_time >= period_start)
  


## Calculating weighted average for CH4% ------------------------------

# Creating month/year column 
processed_logs$Month <- as.yearmon(processed_logs$Date)

# Creating flow total column to include flare flow during May 2023 engine maintenance
processed_logs <- processed_logs%>%
  mutate(ch4_flow_total = ifelse(Date >= "2023-05-01" & Date <= '2023-05-05',G1_flow + flare_flow,
                                 G1_flow))


# Calculating weighted average, weighting CH4% for each timestamp according to the gas flow (G1_flow)
weighted_average <- processed_logs%>%
  group_by(Month)%>%
  summarise('Weighted Average CH4%' = weighted.mean(ch4_sub,ch4_flow_total,na.rm = TRUE)/100)

# Removing the month column from the processed_logs df
processed_logs<- processed_logs%>%
  select(-Month)


# Creating Biogas Flow Summary Table -------------------


# Convert period_start to posixct
period_start <- as.POSIXct(period_start)

# Create flow summary table 
flow_summary <- processed_logs%>%
  filter(date_time >= period_start)%>%
  group_by(Month = floor_date(Date,'month'))%>%
  summarise('Engine Flow (SCF)'= sum(G1_flow),
            'Flare Flow (SCF)'= sum(flare_flow,na.rm = TRUE),
            'Engine kWH' = sum(G1_kwh))





# Cleaning up Final Dataframe --------------------------

## Formatting date for excel --------

# Removing date_time column 
processed_logs <- processed_logs%>%
  select(- date_time,-time_diff)



### Returning column headers to their original values
for(x in 1:max_header){
  colnames(processed_logs)[x]<- og_headers[x]
}

# Removing unecessary columns 
processed_logs <- processed_logs[,-c(10,11,12,13,14,15,16,22,23,24,25,27,28,29,30,31,32,33,34)]
  









# Cleaning up Global Directory ----

# Removing items from global directory
rm(og_headers,max_header,totalizer_list,x,processed_logs_filled,variables_list)

# Garbage Clean
gc()

# Writing Results to Excel ----

# Setting working directory
setwd(target_dir)


# Output file names
date <- format(Sys.Date(),"%m.%d.%y")

# Creating file name for .xlsx summary and .csv processed_logs
file_name = paste('Madera Biogas Flow Summary_WORKING_',date,'.xlsx', sep = '')
processed_name = paste('Madera Processed Logs_',date,'.csv',sep = '')

# Printing available dataframes in global environment
names(which(unlist(eapply(.GlobalEnv,is.data.frame)))) # Choose from this list


# Creating list of dataframes to include as tables in the results spreadsheet
# 'Excel Sheet Name' = 'Dataframe Name'
data_tables <- list('Processed Logs' = processed_logs,
                    'Flow Summary' = flow_summary,
                    'Gap Summary' = flow_gap_summary,
                    'Engine Gap Summary' = G1_gap_summary,
                    'Flare Gap Summary' = flare_gap_summary,
                    'CH4 Fractions' = weighted_average,
                    'CH4 Gap Summary' = Ch4_gap_summary)



# Calling function for all dataframes in list 

ToExcel <- function(file_name, data_tables) {
  if (!file.exists(file_name)) {
    createNewExcelFile(file_name, data_tables)
  } else {
    
    message('\n',file_name,' already exists... \n\n','FILE WILL BE OVERWRITTEN IF PATH NOT CHANGED')
    
    readline(prompt = 'Press [Enter] to continue: ')
    createNewExcelFile(file_name, data_tables)
    
    
  }
}

createNewExcelFile <- function(file_name, data_tables) {
  message('Creating excel file', '\n\n')
  

  wb <- createWorkbook()
  message(file_name, ' to be created in current directory', '\n\n')
  processTables(wb, data_tables)
  saveAndFinish(wb, file_name,overwrite = TRUE)
}

processTables <- function(wb, data_tables) {
  for (sheet_name in names(data_tables)) {
    df <- data_tables[[sheet_name]]
    if (nrow(df) > 0) {
      sheet1 <- addWorksheet(wb, sheetName = sheet_name)
      writeData(wb, sheet = sheet1, x = df, colNames = TRUE)
      fitColumns(wb, sheet1, df)
      if (isTRUE(grepl('Gap', sheet_name, ignore.case = TRUE))) {
        writeGapLinks(wb, sheet1, df)
        fitColumns(wb, sheet1, df)
      }
    } else {
      message("Skipping empty dataframe for sheet:", sheet_name, "\n\n")
    }
  }
}

fitColumns <- function(wb, sheet, df) {
  for (col in 1:ncol(df)) {
    setColWidths(wb, sheet = sheet, cols = col, widths = "auto")
  }
}

writeGapLinks <- function(wb, sheet, df) {
  link_num <- grep('link', colnames(df), ignore.case = TRUE)
  link <- df$link
  writeFormula(wb, sheet = sheet, startCol = link_num, startRow = 2, x = link)
}

clearAndWriteData <- function(wb, data_tables) {
  for (sheet_name in names(data_tables)) {
    df <- data_tables[[sheet_name]]
    if (nrow(df) > 0) {
      empty_data <- data.frame(matrix("", nrow = 0, ncol = ncol(read.xlsx(wb, sheet = sheet_name))))
      writeData(wb, sheet = sheet_name, x = empty_data, startCol = 1, startRow = 1, colNames = TRUE)
      writeData(wb, sheet = sheet_name, x = df, colNames = TRUE)
      fitColumns(wb, sheet_name, df)
      if (isTRUE(grepl('Gap', sheet_name, ignore.case = TRUE))) {
        writeGapLinks(wb, sheet_name, df)
        fitColumns(wb, sheet_name, df)
      }
    } else {
      message("Skipping empty dataframe for sheet:", sheet_name, "\n\n")
    }
  }
}

saveAndFinish <- function(wb, file_name, overwrite = FALSE) {
  saveWorkbook(wb, file_name, overwrite = overwrite)
  message('Finished','\n', file_name, ' saved\n')
}



# Writing results to excel
ToExcel(file_name,data_tables)

# Garbage Clean
gc()
