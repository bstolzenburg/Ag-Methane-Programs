# Biogas Flow Analysis Program 
# Bryan Stolzenburg (Ag Methane Advisors)
# 3.14.24


# Program to process biogas flow logs and output .xlsx results

# Importing Modules ----

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
}

# Loading modules
LoadModules()


# Reading in Gas Logs ---------------

## Reading Logs 

# Creating path variable
path <- file.path(getwd(),'Flow Data')     

# Setting working directory based on path 
setwd(path)

## Importing data and creating merged logs ----
merged_logs <- ldply(list.files(),.fun = read.csv,check.names = FALSE,header = TRUE) 

## Getting Farm Configuration from YAML file -----

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

farm <- '' # Enter farm name here

cfg <- ParseYAML(farm)                                 

# Getting column headers
column_names <- cfg[[1]]

# Getting index values
indexes <- cfg[[2]]



# Cleaning Merged Logs -----

# Adding degrees symbol to temperature column headers
colnames(merged_logs) <- gsub('<b0>','\U00B0',colnames(merged_logs))

## Saving original headers to be re-applied at end

# Saving the original column names as 'headers' variable
og_headers <- colnames(merged_logs)

# Get number of headers in original dataframe 
max_header <- as.integer(length(og_headers))




## Cleaning up column headers ----

### Formatting Dates and Times and sorting dataframe ----

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

# Removing any duplicate rows based on unique date_time column
merged_logs <- merged_logs%>%
  distinct()


# Sorting by date_time in descending order
merged_logs <- merged_logs%>%
  arrange(desc(date_time))


## Cleaning Gas Log Column Headers ---- 

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
  
  # Checking to make sure the new names are correct 
  print(log_columns)
  
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
rm(indexes,cfg,path)




# Processing Gas Logs -----

# Create list of totalizers 
## Filtering updated column names for 'totalizer' 
totalizer_list <- column_names[grepl('totalizer',column_names)]
message('Printing totalizer columns to be processed')
print(totalizer_list)



## Initial Processing of merged logs ----

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

# Calling function
processed_logs <- TotalizerDiff(merged_logs,totalizer_list)

# Gap Analysis ----

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


# Dropping columns from the final dataframe
processed_logs<- processed_logs%>%
  select(-c('time_diff',))

## Processing Totalizer Values ----

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
                                   !!total_diff > 1000000 ~ 0,
                                   TRUE ~ as.numeric(!!total_diff)))
  
  # Creating data substitution label, any time there is a negative totalizer difference 
  logs <- logs%>%
    mutate(!!substitution_method := case_when(!!total_diff < 0 ~ 'Invalid Totalizer Difference, Flow Set to Zero',
                                              !!total_diff > 1000000 ~ 'Invalid Totalizer Difference, Flow Set to Zero',
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
    
    # Creating name for flow totalizer difference column
    diff_name <-paste(totalizer,'diff',sep = '_')
    
    # Creating flow variable name
    new_name <- gsub('totalizer','flow',totalizer)
    
    
    # Creating substitution method variable 
    sub_method <- paste(new_name,'valid',sep = '_')
    
  }
  
  # Calling ProcessLogs function creating new variables for totalizer differences 
  processed_logs <- ProcessLogs(processed_logs,get(totalizer),get(diff_name),new_name,sub_method)
  
  
}

rm(diff_name,new_name,sub_method,totalizer)


## Flare Operational Flow ----

FlareOperation <- function(logs,temp,temp_avg,flare_flow){
  # Quoting variables 
  temp <- enquo(temp)
  temp_avg <- enquo(temp_avg)
  flare_flow <- enquo(flare_flow)
  
  # Determining if flare was operational based on temperature (avg temp & actual temp)
  logs<- logs%>% 
    mutate(flare_oper = case_when((!!temp >= 120 | !!temp_avg >=120) & (!!temp < 2000 | !!temp_avg < 2000) ~ 'Operational',
                                  (!!temp <= 120 & !!temp_avg <= 120) | (!!temp > 2000 & !!temp_avg > 2000) ~ 'Non-operational'))
  
  # Declaring flow as operational or non-operational based on F1_oper
  logs<- logs%>%
    mutate(flare_flow_op = case_when(flare_oper == 'Operational' ~ as.integer(flare_flow),
                                     TRUE ~ as.integer(0)))%>%
    mutate(flare_flow_nonop = case_when(flare_oper == 'Non-operational' ~ as.integer(flare_flow),
                                        TRUE ~ as.integer(0)))
  return(logs)
}

# Processing operational/non-operational flare flow 
processed_logs<-FlareOperation(processed_logs,flare_temp,flare_temp_avg,flare_flow)


## Gap Summaries ----

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

# Function to create gap summary for missing timestamps 
TimestampSummary <- function(logs,missing_timestamp){
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
    select(Date,Time,!!missing_timestamp,G1_totalizer_diff,G1_kwh_totalizer_diff,link)
  
  
  
  return(gap_summary)
  
}

### Creating gap summaries ---- 

## Engine 
G1_gap_summary <- EngineSummary(processed_logs,G1_flow_valid,G1_kwh_valid)

## Flare 
flare_gap_summary <- FlareSummary(processed_logs,flare_flow_valid)

## Missing timestamps 
timestamp_summary <- TimestampSummary(processed_logs,missing_timestamp)


# UNCOMMENT FOLLOWING SECTION IF NEED TO PAD MISSING DATA (i.e. create empty rows for missing timestamps)

# ## Padding Missing Data ----- 
# 
# ## Filling in missing timestamps with empty rows (NA) in the 'processed_logs_filled' df
# 
# # Merge 'Date' and 'Time' columns to create a single timestamp column
# processed_logs$date_time <- as.POSIXct(paste(processed_logs$Date, processed_logs$Time), 
#                                        format = "%Y-%m-%d %H:%M:%S",tz = "GMT")
# 
# 
# 
# # Padding dataset for missing timestamps 
# processed_logs_filled <- pad(processed_logs,by = "date_time")
# 
# # Arrange in descending order 
# processed_logs_filled <- processed_logs_filled%>%
#   arrange(desc(date_time))
# 
# # Replacing NA's in the Date and Time columns 
# processed_logs_filled$Date <- as.Date(processed_logs_filled$date_time)
# processed_logs_filled$Time <- format(processed_logs_filled$date_time,format = "%H:%M:%S" )
# 
# 
# 
# # Filling NAs with most recent feasible totalizer value
# 
# ## Replace NAs with the most recent non-NA value in each column
# processed_logs_filled$G1_flow <- na.locf(processed_logs_filled$G1_flow)
# processed_logs_filled$G1_kwh <- na.locf(processed_logs_filled$G1_kwh)
# processed_logs_filled$missing_timestamp <- na.locf(processed_logs_filled$missing_timestamp)
# 
# # Creating data substitution label
# processed_logs_filled <- processed_logs_filled%>%
#   mutate(flow_data_substitution = ifelse(missing_timestamp != 0,
#                                      "Flow Data Gap Substituted",
#                                      "No Substitution"))
# 
# # Function to recalculate the flow value for periods w/ missing timestamps by dividing the 
# # most recent totalizer difference by the # of missing timestamps and applying it across the 
# # range of missing rows
# 
# fill_flow_variables <- function(variables_list, df) {
#   filled_df <- df
#   
#   for (var in variables_list) {
#     filled_df <- filled_df %>%
#       mutate_at(vars({{ var }}), ~ ifelse(missing_timestamp != 0,
#                                           ./missing_timestamp,
#                                           .))
#   }
#   
#   return(filled_df)
# }
# 
# # Define the list of variables to perform the operation on
# variables_list <- c()                          #INPUT
# 
# # Call the function
# processed_logs_filled <- fill_flow_variables(variables_list, processed_logs_filled)
# 
# # Replacing processed_logs with processed_logs_filled
# processed_logs <- processed_logs_filled
# 
# 
# 
# ### Creating gap_summary for filled timestamps --------
# 
# # Function to create gap summary for missing timestamps 
# GapSummary <- function(logs,missing_timestamp){
#   # Quoting variables 
#   missing_timestamp <- enquo(missing_timestamp)
#   
#   # Filtering processed logs where there are missing timestamps
#   gap_summary <- logs%>%
#     
#     # Adding row numbers so hyperlinks can be created
#     mutate(row_numbers = seq.int(nrow(logs))+1)%>%
#     
#     # Filtering for invalid totalizer differences
#     filter(!!missing_timestamp != 0)%>%
#     
#     # Creating hyperlink string to link to processed logs 
#     mutate(link = makeHyperlinkString(sheet = 'Processed Logs',text = 'Link to Processed_Logs',row = row_numbers, col = 1))%>%
#     
#     # Selecting columns for gap summary 
#     ## Adding in G1_diff and G1_kwh to provide justification for use of totalizer values across gaps
#     select(Date,Time,G1_totalizer_diff,G1_flow,G1_kwh_totalizer_diff,G1_kwh,missing_timestamp,link)
#   
#   
#   
#   return(gap_summary)
#   
# }
# 
# # Creating flow gap substitution summary 
# flow_gap_summary <- GapSummary(processed_logs,missing_timestamp)




# UNCOMMENT FOLLOWING SECTION IF USING CONTINUOUS CH4 ANALYZER

# # Weighted CH4 % Calculations ----
# 
# # Filling in missing CH4 values for empty columns
# processed_logs$ch4_meter[processed_logs$ch4_meter ==0]<- NA
# 
# # Calculating rolling mean window of 7 days before the timestamp
# roll_mean_window <- 168 * (60 / 15)  # Window size: 168 hours = 4 * (60 minutes / 15 minutes)
# 
# ## Creating ch4_sub column to calculate rolling average for missing ch4_meter readings -------
# processed_logs <- processed_logs%>%
#   
#   # Create ch4_substitution label
#   mutate(ch4_substitution = ifelse(is.na(ch4_meter),
#                           "CH4 Reading Substituted",
#                           "No Substitution"))%>%
#   # Substitute NA CH4 readings
#   mutate(ch4_sub = ifelse(is.na(ch4_meter),
#                           rollmean(ch4_meter, k = roll_mean_window, align ='right',na.rm = TRUE),
#                           ch4_meter))
# 
# 
# ## Creating Gap Summary for CH4 Substitution
# Ch4_gap_summary <- processed_logs%>%
#   # Adding row numbers so hyperlinks can be created
#   mutate(row_numbers <<- seq.int(nrow(processed_logs))+1)%>%
#   
#   # Creating hyperlink string to link to processed logs "Date" column
#   mutate(link = makeHyperlinkString(sheet = 'Processed Logs',
#                                     text = 'Link to Processed_Logs',
#                                     row = row_numbers, 
#                                     col = 1))%>%
#   
#   # Filtering for instances where ch4_substitution occurred
#   filter(ch4_substitution != 'No Substitution')%>%
#   
#   # Selecting columns
#   select(Date,Time,ch4_meter,ch4_sub,link)
#   
# 
# 
# ## Calculating weighted average for CH4% --------
# 
# # Creating month/year column 
# processed_logs$Month <- as.yearmon(processed_logs$Date)
# 
# # Calculating weighted average, weighting CH4% for each timestamp according to the gas flow (G1_flow)
# weighted_average <- processed_logs%>%
#   group_by(Month)%>%
#   summarise('Weighted Average CH4%' = weighted.mean(ch4_sub,G1_flow,na.rm = TRUE)/100)
# 
# # Removing the month column from the processed_logs df
# processed_logs<- processed_logs%>%
#   select(-Month)



# Cleaning up Final Dataframe ----

## Formatting date for excel ----

# Removing date_time column so excel will format date/time correctly
processed_logs <- processed_logs%>%
  select(- date_time)


### Returning column headers to their original values
for(x in 1:max_header){
  colnames(processed_logs)[x]<- og_headers[x]
}


# Cleaning up Global Directory ----

# Removing items from global directory
rm(og_headers,max_header,totalizer_list,x)

# Garbage Clean
gc()


# Writing Results to Excel ----

# Output file name
date <- format(Sys.Date(),"%m.%d.%y")

file_name = paste(farm,date,'.xlsx', sep = '_')     

# Resetting working directory
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

# Printing available dataframes in global environment
names(which(unlist(eapply(.GlobalEnv,is.data.frame)))) # Choose from this list


# Creating list of dataframes to include as tables in the results spreadsheet
# 'Excel Sheet Name' = Dataframe Name

# UPDATE AS NEEDED
data_tables <- list('Processed Logs'= processed_logs,                         ####### INPUT
                    'Gap Summary' = timestamp_summary,
                    'Engine Gap Summary' = G1_gap_summary,
                    'Flare Gap Summary' = flare_gap_summary)


# Calling function for all dataframes in list 

ToExcel <- function(file_name, data_tables) {
  if (!file.exists(file_name)) {
    createNewExcelFile(file_name, data_tables)
  } else {
    handleExistingFile(file_name, data_tables)
  }
}

createNewExcelFile <- function(file_name, data_tables) {
  cat('File ', file_name, ' doesn\'t exist... Creating new excel file', '\n\n')
  wb <- createWorkbook()
  cat(file_name, ' created in current directory', '\n\n')
  processTables(wb, data_tables)
  saveAndFinish(wb, file_name)
}

handleExistingFile <- function(file_name, data_tables) {
  cat('File: ', file_name, ' already exists...\n\n')
  user_input <- askForOverwrite(file_name)
  
  if (user_input == 'Y') {
    wb <- loadWorkbook(file_name)
    clearAndWriteData(wb, data_tables)
    saveAndFinish(wb, file_name, TRUE)
  } else {
    new_file_name <- askForNewFileName()
    cat('Creating new excel file...\n\n')
    wb <- createWorkbook()
    processTables(wb, data_tables)
    saveAndFinish(wb, new_file_name)
  }
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
      cat("Skipping empty dataframe for sheet:", sheet_name, "\n\n")
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
      cat("Skipping empty dataframe for sheet:", sheet_name, "\n\n")
    }
  }
}

askForOverwrite <- function(file_name) {
  user_prompt <- paste('Do you want to overwrite file:', file_name, '(Y/N) ', sep = ' ')
  cat('\n')
  user_input <- readline(prompt = user_prompt)
  toupper(trimws(user_input))
}

askForNewFileName <- function() {
  new_file_name <- readline(prompt = 'Enter New File Name (with .xlsx extension and no spaces): ')
  new_file_name
}

saveAndFinish <- function(wb, file_name, overwrite = FALSE) {
  saveWorkbook(wb, file_name, overwrite = overwrite)
  cat('Finished...', file_name, ' saved\n')
}



# Writing results to excel
ToExcel(file_name,data_tables)

# Garbage Clean
gc()



