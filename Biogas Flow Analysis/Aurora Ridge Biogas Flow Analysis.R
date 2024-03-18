# Biogas Flow Analysis Program 
# Bryan Stolzenburg (Ag Methane Advisors)
# 3.15.24


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
}

# Loading modules
LoadModules()


# Reading in Gas Logs ---------------

# Resetting working directory
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

### Reading In Merged Logs 

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

farm <- 'Aurora Ridge'                                       ####### INPUT -----
cfg <- ParseYAML(farm)                                     

# Getting column headers
column_names <- cfg[[1]]

# Getting index values
indexes <- cfg[[2]]



# Cleaning Merged Logs -----

# Adding degrees symbol to temperature column headers
colnames(merged_logs) <- gsub('<b0>','\U00B0',colnames(merged_logs))

## Saving original headers to be re-applied at end of program

# Saving the original column names as 'headers' variable
og_headers <- colnames(merged_logs)

# Get number of headers in original dataframe 
max_header <- as.integer(length(og_headers))




## Cleaning up column headers ----

## Formatting Dates and Times and sorting dataframe ----

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
totalizer_list <- column_names[grepl('totalizer',column_names)]
print('Printing Totalizer Columns...')
print(totalizer_list)


## Initial Process of merged logs ----

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
    mutate(!!new_name := case_when(lead(!!totalizer) == 0 & !!totalizer !=0 ~ 0,
                                   !!total_diff < 0 ~ 0,
                                   TRUE ~ as.numeric(!!total_diff)))
  
  # Creating data substitution label, any time there is a negative totalizer difference, or if there is an extremely large difference, or difference from zero
  logs <- logs%>%
    mutate(!!substitution_method := case_when(!!total_diff < 0 ~ 'Invalid Totalizer Difference, Flow Set to Zero',
                                              lead(!!totalizer) == 0 & !!totalizer !=0 ~ 'Invalid Totalizer Difference, Flow Set to Zero',
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






# Operational Activity ----

## Engine Operational Activity ---
### For periods where there was missing timestamps > 1 week, flow must be considerd non-op
EngineOperation <- function(logs,G1_flow){
  # Quoting variables 
  G1_flow <- enquo(G1_flow)
  
  # Determining if engine experienced a gap > 672 timestamps (7 days) which must be considered non-operational
  logs<- logs%>% 
    mutate(G1_oper = case_when(missing_timestamp > 672 | engine_lag > 672 ~ 'Non-operational, gap > 1 week',
                               TRUE ~ 'Operational'))
  
  # Declaring flow as operational or non-operational based on G1_oper
  logs<- logs%>%
    mutate(G1_flow_op = case_when(G1_oper == 'Operational' ~ G1_flow,
                                     TRUE ~ as.integer(0)))%>%
    mutate(G1_flow_nonop = case_when(G1_oper != 'Operational' ~ G1_flow,
                                        TRUE ~ as.integer(0)))
  return(logs)
}

processed_logs <- EngineOperation(processed_logs,G1_flow)

# Cleaning up directory
rm(engine_lag,flare_lag,flare_lag_filled,non_zero_engine,non_zero_flare)


## Flare Operational Activity ----
FlareOperation <- function(logs,temp,temp_avg,flare_flow){
  # Quoting variables 
  temp <- enquo(temp)
  temp_avg <- enquo(temp_avg)
  flare_flow <- enquo(flare_flow)
  
  # Determining if flare was operational based on temperature (avg temp & actual temp), missing timestamps and length of totalizer lag
  logs<- logs%>% 
    mutate(flare_oper = case_when((!!temp >= 120 | !!temp_avg >=120) & (!!temp < 2000 | !!temp_avg < 2000) & flare_flow_valid != 'Totalizer stuck for more than 24 hours, zero BDE assumed for totalizer' & missing_timestamp ==0  ~ 'Operational',
                                  (!!temp <= 120 & !!temp_avg <= 120) | (!!temp > 2000 & !!temp_avg > 2000) | flare_flow_valid == 'Totalizer stuck for more than 24 hours, zero BDE assumed for totalizer' | missing_timestamp !=0  ~ 'Non-operational'))
  
  # Declaring flow as operational or non-operational based on F1_oper
  logs<- logs%>%
    mutate(flare_flow_op = case_when(flare_oper == 'Operational' ~ flare_flow,
                                     TRUE ~ as.integer(0)))%>%
    mutate(flare_flow_nonop = case_when(flare_oper == 'Non-operational' ~ flare_flow,
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

# Gap summary for totalizer lag substitutions
flare_lag_summary <- processed_logs%>%
  mutate(row_numbers = seq.int(nrow(processed_logs))+1)%>%
  filter(flare_lag !=0)%>%
  mutate(link = makeHyperlinkString(sheet = 'Processed Logs',text = 'Link to Processed_Logs',row = row_numbers, col = 1))%>%
  select(Date,Time,flare_totalizer,flare_totalizer_diff,flare_flow,average_flare_flow,flare_lag,flare_flow_valid,link)



### Creating gap summaries ---- 

## Engine 
G1_gap_summary <- EngineSummary(processed_logs,G1_flow_valid,G1_kwh_valid)

## Flare 
flare_gap_summary <- FlareSummary(processed_logs,flare_flow_valid)

## Missing timestamps 
timestamp_summary <- TimestampSummary(processed_logs,missing_timestamp)


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

# Creating Biogas Flow Summary Table -----

flow_summary <- processed_logs%>%
  group_by(Month = floor_date(Date,'month'))%>%
  summarise('Engine Flow (SCF)'= sum(G1_flow),
            'Flare Flow (Op)'= sum(flare_flow_op,na.rm = TRUE),
            'Flare Flow (Non-Op)' = sum(flare_flow_nonop,na.rm = TRUE),
            'Engine kWH' = sum(G1_kwh))


# Writing Results to Excel ----

# Resetting working directory
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

# Output file name
date <- format(Sys.Date(),"%m.%d.%y")

file_name = paste(farm,'BiogasFlowSummary',date,'.xlsx', sep = '_')                                  

# Printing available dataframes in global environment
names(which(unlist(eapply(.GlobalEnv,is.data.frame)))) # Choose from this list


# Creating list of dataframes to include as tables in the results spreadsheet
# 'Excel Sheet Name' = 'Dataframe Name'
# UPDATE AS NEEDED
data_tables <- list('Processed Logs'= processed_logs,                         ####### INPUT-----
                    'Flow Summary' = flow_summary,
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

