# Biogas Flow Analysis Program 
# Bryan Stolzenburg (Ag Methane Advisors)
# 7.17.24

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
                        'Offset')

# Setting working directory
setwd(target_dir)


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
  
  # Creating path to Log_Columns.yml                                            # CUSTOMIZE FILE PATH AS NEEDED
  yml_path <- file.path(path_home(),'Patrick J Wood Dropbox',
                        '_operations',
                        'Software',
                        'Github',
                        'Ag-Methane-Programs',
                        'Biogas Flow Analysis',
                        'Log_Columns.yml')
  
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

farm <- 'Madera'                                                                # ENTER FARM NAME 

cfg <- ParseYAML(farm)                                 

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

## Cleaning Gas Log Column Headers ------------------------------------------------------------------------

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

## Processing merged logs------------

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

# Data Substitution -----------------------------------------

## Padding Missing Data --------------------------

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



## Creating Summary of Padded Data --------------

# Identify the rows with NAs in the original data
missing_data <- processed_logs_filled %>%
  mutate(missing_flag = is.na(Date)) %>%
  arrange(desc(date_time))

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

# Add the values for G1_totalizer and flare_totalizer before start_gap and after end_gap
missing_data <- missing_data %>%
  rowwise() %>%
  mutate(
    before_gap_idx = which.max(processed_logs_filled$date_time <= start_gap) + 1,
    after_gap_idx = which.min(processed_logs_filled$date_time >= end_gap) - 1,
    G1_totalizer_before_gap = processed_logs_filled$G1_totalizer[before_gap_idx],
    flare_totalizer_before_gap = processed_logs_filled$flare_totalizer[before_gap_idx],
    G1_totalizer_after_gap = processed_logs_filled$G1_totalizer[after_gap_idx],
    flare_totalizer_after_gap = processed_logs_filled$flare_totalizer[after_gap_idx]
  ) %>%
  ungroup()

# Select the desired columns for the summary
missing_summary <- missing_data %>%
  select(
    start_gap, end_gap, missing_timestamps, missing_intervals,
    G1_totalizer_before_gap, flare_totalizer_before_gap,
    G1_totalizer_after_gap, flare_totalizer_after_gap
  )






# Replacing NA's in the Date and Time columns in processed_logs_filled
processed_logs_filled$Date <- as.Date(processed_logs_filled$date_time)
processed_logs_filled$Time <- format(processed_logs_filled$date_time,format = "%H:%M:%S" )

rm(missing_data)




## Performing Data Substitution ----------













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







## Creating Gap Summaries ----

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



### Creating gap summaries ---- 
##                                                                              # ADD ADDITIONAL FUNCTION CALLS IF GAP SUMMARY FOR OTHER DEVICES NEEDED
## Engine 
G1_gap_summary <- EngineSummary(processed_logs,G1_flow_valid,G1_kwh_valid)

## Flare 
flare_gap_summary <- FlareSummary(processed_logs,flare_flow_valid)






# Creating Biogas Flow Summary Table -------------------            
                                                                                # CUSTOMIZE FLOW SUMMARY TABLES AS NEEDED

# Create flow summary table 
flow_summary <- processed_logs%>%
  group_by(Month = floor_date(Date,'month'))%>%
  summarise('Engine Flow (SCF)'= sum(G1_flow),
            'Flare Flow OP (SCF)'= sum(flare_flow_op,na.rm = TRUE),
            'Flare Flow Non-Op (SCF)' = sum(flare_flow_nonop,na.rm = TRUE),
            'Engine kWH' = sum(G1_kwh))


# Cleaning up Final Dataframe -----------------------------------------

## Formatting date for excel ----

# Removing date_time column so excel will format date/time correctly
processed_logs <- processed_logs%>%
  select(- date_time,-time_diff)


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

setwd(target_dir)

# Output file names
date <- format(Sys.Date(),"%m.%d.%y")

# Creating file name for .xlsx summary and .csv processed_logs                  # CUSTOMIZE FILE NAME AS NEEDED


file_name = paste('Madera 2023-2024 Offset Flow Summary_WORKING_',date,'.xlsx', sep = '')               

# Printing available dataframes in global environment
names(which(unlist(eapply(.GlobalEnv,is.data.frame)))) # Choose from this list

                                                                                # CUSTOMIZE data_tables AS NEEDED
# Creating list of dataframes to include as tables in the results spreadsheet   
# 'Excel Sheet Name' = 'Dataframe Name'
data_tables <- list('Processed Logs' = processed_logs,
                    'Flow Summary' = flow_summary,
                    'Gap Summary' = timestamp_summary,
                    'Engine Gap Summary' = G1_gap_summary,
                    'Flare Gap Summary' = flare_gap_summary)



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




