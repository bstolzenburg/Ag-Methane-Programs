# Biogas Flow Analysis Program 
# Bryan Stolzenburg (Ag Methane Advisors)
# 1.24.2023

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
}

# Loading modules
LoadModules()


# Readinig in Gas Logs ---------------

# Resetting working directory
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

### Reading In Merged Logs 

# 2: Multiple Biogas Flow CSVs that need to be merged 

### Make sure that all csv's to be read in are in the 'Flow Data' folder located in the same dir as this .R file

# Creating path variable
path <- file.path(getwd(),'Flow Data')     

# Setting working directory based on path 
setwd(path)

# Creating merged logs
merged_logs <- ldply(list.files(),.fun = read.csv,check.names = FALSE,header = TRUE) 



# Getting Farm Configuration from YAML file -----

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

cfg <- ParseYAML('INPUT FARM NAME HERE')                                     ####### INPUT #######

# Getting column headers
column_names <- cfg[[1]]

# Getting index values
indexes <- cfg[[2]]



# Cleaning up Merged Logs -----

# Adding degrees symbol to column headers
colnames(merged_logs) <- gsub('<b0>','\U00B0',colnames(merged_logs))

## Saving original headers

# Correcting for duplicate header names 
colnames(merged_logs) <- tidy_names(colnames(merged_logs),syntactic = FALSE)

# Saving the original column names as 'headers' variable
og_headers <- colnames(merged_logs)



# Get number of headers in original dataframe 
# -1 to exclude date_time column from original headers
max_header <- as.integer(length(og_headers))

### Cleaning up column headers



### Formatting Dates and Times and sorting dataframe

# Convert all dates to same format
merged_logs$Date <- as.Date(parse_date_time(merged_logs$Date,c('mdy','ymd')))
merged_logs$Date <-format.Date(merged_logs$Date, '%m/%d/%Y' )



# Function to create date_time variable from separate date/time columns
DateTime<- function(logs){
  # Convert Time from character to Chron
  logs$Time <- chron(times. = logs$Time)
  
  # Merge Date and Time to create date_time column
  logs$date_time <- chron(dates. = as.character(logs$Date),times. = logs$Time)
  
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



# Cleaning Gas Log Column Headers ---- 

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


### Processing merged logs

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
      mutate(!! diff_name := case_when(date_time == time1 ~ as.integer(0),
                                    TRUE ~ as.integer(get(totalizer) - lead(get(totalizer)))))
  }
  
  return(processed_logs)
}

processed_logs <- TotalizerDiff(merged_logs,totalizer_list)




# Processing Totalizer Values
ProcessLogs<-function(logs,totalizer,total_diff,new_name,substitution_method){
  # Quoting variables in function 
  totalizer <- enquo(totalizer)
  total_diff <- enquo(total_diff)
  new_name <- quo_name(new_name)
  substitution_method <- quo_name(substitution_method)
  
  # Correcting for zeros in the totalizer columnn  
  logs<- logs%>%
    mutate(!!new_name := case_when(!!totalizer == 0 ~ as.integer(0),
                                   !!total_diff < 0 ~ as.integer(0),
                                   !!total_diff > 1000000 ~ as.integer(0),
                                   TRUE ~ as.integer(!!total_diff)))
  
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

## Flare Operational Activity
FlareOperation <- function(logs,thermocouple,flare_flow){
  # Quoting variables 
  thermocouple <- enquo(thermocouple)
  flare_flow <- enquo(flare_flow)
  
  # Converting flare flow to numeric
  flare_flow <- as.numeric(flare_flow)
  
  # Determining if flare was operational based on temperature
  logs<- logs%>% 
    mutate(flare_oper = case_when(!!thermocouple >= 120 & !!thermocouple < 2000 ~ 'Operational',
                               !!thermocouple <= 120 | !!thermocouple > 2000 ~ 'Non-operational'))
  
  # Declaring flow as operational or non-operational based on F1_oper
  logs<- logs%>%
    mutate(flare_flow_op = case_when(flare_oper == 'Operational' ~ !!flare_flow,
                                  TRUE ~ 0))%>%
    mutate(flare_flow_nonop = case_when(flare_oper == 'Non-operational' ~ !!flare_flow,
                                     TRUE ~ 0))
  return(logs)
}

# Processing operational/non-operational flare flow 
processed_logs<-FlareOperation(processed_logs,flare_temp,flare_flow)



# Flow Totals ----

# Calculating total flow 
processed_logs<-processed_logs%>%
  mutate(Total_flow = G1_flow + flare_flow_op)





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
  select(-c('time_diff'))



# Gap Summaries ----

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



### Creating gap summaries 

## Engine 
G1_gap_summary <- EngineSummary(processed_logs,G1_flow_valid,G1_kwh_valid)

## Flare 
flare_gap_summary <- FlareSummary(processed_logs,flare_flow_valid)

## Missing timestamps 
timestamp_gap_summary <- TimestampSummary(processed_logs,missing_timestamp)




# Cleaning up Final Dataframe ----

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

# Resetting working directory
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

# Output file name
file_name = 'INSERT FILE NAME HERE.xlsx'                                              ####### INPUT #######

# Printing available dataframes in global environment
names(which(unlist(eapply(.GlobalEnv,is.data.frame)))) # Choose from this list


# Creating list of dataframes to include as tables in the results spreadsheet
# 'Excel Sheet Name' = 'Dataframe Name'
data_tables <- list('Processed Logs'= 'processed_logs',                               ####### INPUT #######
                    'Gap Summary' = 'timestamp_gap_summary',
                    'Engine Gap Summary' = "G1_gap_summary",
                    'Flare Gap Summary' = 'flare_gap_summary')



# Function to write results tables to excel spreadsheet
ToExcel <- function(file,tables_list){
  
  # Check if file exists
  if(file.exists(file)== FALSE){
    
    print("File doesn't exist... Creating new excel file")
    
    # Create excel workbook
    wb <- createWorkbook(file_name)
    
    # Iterate through list of dataframes in list and create worksheets/add data tables
    for (i in 1:length(data_tables)){
      # Extracting dataframe from list item
      df <- get(as.character(data_tables[i]))
      
      # Get dataframe name 
      df_name <- names(data_tables[i])
      
      # Get excel table name
      table_name <- gsub(' ','_',df_name)
      
      # Add excel sheet for data tables
      sheet1 <- addWorksheet(wb,sheetName = df_name)
      
      # Add data table to sheet 
      writeDataTable(wb, sheet = sheet1,df,colName = TRUE,tableName = table_name)
      
      # Writing hyperlink formulas for gap summary
      if (isTRUE(grepl('Gap',df_name,ignore.case = TRUE))){
        
        # Get index of link column 
        link_num <- grep('link',colnames(df),ignore.case = TRUE)
        
        # Get link variable name 
        link <- df$link
        
        # Adding the hyperlinks to the Gap Summary Table
        writeFormula(wb, sheet = sheet1,startCol = link_num , startRow = 2, x = link)
      }
    }
    
    # Saving workbook 
    saveWorkbook(wb,file_name)
  
    print('Finished')
    
  }else{
    
    # If file already exists
    print('File already exists...')
    
    user_prompt <- paste('Do you want to overwrite file:',file_name,'(Y/N) ',sep = ' ')
    
    # Getting user input
    user_input <- readline(prompt = user_prompt)
    
    # Cleaning user input
    user_input <- toupper(user_input)
    
    user_input <- trimws(user_input)
    
    # Checking user input
    if(user_input == 'Y'){
      
      print('Overwriting file...')
      
      # Loading existing workbook
      wb <- loadWorkbook(file_name)
      
      
      # Iterate through list of dataframes in list and create worksheets/add data tables
      for (i in 1:length(data_tables)){
        # Extracting dataframe from list item
        df <- get(as.character(data_tables[i]))
        
        # Get dataframe name 
        df_name <- names(data_tables[i])
        
        # Get excel table name
        table_name <- gsub(' ','_',df_name)
        
        # Removing the existing table in the 'Biogas Flow tab'
        removeTable(wb,sheet = df_name,table = getTables(wb, df_name))
        
        # Add data table to sheet 
        writeDataTable(wb, sheet = df_name,df,colName = TRUE,tableName = table_name)
        
        # Writing hyperlink formulas for gap summary
        if (isTRUE(grepl('Gap',df_name,ignore.case = TRUE))){
          
          # Get index of link column 
          link_num <- grep('link',colnames(df),ignore.case = TRUE)
          
          # Get link variable name 
          link <- df$link
          
          # Adding the hyperlinks to the Gap Summary Table
          writeFormula(wb, sheet = df_name,startCol = link_num , startRow = 2, x = link)
        }
      }
      
      # Saving workbook 
      saveWorkbook(wb,file_name,overwrite = TRUE)
      
      print('Finished')
      
    }else{
      
      # Get new file name
      file_name <- readline(prompt = 'Enter New File Name (with .xlsx extension): ')
      
      print('Creating new excel file...')
      
      # Create excel workbook
      wb <- createWorkbook(file_name)
      
      # Iterate through list of dataframes in list and create worksheets/add data tables
      for (i in 1:length(data_tables)){
        # Extracting dataframe from list item
        df <- get(as.character(data_tables[i]))
        
        # Get dataframe name 
        df_name <- names(data_tables[i])
        
        # Get excel table name
        table_name <- gsub(' ','_',df_name)
        
        # Add excel sheet for data tables
        sheet1 <- addWorksheet(wb,sheetName = df_name)
        
        # Add data table to sheet 
        writeDataTable(wb, sheet = sheet1,df,colName = TRUE,tableName = table_name)
        
        # Writing hyperlink formulas for gap summary
        if (isTRUE(grepl('Gap',df_name,ignore.case = TRUE))){
          
          # Get index of link column 
          link_num <- grep('link',colnames(df),ignore.case = TRUE)
          
          # Get link variable name 
          link <- df$link
          
          # Adding the hyperlinks to the Gap Summary Table
          writeFormula(wb, sheet = sheet1,startCol = link_num , startRow = 2, x = link)
        }
      }
      # Saving workbook 
      saveWorkbook(wb,file_name,overwrite = TRUE)
      
      print('Finished')
    }
  }
}

# Writing results to excel
ToExcel(file_name,tables)

