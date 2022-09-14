![ag meth advisors letterhead 2_0](https://user-images.githubusercontent.com/111012013/185433382-fa9893fa-843a-42da-b9ac-e35d2ea4574d.jpeg)

# Documentation for Madera Biogas Flow Analysis

**8/18/2022**

**Bryan Stolzenburg**

# Program Details

- Program to aggregate/process carbon catcher biogas logs and calculate monthly flow totals:
  - Read in raw biogas logs (.csv) to R dataframe
  - Process biogas logs / calculate monthly flow totals
  - Differentiate operational/non-operational flow
  - Format processed logs
  - Write processed logs to excel spreadsheet

The following document outlines the program. Chunks of code are outlined in boxes, with descriptions above each code chunk. 

## Loading R Modules

**Modules for the following:**

- Reading excel data into R 
- Handling time series data
- Writing processed data to excel 
- Formatting excel results 

```r
# Biogas Flow Analysis Program 
# Bryan Stozlenburg (Ag Methane Advisors)
# 7.19.22

# Program to process merged_logs and write results to excel file


# Loading Program Modules
library(readr)
library(dplyr)
library(tidyr)
library(writexl)
library(lubridate)
library(openxlsx)
library(chron)
```

# Reading Raw Gas Logs

- Reading raw biogas logs (.csv) into R dataframe
- Editing column headers
  - Saving original column headers

```r
### Reading In Merged Logs 

# Reading raw logs into merged_logs dataframe

merged_logs <- read.csv('verweymadera_weekly_2022_merged.csv',check.names = FALSE)


# Saving the original column names as 'headers' variable
headers <- colnames(merged_logs)

# Get number of headers in original dataframe 
max_header <- as.integer(length(headers))



### Cleaning up column headers

## Creating dataframe of the headers, along with corresponding index
col_index <- 1:length(headers)

log_columns <- data.frame(col_index,headers)

print(log_columns)

# Creating list of headers to be changed 
indexes_to_change <- c(5,7,19,20,21) ###### Input the index for the headers to be changed 

change_headers <- data.frame(indexes_to_change)

# Filtering for only the columns where the header must be changed
log_columns<- log_columns%>% 
  filter(col_index %in% change_headers$indexes_to_change)

# Printing the column headers that will be changed 
print(log_columns)

# Creating datafrmae of new names (Input the new names in the order they appear in the console)
new_names <- c('engine_totalizer',
               'engine_kwh',
               'flare_totalizer',
               'flare_temp',
               'average_flare_temp') ### Input the new names in the order they appear in the console

# Adding the new names to the log_columns dataframe 
log_columns$new_name <- new_names 

# Checking to make sure the new names are correct 
print(log_columns) # Check the console to make sure the columns are correctly named

# Creating list of new header names 
new_names <- headers

# Changing the names of the headers in the new_names item dataframe
for (x in log_columns$col_index){
  new_names[x]<- log_columns$new_name[log_columns$col_index == x]
}

# Check to make sure headers are correct
print(new_names)

# Changing header names of merged logs 
colnames(merged_logs)<-new_names

# Removing items from the global directory 
rm(change_headers, log_columns,col_index,indexes_to_change,new_names,x)
```

## Processing Raw Logs

### Calculating engine and flare flow based on totalizer values

```r
### Processing merged logs


# Calculating engine flow based on totalizer values
processed_logs <- merged_logs %>%
  # Calculating G1 flow based on totalizer
  mutate(G1_diff = engine_totalizer - lead(engine_totalizer))%>%
  # Calculating G1 kWH/15 minutes
  mutate(G1_kwh_diff = engine_kwh - lead(engine_kwh))%>%
  #  Calculating F1 flow based on totalizer
  mutate(F1_diff = flare_totalizer - lead(flare_totalizer))
```

### Correcting Totalizer Values

Correcting totalizer values when there is: 

- Zeros in totalizer column 
- Negative totalizer differences 

Creates data substitution label when corrections are made

```r
## Function to process and correct totalizer values
ProcessLogs<-function(logs,totalizer,total_diff,new_name,substitution_method){
  # Quoting variables in function 
  totalizer <- enquo(totalizer)
  total_diff <- enquo(total_diff)
  new_name <- quo_name(new_name)
  substitution_method <- quo_name(substitution_method)

  # Correcting for zeros in the totalizer columnn  
  logs<- logs%>%
    mutate(!!new_name := case_when(!!totalizer ==0 ~ as.integer(0),
                                   !!total_diff < 0 ~ as.integer(0),
                                   TRUE ~ as.integer(!!total_diff)))

  # Creating data substitution label (using same logic as above)
  logs <- logs%>%
    mutate(!!substitution_method := case_when(!!totalizer ==0 ~ 'Invalid Totalizer Difference, Flow Set to Zero',
                                              !!total_diff < 0 ~ 'Invalid Totalizer Difference, Flow Set to Zero',
                                              TRUE ~ "Totalizer Feasible"))

  return(logs)
}


## Function Use 
# ProcessLogs(processed_logs,device totalizer, device totalizer difference, name of new flow column, name of new flow validation column)


### Calling the funciton to process totalizer values 

## Processing totalizer values 
# Engine 1 flow 
processed_logs <- ProcessLogs(processed_logs,engine_totalizer,G1_diff, "G1_flow","G1_flow_valid")
# Engine 1 kwh
processed_logs <- ProcessLogs(processed_logs,engine_kwh,G1_kwh_diff, "G1_kwh","G1_kwh_valid")

# Flare flow 
processed_logs<- ProcessLogs(processed_logs,flare_totalizer,F1_diff,'F1_flow','F1_flow_valid')
```

### Determining Flare Operational Status

Differentiates flare flow based on **thermocouple temperature (between 120 and 2000 C)** 

```r
## Function to determine operational activity of flare, differentiate flow accordingly 
FlareOperation <- function(logs,thermocouple,flare_flow){
  # Quoting variables 
  thermocouple <- enquo(thermocouple)
  flare_flow <- enquo(flare_flow)

  # Determining if flare was operational 
  logs<- logs%>% 
    mutate(F1_oper = case_when(!!thermocouple >= 120 & !!thermocouple < 2000 ~ 'Operational',
                               !!thermocouple <= 120 | !!thermocouple > 2000 ~ 'Non-operational'))

  # Declaring flow as operational or non-operational based on F1_oper
  logs<- logs%>%
    mutate(F1_flow_op = case_when(F1_oper == 'Operational' ~ as.integer(F1_flow),
                                  TRUE ~ as.integer(0)))%>%
    mutate(F1_flow_nonop = case_when(F1_oper == 'Non-operational' ~ as.integer(F1_flow),
                                     TRUE ~ as.integer(0)))
  return(logs)
}

# Processing operational/non-operational flare flow 
processed_logs<-FlareOperation(processed_logs,flare_temp,F1_flow)
```

## Calculating Total Flow

```r
# Calculating total flow 
processed_logs<-processed_logs%>%
  mutate(Total_flow = G1_flow + F1_flow)
```

## Gap Analysis

Checking for missing timestamps, creating 'missing timestamps' column that counts the number of missing timestamps for each row

```r
### Gap Analysis


# Converting date to correct format 

#processed_logs$Date <-format.Date(processed_logs$Date,"%m/%d/%Y")



# Function to create date_time variable from separate date/time columns
DateTime<- function(logs){
  # Convert Time from character to Chron
  logs$Time <- chron(times. = logs$Time)

  # Merge Date and Time to create date_time column
  logs$date_time <- chron(dates. = logs$Date,times. = logs$Time)

  return(logs)
}

# Creating date_time variable
processed_logs <- DateTime(processed_logs)



# Calculating the time difference based on date_time
processed_logs<-processed_logs%>%
  mutate(time_diff = as.numeric(difftime(date_time,lead(date_time),units = c('mins'))))




# Creating a missing timestamp column (1 timestamp = 15 minutes)
processed_logs<-processed_logs%>%
  mutate(missing_timestamp = case_when(time_diff == 15 ~ 0,
                                       time_diff > 15 ~ time_diff/15,
                                       is.na(time_diff)==TRUE ~ 0))


# Converting date from character to date variable
processed_logs$Date<-mdy(processed_logs$Date)

# Dropping columns from the final dataframe
processed_logs<- processed_logs%>%
  select(-c('time_diff','date_time'))
```

## Creating Gap Summary

Creating summary table to identify the following for all devices: 

- Totalizer Corrections 
- Missing Timestamps 

```r
### Gap Summaries

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
    mutate(link = makeHyperlinkString(sheet = 'Biogas Flow',text = 'Link to Processed_Logs',row = row_numbers, col = 1))%>%

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
    mutate(link = makeHyperlinkString(sheet = 'Biogas Flow',text = 'Link to Processed_Logs',row = row_numbers, col = 1))%>%

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
    mutate(link = makeHyperlinkString(sheet = 'Biogas Flow',text = 'Link to Processed_Logs',row = row_numbers, col = 1))%>%

    # Selecting columns for gap summary 
    select(Date,Time,!!missing_timestamp,link)



  return(gap_summary)

}


### Creating gap summaries 

## Engine 
G1_gap_summary <- EngineSummary(processed_logs,G1_flow_valid,G1_kwh_valid)

## Flare 
F1_gap_summary <- FlareSummary(processed_logs,F1_flow_valid)

## Missing timestamps 
timestamp_gap_summary <- TimestampSummary(processed_logs,missing_timestamp)
```

## Writing Processed Logs to Excel

```r
### Writing results to excel 


# Writing processed dataframe to excel

file_name = 'Madera 2022 Biogas Flow Summary.xlsx'

# Checking to see if file already exists 

if(file.exists(file_name)==TRUE){

  # If file already exists, remove the existing table and append the updated processed logs table 
  print('File Exists... Appending to existing file')

  # Loading the excel file 
  wb <- loadWorkbook(file_name)

  ### Adding tables to the excel sheet


  ## Processed Logs
  # Removing the existing table in the 'Biogas Flow tab'
  removeTable(wb,sheet = 'Biogas Flow',table = getTables(wb, 'Biogas Flow'))

  # Adding the processed_logs dataframe to the Biogas Flow tab 
  writeDataTable(wb,sheet = 'Biogas Flow',processed_logs,colName = TRUE,tableName = 'Processed_Logs')



  ## Engine Summary
  # Removing existing engine summary
  removeTable(wb,sheet = 'Engine Summary',table = getTables(wb, 'Engine Summary'))

  # Adding the gap_summary dataframe 
  writeDataTable(wb,sheet = 'Engine Summary',G1_gap_summary,colName = TRUE, tableName = 'G1_Gap_Summary' )

  # Adding the hyperlinks to the Gap Summary Table
  writeFormula(wb, sheet = 'Engine Summary',startCol = 5, startRow = 2, x = G1_gap_summary$link)



  ## Flare Summary
  # Removing existing flare summary
  removeTable(wb,sheet = 'Flare Summary',table = getTables(wb, 'Flare Summary'))

  # Adding the gap_summary dataframe 
  writeDataTable(wb,sheet = 'Flare Summary',F1_gap_summary,colName = TRUE, tableName = 'F1_Gap_Summary' )

  # Adding the hyperlinks to the Gap Summary Table
  writeFormula(wb, sheet = 'Flare Summary',startCol = 4, startRow = 2, x = F1_gap_summary$link)



  ## Gap Summary 
  # Removing existing gap summary
  removeTable(wb,sheet = 'Gap Summary',table = getTables(wb, 'Gap Summary'))

  # Adding the gap_summary dataframe 
  writeDataTable(wb,sheet = 'Gap Summary',timestamp_gap_summary,colName = TRUE, tableName = 'Gap_Summary' )

  # Adding the hyperlinks to the Gap Summary Table
  writeFormula(wb, sheet = 'Gap Summary',startCol = 4, startRow = 2, x = timestamp_gap_summary$link)


  # Saving workbook 
  saveWorkbook(wb,file_name,overwrite = TRUE)


}else{
  # If the file doesnt exist, create a new file and append the processed logs table 
  print('File doesnt exist... Creating new file')

  # Creating excel file
  wb<- createWorkbook(file_name)

  # Creating sheets 
  sheet1<- addWorksheet(wb,sheetName = 'Biogas Flow')
  sheet2<- addWorksheet(wb,sheetName = 'Flow Summary')
  sheet3<- addWorksheet(wb,sheetName = 'Engine Summary')
  sheet4<- addWorksheet(wb,sheetName = 'Flare Summary')
  sheet5<- addWorksheet(wb,sheetName = 'Gap Summary')

  # Writing processed_logs dataframe to 'Biogas Flow' tab
  writeDataTable(wb, sheet = sheet1,processed_logs,colName = TRUE,tableName = 'Processed_Logs')

  ## Engine Summary
  # Adding the gap_summary dataframe 
  writeDataTable(wb,sheet = 'Engine Summary',G1_gap_summary,colName = TRUE, tableName = 'G1_Gap_Summary' )

  # Adding the hyperlinks to the Gap Summary Table
  writeFormula(wb, sheet = 'Engine Summary',startCol = 5, startRow = 2, x = G1_gap_summary$link)



  ## Flare Summary
  # Adding the gap_summary dataframe 
  writeDataTable(wb,sheet = 'Flare Summary',F1_gap_summary,colName = TRUE, tableName = 'F1_Gap_Summary' )

  # Adding the hyperlinks to the Gap Summary Table
  writeFormula(wb, sheet = 'Flare Summary',startCol = 4, startRow = 2, x = F1_gap_summary$link)



  ## Missing Timestamp Summary
  # Adding the gap_summary dataframe 
  writeDataTable(wb,sheet = 'Gap Summary',timestamp_gap_summary,colName = TRUE, tableName = 'Gap_Summary' )

  # Adding the hyperlinks to the Gap Summary Table
  writeFormula(wb, sheet = 'Gap Summary',startCol = 4, startRow = 2, x = timestamp_gap_summary$link)






  # Saving workbook 
  saveWorkbook(wb,file_name, overwrite = FALSE)
}

print('Finished')
```
