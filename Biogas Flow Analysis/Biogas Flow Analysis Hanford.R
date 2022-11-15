# Biogas Flow Analysis Program 
# Bryan Stozlenburg (Ag Methane Advisors)
# 5.18.22

# Program to process merged gas logs and write results to excel

### Verwey Hanford Biogas Flow Analysis 

# Loading Modules
library(dplyr)
library(tidyr)
library(writexl)
library(lubridate)
library(openxlsx)
library(chron)




### Reading In Merged Logs 

## Reading in Individual Merged Logs 

merged_logs <-read.csv('Verweyhanford_weekly_220831-210901_merged.csv',check.names = FALSE)

# # Reformatting date 
# merged_logs$Date <- strptime(as.character(merged_logs$Date),'%Y/%m/%d')
# merged_logs$Date <- format(merged_logs$Date,'%m/%d/%Y')




### Cleaning up column headers

# Getting rid of the duplicate header names 
merged_logs<-merged_logs[-c(6,8,9,17,18,20,25,26,28)]

# Saving the original column names as 'headers' variable 
headers <- colnames(merged_logs)

# Get number of headers in original dataframe 
max_header <- as.integer(length(headers))

## Creating dataframe of the headers with corresponding index
col_index <- 1:length(headers)

log_columns <- data.frame(col_index,headers)

print(log_columns)

# Creating list of headers to be changed 
indexes_to_change <- c(5,6,9,10,11,17,14,22,19) ### Input the index numbers of headers you want to change

change_headers <- data.frame(indexes_to_change)

log_columns<- log_columns%>% 
  filter(col_index %in% change_headers$indexes_to_change)

print(log_columns)

# Creating datafrmae of new names (Input the new names in the order they appear in the console)
new_names <- c('G1_totalizer',
               'G1_kwh_totalizer',
               'F1_totalizer',
               'thermocouple',
               'average_flare_temp',
               'G2_kwh_totalizer',
               'G2_totalizer',
               'G3_kwh_totalizer',
               'G3_totalizer') ### Input the new names in the order they appear in the console

# Adding the new names to the log_columns dataframe 
log_columns$new_name <- new_names 

# Creating list of new header names 
new_names <- headers

for (x in log_columns$col_index){
  new_names[x]<- log_columns$new_name[log_columns$col_index == x]
}

# Check to make sure headers are correct
print(new_names)

# Changing header names of merged logs 
colnames(merged_logs)<-new_names




### Removing items from global environment 
rm(change_headers,log_columns,col_index,indexes_to_change,new_names,x)





### Processing merged logs

### Calculating raw totalizer differences

# Calculating raw flow per timestamp based on totalizer differences
processed_logs <- merged_logs %>%
  # Calculating G1 flow based on totalizer
  mutate(G1_diff = G1_totalizer - lead(G1_totalizer))%>%
  # Calculating G1 kWH/15 minutes
  mutate(G1_kwh_diff = G1_kwh_totalizer - lead(G1_kwh_totalizer))%>%
  
  #  Calculating F1 flow based on totalizer
  mutate(F1_diff = F1_totalizer - lead(F1_totalizer))%>%
  
  # Calculating G2 flow based on totalizer
  mutate(G2_diff = G2_totalizer - lead(G2_totalizer))%>%
  # Calculating G2 kWH/15 minutes
  mutate(G2_kwh_diff = G2_kwh_totalizer - lead(G2_kwh_totalizer))%>%
  
  # Calculating G3 flow based on totalizer
  mutate(G3_diff = G3_totalizer - lead(G3_totalizer))%>%
  # Calculating G3 kWH/15 minutes
  mutate(G3_kwh_diff = G3_kwh_totalizer - lead(G3_kwh_totalizer))
  



## Creating function to process totalizer values

ProcessLogs<-function(logs,totalizer,total_diff,new_name,substitution_method){
  # Quoting variables in function 
  totalizer <- enquo(totalizer)
  total_diff <- enquo(total_diff)
  new_name <- quo_name(new_name)
  substitution_method <- quo_name(substitution_method)
  
  # Correcting for totalizer zeros/negative totalizer differences 
  logs<- logs%>%
    mutate(!!new_name := case_when(!!totalizer ==0 ~ as.integer(0),
                                   lead(!!totalizer)==0 ~ as.integer(0),
                                   !!total_diff < 0 ~ as.integer(0),
                                   TRUE ~ as.integer(!!total_diff)))
  
  # Creating data substitution label (using same logic as above)
  logs <- logs%>%
    mutate(!!substitution_method := case_when(!!totalizer ==0 ~ 'Invalid Totalizer Difference, Flow Set to Zero',
                                              lead(!!totalizer)==0 ~ 'Invalid Totalizer Difference, Flow Set to Zero',
                                              !!total_diff < 0 ~ 'Invalid Totalizer Difference, Flow Set to Zero',
                                              TRUE ~ "Totalizer Feasible"))
  
  return(logs)
}

## Processing totalizer differences 

# Engine 1 flow 
processed_logs <- ProcessLogs(processed_logs,processed_logs$G1_totalizer,processed_logs$G1_diff, "G1_flow","G1_flow_valid")
# Engine 1 kwh 
processed_logs <- ProcessLogs(processed_logs,processed_logs$G1_kwh_totalizer,processed_logs$G1_kwh_diff, "G1_kwh","G1_kwh_valid")

# Flare flow 
processed_logs <- ProcessLogs(processed_logs,processed_logs$F1_totalizer,processed_logs$F1_diff, "F1_flow","F1_flow_valid")

# Engine 2 flow 
processed_logs <- ProcessLogs(processed_logs,processed_logs$G2_totalizer,processed_logs$G2_diff, "G2_flow","G2_flow_valid")
# Engine 2 kwh 
processed_logs <- ProcessLogs(processed_logs,processed_logs$G2_kwh_totalizer,processed_logs$G2_kwh_diff, "G2_kwh","G2_kwh_valid")

# Engine 3 flow 
processed_logs <- ProcessLogs(processed_logs,processed_logs$G3_totalizer,processed_logs$G3_diff, "G3_flow","G3_flow_valid")
# Engine 3 kwh 
processed_logs <- ProcessLogs(processed_logs,processed_logs$G3_kwh_totalizer,processed_logs$G3_kwh_diff, "G3_kwh","G3_kwh_valid")



## Function to determine operational activity of flare, differentiate flow accordingly 
FlareOperation <- function(logs,thermocouple,flare_flow){
  # Quoting variables 
  thermocouple <- enquo(thermocouple)
  flare_flow <- enquo(flare_flow)
  
  # Determining if flare was operational 
  logs<- logs%>% 
    mutate(F1_oper = case_when(!!thermocouple > 120 & !!thermocouple < 2000 ~ 'Operational',
                                !!thermocouple < 120 | !!thermocouple > 2000 ~ 'Non-operational'))
  
  # Declaring flow as operational or non-operational based on F1_oper
  logs<- logs%>%
    mutate(F1_flow_op = case_when(F1_oper == 'Operational' ~ as.integer(F1_flow),
                                  TRUE ~ as.integer(0)))%>%
    mutate(F1_flow_nonop = case_when(F1_oper == 'Non-operational' ~ as.integer(F1_flow),
                                     TRUE ~ as.integer(0)))
  return(logs)
}

# Processing operational/non-operational flare flow 
processed_logs <- FlareOperation(processed_logs,processed_logs$thermocouple,processed_logs$F1_flow)



# Calculating total flow 
processed_logs<-processed_logs%>%
  mutate(Total_flow = G1_flow + F1_flow_op + F1_flow_nonop + G2_flow + G3_flow)








### Gap Analysis

# Function to create date_time variable
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
  mutate(time_diff = date_time - lead(date_time))




# Converting the time difference to number of minutes
processed_logs$time_diff <- 60 * 24 * as.numeric(times(processed_logs$time_diff))


# Creating a missing timestamp column (1 timestamp = 15 minutes)
processed_logs<-processed_logs%>%
  mutate(missing_timestamp = case_when(time_diff == 15 ~ 0,
                          time_diff > 15 ~ time_diff*(1/15),
                          time_diff == NA ~ 0))



# Converting date from character to date variable
processed_logs$Date<-mdy(processed_logs$Date)

# Dropping the time_diff columns from the final dataset
processed_logs<- processed_logs%>%
  select(-c('time_diff','missing_timestamp'))


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


### Creating gap summary dataframes 

## Creating gap summaries for the engines 
# Engine 1
G1_gap_summary <- EngineSummary(processed_logs,G1_flow_valid,G1_kwh_valid)

# Engine 2 
G2_gap_summary <- EngineSummary(processed_logs,G2_flow_valid,G2_kwh_valid)

# Engine 3
G3_gap_summary <- EngineSummary(processed_logs,G3_flow_valid,G3_kwh_valid)


## Creating gap summary for flare 
F1_gap_summary <- FlareSummary(processed_logs,F1_flow_valid)








### Organizing final results 
# Returning column headers to their original values 
colnames(processed_logs)[1:max_header]<-headers
colnames(merged_logs)[1:max_header]<-headers










### Writing results to excel 




## Writing processed dataframe to excel
print('Starting to write processed logs to xlsx')

# Setting xlsx file name
file_name = 'Hanford Biogas Flow Summary.xlsx'

# Checking to see if file already exists in the directory

if(file.exists(file_name)==TRUE){
  
  # If file already exists, remove the existing tables and append the updated processed logs table and gap summary table
  print('File Exists... Appending to existing file')
  
  # Loading the excel file 
  wb <- loadWorkbook(file_name)
  
  # Removing the existing Processed_Logs table in the 'Biogas Flow tab'
  removeTable(wb,sheet = 'Biogas Flow',table = getTables(wb, 'Biogas Flow'))
  
  
  
  # Adding the processed_logs dataframe to the Biogas Flow tab 
  writeDataTable(wb,sheet = 'Biogas Flow',processed_logs,colName = TRUE,tableName = 'Processed_Logs')
  
  
  
  ### Adding the gap summaries 
  
  # Engine 1
  
  # Removing the existing gap summary table 
  removeTable(wb,sheet = 'Engine 1 Summary', table = getTables(wb,'Engine 1 Summary'))
  # Adding the gap_summary dataframe to the Gap Summary tab
  writeDataTable(wb,sheet = 'Engine 1 Summary',G1_gap_summary,colName = TRUE, tableName = 'G1_Gap_Summary' )
  
  # Adding the hyperlinks to the Gap Summary Table
  writeFormula(wb, sheet = 'Engine 1 Summary',startCol = 5, startRow = 2, x = G1_gap_summary$link)
  
  # Engine 2
  
  # Removing the existing gap summary table 
  removeTable(wb,sheet = 'Engine 2 Summary', table = getTables(wb,'Engine 2 Summary'))
  
  # Adding the gap_summary dataframe to the Gap Summary tab
  writeDataTable(wb,sheet = 'Engine 2 Summary',G2_gap_summary,colName = TRUE, tableName = 'G2_Gap_Summary' )
  
  # Adding the hyperlinks to the Gap Summary Table
  writeFormula(wb, sheet = 'Engine 2 Summary',startCol = 5, startRow = 2, x = G2_gap_summary$link)
  
  # Engine 3
  
  # Removing the existing gap summary table 
  removeTable(wb,sheet = 'Engine 3 Summary', table = getTables(wb,'Engine 3 Summary'))
  
  # Adding the gap_summary dataframe to the Gap Summary tab
  writeDataTable(wb,sheet = 'Engine 3 Summary',G3_gap_summary,colName = TRUE, tableName = 'G3_Gap_Summary' )
  
  # Adding the hyperlinks to the Gap Summary Table
  writeFormula(wb, sheet = 'Engine 3 Summary',startCol = 5, startRow = 2, x = G3_gap_summary$link)
  
  # Flare
  
  # Removing the existing gap summary table 
  removeTable(wb,sheet = 'Flare Summary', table = getTables(wb,'Flare Summary'))
  
  # Adding the gap_summary dataframe to the Gap Summary tab
  writeDataTable(wb,sheet = 'Flare Summary',F1_gap_summary,colName = TRUE, tableName = 'F1_Gap_Summary' )
  
  # Adding the hyperlinks to the Gap Summary Table
  writeFormula(wb, sheet = 'Flare Summary',startCol = 4, startRow = 2, x = F1_gap_summary$link)
  

  
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
  sheet3<- addWorksheet(wb,sheetName = 'Engine 1 Summary')
  sheet4<- addWorksheet(wb,sheetName = 'Engine 2 Summary')
  sheet5<- addWorksheet(wb,sheetName = 'Engine 3 Summary')
  sheet6<- addWorksheet(wb,sheetName = 'Flare Summary')
  
  # Writing dataframe to 'Biogas Flow' tab of the excel workbook
  writeDataTable(wb, sheet = sheet1,processed_logs,colName = TRUE,tableName = 'Processed_Logs')
  
  ### Adding the gap summaries 
  
  # Engine 1
  # Adding the gap_summary dataframe to the Gap Summary tab
  writeDataTable(wb,sheet = 'Engine 1 Summary',G1_gap_summary,colName = TRUE, tableName = 'G1_Gap_Summary' )
  
  # Adding the hyperlinks to the Gap Summary Table
  writeFormula(wb, sheet = 'Engine 1 Summary',startCol = 5, startRow = 2, x = G1_gap_summary$link)
  
  # Engine 2
  # Adding the gap_summary dataframe to the Gap Summary tab
  writeDataTable(wb,sheet = 'Engine 2 Summary',G2_gap_summary,colName = TRUE, tableName = 'G2_Gap_Summary' )
  
  # Adding the hyperlinks to the Gap Summary Table
  writeFormula(wb, sheet = 'Engine 2 Summary',startCol = 5, startRow = 2, x = G2_gap_summary$link)
  
  # Engine 3
  # Adding the gap_summary dataframe to the Gap Summary tab
  writeDataTable(wb,sheet = 'Engine 3 Summary',G3_gap_summary,colName = TRUE, tableName = 'G3_Gap_Summary' )
  
  # Adding the hyperlinks to the Gap Summary Table
  writeFormula(wb, sheet = 'Engine 3 Summary',startCol = 5, startRow = 2, x = G3_gap_summary$link)
  
  # Flare
  # Adding the gap_summary dataframe to the Gap Summary tab
  writeDataTable(wb,sheet = 'Flare Summary',F1_gap_summary,colName = TRUE, tableName = 'F1_Gap_Summary' )
  
  # Adding the hyperlinks to the Gap Summary Table
  writeFormula(wb, sheet = 'Flare Summary',startCol = 4, startRow = 2, x = F1_gap_summary$link)
  
  
  # Saving workbook 
  saveWorkbook(wb,file_name, overwrite = FALSE)
}

print('Finished')

gc()






