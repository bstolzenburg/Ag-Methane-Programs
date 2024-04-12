# Bryan Stolzenburg 
# Madera Electricity Query File 
# 4.5.24
## Updates: 
## Added code to dynamically set working environment

library(conflicted)
library(dplyr)
library(tidyverse)
library(readxl)
library(zoo)
library(lubridate)
library(openxlsx)

# Resolving conflicts with packages 
## Filter
conflicts_prefer(dplyr::filter)


# Setting Working Environment -----
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
                        'Data',
                        'Electricity',
                        'LCFS Electricity Data')

# Check if the current working directory is not the target directory
setwd(target_dir)

  
# Function to combine .xlsx files in folder and summarize
GetData <- function(folder){
  
  ## Setting path to folder with data
  
  # Creating path variable
  path <- file.path(getwd(), folder) 
  
  # Set working directory
  setwd(path)
  
  message('Getting data: ',folder)
  
  ## Importing data
  
  # Initialize empty df
  df <- NULL
  
  # Initialize list to store filenames with NA values
  files_with_na <- c()
  
  # Initialize list to store filenames with different column names
  files_with_different_columns <- c()
  
  # Read .xlsx files in folder into dataframe
  tryCatch({
    file_list <- list.files()
    first_file <- read_excel(file_list[1], na = "")  # Read the first file
    df_list <- lapply(file_list, function(file) {
      tryCatch({
        data <- read_excel(file, na = "")
        if (any(is.na(data))) {
          files_with_na <<- c(files_with_na, file)
          data[is.na(data)] <- 0  # Replace NA values with zeros
        }
        return(data)
      }, error = function(e) {
        message("Error reading file:", file, "-", conditionMessage(e), "\n")
        return(NULL)
      })
    })
    
    # Check for files with different column names
    for (i in seq_along(df_list)) {
      if (!identical(colnames(first_file), colnames(df_list[[i]]))) {
        files_with_different_columns <- c(files_with_different_columns, file_list[i])
      }
    }
    
    # Combine all data frames in the list
    df <- plyr::ldply(df_list, .id = "file")
  }, error = function(e) {
    message("Error accessing files in folder:", conditionMessage(e), "\n")
  })
  
  # Check if df is still NULL
  if (is.null(df)) {
    cat("No valid data was loaded due to errors.\n")
  } else {
    message("Data loaded successfully.\n")
    # Further data processing if needed
  }
  
  # Identify files with NA values
  if (length(files_with_na) > 0) {
    message("The following files contain NA values:", paste(files_with_na, collapse = ", "), "\n")
  }
  
  # Identify files with different column names compared to the first file
  if (length(files_with_different_columns) > 0) {
    message("The following files have different column names compared to the first file:", paste(files_with_different_columns, collapse = ", "), "\n")
  }
  
  # Reset working directory to 'LCFS Electricity Data' file directory
  setwd(target_dir)
  
  return(df)
}

# Reading in PGE Data -----
pge_data <- GetData('PG&E Data')

# Cleaning PG&E Data ----

# Renaming columns 
names(pge_data)[names(pge_data) == 'elec_intvl_end_dttm'] <- 'Timestamp'
names(pge_data)[names(pge_data) == 'kwh_d'] <- 'kwh_delivered'
names(pge_data)[names(pge_data) == 'kwh_r'] <- 'kwh_received'

# Removing columns 
pge_data <- pge_data %>%
  select(Timestamp, kwh_received, kwh_delivered)

# Removing duplicate timestamps 
pge_data <- pge_data %>%
  distinct()



## Crosschecking pge_data ----
pge_xchk <- pge_data%>%
  filter(is.na(Timestamp))

if(length(pge_xchk > 0)){
  message('Following NAs found')
  print(head(pge_xchk))
}else{
  message('No NAs found')
}

# Summarizing PG&E Data ----

# Summarizing data by month 
pge_monthly_summary <- pge_data %>%
  group_by(Month = floor_date(Timestamp, 'month')) %>%
  summarise('kWH Received' = sum(kwh_received),
            'kWh Delivered' = sum(kwh_delivered))



# Filter dates (> August 2021)
pge_monthly_summary <- pge_monthly_summary %>%
  filter(Month > '2021-07-01')



# Reading in Also Energy Data ----
alsoenergy_data <- GetData('Also Energy Data')

# Cleaning Also Energy Data ----
names(alsoenergy_data)[1] <- 'Timestamp'
names(alsoenergy_data)[2] <- 'G1_kwh'

# Converting to correct timezone 
alsoenergy_data$Timestamp <- with_tz(alsoenergy_data$Timestamp,tzone = 'UTC')


# Removing duplicate rows
alsoenergy_data <- alsoenergy_data%>%
  arrange(Timestamp)%>%
  mutate(occurance = row_number())%>%
  distinct_all()
  

## Crosschecking alsoenergy_data ----
alsoenergy_xchk <- alsoenergy_data%>%
  filter(is.na(Timestamp))

if(length(alsoenergy_xchk > 0)){
  message('Following NAs found')
  print(head(alsoenergy_xchk))
}else{
  message('No NAs found')
}

# Crosschecking
start <- ymd_hms('2023-11-01 00:00:00')
end <- ymd_hms('2023-12-01 00:00:00')

xchk <- alsoenergy_data%>%
  filter(Timestamp >= start & Timestamp < end)

print(sum(xchk$G1_kwh))


# Summarizing Also Energy Data ----

# Summarizing data by month
alsoenergy_monthly_summary <- alsoenergy_data %>%
  group_by(Month = floor_date(Timestamp, 'month')) %>%
  summarise('Engine Production (kWh)' = sum(G1_kwh, na.rm = TRUE))

# Filter dates (> August 2021)
alsoenergy_monthly_summary <- alsoenergy_monthly_summary %>%
  filter(Month > '2021-07-01')



# Formatting dates for excel ----
pge_monthly_summary$Month <- format(pge_monthly_summary$Month,'%m/%d/%Y')
alsoenergy_monthly_summary$Month <- format(alsoenergy_monthly_summary$Month,'%m/%d/%Y')


# Export data to Excel -----

# Create list of dataframes to export 
data_tables <- list('PG&E Data'= pge_monthly_summary,
                    'Also Energy Data' = alsoenergy_monthly_summary)

# Generating File Name
date <- format(Sys.Date(),"%m.%d.%y")

file_name = paste('Madera Electricity Summary WORKING_',date,'.xlsx',sep = '')

ToExcel <- function(file_name, data_tables) {
  # Check if the file already exists
  if (file.exists(file_name)) {
    cat('File: ', file_name, ' already exists...\n\n')
    
    user_prompt <- readline(prompt = paste('Do you want to overwrite file:', file_name, '(Y/N) ', sep = ' '))
    cat('\n')
    
    user_input <- toupper(trimws(user_prompt))
    
    # If user selects yes (they want to overwrite)
    if (user_input == 'Y') {
      print('Overwriting file...')
    } else {
      new_file_name <- readline(prompt = 'Enter New File Name (with .xlsx extension): ')
      cat('Creating new excel file...\n\n')
      file_name <- new_file_name
    }
  }
  
  # Creating workbook
  wb <- createWorkbook()
  
  # Iterating through dataframes in list 'data_tables' and adding data and worksheets to .xlsx file
  for (sheet_name in names(data_tables)) {
    # Creating sheet name
    df <- data_tables[[sheet_name]]
    
    # If there are rows in the dataframe, create a sheet and write the data
    if (nrow(df) > 0) {
      sheet1 <- addWorksheet(wb, sheetName = sheet_name)
      writeData(wb, sheet = sheet1, x = df, colNames = TRUE)
      
      # Fit columns to width
      for (col in 1:ncol(df)) {
        setColWidths(wb, sheet = sheet1, cols = col, widths = "auto")
      }
    } else {
      cat("Skipping empty dataframe for sheet:", sheet_name, "\n\n")
    }
  }
  
  # Saving workbook with specified file name
  saveWorkbook(wb, file_name, overwrite = TRUE)
  cat('Finished, workbook saved\n\n')
}



# Writing results to excel 
ToExcel(file_name,data_tables)






