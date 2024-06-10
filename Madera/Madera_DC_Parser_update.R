# Program to parse .xls monitor reports

library(plyr)
library(readxl)
library(dplyr)
library(stringr)
library(tibble)
library(tidyverse)

# Setting Dynamic Working Environment -----
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
                        'Livestock',
                        'DC305')

# Setting working directory
setwd(target_dir)


# Creating path variable for the 'Flow Data' folder
path <- file.path(getwd(),'Formatted')




# Getting list of files
file_list <- list.files(path, full.names = TRUE)





# Read and Clean Files ---------------
read_and_clean <- function(file){
  # Read in file 
  data <- suppressMessages(suppressWarnings(read_xls(file,col_names = FALSE)))
  
  
  # Transpose data 
  data_t <- as.data.frame(t(data))
  
  # Using first row as column headers 
  colnames(data_t) <- as.character(unlist(data_t[1,]))
  
  # Removing duplicate first row 
  data_t <- data_t[-1,]
  
  # Renaming the date column 
  names(data_t)[1] <- 'Date'

  # # Removing the "Goal" Row (row 12)
  data_clean <- data_t[-12,]
  
  # Converting serial number to date
  data_clean$Date <- as.Date(as.numeric(data_clean$Date), origin = '1899-12-30')
  
  # Adding file name 
  data_clean <- data_clean%>%
    mutate(file_name = basename(file))
  
  # Setting row names to null
  row.names(data_clean) <- NULL
  
  
  
  
  
  return(data_clean)
  
}


# Read all and merge horozontally
all_data <- lapply(file_list,read_and_clean)%>%
  bind_rows()



# Cleaning final dataframe -----------

# Dropping columns
all_data <- all_data[,-c(2:36)]

# Dropping NAs 
all_data <- all_data%>%
  drop_na()





## Handling duplicates ---------------

# Drop duplicate rows, considering all columns except 'file_name'
## Dropping rows where all of the values are the same
data_cleaned <- all_data%>%
  distinct(across(-file_name),.keep_all = TRUE)

# Identifying rows with duplicate date values
## These are rows with duplicate dates but different livestock values
duplicate_dates <- data_cleaned%>%
  filter(duplicated(Date) | duplicated(Date,fromLast = TRUE))

# Printing duplicates 
message('\n The folloiwng rows contained duplicate dates with different values from separate files.... \n')
duplicate_dates
message('\n Please select the data you want to keep below \n')


## Manually selecting data we want to use for duplicates  ----------------------





# Arranging dataframe in chronological order  
final_df <- data_cleaned %>%
  arrange(Date)









# Writing Results to .csv --------------
# Getting file path for output

# Getting today's date
date <- format(Sys.Date(),"%m.%d.%y")

# Creating file path for working summary file
result_path <- paste('Madera_monitor_parsed_WORKING_',date,'.csv',sep = '')

# Writing results to .csv
write.csv(final_df,file = result_path, row.names = FALSE)




