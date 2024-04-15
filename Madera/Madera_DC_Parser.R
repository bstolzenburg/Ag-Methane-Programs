# Program to parse .xls monitor reports

library(plyr)
library(readxl)
library(dplyr)
library(stringr)
library(tibble)

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


# Read all and merge horozontally
all_data <- do.call(cbind,lapply(file_list,read_xls))

# Identifying duplicated columns
duplicates <- duplicated(names(all_data))

# Removing duplicated columns 
cleaned_df <- all_data[,!duplicates]

# Removing "Goal" Column
cleaned_df <- cleaned_df%>%
  select(-Goal)

# Transposing data 
transposed_df <- t(cleaned_df)

# Converting back to dataframe
transposed_df <- as.data.frame(transposed_df)

# Getting first row to use as column names
colnames(transposed_df) <- as.character(unlist(transposed_df[1,]))

# Remove the duplicate first row
transposed_df <- transposed_df[-1,]

# Making row names the "Date" Column
transposed_df <- transposed_df%>%
  rownames_to_column('Date')

# Converting date column to formal "Date"
transposed_df$Date <- as.Date(as.numeric(transposed_df$Date), origin = '1899-12-30')

# Sorting by dates ascending
final_df <- transposed_df %>%
  arrange(Date)

# Removing data columns we dont need 
final_df <- final_df[,-c(2:36)]


# Getting file path for output
result_path <- 'Madera_monitor_parsed.csv'

# Writing results to .csv
write.csv(final_df,file = result_path, row.names = FALSE)




