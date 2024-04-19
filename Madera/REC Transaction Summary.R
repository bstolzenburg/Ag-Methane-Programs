# Load necessary libraries
library(readr)
library(dplyr)
library(purrr)
library(openxlsx)
library(lubridate)

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
                        'WREGIS',
                        'RECs')

# Setting working directory
setwd(target_dir)



# Define the path to the transactions folder
folder_path <- "./Transactions"

# List all CSV files in the folder
csv_files <- list.files(path = folder_path, pattern = "\\.csv$", full.names = TRUE)

# Read and combine CSV files
transactions <- map_dfr(csv_files, ~read_csv(.x, skip = 2))

# Formatting Vintage column 
transactions$Vintage <- as.Date(paste0('01/',
                                       transactions$Vintage),
                                format = "%d/%m/%Y")

# Filtering for relevant timeframe and sorting by 'Vintage'
transactions <- transactions%>%
  filter(Vintage >= '2021-08-01' )%>%
  arrange(Vintage)

print(unique(transactions$Generator))

# Filter by the "Generator" column
filtered_transactions <- transactions %>%
  filter(Generator == "Verwey-Madera Dairy Digester Genset #1 - Verwey-Madera Dairy Digester Genset #1" & `Transaction Type` != 'External Transfer')

# Ensuring that there are no duplicate issuances (i.e. no duplicate Vintage/REC Quantity Pairs)
filtered_transactions <- filtered_transactions%>%
  distinct(Vintage,`Quantity (RECs)`,.keep_all = TRUE)

# Remove any duplicate rows
unique_transactions <- distinct(filtered_transactions)



# Select specific columns
selected_columns <- c("Vintage", "Transaction Type", "Destination Account", "Notes", "Compliance Period", "Quantity (RECs)")
transactions_filtered <- unique_transactions %>% select(all_of(selected_columns))


# Separate dataframes for "Retirements" and "Issuances"
retirements <- filter(transactions_filtered, `Transaction Type` == "Retirement")
issuances <- filter(transactions_filtered, `Transaction Type` == "Issuance")

# Function to consolidate transactions and create notes
consolidate_transactions <- function(df) {
  df %>%
    group_by(Vintage) %>%
    summarise(
      `Transaction Type` = first(`Transaction Type`),
      `Destination Account` = first(`Destination Account`),
      `Compliance Period` = first(`Compliance Period`),
      `Quantity (RECs)` = sum(`Quantity (RECs)`),
      Notes = ifelse(n() > 1, paste0("Combined RECs from ", n(), " different transactions"), NA_character_)
    ) %>%
    ungroup()
}

# Apply the consolidation function to both dataframes
retirements_consolidated <- consolidate_transactions(retirements)
issuances_consolidated <- consolidate_transactions(issuances)



# Get today's date and format it
today_date <- format(Sys.Date(), "%m.%d.%y")

# Define the initial file path with today's date
file_path <- paste0("REC Transaction Summary ", today_date, ".xlsx")

# Function to handle existing file
handle_existing_file <- function(file_path) {
  repeat {
    # Check if file exists
    if (file.exists(file_path)) {
      cat("File", file_path, "already exists.\n")
      user_input <- readline(prompt = "Do you want to overwrite it? (Y/N): ")
      
      if (toupper(user_input) == "Y") {
        return(file_path)
      } else if (toupper(user_input) == "N") {
        new_file_name <- readline(prompt = "Enter new file name (without extension): ")
        file_path <- paste0(new_file_name, " ", today_date, ".xlsx")
      } else {
        cat("Invalid input. Please enter Y (Yes) or N (No).\n")
      }
    } else {
      return(file_path)
    }
  }
}

# Apply the function to handle the file path
final_file_path <- handle_existing_file(file_path)

# Export to Excel
write.xlsx(list(Retirements = retirements_consolidated, 
                Issuances = issuances_consolidated), 
           file = final_file_path)
