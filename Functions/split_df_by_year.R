# Group data according to the year ----

# Defining function to add year to date and format month for excel
split_by_year <- function(data, date_column) {
  
  # Extract the year from the date column
  data$Year <- format(data[[date_column]], "%Y")
  
  # Convert month to excel format
  data[[date_column]] <- format(data[[date_column]],'%m/%d/%Y')
  
  # Split the dataframe into a list of dataframes based on the year
  list_of_dfs <- split(data, data$Year)
  
  # Optionally, remove the 'Year' column from each dataframe
  list_of_dfs <- lapply(list_of_dfs, function(df) {
    df$Year <- NULL
    return(df)
  })
  
  return(list_of_dfs)
}

# Calling function to get lists of dataframes organized by year
alsoenergy_list <- split_by_year(alsoenergy_monthly_summary,'Month')
pge_list <-  split_by_year(pge_monthly_summary,'Month')

# Convert lists to dataframes in global environment 
for (year in names(alsoenergy_list)){
  assign(paste0("alsoenergy_", year), alsoenergy_list[[year]], envir = .GlobalEnv)
}