# Writing Results to Excel ----

# Resetting working directory
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

# Output file name
date <- format(Sys.Date(),"%m.%d.%y")

file_name = paste('Madera Biogas Flow Summary_WORKING_',date,'.xlsx', sep = '')                                           ####### INPUT 

# Printing available dataframes in global environment
names(which(unlist(eapply(.GlobalEnv,is.data.frame)))) # Choose from this list


# Creating list of dataframes to include as tables in the results spreadsheet
# 'Excel Sheet Name' = 'Dataframe Name'
data_tables <- list('Processed Logs'= processed_logs,                               ####### INPUT
                    'Flow Summary' = flow_summary,
                    'Gap Summary' = flow_gap_summary,
                    'Engine Gap Summary' = G1_gap_summary,
                    'Flare Gap Summary' = flare_gap_summary,
                    'CH4 Fractions' = weighted_average,
                    'CH4 Gap Summary' = Ch4_gap_summary)



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
