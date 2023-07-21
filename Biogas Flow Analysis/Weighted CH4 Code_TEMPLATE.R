# Template code for calculating weighted CH4 % based on flow

# Weighted CH4 % Calculations ----

# Filling in missing CH4 values for empty columns
processed_logs$ch4_meter[processed_logs$ch4_meter ==0]<- NA

# Calculating rolling mean window of 7 days before the timestamp
roll_mean_window <- 168 * (60 / 15)  # Window size: 168 hours = 4 * (60 minutes / 15 minutes)

## Creating ch4_sub column to calculate rolling average for missing ch4_meter readings -------
processed_logs <- processed_logs%>%
  
  # Create ch4_substitution label
  mutate(ch4_substitution = ifelse(is.na(ch4_meter),
                                   "CH4 Reading Substituted",
                                   "No Substitution"))%>%
  # Substitute NA CH4 readings
  mutate(ch4_sub = ifelse(is.na(ch4_meter),
                          rollmean(ch4_meter, k = roll_mean_window, align ='right',na.rm = TRUE),
                          ch4_meter))


## Creating Gap Summary for CH4 Substitution
Ch4_gap_summary <- processed_logs%>%
  # Adding row numbers so hyperlinks can be created
  mutate(row_numbers <<- seq.int(nrow(processed_logs))+1)%>%
  
  # Creating hyperlink string to link to processed logs "Date" column
  mutate(link = makeHyperlinkString(sheet = 'Processed Logs',
                                    text = 'Link to Processed_Logs',
                                    row = row_numbers, 
                                    col = 1))%>%
  
  # Filtering for instances where ch4_substitution occurred
  filter(ch4_substitution != 'No Substitution')%>%
  
  # Selecting columns
  select(Date,Time,ch4_meter,ch4_sub,link)



## Calculating weighted average for CH4% --------

# Creating month/year column 
processed_logs$Month <- as.yearmon(processed_logs$Date)

# Calculating weighted average, weighting CH4% for each timestamp according to the gas flow (G1_flow)
weighted_average <- processed_logs%>%
  group_by(Month)%>%
  summarise('Weighted Average CH4%' = weighted.mean(ch4_sub,G1_flow,na.rm = TRUE)/100)

# Removing the month column from the processed_logs df
processed_logs<- processed_logs%>%
  select(-Month)