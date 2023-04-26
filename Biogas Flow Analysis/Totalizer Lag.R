# Function to process for totalizer lag
TotalizerLag <- function(logs, totalizer){
  # Rearranging the logs in ascending order
  logs <- logs%>%
    arrange(date_time)
  
  # Reversing the order of the totalizer column
  totalizer <- rev(totalizer)
  
  # Creating empty vector to track the lag counter
  totalizer_lag <- vector()
  
  # Iterating through logs to identify and count totalizer lag
  for (i in 1:nrow(logs)){
    if (i != 1){
      if (totalizer[i] == totalizer[i-1]){
        totalizer_lag <- append(totalizer_lag,totalizer_lag[i-1]+1)
      }else{
        totalizer_lag <- append(totalizer_lag,0)
      }
    }else {
      totalizer_lag <- append(totalizer_lag,0)
    }
  }
  
  # Reversing the order of the lag counter
  totalizer_lag <- rev(totalizer_lag)
  
  return(totalizer_lag)
}

# Creating totalizer lag column for engine
processed_logs$engine_lag <- TotalizerLag(processed_logs,processed_logs$engine_totalizer)

# Creating totalizer lag column for flare
processed_logs$flare_lag <- TotalizerLag(processed_logs,processed_logs$flare_totalizer)


# Correcting for totalizer lag by calculating average flow during lag
lag<- processed_logs%>%
  select(date_time,G1_flow,engine_lag)%>%
  filter((engine_lag == 0 & lead(engine_lag)!= 0) | engine_lag !=0)%>%
  mutate(average_flow = case_when(G1_flow != 0 ~ G1_flow/lead(engine_lag),
                                  TRUE ~ 0))

for (i in 1:nrow(lag)){
  if(lag$engine_lag[i] == 0){
    input <- lag$average_flow[i]
  }
  if(lag$engine_lag[i] != 0){
    lag$average_flow[i]<-input
  }
}

lag <- lag %>%
  select(date_time,average_flow)

# Merging the two dataframes 
processed_logs1<- left_join(processed_logs,lag,by = 'date_time')