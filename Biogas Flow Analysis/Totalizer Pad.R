# Padding G1_totalizer ----

## Creating new totalizer column w/ NA instead of zero to pad totalizer gaps
processed_logs <- processed_logs%>%
  mutate(G1_totalizer_pad = ifelse(G1_totalizer == 0,NA_real_,G1_totalizer))%>%
  mutate(G1_totalizer_pad = na.locf(G1_totalizer_pad,fromLast = TRUE))

# Substituting NA's in G1_totalizer_pad w/ the last non-NA totalizer value
processed_logs$G1_totalizer_pad <- na.locf(processed_logs$G1_totalizer_pad,fromLast = TRUE)

# Filling G1_totalizer_diff w/ G1_totalizer_pad difference when G1_totalizer == 0
processed_logs <- processed_logs%>%
  mutate(G1_totalizer_diff = ifelse(lead(G1_totalizer) == 0,G1_totalizer_pad - lead(G1_totalizer_pad),G1_totalizer_diff))

# Padding flare_totalizer -----

## Creating new totalizer column w/ NA instead of zero to pad totalizer gaps
processed_logs <- processed_logs%>%
  mutate(flare_totalizer_pad = ifelse(flare_totalizer == 0,NA_real_,flare_totalizer))%>%
  mutate(flare_totalizer_pad = na.locf(flare_totalizer_pad,fromLast = TRUE))

# Substituting NA's in flare_totalizer_pad w/ the last non-NA totalizer value
processed_logs$flare_totalizer_pad <- na.locf(processed_logs$flare_totalizer_pad,fromLast = TRUE)

# Filling flare_totalizer_diff w/ flare_totalizer_pad difference when flare_totalizer == 0
processed_logs <- processed_logs%>%
  mutate(flare_totalizer_diff = ifelse(lead(flare_totalizer) == 0,flare_totalizer_pad - lead(flare_totalizer_pad),flare_totalizer_diff))