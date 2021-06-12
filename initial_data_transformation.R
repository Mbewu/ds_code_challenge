# Import libraries
library(tidyverse)
library(jsonlite)
library(units)
library(sf)
library(h3jsr)  

rm(list = ls())

# Note that I was having some troubles getting h3jsr to work out of the box
# and I needed to do the following (on linux):
# 1) apt-get install -y libudunits2-dev libgdal-dev libgeos-dev libproj-dev (in unix terminal)
# 2) remotes::install_github("obrl-soil/h3jsr" (in R)


start <- Sys.time()

# Download data if not present
# TODO: using local data for now

end_download <- Sys.time()
download_time <- difftime(end_download, start)
print(paste("download_time =",download_time))


start_load <- Sys.time()

# Read in data
# For testing purposes we are only going to read in a subset of the data
hex_8_data <- read_json("data/city-hex-polygons-8.geojson",simplifyVector = TRUE)
sr_data <- read_csv("data/sr.csv",n_max = 1000,skip = 0, col_types = cols());

end_load <- Sys.time()
load_time <- difftime(end_load, start_load)
print(paste("load_data_time =",load_time))


start_calc_h3 <- Sys.time()

colnames(sr_data)[1] <- "Id"

# Add the h3_level_8_index to sr_data using h3jsr
sr_data$h3_level8_index <- '0'
for(sr_row in seq_along(1:nrow(sr_data))){
  lat <- pull(sr_data[sr_row,"Latitude"])
  lon <- pull(sr_data[sr_row,"Longitude"])
  
  if(!is.na(lat) && !is.na(lon)) {
    # find the h3 index of this
    sfc_point <- st_sfc(st_point(c(lon, lat)), crs = 4326)
    sr_data[sr_row,"h3_level8_index"] <- point_to_h3(sfc_point, res = 8)
  }
}

end_calc_h3 <- Sys.time()
calc_h3_time <- difftime(end_calc_h3, start_calc_h3)
print(paste("calc_h3_time =",calc_h3_time))

start_join_data <- Sys.time()
# Join the tables by h3_level_8_index
hex_8_data$features$h3_level8_index <- hex_8_data$features$properties$index
sr_data_combined <- sr_data %>% left_join(hex_8_data$features,by="h3_level8_index")

end_join_data <- Sys.time()
join_data_time <- difftime(end_join_data, start_join_data)
print(paste("join_data_time =",join_data_time))

total_join_data_time <- difftime(end_join_data, start)
print(paste("total_join_data_time =",total_join_data_time))

# VALIDATION ==================================================================


start_validation <- Sys.time()
# Download validation data if not present
# TODO

# Load validation dataset
# For testing purposes we are only going to read in a subset of the data
sr_hex_data <- read_csv("data/sr_hex.csv",n_max = 1000,skip = 0, col_types = cols());

# Compare indexes to make sure they are the same (assume in same order for now)
h3_indexes_true <- sr_hex_data$h3_level8_index
h3_indexes_computed <- sr_data_combined$h3_level8_index

validation_result <- all(h3_indexes_true == h3_indexes_computed)
if(validation_result) {
  print("Validation passed! :)")
} else
{
  print("Validation failed! :(")
}


end_validation <- Sys.time()
validation_time <- difftime(end_validation, start_validation)
print(paste("validation_time =",validation_time))


total_time <- difftime(end_validation, start)
print(paste("total_time =",total_time))
