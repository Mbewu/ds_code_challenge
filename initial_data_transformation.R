# Import libraries
library(tidyverse)
library(jsonlite)
library(units)
library(sf)
library(h3jsr)
library(R.utils)
library(future.apply)

rm(list = ls())

# PARAMETERS ===================================================================
# Number of processors to use
num_procs <- 1
# Number of lines to read (service requests to process), Inf for whole file
num_lines <- 10000
# Number of batches to split data into (minimum set to 10)
num_batches_input <- 10
# Error threshold to terminate program if not enough h3 indexes can be calculated
join_error_threshold <- 0.2

plan(multiprocess, workers = num_procs)


# Note that I was having some troubles getting h3jsr to work out of the box
# and I needed to do the following (on linux):
# 1) apt-get install -y libudunits2-dev libgdal-dev libgeos-dev libproj-dev (in unix terminal)
# 2) remotes::install_github("obrl-soil/h3jsr" (in R)

# LOAD DATA ===================================================================

start <- Sys.time()

# Download data if not present
dir.create("data",showWarnings = FALSE)

hex_8_data_filename <- "data/city-hex-polygons-8.geojson"
if(!file.exists(hex_8_data_filename)) {
  hex_8_data_url <- "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/city-hex-polygons-8.geojson"
  download.file(hex_8_data_url, hex_8_data_filename)
}

sr_data_filename <- "data/sr.csv"
sr_gz_data_filename <- "data/sr.csv.gz"
if(!file.exists(sr_data_filename)) {
  sr_data_url <- "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/sr.csv.gz"
  download.file(sr_data_url, sr_gz_data_filename)
  gunzip(sr_gz_data_filename, remove=FALSE)
}

end_download <- Sys.time()
download_time <- difftime(end_download, start)
print(paste("download_time =",download_time))



start_load <- Sys.time()

# Read in data
# For testing purposes we are only going to read in a subset of the data
hex_8_data <- read_json(hex_8_data_filename,simplifyVector = TRUE)
sr_data <- read_csv(sr_data_filename,n_max = num_lines,skip = 0, col_types = cols());
n_rows <- nrow(sr_data)

end_load <- Sys.time()
load_time <- difftime(end_load, start_load)
print(paste("load_data_time =",load_time))



# JOIN DATA ====================================================================

start_calc_h3 <- Sys.time()



# Initialise some variables for batch loop
sr_data$h3_level8_index <- 0
lat_idx <- which(colnames(sr_data)=="Latitude")
lon_idx <- which(colnames(sr_data)=="Longitude")
options(digits=9)

num_fail <- 0
num_attempts <- 0
portion_complete <- 0

min_batches <- 10
num_batches <- max(num_batches_input,min_batches)
batch_size <- as.integer(n_rows/num_batches)

start_idxs <- seq(1,n_rows,batch_size)
end_idxs <- start_idxs + batch_size
end_idxs[length(end_idxs)] <- n_rows

# Loop over batches
for(i in 1:length(start_idxs))  {
  start_idx = start_idxs[i]
  end_idx = end_idxs[i]
  
  # Parallel loop over each batch
  sr_data$h3_level8_index[start_idx:end_idx] <-
    future_apply(sr_data[start_idx:end_idx,],1, function(sr_row) {
      
    lat <- as.numeric(sr_row[lat_idx])
    lon <- as.numeric(sr_row[lon_idx])
    
    # h3 index is 0 if 
    h3_level8_index <- 0
    if(!is.na(lat) && !is.na(lon)) {
      # find the h3 index of this
      sfc_point <- st_sfc(st_point(c(lon, lat)), crs = 4326)
      h3_level8_index <- point_to_h3(sfc_point, res = 8)
    }
  
    return(h3_level8_index)
  })

  
  # Exit if error threshold is reached
  num_attempts <- end_idx
  num_fail <- num_fail +
    sum(!is.na(as.numeric(sr_data$h3_level8_index[start_idx:end_idx])))
  
  if(num_fail/num_attempts > join_error_threshold) {
    percent_fail <- as.integer(num_fail/num_attempts * 100)
    stop(paste0("Too many service requests (",percent_fail,
                "%) have invalid location data. Exiting."))
  }

  # Progress indicator
  if(i %% as.integer(length(start_idxs)/10) == 0) {
    portion_complete <<- portion_complete + 1
    print(paste0(10*portion_complete,"% complete..."))
  }
  

}

print(paste0(num_fail," out of ",nrow(sr_data)," records failed to join."))

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

# VALIDATION ==================================================================


start_validation <- Sys.time()
# Download validation data if not present
# TODO

# Load validation dataset
# For testing purposes we are only going to read in a subset of the data
sr_hex_data <- read_csv("data/sr_hex.csv",n_max = num_lines,skip = 0, col_types = cols());

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
