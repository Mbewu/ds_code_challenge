# Import libraries
library(tidyverse)
library(jsonlite)
library(logging)

rm(list = ls())
basicConfig()
addHandler(writeToFile, file="further_data_transformations.log", level='DEBUG')
loginfo("Running further_data_transformations.R",1)

# LOAD DATA ==========================================================

start <- Sys.time()
num_lines <- 10000
# Download data if not present
dir.create("data",showWarnings = FALSE)

sr_data_filename <- "data/sr.csv"
sr_gz_data_filename <- "data/sr.csv.gz"
if(!file.exists(sr_data_filename)) {
  sr_data_url <- "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/sr.csv.gz"
  download.file(sr_data_url, sr_gz_data_filename)
  gunzip(sr_gz_data_filename, remove=FALSE)
}

end_download <- Sys.time()
download_time <- difftime(end_download, start)
#print(paste("download_time =",download_time))
loginfo(paste("download_time =",download_time),1)

start_load <- Sys.time()

# Read in data
# For testing purposes we are only going to read in a subset of the data
sr_hex_data <- read_csv("data/sr_hex.csv",n_max = num_lines,skip = 0, col_types = cols())

end_load <- Sys.time()
load_time <- difftime(end_load, start_load)
print(paste("load_data_time =",load_time))

# DATA ANONYMISATION ==========================================================


start_anonymisation <- Sys.time()


# Remove SubCouncil2016, Wards2016, OfficialSuburbs, Code and h3_level8_index
drop_cols <- c("SubCouncil2016","Wards2016","OfficialSuburbs","Code","h3_level8_index")
sr_hex_data <- sr_hex_data[ , !(names(sr_hex_data) %in% drop_cols)]

# Shuffle CodeGroup and directorate and department together
shuffle_cols_dep <- c("CodeGroup","directorate","department")
sr_hex_data[, shuffle_cols_dep] <- sr_hex_data[sample(1:nrow(sr_hex_data)), shuffle_cols_dep]

# Shuffle NotificationNumber
shuffle_cols_not <- c("NotificationNumber")
sr_hex_data[, shuffle_cols_not] <- sr_hex_data[sample(1:nrow(sr_hex_data)), shuffle_cols_not]



# Do numerical transforms

# Spatial transforms
# add/subtract max 350m from the latitude and longitude
# 350m ~ 0.00314 deg lat and 0.00382 deg lon around Cape Town
set.seed(NULL)
lat_350 <- 0.00314
lon_350 <- 0.00382
# check it gives different random numbers
sr_hex_data <- sr_hex_data %>% mutate(Latitude = Latitude + runif(n(),-lat_350,lat_350),
                                      Longitude = Longitude + runif(n(),-lon_350,lon_350))






# Temporal transforms
# convert CompletionTimestamp to POSIXct
sr_hex_data <- sr_hex_data %>% mutate(CompletionTimestamp = str_sub(CompletionTimestamp,end=-7),
                                      CompletionTimestamp = as.POSIXct(CompletionTimestamp,format = "%Y-%m-%d %H:%M:%S",tz = "UTC"),
                                      CompletionTimestamp = CompletionTimestamp - 2*60*60)

# Add/subtract 3hrs from CreationTimestamp and CompletionTimeStamp/ModificationTimestamp
#  and adjust Duration and CreationDate and CompletionDate accordingly.
# Make sure Duration doesn't go negative by only allowing CreationTimestamp to
#  move forward 1/2 Duration and CompletionTimestamp only move back 1/2 Duration
sr_hex_data <- sr_hex_data %>% mutate(CreationTimestamp = CreationTimestamp + 
                                        as.integer(round(runif(n(),-3*60*60,min(3*60*60,Duration/2*60*60*24)))),
                                      CompletionPerturbation = as.integer(round(runif(n(),max(-3*60*60,-Duration/2*60*60*24),3*60*60))),
                                      CompletionTimestamp = CompletionTimestamp + CompletionPerturbation,
                                      
                                      ModificationTimestamp = ModificationTimestamp + CompletionPerturbation,
                                      Duration = (as.integer(CompletionTimestamp) - as.integer(CreationTimestamp)) / 60 / 60 / 24,
                                      CreationDate = as.Date(CreationTimestamp),
                                      CompletionDate = as.Date(CompletionTimestamp)) %>%
                              select(-CompletionPerturbation)


end_anonymisation <- Sys.time()
anonymisation_time <- difftime(end_anonymisation, start_anonymisation)
print(paste("anonymisation_time =",anonymisation_time))


# WRITE TO FILE ================================================================

start_write <- Sys.time()

# Format CreationTimestamp, CompletionTimestamp, ModificationTimestamp for output
timestamp_format <- "%Y-%m-%d %H:%M:%S%z"
sr_hex_data <- sr_hex_data %>% 
  mutate(CreationTimestamp = format(CreationTimestamp,format = timestamp_format),
         CompletionTimestamp = format(CompletionTimestamp,format = timestamp_format),
         ModificationTimestamp = format(ModificationTimestamp,format = timestamp_format))
                                      
# Write to file
col_names <- colnames(sr_hex_data)
col_names[1] <- ""
write.table(sr_hex_data,file="data/sr_hex_anon.csv",
            row.names = FALSE, col.names = col_names, sep=",",quote=FALSE)

end_write <- Sys.time()
write_time <- difftime(end_write, start_write)
print(paste("write_time =",write_time))

total_time <- difftime(end_write, start)
print(paste("total_time =",total_time))
