# Import libraries
library(paws)
library(jsonlite)
library(tidyverse)
library(tools)

rm(list = ls())
this.dir <- dirname(parent.frame(2)$ofile)
setwd(this.dir)

start <- Sys.time()
# 
# # Setup AWS S3 access
# aws_credentials_url <- "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/ds_code_challenge_creds.json"
# secrets <- fromJSON(aws_credentials_url)
# 
# Sys.setenv(
#   "AWS_ACCESS_KEY_ID" = secrets$s3$access_key,
#   "AWS_SECRET_ACCESS_KEY" = secrets$s3$secret_key,
#   "AWS_DEFAULT_REGION" = "af-south-1"
# )

# Setup AWS S3 access for test
test_secrets_writeonly <- read_csv('data/writeonlyawscredentials.csv')

Sys.setenv(
  "AWS_ACCESS_KEY_ID" = test_secrets_writeonly$access_key,
  "AWS_SECRET_ACCESS_KEY" = test_secrets_writeonly$secret_key,
  "AWS_DEFAULT_REGION" = "us-east-2",
  "AWS_S3_ENDPOINT" = ""
)



# PREPARE DATA ==========================================================

start <- Sys.time()

# Download data if not present
# TODO: using local data for now

end_download <- Sys.time()
download_time <- difftime(end_download, start)
print(paste("download_time =",download_time))

start_load <- Sys.time()

# Read in data
# For testing purposes we are only going to read in a subset of the data
sr_hex_data <- read_csv("data/sr_hex.csv",n_max = 1000,skip = 0, col_types = cols())

end_load <- Sys.time()
load_time <- difftime(end_load, start_load)
print(paste("load_data_time =",load_time))


start_save_subset <- Sys.time()

# Choose a subset of the columns
cols <- c("NotificationNumber","h3_level8_index","Latitude","Longitude")
#cols <- c("NotificationNumber")
sr_hex_data <- sr_hex_data[ , cols]

# Write the data to file
sr_sub_filename <- "data/sr_hex_sub_james_mbewu.csv"
write.table(sr_hex_data,file=sr_sub_filename,
            row.names = FALSE, sep=",",quote=FALSE)

end_save_subset <- Sys.time()
save_subset_time <- difftime(end_save_subset, start_save_subset)
print(paste("save_subset_time =",save_subset_time))



# UPLOAD DATA =================================================================

start_upload <- Sys.time()

# Setup connection to AWS S3
s3 <- paws::s3()

# Upload file
result <- s3$put_object(
  Body = sr_sub_filename,
  Bucket = "testbucketmbewu",
  Key = "sr_hex_sub_james_mbewu.csv"
)

# result <- s3$put_object(
#   Body = sr_sub_filename,
#   Bucket = "cct-ds-code-challenge-input-data",
#   Key = "sr_hex_sub_james_mbewu.csv"
# )

end_upload <- Sys.time()
upload_time <- difftime(end_upload, start_upload)
print(paste("upload_time =",upload_time))





# VALIDATION ===================================================================

start_verification <- Sys.time()
# compare the eTag and the md5sum
eTag <- str_extract(result$ETag,"[:alnum:]+")
num_parts <- as.numeric(str_match(string = eTag,
                        pattern = '-(\\d+)$')[2])

# Check it wasn't a multipart upload
if(is.na(num_parts)){
  md5 <- as.character(md5sum(sr_sub_filename))
  if(md5 == eTag) { 
    print("File successfully uploaded. :)") 
  }
  else{ 
    print("File was NOT successfully uploaded. :(") 
  }
} else{
  print("Multipart upload verification not supported yet.") 
}


end_verification <- Sys.time()
verification_time <- difftime(end_verification, start_verification)
print(paste("verification_time =",verification_time))


total_time <- difftime(end_verification, start)
print(paste("total_time =",total_time))



