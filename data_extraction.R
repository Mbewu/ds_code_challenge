# Import libraries
library(paws)
library(jsonlite)
library(tidyverse)

rm(list = ls())

start <- Sys.time()

# Setup AWS S3 access
aws_credentials_url <- "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/ds_code_challenge_creds.json"
secrets <- fromJSON(aws_credentials_url)

Sys.setenv(
  "AWS_ACCESS_KEY_ID" = secrets$s3$access_key,
  "AWS_SECRET_ACCESS_KEY" = secrets$s3$secret_key,
  "AWS_DEFAULT_REGION" = "af-south-1"
)

# Setup connection to AWS S3
s3 <- paws::s3()

# EXTRACT RES 8 FEATURES ===============================

# Setup query to select only data from features with resolution 8 
query <- "SELECT d.type, d.properties, d.geometry FROM  S3Object[*].features[*] d WHERE d.properties.resolution = 8"

# Perform query
extracted_h3_res8 <- s3$select_object_content(
  Bucket = "cct-ds-code-challenge-input-data",
  Key = "city-hex-polygons-8-10.geojson",
  Expression = query,
  ExpressionType = "SQL",
  RequestProgress = list(
    Enabled = TRUE
  ),
  InputSerialization = list(
    JSON = list(
      Type = "DOCUMENT"
    )
  ),
  OutputSerialization = list(
    JSON = list(
      RecordDelimiter = ","
    )
  )
)

# Parse query response to be in json array format and convert to df
extracted_h3_res8 <- extracted_h3_res8$Payload$Records$Payload %>% 
  str_sub(1,-1L -1)
extracted_h3_res8 <- paste0("[",extracted_h3_res8,"]")

extracted_h3_res8_df <- extracted_h3_res8 %>% 
  jsonlite::fromJSON()


end_extraction <- Sys.time()
extraction_time <- difftime(end_extraction, start)
print(paste("extraction_time =",extraction_time))


# VALIDATION =================

start_validation <- Sys.time()

# Get city-hex-polygons-8.geojson for validation
h3_res8_obj <- s3$get_object(Bucket = "cct-ds-code-challenge-input-data",
              Key = "city-hex-polygons-8.geojson")

# Convert from raw to df
h3_res8_df <- h3_res8_obj$Body %>%
  rawToChar() %>%
  jsonlite::fromJSON()

# Compare indexes to make sure they are the same (assume in same order)
indexes_from_8_10_data <- extracted_h3_res8_df$properties$index
indexes_from_8_data <- h3_res8_df$features$properties$index

validation_result <- all(indexes_from_8_data == indexes_from_8_10_data)
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