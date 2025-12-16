library(move2)
library(arrow)

# Movebank study ID
study_id <- 3445611111

# Set up Movebank credentials
# Option 1: Use environment variables (recommended for security)
if (Sys.getenv("MOVEBANK_USERNAME") != "" && Sys.getenv("MOVEBANK_PASSWORD") != "") {
  movebank_store_credentials(
    username = Sys.getenv("XXX"),
    password = Sys.getenv("XXX")
  )
} else {
  # Option 2: Prompt for credentials interactively (won't work in subprocess)
  # Option 3: Read from a file (create a file with credentials)
  cred_file <- "~/.movebank_credentials.txt"
  if (file.exists(cred_file)) {
    creds <- readLines(cred_file)
    movebank_store_credentials(
      username = creds[1],
      password = creds[2]
    )
  } else {
    stop("No Movebank credentials found. Please set MOVEBANK_USERNAME and MOVEBANK_PASSWORD environment variables or create ~/.movebank_credentials.txt")
  }
}

# Download deployment metadata
metadata <- movebank_download_deployment(study_id)

# Save as parquet for easy reading in Python
parquet_path <- "movebank_metadata.parquet"
write_parquet(metadata, parquet_path)

# Also save as CSV as backup
csv_path <- "movebank_metadata.csv"
write.csv(metadata, csv_path, row.names = FALSE)

# Print success message with file locations
cat("Metadata downloaded and saved successfully\n")
cat("Parquet file saved to:", normalizePath(parquet_path), "\n")
cat("CSV file saved to:", normalizePath(csv_path), "\n")
cat("Number of deployments:", nrow(metadata), "\n")