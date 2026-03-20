# Load required libraries
library(move2)
library(dplyr)
library(lubridate)
library(sf)
library(ggplot2)
library(dbscan)
library(RColorBrewer)
library(leaflet)

# Step 1: Define parameters for data retrieval and download data from Movebank
time_interval <- "10 mins"
date_start <- as.POSIXct("2024-01-01 15:00:00", tz = "UTC")  # Start date for January 2025
date_end <- as.POSIXct("2025-03-13 03:00:00", tz = "UTC")    # End date for January 2025

# Download and process baboon data
baboon_data <- movebank_download_study(
  study_id = 3445611111,
  sensor_type_id = "gps",
  timestamp_start = date_start,
  timestamp_end = date_end,  # Limit the data to January 2025
  remove_movebank_outliers = TRUE
)

# Retrieve metadata and merge with main data
metadata <- mt_track_data(baboon_data)
baboon_data <- baboon_data %>%
  left_join(metadata %>% select(individual_local_identifier, group_id), 
            by = "individual_local_identifier") %>%
  mt_filter_per_interval(unit = time_interval)

# Convert timestamps to proper format and ensure they are within January 2025
baboon_data <- baboon_data %>%
  mutate(timestamp = as.POSIXct(timestamp, tz = "UTC"))# %>%  # Ensure timestamp is in UTC
  #filter(timestamp >= as.POSIXct("2025-01-01 00:00:00", tz = "UTC") & 
  #         timestamp <= as.POSIXct("2025-01-31 23:59:59", tz = "UTC"))

# Extract coordinates and filter data for nighttime (21:00 to 5:00)
baboon_data <- baboon_data %>%
  mutate(
    location_long = as.numeric(st_coordinates(baboon_data)[, 1]),
    location_lat = as.numeric(st_coordinates(baboon_data)[, 2])
  ) %>%
  filter(hour(timestamp) >= 15 | hour(timestamp) < 4) %>% 
  filter(!is.na(location_long) & !is.na(location_lat))

# Convert to spatial data for clustering
baboon_sf <- st_as_sf(baboon_data, coords = c("location_long", "location_lat"), crs = 4326)
baboon_sf <- baboon_sf[!st_is_empty(baboon_sf), ]

# Step 2: Cluster sleeping sites using DBSCAN
dbscan_result <- dbscan::dbscan(st_coordinates(baboon_sf), eps = 0.002, minPts = 50)
baboon_sf$cluster <- as.factor(dbscan_result$cluster) #############################

# Step 3: Create a list of unique days (just days, no year or month) and the year-month
site_locs <- baboon_sf %>%
  group_by(cluster) %>%  # Group by cluster and group_id
  reframe(
    avg_long = median(location_long, na.rm = TRUE),
    avg_lat = median(location_lat, na.rm = TRUE))
# 
# group_counts <- baboon_sf %>%
#   group_by(cluster, group_id) %>%  # Group by cluster and group_id
#   reframe(
#     avg_long = mean(location_long, na.rm = TRUE),
#     avg_lat = mean(location_lat, na.rm = TRUE),
#     year_month = unique(format(timestamp, "%Y,%m")),  # Year and Month
#     unique_days = paste(unique(format(timestamp, "%d")), collapse = ", ")  # Only extract day (no year/month)
#   )

# Step 4: Create leaflet map with both Esri satellite imagery and OpenStreetMap
leaflet() %>%
  addProviderTiles(providers$Esri.WorldImagery, group = "Satellite") %>%  # Use satellite imagery
  addProviderTiles(providers$OpenStreetMap, group = "OpenStreetMap") %>%  # Add OpenStreetMap layer
  addCircleMarkers(
    data = site_locs,
    lng = ~avg_long,
    lat = ~avg_lat,
    radius = 5,
    color = "red",
    stroke = FALSE,
    fillOpacity = 0.7
  ) %>%
  addLabelOnlyMarkers(
    data = site_locs,
    lng = ~avg_long,
    lat = ~avg_lat,
    label = ~paste(cluster),  # Format label with Year, Month and Days
    labelOptions = labelOptions(
      noHide = TRUE,
      direction = "top",
      textsize = "12px",
      fontWeight = "bold"
    )  ) %>%
  addLayersControl(
    baseGroups = c("Satellite", "OpenStreetMap"),  # Define base layers to toggle
    options = layersControlOptions(collapsed = FALSE)  # Make control visible
  ) %>%
  setView(lng = mean(site_locs$avg_long), lat = mean(site_locs$avg_lat), zoom = 12)  # Center the map

# leaflet() %>%
#   addProviderTiles(providers$Esri.WorldImagery, group = "Satellite") %>%  # Use satellite imagery
#   addProviderTiles(providers$OpenStreetMap, group = "OpenStreetMap") %>%  # Add OpenStreetMap layer
#   addCircleMarkers(
#     data = group_counts,
#     lng = ~avg_long,
#     lat = ~avg_lat,
#     radius = 5,
#     color = "red",
#     stroke = FALSE,
#     fillOpacity = 0.7
#   ) %>%
#   addLabelOnlyMarkers(
#     data = group_counts,
#     lng = ~avg_long,
#     lat = ~avg_lat,
#     label = ~paste(group_id, " [", year_month, "] (", unique_days, ")"),  # Format label with Year, Month and Days
#     labelOptions = labelOptions(
#       noHide = TRUE, 
#       direction = "top", 
#       textsize = "12px", 
#       fontWeight = "bold"
#     )
#   ) %>%
#   addLayersControl(
#     baseGroups = c("Satellite", "OpenStreetMap"),  # Define base layers to toggle
#     options = layersControlOptions(collapsed = FALSE)  # Make control visible
#   ) %>%
#   setView(lng = mean(group_counts$avg_long), lat = mean(group_counts$avg_lat), zoom = 12)  # Center the map
