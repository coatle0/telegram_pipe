
library(dplyr)
library(tidyr)
library(readxl)

# Function to extract stock events and sector events
extract_events <- function(file_path) {
  
  # Read Excel file
  df <- read_excel(file_path, col_names = TRUE)
  
  # Ensure column names are correct (time, X1...X27)
  # If columns are not named X1...X27, we might need to rename or assume position
  # Based on instruction: "First sheet, Columns: time, X1~X27"
  
  # Convert to long format for easier processing
  # We need to preserve row index (time sequence)
  df <- df %>% mutate(row_id = row_number())
  
  df_long <- df %>%
    pivot_longer(cols = starts_with("X"), names_to = "slot_name", values_to = "name") %>%
    mutate(
      slot = as.integer(gsub("X", "", slot_name)),
      sector = ceiling(slot / 3)
    ) %>%
    arrange(row_id, slot)
  
  # Handle duplicate stock names in the same row: keep the one with the smallest slot
  df_long <- df_long %>%
    group_by(row_id, name) %>%
    filter(slot == min(slot)) %>%
    ungroup()
  
  # Self-join to compare t and t-1
  # We need to join based on 'name' and 'row_id' (current vs prev)
  
  # Prepare previous state
  df_prev <- df_long %>%
    mutate(row_id_next = row_id + 1) %>%
    select(row_id_next, name, slot, time) %>%
    rename(
      row_id = row_id_next,
      slot_prev = slot,
      time_prev = time
    )
  
  # Join current with previous
  df_joined <- df_long %>%
    inner_join(df_prev, by = c("row_id", "name")) %>%
    rename(
      slot_curr = slot,
      time_curr = time,
      sector_curr = sector
    )
  
  # Calculate move_left
  events <- df_joined %>%
    mutate(move_left = slot_prev - slot_curr) %>%
    filter(move_left >= 3) # Condition: move_left >= 3
  
  # Select required columns for Stock Events (A)
  stock_events <- events %>%
    select(row_id, time_prev, time_curr, name, slot_prev, slot_curr, move_left, sector_curr) %>%
    arrange(row_id, slot_curr)
  
  # Identify Sector Events (B)
  # If any stock event occurs in a sector at time t, the whole sector is triggered
  sector_events_agg <- stock_events %>%
    group_by(row_id, time_curr, sector_curr) %>%
    summarise(
      movers = paste(unique(name), collapse = " | "),
      max_move_left = max(move_left),
      .groups = "drop"
    ) %>%
    arrange(row_id, sector_curr)
  
  return(list(
    stock_events = stock_events,
    sector_events = sector_events_agg,
    raw_long = df_long # Return raw long data for highlighting if needed, though raw df is usually better
  ))
}
