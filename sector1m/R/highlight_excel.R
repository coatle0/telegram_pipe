
library(openxlsx)
library(dplyr)

# Function to highlight excel
highlight_excel <- function(input_path, output_path, sector_events, stock_events) {
  
  wb <- loadWorkbook(input_path)
  sheet_name <- names(wb)[1] # Assume first sheet
  
  # Define styles
  style_sector <- createStyle(fgFill = "#FFFFE0") # Light Yellow
  style_stock <- createStyle(fgFill = "#90EE90")  # Light Green
  
  # 1. Highlight Sector Events (Light Yellow)
  # For each sector event: row_id corresponds to the row index in Excel data part.
  # Excel row = row_id + 1 (header)
  # Columns: Time is col 1. X1 is col 2.
  # Sector k corresponds to slots (3k-2) to 3k.
  # Excel columns for X(slot) = slot + 1
  
  for (i in 1:nrow(sector_events)) {
    row_idx <- sector_events$row_id[i] + 1 # +1 for header
    sec <- sector_events$sector_curr[i]
    
    start_slot <- (sec - 1) * 3 + 1
    end_slot <- sec * 3
    
    start_col <- start_slot + 1 # +1 for Time column
    end_col <- end_slot + 1
    
    addStyle(wb, sheet = sheet_name, style = style_sector, rows = row_idx, cols = start_col:end_col, gridExpand = TRUE)
  }
  
  # 2. Highlight Stock Events (Light Green) - Overwrite if necessary
  # Highlight the specific cell of the stock in the current time
  for (i in 1:nrow(stock_events)) {
    row_idx <- stock_events$row_id[i] + 1
    slot <- stock_events$slot_curr[i]
    col_idx <- slot + 1
    
    addStyle(wb, sheet = sheet_name, style = style_stock, rows = row_idx, cols = col_idx, gridExpand = FALSE)
  }
  
  saveWorkbook(wb, output_path, overwrite = TRUE)
}
