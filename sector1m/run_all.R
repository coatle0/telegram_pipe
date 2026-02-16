
# Set working directory to project root if needed, or assume relative paths
# setwd("C:/autoai/trea_tchain/osat/sector1m")

source("R/extract_events.R")
source("R/highlight_excel.R")

# Paths
input_file <- "data/260108_rtname.xlsx"
output_dir <- "output"

# Create output dir if not exists
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

cat("Processing events...\n")

# 1. Extract Events
res <- extract_events(input_file)
stock_events <- res$stock_events
sector_events <- res$sector_events

# 2. Save CSVs
write.csv(stock_events, file.path(output_dir, "movers_left3_stock_events.csv"), row.names = FALSE)
write.csv(sector_events, file.path(output_dir, "movers_left3_sector_events.csv"), row.names = FALSE)

cat("Generating highlighted Excel...\n")

# 3. Generate Highlighted Excel
highlight_excel(input_file, file.path(output_dir, "260108_rtname_highlight_sector.xlsx"), sector_events, stock_events)

cat("Done.\n\n")

# 4. Verification Output
cat("=== Verification Output ===\n")
cat("Stock Events Count:", nrow(stock_events), "\n")
cat("Sector Events Count:", nrow(sector_events), "\n\n")

cat("Top 5 Sector Events:\n")
print(head(sector_events, 5))

# Check for time "09:04"
# Need to reload raw data to check content
df_raw <- read_excel(input_file)

# Handle POSIXct time format from Excel
if (inherits(df_raw$time, "POSIXct")) {
  df_raw$time_str <- format(df_raw$time, "%H:%M")
} else {
  df_raw$time_str <- as.character(df_raw$time)
}

row_0904 <- df_raw %>% filter(time_str == "09:04")

if (nrow(row_0904) > 0) {
  cat("\nRow at 09:04 (X1~X9):\n")
  print(row_0904[, c("time", paste0("X", 1:9))])
} else {
  cat("\nTime '09:04' not found in exact string match. Checking first few times:\n")
  print(head(df_raw$time))
}
