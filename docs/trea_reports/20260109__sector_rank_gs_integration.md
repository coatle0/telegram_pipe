# Sector Rank Calculation & Google Sheet Integration Analysis Report

**Date:** 2026-01-09
**Author:** TREA (Codebase Historian)
**Scope:** R Codebase (mypkg_R)

## Context
Analysis of the R codebase to understand how `sector_rank` is calculated, how `prices_run_idx` is managed, and how Google Sheet data is integrated. The goal is to determine the safest method to reorder sectors based on external Google Sheet inputs.

## File Inventory
Key files identified in `mypkg_R/`:

*   **shield.R**: Korean market sector analysis; calculates `prices_run_idx`.
*   **shield_us.R**: US market sector analysis; similar logic to shield.R.
*   **pead_us.R**: US PEAD strategy; calculates `prices_run_idx`.
*   **vmon_start.R**: Main initialization; reads Google Sheet indices.
*   **init_jm.R**: Portfolio initialization; integrates Google Sheet data.
*   **pf_man.R**: Portfolio management; reads/writes Google Sheet data.
*   **pf_shield.R**: Portfolio shield calculations.
*   **put_ir_gs.R**: Google Sheet IR data management.
*   **update_kidx.R**: Korean index updates.
*   **update_myidx.R**: My index updates.

## Function Index
Relevant functions for Google Sheet interaction:

| function_name | file | inputs | outputs | side_effects | calls | called_by |
|---|---|---|---|---|---|---|
| `read_gs_idx` | mypkg | idx_fn | list(idx, wt, ref) | Google Sheet auth | `gs4_auth` | `vmon_start.R`, `init_jm.R` |
| `read_asgs_idx` | mypkg | sheet_name | list(smb, wt, ref) | Google Sheet auth | `gs4_auth` | `shield.R`, `shield_us.R` |
| `read_rtgs_idx` | mypkg | sheet_name | list(smb, wt, ref) | Google Sheet auth | `gs4_auth` | `vmon_start.R`, `init_jm.R` |

## Data Objects Map

### prices_run_idx
*   **Type/Shape**: xts/matrix with stock symbols as columns.
*   **Created At**:
    *   `shield.R`: Line 47
    *   `shield_us.R`: Line 51
    *   `pead_us.R`: Line 49
*   **Mutated At**: Line 53 in shield files (via sorting logic).
*   **Used At**: Sorting and ranking calculations immediately after creation.

### sector_rank
*   **Created At**: `shield.R`: Line 53 via `order(colSums(tail(prices_run_idx[,-1])), decreasing = T)`.
*   **Used At**: Column selection for `prices_run_idx_sort` immediately after.

### idx_gs_lst / wt_gs_lst / ref_gs_lst
*   **Type/Shape**: List structure:
    *   `[[1]]`: symbols
    *   `[[2]]`: weights
    *   `[[3]]`: reference dates
*   **Naming Rules**: Consistent pattern (`idx_gs_lst`, `kweight_lst`, `ksmb_lst`).
*   **Produced**: Google Sheet reading functions.
*   **Consumed**: Throughout shield and initialization files.

## Naming Conventions & Exceptions
*   **Sector Name Set**: `kr_idx`, `mybiz`, `mybiz.us`, `pead.us`, `pf_idx`, `focus_idx`.
*   **Suffix Set**: `_idx` (index), `_wt` (weights), `_ref` (ref dates), `_lst` (lists).
*   **Exceptions/Typos**: None found (e.g., no "plaform" vs "platform" inconsistencies).

## Design Decision
**Revised (User Request):** The sector ranking logic should be centralized in `update_kidx.R` (and potentially `update_myidx` workflow). This ensures that the sector order is determined *once* during the index update process and persisted to Google Sheets, rather than being re-calculated dynamically in every run of `shield.R`.

## Patch Plan (Minimal Change)
1.  **Patch 1 (`update_kidx.R`)**: Add logic to calculate sector rank (or define a fixed order) and save it to a new Google Sheet range/sheet (e.g., `sector_rank`).
2.  **Patch 2 (`shield.R`)**: Modify `shield.R` to read this persisted `sector_rank` from Google Sheets instead of calculating it via `colSums`.
3.  **Patch 3**: Add fallback logic in `shield.R` if the external rank is missing.

## Code Patch
### Part 1: update_kidx.R (Conceptual)
```r
# ... existing code calling update_myidx ...

# New Logic: Calculate and Save Sector Rank
# Note: Requires loading price data similar to shield.R to calculate rank
# OR defining a static order if that's the intent.

# Example: Saving a determined rank to GS
library(googlesheets4)
ssid <- "..." # ID of the spreadsheet
sector_rank_df <- data.frame(
  sector = c("sector1", "sector2", ...),
  rank = c(1, 2, ...)
)
sheet_write(sector_rank_df, ss = ssid, sheet = "sector_rank")
```

### Part 2: shield.R (Modification)
```r
# ... existing code ...

# OLD:
# prices_run_idx_sort<-prices_run_idx[,(order(colSums(tail(prices_run_idx[,-1])),decreasing = T)+1)[1:2]]

# NEW:
sector_rank_gs <- tryCatch(read_sheet(ssid, sheet="sector_rank"), error=function(e) NULL)

if (!is.null(sector_rank_gs)) {
  # Apply external rank
  # (Implementation of reorder_sectors function required here)
  prices_run_idx_sort <- reorder_sectors(prices_run_idx, sector_rank_gs)
} else {
  # Fallback to original logic
  prices_run_idx_sort <- prices_run_idx[,(order(colSums(tail(prices_run_idx[,-1])),decreasing = T)+1)[1:2]]
}
```

## Optimal Insertion Point
*   **Primary**: `update_kidx.R` (end of file) for *determining* and *saving* the order.
*   **Secondary**: `shield.R` (line 52 approx) for *reading* and *applying* the order.

## Risks & Assumptions
*   **Risk**: `update_kidx.R` typically runs lighter updates; adding price loading logic (to calculate rank) might increase runtime or dependencies.
*   **Assumption**: The user intends for `update_kidx.R` to be the "source of truth" for sector order.
*   **Typo Note**: User referred to `update_kinx.R`; interpreted as `update_kidx.R`.

## Status Checklist
*   [x] Static Analysis Complete
*   [x] Key Files Identified
*   [x] Data Flow Mapped
*   [x] Patch Designed (Revised for `update_kidx.R`)
*   [ ] Patch Applied (Pending Implementation)
