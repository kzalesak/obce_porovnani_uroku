# Data Processing Pipeline: Quickstart Guide

This pipeline is designed to clean, enrich, and analyze financial data using IČO identifiers and ARES registry data.

## 🛠️ Prerequisites

Ensure you have Python installed with the following libraries:
* **pandas & numpy**: For data manipulation.
* **requests**: For ARES API communication.
* **python-Levenshtein**: For fuzzy string matching.

use `pip install pandas numpy requests levenshtein`

## 📂 Initial Setup

1.  **Raw Data**: Place your initial semicolon-separated `.csv` files in `data/raw/`.
2.  **Codelists**: Place the legal forms codelist (`VAZ0056_0149_CS.csv`) in `data/sazebniky/`.

---

## 🚀 Execution Steps

### 1. Header Cleaning
Run: `python s1_header_clean.py`
* **What happens**: Cleans column names (removes quotes/extra spaces) and converts files to Tab-Separated Values (TSV).
* **Output**: `data/clean/s1_header/`.

### 2. ARES Enrichment
Run: `python s2.1_find_ico.py`
* **What happens**: Validates IČO numbers against the ARES database to fetch official subject names and legal forms.
* **Output**: Enriched datasets and lists of missing IČOs in `data/clean/s2.1_found_ICO/`.

### 3. Identify Typos (Levenshtein)
Run: `python s2.2_levenshtein_clean.py`
* **What happens**: Compares missing IČOs with valid ones to find potential typos (distance = 1).
* **Manual Step**: Review the analysis in `data/levenshtein_temp/dist1/`. Once corrected, place files in `data/clean/s2.2_levenshtein_clean/`.

### 4. Generate Unique IDs
Run: `python s3_generate_IDs.py`
* **What happens**: Creates a stable `COMPOSITE_ID` for every row based on IČOs, dates, and transaction amounts.
* **Output**: Unique datasets in `data/clean/s3_final_ids/`.

### 5. Interest Rate Analysis
Run: `python s4_kontrola_fixnosti.py`
* **What happens**: Merges data across different years to classify interest rates as **FIXED** or **VARIABLE**.
* **Output**: A final matrix for manual review in `data/clean/kontrola_fixnosti/`.

### 6. Early Repayment Check
Run: `python s4.1_kontrola_splatnosti.py`
* **What happens**: Matches loans across consecutive years to detect loans that were **extended**, **shortened**, or **repaid early**. A loan whose `Sjednaná výše` (agreed amount) uniquely matches one candidate in the next year is auto-resolved with no human input (amount is ~99.95% stable for the same loan year-to-year, so this alone clears the vast majority of cases — mainly the "collision" scenario where a municipality has multiple loans from the same lender starting the same day). Only genuinely ambiguous cases (duplicate amounts, or no amount match at all) need a human — the run never blocks.
* **Manual Step**: Ambiguous candidates are written to `data/clean/s4.1_kontrola_predcasneho_splaceni_temp/review_needed.tsv`. Open it in Excel/Sheets, fill the `DECISION` column with `Y`/`N`, save, then run `python s4.1_kontrola_splatnosti.py --apply-review` to fold your decisions into `decision_cache.tsv` and produce the final output. Re-running the plain command while `review_needed.tsv` has unapplied edits will refuse to proceed (pass `--force` to discard them instead).
* **Other flags**: `--count` does a dry run reporting how many cases would still need review, without writing anything.
* **Output**: Updated yearly datasets (unresolved rows marked `PENDING_REVIEW`) plus `early_payoffs_summary.tsv` in `data/clean/s4.1_kontrola_predcasneho_splaceni/`. Cache lives in `data/clean/s4.1_kontrola_predcasneho_splaceni_temp/decision_cache.tsv`.

---

## 🔑 Key Column Mapping
The pipeline automatically identifies these critical fields in your data:
* **ZC_PARTP:ZC_PARTP**: Provider IČO.
* **ZC_ICO:ZC_ICO**: Recipient IČO.
* **ZC_DATUS:ZC_DATUS**: Date of record.
* **ZU_VYUV**: Amount.
* **ZC_URSA:ZC_URSA**: Interest rate.