import os
import sys
import argparse
import pandas as pd
import Levenshtein
import csv
import glob
import re
from collections import Counter
from datetime import datetime

# --- 1. Paths ---
INPUT_DIR = 'data/clean/s3_final_ids'
OUTPUT_DIR = 'data/clean/s4.1_kontrola_predcasneho_splaceni'
TEMP_DIR = 'data/clean/s4.1_kontrola_predcasneho_splaceni_temp'
CACHE_FILE = os.path.join(TEMP_DIR, 'decision_cache.tsv')
REVIEW_FILE = os.path.join(TEMP_DIR, 'review_needed.tsv')

COL_MUNI = 'Účetní jednotkaZC_UCJED:ZC_UCJED'
COL_LEND_ICO = 'IČO poskytovatele/BIC/ZC_PARTP:ZC_PARTP'
COL_LEND_NAME = 'Název poskytovatele/BIC/ZC_NAZPOS:ZC_NAZPOS'
COL_MATURITY = 'Termín splatnosti/BIC/ZC_TERSPL:ZC_TERSPL'
COL_SJEDNANA = 'Sjednaná výše U/Z/NFVKYF_0002:ZU_VYUV'
COL_CERPANA = 'Čerpaná výše U/Z/NVFKYF_0004:ZU_CEUV'
COL_UCEL = 'Účel/BIC/ZC_UCFP:ZC_UCFP'

# Relative tolerance for "same Sjednaná výše". Empirically, for loans confirmed
# identical by exact ID match, 99.95% have an IDENTICAL Sjednaná výše and the
# rest differ by <0.1% (rounding noise) - see plan doc for the analysis.
AMOUNT_TOL = 0.001

# --- 2. TSV Cache Management ---
def load_cache():
    cache = {}
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='\t')
            next(reader, None)
            for row in reader:
                if len(row) >= 3:
                    cache[(row[0], row[1])] = row[2]
    return cache

def save_to_cache(id_y, id_y_plus_1, decision):
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    file_exists = os.path.exists(CACHE_FILE)
    with open(CACHE_FILE, 'a', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter='\t')
        if not file_exists:
            writer.writerow(['ID_Year_Y', 'ID_Year_Y1', 'Decision'])
        writer.writerow([id_y, id_y_plus_1, decision])

# --- 3. Helpers ---
def get_fuzzy_key(row):
    """Extracts ONLY datum sjednani and datum splatnosti from the ID."""
    parts = str(row['ID']).split('-')
    if len(parts) >= 4:
        return f"{parts[2]}-{parts[3]}"
    return ""

def parse_date(date_str):
    try:
        return pd.to_datetime(date_str, format='%Y%m%d')
    except Exception:
        return pd.NaT

def parse_amount(value):
    try:
        return float(str(value).strip().replace(' ', '').replace(',', '.'))
    except (TypeError, ValueError):
        return None

def amounts_match(a, b, tol=AMOUNT_TOL):
    """Relative-tolerance comparison. Sjednaná výše is near-immutable for the
    same loan year-to-year (99.95% identical), so this is a strong signal."""
    fa, fb = parse_amount(a), parse_amount(b)
    if fa is None or fb is None:
        return False
    if fa == 0 and fb == 0:
        return True
    denom = max(abs(fa), abs(fb))
    return denom > 0 and abs(fa - fb) / denom <= tol

def amount_diff_pct(a, b):
    fa, fb = parse_amount(a), parse_amount(b)
    if fa is None or fb is None:
        return ""
    denom = max(abs(fa), abs(fb), 1e-9)
    return round(abs(fa - fb) / denom * 100, 3)

def ucel_similarity(a, b):
    """Case-insensitive Levenshtein ratio. Účel is hand-typed and drifts
    (85.5% exact, 93.2% >=0.7 ratio) - display-only signal, never used to
    auto-accept a match."""
    sa = str(a).strip().lower() if pd.notnull(a) else ""
    sb = str(b).strip().lower() if pd.notnull(b) else ""
    if not sa and not sb:
        return 1.0
    return round(Levenshtein.ratio(sa, sb), 3)

def prep_dataset_dates(df, muni_col, lender_col):
    """Extracts dates and calculates collision counts for smart matching."""
    df['START_DATE'] = df['ID'].apply(lambda x: parse_date(str(x).split('-')[2]) if len(str(x).split('-')) >= 4 else pd.NaT)
    df['MAT_DATE'] = df['ID'].apply(lambda x: parse_date(str(x).split('-')[3]) if len(str(x).split('-')) >= 4 else pd.NaT)

    # Create a composite key for collision detection (Muni + Lender + StartDate)
    df['MLS_KEY'] = df[muni_col].astype(str) + "_" + df[lender_col].astype(str) + "_" + df['START_DATE'].astype(str)

    # Count occurrences of this exact combination in the year
    counts = df.groupby('MLS_KEY').size()
    df['MLS_COUNT'] = df['MLS_KEY'].map(counts)

    if 'LOAN_YOY_DATE_CHANGE' not in df.columns:
        df['LOAN_YOY_DATE_CHANGE'] = ""
    return df

def reason_kind(reason):
    """Buckets a free-text reason string into a short category for reporting."""
    if reason.startswith("Auto-matched by unique"):
        return "Auto-matched by unique Sjednaná výše"
    if reason.startswith("Collision"):
        return "Collision (multiple loans share start date)"
    if reason.startswith("Small duration change"):
        return "Small duration change (<30 days, possible typo)"
    if reason.startswith("Odd Pattern"):
        return "Odd bounce pattern (Extended<->Shortened) + fuzzy match"
    if reason.startswith("Fuzzy Typo Match"):
        return "Fuzzy typo match, different start dates"
    return reason or "(unknown)"

def compute_delta_days(loan_y1, cand_y2):
    if pd.notnull(loan_y1['MAT_DATE']) and pd.notnull(cand_y2['MAT_DATE']):
        return (cand_y2['MAT_DATE'] - loan_y1['MAT_DATE']).days
    return 0

def apply_match(datasets, y2, loan_y1, comp_id_y2, delta_days):
    idx_y2 = datasets[y2].index[datasets[y2]['COMPOSITE_ID'] == comp_id_y2].tolist()[0]
    # Modify ONLY the regular ID and suffix. Leave COMPOSITE_ID completely alone.
    datasets[y2].at[idx_y2, 'ID'] = loan_y1['ID']
    datasets[y2].at[idx_y2, 'DEDUP_SUFFIX'] = loan_y1['DEDUP_SUFFIX']
    if delta_days > 0:
        datasets[y2].at[idx_y2, 'LOAN_YOY_DATE_CHANGE'] = "Extended"
    elif delta_days < 0:
        datasets[y2].at[idx_y2, 'LOAN_YOY_DATE_CHANGE'] = "Shortened"

def make_review_row(y1, y2, loan_y1, cand_y2, reason):
    return {
        'GROUP_ID': f"{loan_y1['MLS_KEY']}__{y2}",
        'YEAR_Y1': y1,
        'YEAR_Y2': y2,
        'Y1_COMPOSITE_ID': loan_y1['COMPOSITE_ID'],
        'Y2_COMPOSITE_ID': cand_y2['COMPOSITE_ID'],
        'MUNI': loan_y1[COL_MUNI],
        'LENDER_NAME': loan_y1[COL_LEND_NAME],
        'LENDER_ICO': loan_y1[COL_LEND_ICO],
        'Y1_ID_DATES': get_fuzzy_key(loan_y1),
        'Y2_ID_DATES': get_fuzzy_key(cand_y2),
        'Y1_SJEDNANA': loan_y1[COL_SJEDNANA],
        'Y2_SJEDNANA': cand_y2[COL_SJEDNANA],
        'AMOUNT_DIFF_PCT': amount_diff_pct(loan_y1[COL_SJEDNANA], cand_y2[COL_SJEDNANA]),
        'Y1_CERPANA': loan_y1[COL_CERPANA],
        'Y2_CERPANA': cand_y2[COL_CERPANA],
        'Y1_UCEL': loan_y1[COL_UCEL],
        'Y2_UCEL': cand_y2[COL_UCEL],
        'UCEL_SIMILARITY': ucel_similarity(loan_y1[COL_UCEL], cand_y2[COL_UCEL]),
        'PREV_YOY': loan_y1.get('LOAN_YOY_DATE_CHANGE', ''),
        'REASON': reason,
        'DECISION': '',
    }

# --- 4. Review-queue file I/O ---
def review_file_has_unapplied_edits(decision_cache):
    """True if review_needed.tsv has Y/N decisions not yet folded into the
    cache - guards against a plain re-run silently discarding manual work."""
    if not os.path.exists(REVIEW_FILE):
        return False
    df = pd.read_csv(REVIEW_FILE, sep='\t', dtype=str).fillna('')
    if 'DECISION' not in df.columns:
        return False
    for _, row in df.iterrows():
        decision = row['DECISION'].strip().upper()
        if decision in ('Y', 'N'):
            key = (row['Y1_COMPOSITE_ID'], row['Y2_COMPOSITE_ID'])
            if key not in decision_cache:
                return True
    return False

def ingest_review_file(decision_cache):
    if not os.path.exists(REVIEW_FILE):
        print(f"No review file found at {REVIEW_FILE} - nothing to apply.")
        return
    df = pd.read_csv(REVIEW_FILE, sep='\t', dtype=str).fillna('')
    applied = 0
    blanks = 0
    for _, row in df.iterrows():
        decision = row.get('DECISION', '').strip().upper()
        if decision not in ('Y', 'N'):
            blanks += 1
            continue
        key = (row['Y1_COMPOSITE_ID'], row['Y2_COMPOSITE_ID'])
        if key in decision_cache:
            continue
        decision_cache[key] = decision
        save_to_cache(key[0], key[1], decision)
        applied += 1
    print(f"Applied {applied} new decision(s) from review_needed.tsv into decision_cache.tsv.")
    if blanks:
        print(f"{blanks} row(s) still have no Y/N decision and will be re-flagged for review.")

def write_review_queue(review_rows):
    os.makedirs(TEMP_DIR, exist_ok=True)
    if not review_rows:
        if os.path.exists(REVIEW_FILE):
            os.remove(REVIEW_FILE)
        print("No ambiguous candidates remain - review_needed.tsv not written.")
        return
    df = pd.DataFrame(review_rows).sort_values(['GROUP_ID', 'Y1_COMPOSITE_ID'])
    df.to_csv(REVIEW_FILE, sep='\t', index=False)
    print(f"Wrote {len(review_rows)} candidate pair(s) needing review to {REVIEW_FILE}")
    print("Open it in Excel/Sheets, fill the DECISION column with Y/N, save, "
          "then run again with --apply-review.")

# --- 5. Matching pass (shared by --count and the real run) ---
def run_matching_pass(datasets, years, decision_cache, count_mode):
    early_payoffs = []
    review_rows = []

    stats = Counter()
    reason_counts = Counter()

    for i in range(len(years) - 1):
        y1, y2 = years[i], years[i + 1]
        print(f"\n{'='*50}\nCOMPARING {y1} -> {y2}\n{'='*50}")

        df1 = prep_dataset_dates(datasets[y1], COL_MUNI, COL_LEND_ICO)
        df2 = prep_dataset_dates(datasets[y2], COL_MUNI, COL_LEND_ICO)

        df1['Maturity_Year'] = df1[COL_MATURITY].str[:4].astype(float)
        expected = df1[df1['Maturity_Year'] > y1]

        df2_composites = set(df2['COMPOSITE_ID'].dropna())
        claimed_candidates = set()

        pair_counts = Counter()

        for _, loan_y1 in expected.iterrows():
            comp_id_y1 = loan_y1['COMPOSITE_ID']
            if comp_id_y1 in df2_composites:
                continue

            pair_counts['unresolved_loans'] += 1

            # Exclude Y2 rows already claimed by another Y1 loan this pass.
            candidates = df2[
                (df2[COL_MUNI] == loan_y1[COL_MUNI]) &
                (df2[COL_LEND_ICO] == loan_y1[COL_LEND_ICO]) &
                (~df2['COMPOSITE_ID'].isin(claimed_candidates))
            ]

            if len(candidates) == 0:
                pair_counts['no_candidates'] += 1
                if not count_mode:
                    idx_y1 = df1.index[df1['COMPOSITE_ID'] == comp_id_y1].tolist()[0]
                    datasets[y1].at[idx_y1, 'LOAN_YOY_DATE_CHANGE'] = "Repaid Early"
                    ep_dict = loan_y1.to_dict()
                    ep_dict['LOAN_YOY_DATE_CHANGE'] = "Repaid Early"
                    early_payoffs.append(ep_dict)
                continue

            match_found = False
            has_pending = False

            # --- Amount pass: Sjednaná výše is ~99.95% stable for the same
            # loan year-to-year, so a UNIQUE amount match is decisive.
            amount_match_mask = candidates[COL_SJEDNANA].apply(
                lambda v: amounts_match(loan_y1[COL_SJEDNANA], v)
            )
            amount_matches = candidates[amount_match_mask]

            if len(amount_matches) == 1:
                cand_y2 = amount_matches.iloc[0]
                comp_id_y2 = cand_y2['COMPOSITE_ID']
                delta_days = compute_delta_days(loan_y1, cand_y2)
                direction = "Extended" if delta_days > 0 else ("Shortened" if delta_days < 0 else "No date change")
                reason = f"Auto-matched by unique Sjednaná výše match. {direction} by {abs(delta_days)} days."

                stats['auto_matched'] += 1
                stats['auto_amount_matched'] += 1
                pair_counts['auto_matched'] += 1
                claimed_candidates.add(comp_id_y2)

                if not count_mode:
                    print(f"[AMOUNT] {reason} (Muni: {loan_y1[COL_MUNI]})")
                    apply_match(datasets, y2, loan_y1, comp_id_y2, delta_days)
                match_found = True

            else:
                # Amount didn't uniquely disambiguate (0 or 2+ candidates
                # share the amount) - fall back to the original date-based
                # heuristics, unchanged from before.
                for _, cand_y2 in candidates.iterrows():
                    comp_id_y2 = cand_y2['COMPOSITE_ID']

                    is_same_start = (loan_y1['START_DATE'] == cand_y2['START_DATE']) and pd.notnull(loan_y1['START_DATE'])
                    delta_days = compute_delta_days(loan_y1, cand_y2)
                    is_dedup_mismatch = (loan_y1['ID'] == cand_y2['ID']) and (loan_y1['DEDUP_SUFFIX'] != cand_y2['DEDUP_SUFFIX'])
                    dist = Levenshtein.distance(get_fuzzy_key(loan_y1), get_fuzzy_key(cand_y2))

                    requires_manual = False
                    auto_match = False
                    reason = ""

                    # --- The Smart Logic Engine (date-based, unchanged) ---
                    if is_same_start:
                        if loan_y1['MLS_COUNT'] > 1 or cand_y2['MLS_COUNT'] > 1:
                            requires_manual = True
                            reason = "Collision: Multiple loans share this start date."
                        elif abs(delta_days) > 0 and abs(delta_days) < 30:
                            requires_manual = True
                            reason = f"Small duration change ({delta_days} days). Possible typo."
                        elif delta_days != 0:
                            prev_change = str(loan_y1.get('LOAN_YOY_DATE_CHANGE', ''))
                            new_dir = "Extended" if delta_days > 0 else "Shortened"
                            if (prev_change == "Extended" and new_dir == "Shortened") or (prev_change == "Shortened" and new_dir == "Extended"):
                                if dist <= 2:
                                    requires_manual = True
                                    reason = f"Odd Pattern (Was {prev_change}, now {new_dir}) + Fuzzy Typo Match."
                            else:
                                auto_match = True
                                reason = f"Auto-Matched 1:1. {new_dir} by {abs(delta_days)} days."
                    else:
                        if is_dedup_mismatch or dist <= 2:
                            requires_manual = True
                            reason = f"Fuzzy Typo Match (Dist: {dist}). Different start dates."

                    if not (auto_match or requires_manual):
                        continue

                    cache_key = (comp_id_y1, comp_id_y2)
                    decision = 'Y' if auto_match else decision_cache.get(cache_key)

                    if auto_match:
                        stats['auto_matched'] += 1
                        stats['auto_date_matched'] += 1
                        pair_counts['auto_matched'] += 1
                    elif decision:
                        stats['manual_cached'] += 1
                        pair_counts['manual_cached'] += 1
                    else:
                        stats['manual_new'] += 1
                        pair_counts['manual_new'] += 1
                        reason_counts[reason_kind(reason)] += 1

                    if not decision:
                        if count_mode:
                            # Upper bound: keep scanning other candidates too.
                            continue
                        review_rows.append(make_review_row(y1, y2, loan_y1, cand_y2, reason))
                        has_pending = True
                        continue

                    if decision == 'Y':
                        if auto_match:
                            print(f"[SMART] {reason} (Muni: {loan_y1[COL_MUNI]})")
                        claimed_candidates.add(comp_id_y2)
                        if not count_mode:
                            apply_match(datasets, y2, loan_y1, comp_id_y2, delta_days)
                        match_found = True
                        break

            if not match_found:
                if count_mode:
                    pair_counts['no_match_in_dry_run'] += 1
                    continue
                if has_pending:
                    idx_y1 = df1.index[df1['COMPOSITE_ID'] == comp_id_y1].tolist()[0]
                    datasets[y1].at[idx_y1, 'LOAN_YOY_DATE_CHANGE'] = "PENDING_REVIEW"
                    pair_counts['pending_review'] += 1
                else:
                    idx_y1 = df1.index[df1['COMPOSITE_ID'] == comp_id_y1].tolist()[0]
                    datasets[y1].at[idx_y1, 'LOAN_YOY_DATE_CHANGE'] = "Repaid Early"
                    ep_dict = loan_y1.to_dict()
                    ep_dict['LOAN_YOY_DATE_CHANGE'] = "Repaid Early"
                    early_payoffs.append(ep_dict)

        stats['unresolved_loans'] += pair_counts['unresolved_loans']

        if count_mode:
            print(f"  Loans expected but missing by COMPOSITE_ID: {pair_counts['unresolved_loans']}")
            print(f"    - No same muni+lender candidate at all (clean repaid-early): {pair_counts['no_candidates']}")
            print(f"    - Auto-matched, no review ever needed:                       {pair_counts['auto_matched']}")
            print(f"    - Ambiguous but already answered in decision_cache.tsv:      {pair_counts['manual_cached']}")
            print(f"    - Ambiguous, NOT cached -> would need review right now:      {pair_counts['manual_new']}")
            print(f"    - Candidates existed but none matched any signal (clean repaid-early): {pair_counts['no_match_in_dry_run']}")

    if count_mode:
        print(f"\n{'='*50}\nSUMMARY ACROSS ALL {len(years)-1} YEAR-PAIRS\n{'='*50}")
        print(f"Loans expected but missing by COMPOSITE_ID (total): {stats['unresolved_loans']}")
        print(f"  Auto-matched (no review ever):                     {stats['auto_matched']}")
        print(f"    of which by unique amount match:                 {stats['auto_amount_matched']}")
        print(f"    of which by date logic (unchanged):               {stats['auto_date_matched']}")
        print(f"  Ambiguous, already cached (no new review row):     {stats['manual_cached']}")
        print(f"  Ambiguous, NOT cached (would need review today):   {stats['manual_new']}")
        if reason_counts:
            print("\nBreakdown of the 'would need review today' cases by reason:")
            for kind, n in reason_counts.most_common():
                print(f"  {n:5d}  {kind}")
        print(f"\n(Note: 'would need review today' is an upper bound - a real run "
              f"stops looking at a loan's other candidates as soon as one gets a 'Y'.)")

    return early_payoffs, review_rows

# --- 6. Main ---
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--count', action='store_true',
        help="Dry run: walk the same matching logic but never write output or "
             "the review queue. Report how many candidate pairs would still "
             "need manual review."
    )
    parser.add_argument(
        '--apply-review', action='store_true',
        help="Ingest Y/N decisions from review_needed.tsv into decision_cache.tsv, "
             "then run the normal matching pass."
    )
    parser.add_argument(
        '--force', action='store_true',
        help="Proceed even if review_needed.tsv has unapplied edits (they will "
             "be overwritten). Without this flag, run --apply-review first."
    )
    args = parser.parse_args()
    count_mode = args.count

    if not count_mode:
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    datasets = {}
    file_mapping = {}

    print("--- Scanning Directory ---")
    search_pattern = os.path.join(INPUT_DIR, '*.tsv')
    file_paths = glob.glob(search_pattern)

    for filepath in file_paths:
        filename = os.path.basename(filepath)
        match = re.search(r'_(\d{4})\d{3}\.tsv$', filename)
        if match:
            year = int(match.group(1))
            datasets[year] = pd.read_csv(filepath, sep='\t', dtype=str)
            file_mapping[year] = filename
            print(f"Loaded: {filename} ({year})")

    years = sorted(datasets.keys())
    if len(years) < 2:
        print("Error: Need at least 2 datasets to perform comparison.")
        return

    decision_cache = load_cache()

    if args.apply_review:
        ingest_review_file(decision_cache)
    elif not count_mode and review_file_has_unapplied_edits(decision_cache) and not args.force:
        print(f"\n{REVIEW_FILE} has Y/N decisions that haven't been applied yet.")
        print("Run with --apply-review to fold them into decision_cache.tsv first, "
              "or pass --force to discard them and rescan from scratch.")
        sys.exit(1)

    early_payoffs, review_rows = run_matching_pass(datasets, years, decision_cache, count_mode)

    if count_mode:
        return

    # --- 7. Clean up temps & Save ---
    write_review_queue(review_rows)

    print(f"\nSaving files to {OUTPUT_DIR}...")
    temp_cols = ['START_DATE', 'MAT_DATE', 'MLS_KEY', 'MLS_COUNT', 'Maturity_Year']

    for year, df in datasets.items():
        df = df.drop(columns=[c for c in temp_cols if c in df.columns], errors='ignore')
        df.to_csv(os.path.join(OUTPUT_DIR, file_mapping[year]), sep='\t', index=False)

    if early_payoffs:
        ep_df = pd.DataFrame(early_payoffs).drop(columns=[c for c in temp_cols], errors='ignore')
        ep_df.to_csv(os.path.join(OUTPUT_DIR, 'early_payoffs_summary.tsv'), sep='\t', index=False)
        print(f"Success! Found {len(early_payoffs)} early payoffs.")
    else:
        print("Success! No early payoffs detected.")

if __name__ == "__main__":
    main()
