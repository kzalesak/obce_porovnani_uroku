import os
import sys
import pandas as pd
import Levenshtein
import csv
import glob
import re
from datetime import datetime

# --- 1. Cross-Platform Single Keypress Handler ---
def get_single_keypress():
    """Reads a single keypress. Supports Ctrl+C, Q (Quit), and S (Skip)."""
    if os.name == 'nt':
        import msvcrt
        ch = msvcrt.getch()
        try:
            char = ch.decode('utf-8').upper()
        except:
            return None
    else:
        import tty
        import termios
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(sys.stdin.fileno())
            char = sys.stdin.read(1).upper()
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    
    if char == '\x03':
        print("\n[Ctrl+C] Interrupted by user. Exiting...")
        sys.exit(0)
    return char

# --- 2. TSV Cache Management ---
CACHE_FILE = 'data/clean/s4.1_kontrola_predcasneho_splaceni_temp/decision_cache.tsv'

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
    except:
        return pd.NaT

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

# --- 4. Main Logic ---
def main():
    input_dir = 'data/clean/s3_final_ids'
    output_dir = 'data/clean/s4.1_kontrola_predcasneho_splaceni'
    os.makedirs(output_dir, exist_ok=True)
    
    datasets = {}
    file_mapping = {}
    
    print("--- Scanning Directory ---")
    search_pattern = os.path.join(input_dir, '*.tsv')
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
    early_payoffs = []

    COL_MUNI = 'Účetní jednotkaZC_UCJED:ZC_UCJED'
    COL_LEND_ICO = 'IČO poskytovatele/BIC/ZC_PARTP:ZC_PARTP'
    COL_LEND_NAME = 'Název poskytovatele/BIC/ZC_NAZPOS:ZC_NAZPOS'
    COL_MATURITY = 'Termín splatnosti/BIC/ZC_TERSPL:ZC_TERSPL'
    COL_SJEDNANA = 'Sjednaná výše U/Z/NFVKYF_0002:ZU_VYUV'
    COL_CERPANA = 'Čerpaná výše U/Z/NVFKYF_0004:ZU_CEUV'
    COL_UCEL = 'Účel/BIC/ZC_UCFP:ZC_UCFP'

    for i in range(len(years) - 1):
        y1, y2 = years[i], years[i+1]
        print(f"\n{'='*50}\nCOMPARING {y1} -> {y2}\n{'='*50}")
        
        df1 = prep_dataset_dates(datasets[y1], COL_MUNI, COL_LEND_ICO)
        df2 = prep_dataset_dates(datasets[y2], COL_MUNI, COL_LEND_ICO)
        
        df1['Maturity_Year'] = df1[COL_MATURITY].str[:4].astype(float)
        expected = df1[df1['Maturity_Year'] > y1]
        
        df2_composites = set(df2['COMPOSITE_ID'].dropna())

        for _, loan_y1 in expected.iterrows():
            comp_id_y1 = loan_y1['COMPOSITE_ID']
            if comp_id_y1 in df2_composites:
                continue
                
            candidates = df2[(df2[COL_MUNI] == loan_y1[COL_MUNI]) & (df2[COL_LEND_ICO] == loan_y1[COL_LEND_ICO])]
            match_found = False
            
            for _, cand_y2 in candidates.iterrows():
                comp_id_y2 = cand_y2['COMPOSITE_ID']
                
                # Smart Match Variables
                is_same_start = (loan_y1['START_DATE'] == cand_y2['START_DATE']) and pd.notnull(loan_y1['START_DATE'])
                delta_days = (cand_y2['MAT_DATE'] - loan_y1['MAT_DATE']).days if pd.notnull(loan_y1['MAT_DATE']) and pd.notnull(cand_y2['MAT_DATE']) else 0
                is_dedup_mismatch = (loan_y1['ID'] == cand_y2['ID']) and (loan_y1['DEDUP_SUFFIX'] != cand_y2['DEDUP_SUFFIX'])
                dist = Levenshtein.distance(get_fuzzy_key(loan_y1), get_fuzzy_key(cand_y2))
                
                requires_manual = False
                auto_match = False
                reason = ""

                # --- The Smart Logic Engine ---
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
                        
                        # Check for odd bouncing patterns
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

                # --- Action Execution ---
                if auto_match or requires_manual:
                    decision = 'Y' if auto_match else None
                    cache_key = (comp_id_y1, comp_id_y2)
                    
                    if not auto_match:
                        decision = decision_cache.get(cache_key)

                    if not decision:
                        print(f"\n--- REVIEW NEEDED ({y1} vs {y2}) ---")
                        print(f"Reason: {reason}")
                        print(f"Muni IČO: {loan_y1[COL_MUNI]} | Lender: {loan_y1[COL_LEND_NAME]} ({loan_y1[COL_LEND_ICO]})")
                        print(f"[{y1}] ID Dates: {get_fuzzy_key(loan_y1)} | Sjednáno: {loan_y1[COL_SJEDNANA]} | Čerpáno: {loan_y1[COL_CERPANA]} | YOY: {loan_y1.get('LOAN_YOY_DATE_CHANGE','')}")
                        print(f"[{y2}] ID Dates: {get_fuzzy_key(cand_y2)} | Sjednáno: {cand_y2[COL_SJEDNANA]} | Čerpáno: {cand_y2[COL_CERPANA]}")
                        print(f"ACTION: [Y]es, same loan | [N]o, different | [S]kip | [Q]uit: ", end="", flush=True)

                        while True:
                            decision = get_single_keypress()
                            if decision in ['Y', 'N', 'S', 'Q']: break
                        
                        print(decision)
                        if decision == 'Q': sys.exit(0)
                        if decision == 'S': continue
                        
                        decision_cache[cache_key] = decision
                        save_to_cache(comp_id_y1, comp_id_y2, decision)
                    
                    if decision == 'Y':
                        if auto_match:
                            print(f"[SMART] {reason} (Muni: {loan_y1[COL_MUNI]})")
                            
                        idx_y2 = df2.index[df2['COMPOSITE_ID'] == comp_id_y2].tolist()[0]
                        
                        # Modify ONLY the regular ID and suffix. Leave COMPOSITE_ID completely alone.
                        datasets[y2].at[idx_y2, 'ID'] = loan_y1['ID']
                        datasets[y2].at[idx_y2, 'DEDUP_SUFFIX'] = loan_y1['DEDUP_SUFFIX']
                        
                        # Set the Date Change classification
                        if delta_days > 0:
                            datasets[y2].at[idx_y2, 'LOAN_YOY_DATE_CHANGE'] = "Extended"
                        elif delta_days < 0:
                            datasets[y2].at[idx_y2, 'LOAN_YOY_DATE_CHANGE'] = "Shortened"
                            
                        df2_composites.add(comp_id_y2) # Add to tracker so we don't evaluate it again
                        match_found = True
                        break
            
            if not match_found:
                # Mark as Repaid Early in Y1 dataset so the history is preserved
                idx_y1 = df1.index[df1['COMPOSITE_ID'] == comp_id_y1].tolist()[0]
                datasets[y1].at[idx_y1, 'LOAN_YOY_DATE_CHANGE'] = "Repaid Early"
                
                ep_dict = loan_y1.to_dict()
                ep_dict['LOAN_YOY_DATE_CHANGE'] = "Repaid Early"
                early_payoffs.append(ep_dict)

    # --- 5. Clean up temps & Save ---
    print("\nSaving files to s5_kontrola_predcasneho_splaceni...")
    temp_cols = ['START_DATE', 'MAT_DATE', 'MLS_KEY', 'MLS_COUNT', 'Maturity_Year']
    
    for year, df in datasets.items():
        df = df.drop(columns=[c for c in temp_cols if c in df.columns], errors='ignore')
        df.to_csv(os.path.join(output_dir, file_mapping[year]), sep='\t', index=False)
        
    if early_payoffs:
        ep_df = pd.DataFrame(early_payoffs).drop(columns=[c for c in temp_cols], errors='ignore')
        ep_df.to_csv(os.path.join(output_dir, 'early_payoffs_summary.tsv'), sep='\t', index=False)
        print(f"Success! Found {len(early_payoffs)} early payoffs.")
    else:
        print("Success! No early payoffs detected.")

if __name__ == "__main__":
    main()