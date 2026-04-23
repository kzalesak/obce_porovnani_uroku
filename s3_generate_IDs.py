import pandas as pd
import hashlib
import numpy as np
import os
import re

# ================= NASTAVENÍ CEST =================
INPUT_DIR = 'data/clean'
OUTPUT_DIR = 'data/clean/s3_final_ids'
EXCLUDED_DIR = os.path.join(OUTPUT_DIR, 'excluded', 'duplicates')

def get_col(df, keyword):
    """Pomocná funkce pro nalezení sloupce podle technického ID."""
    for col in df.columns:
        if keyword in col:
            return col
    raise ValueError(f"Sloupec obsahující '{keyword}' nebyl nalezen v souboru.")

def clean_pad(series, pad_len):
    """Zajistí string, odstraní .0 a doplní nuly zleva."""
    s = series.astype(str).str.strip()
    s = s.str.replace(r'\.0$', '', regex=True)
    s = s.replace('nan', '0')
    return s.str.zfill(pad_len)

def short_hash(val):
    """Vytvoří stabilní krátký hash z textu."""
    clean_val = str(val).strip().lower()
    return hashlib.md5(clean_val.encode('utf-8')).hexdigest()[:8]

def process_files():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(EXCLUDED_DIR, exist_ok=True)

    files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.tsv') and os.path.isfile(os.path.join(INPUT_DIR, f))]
    
    if not files:
        print(f"V {INPUT_DIR} nebyly nalezeny žádné .tsv soubory.")
        return

    processed_years = []
    report_lines = ["=== Analýza duplicitních záznamů a odstraňování vadných dat ===\n"]
    cross_report_lines = ["\n=== Křížové odstranění ID napříč roky ==="]
    
    file_store = {}
    global_excluded_ids = set()

    # ================= FÁZE 1: LOKÁLNÍ ZPRACOVÁNÍ A SBĚR ID =================
    for filename in files:
        file_path = os.path.join(INPUT_DIR, filename)
        print(f"Fáze 1 (Načítání): {filename}")
        
        year_match = re.search(r'\d{4}', filename)
        data_year = year_match.group(0) if year_match else "XXXX"
        if data_year != "XXXX":
            processed_years.append(data_year)
        
        df = pd.read_csv(file_path, sep='\t', dtype=str)

        try:
            df['DATAYEAR'] = data_year
            col_ico = get_col(df, 'ZC_ICO:ZC_ICO')
            col_ico_posk = get_col(df, 'ZC_PARTP:ZC_PARTP')
            col_datus = get_col(df, 'ZC_DATUS:ZC_DATUS')
            col_terspl = get_col(df, 'ZC_TERSPL:ZC_TERSPL')
            col_vyse = get_col(df, 'ZU_VYUV')
            col_cerp = get_col(df, 'ZU_CEUV') 
            col_posk = get_col(df, 'ZC_NAZPOS:ZC_NAZPOS')
            col_ucel = get_col(df, 'ZC_UCFP:ZC_UCFP')

            # Převod částek
            vyse_num_all = pd.to_numeric(df[col_vyse].astype(str).str.replace(' ', '').str.replace(',', '.'), errors='coerce').fillna(0)
            cerp_num_all = pd.to_numeric(df[col_cerp].astype(str).str.replace(' ', '').str.replace(',', '.'), errors='coerce').fillna(0)

            p1 = clean_pad(df[col_ico], 8)
            p2 = clean_pad(df[col_ico_posk], 8)
            p3 = df[col_datus].astype(str).str.strip()
            p4 = df[col_terspl].astype(str).str.strip()
            p5 = vyse_num_all.astype(int).astype(str)
            df['ID'] = p1 + '-' + p2 + '-' + p3 + '-' + p4 + '-' + p5

            df['DEDUP_SUFFIX'] = np.nan
            df['DEDUP_SUFFIX'] = df['DEDUP_SUFFIX'].astype(object) 
            
            is_duplicate = df.duplicated(subset=['ID'], keep=False)

            if is_duplicate.any():
                h_posk = df.loc[is_duplicate, col_posk].apply(short_hash)
                h_ucel = df.loc[is_duplicate, col_ucel].apply(short_hash)
                df.loc[is_duplicate, 'DEDUP_SUFFIX'] = h_posk + '-' + h_ucel

            df['COMPOSITE_ID'] = df.apply(
                lambda row: f"{row['ID']}-{row['DEDUP_SUFFIX']}-{row['DATAYEAR']}" if pd.notna(row['DEDUP_SUFFIX']) else f"{row['ID']}-{row['DATAYEAR']}", 
                axis=1
            )

            has_dedup = df['DEDUP_SUFFIX'].notna() & (df['DEDUP_SUFFIX'].astype(str).str.strip() != '') & (df['DEDUP_SUFFIX'].astype(str).str.lower() != 'nan')
            is_zero = (vyse_num_all == 0)
            
            df_main = df[~has_dedup & ~is_zero].copy()
            df_zero = df[is_zero].copy()
            if not df_zero.empty:
                df_zero['reason_of_removal'] = 'ZERO_LOAN'

            df_dedup = df[has_dedup & ~is_zero].copy()
            if not df_dedup.empty:
                df_dedup['reason_of_removal'] = 'DEDUP_POPULATED'
                global_excluded_ids.update(df_dedup['ID'].unique())

            excluded_dfs = []
            if not df_dedup.empty: excluded_dfs.append(df_dedup)
            if not df_zero.empty: excluded_dfs.append(df_zero)
            df_excluded = pd.concat(excluded_dfs, ignore_index=True) if excluded_dfs else pd.DataFrame()

            file_store[filename] = {
                'year': data_year,
                'df_main': df_main,
                'df_excluded': df_excluded,
                'zero_count': len(df_zero),
                'local_dedup_count': len(df_dedup),
                'col_vyse': col_vyse,
                'col_cerp': col_cerp,
                'total_rows_start': len(df),
                'unique_ids_start': df['ID'].nunique(),
                'total_vyse_year': vyse_num_all.sum(),
                'total_cerp_year': cerp_num_all.sum()
            }

        except Exception as e:
            print(f"Chyba ve Fázi 1 ({filename}): {e}")

    # ================= FÁZE 2: KŘÍŽOVÉ ČIŠTĚNÍ A REPORT =================
    print("\nFáze 2: Křížové čištění a ukládání...")
    for filename, data in file_store.items():
        df_main = data['df_main']
        df_excluded = data['df_excluded']
        
        mask_cross_remove = df_main['ID'].isin(global_excluded_ids)
        cross_removed_df = df_main[mask_cross_remove].copy()
        
        df_main = df_main[~mask_cross_remove]
        cross_removed_count = 0

        if not cross_removed_df.empty:
            cross_removed_df['reason_of_removal'] = 'CROSS_REMOVED'
            cross_removed_count = len(cross_removed_df)
            df_excluded = pd.concat([df_excluded, cross_removed_df], ignore_index=True) if not df_excluded.empty else cross_removed_df
            
            cross_vyse_num = pd.to_numeric(cross_removed_df[data['col_vyse']].astype(str).str.replace(' ', '').str.replace(',', '.'), errors='coerce').fillna(0)
            cross_report_lines.append(f"  {filename}: dodatečně odebráno {cross_removed_count} řádků (CROSS_REMOVED).")
        else:
            cross_report_lines.append(f"  {filename}: 0 řádků křížově odebráno.")

        # Ukládání hlavního souboru
        df_main.to_csv(os.path.join(OUTPUT_DIR, filename), sep='\t', index=False)
        
        # Ukládání REMOVED souboru
        if not df_excluded.empty:
            name_base = os.path.splitext(filename)[0]
            removed_filename = f"{name_base}_REMOVED.tsv"
            df_excluded.to_csv(os.path.join(EXCLUDED_DIR, removed_filename), sep='\t', index=False)

        # Výpočty pro nejistotu (DEDUP + CROSS, BEZ ZERO_LOAN)
        # Filtrujeme pouze záznamy, které nejsou ZERO_LOAN pro výpočet ztráty
        df_uncertainty = df_excluded[df_excluded['reason_of_removal'] != 'ZERO_LOAN'].copy()
        
        if not df_uncertainty.empty:
            unc_vyse = pd.to_numeric(df_uncertainty[data['col_vyse']].astype(str).str.replace(' ', '').str.replace(',', '.'), errors='coerce').fillna(0)
            unc_cerp = pd.to_numeric(df_uncertainty[data['col_cerp']].astype(str).str.replace(' ', '').str.replace(',', '.'), errors='coerce').fillna(0)
            
            sum_unc_vyse = unc_vyse.sum()
            sum_unc_cerp = unc_cerp.sum()
            
            pct_rows_unc = (len(df_uncertainty) / data['total_rows_start'] * 100) if data['total_rows_start'] > 0 else 0
            pct_vyse_unc = (sum_unc_vyse / data['total_vyse_year'] * 100) if data['total_vyse_year'] > 0 else 0
            pct_cerp_unc = (sum_unc_cerp / data['total_cerp_year'] * 100) if data['total_cerp_year'] > 0 else 0
        else:
            pct_rows_unc = pct_vyse_unc = pct_cerp_unc = 0

        # Sestavení reportu
        out = [
            f"File: {filename} (Rok: {data['year']})",
            f"  Celkem řádků na vstupu: {data['total_rows_start']}",
            f"  --- Důvody odstranění ---",
            f"  Lokální duplicity (DEDUP_POPULATED): {data['local_dedup_count']}",
            f"  Nulová výše úvěru (ZERO_LOAN):       {data['zero_count']} (Pozn: Nezapočítává se do ztráty dat)",
            f"  Křížově odstraněno (CROSS_REMOVED):  {cross_removed_count}",
            f"  Celkem vyřazeno z analýzy:           {len(df_excluded)}",
            f"",
            f"  --- Ztráta dat pro výpočet nejistoty (pouze DEDUP a CROSS) ---",
            f"  Odstraněný podíl úvěrů (řádky): {pct_rows_unc:.3f} %",
            f"  Odstraněný podíl financí (Sjednaná): {pct_vyse_unc:.3f} %",
            f"  Odstraněný podíl financí (Čerpaná):  {pct_cerp_unc:.3f} %",
            f""
        ]
        
        # Histogram a kategorie pro nenulové odstraněné
        df_stats = df_excluded[df_excluded['reason_of_removal'] != 'ZERO_LOAN'].copy()
        if not df_stats.empty:
            df_stats['v'] = pd.to_numeric(df_stats[data['col_vyse']].astype(str).str.replace(' ', '').str.replace(',', '.'), errors='coerce').fillna(0)
            sub_50 = df_stats[df_stats['v'] < 50_000_000]
            out.append(f"  --- Histogram (0 - 50 mil CZK, bez ZERO_LOAN) ---")
            if not sub_50.empty:
                bins = [0, 10e6, 20e6, 30e6, 40e6, 50e6]
                labels = [" 0-10m ", "10-20m ", "20-30m ", "30-40m ", "40-50m "]
                sub_50_hist = sub_50.copy()
                sub_50_hist['bin'] = pd.cut(sub_50_hist['v'], bins=bins, labels=labels, right=False)
                counts = sub_50_hist['bin'].value_counts().sort_index()
                max_c = counts.max()
                for label, count in counts.items():
                    bar = '█' * (int((count/max_c)*30) if max_c > 0 else 0)
                    out.append(f"    {label}: {count:4} | {bar}")
            
        out.append("-" * 60)
        report_lines.append("\n".join(out))

    # Finální zápis reportu
    if processed_years:
        final_report = "\n".join(report_lines) + "\n\n" + "\n".join(cross_report_lines)
        print(final_report)
        unique_years = sorted(list(set(processed_years)))
        report_path = os.path.join(EXCLUDED_DIR, f"duplicates_removed_{'_'.join(unique_years)}.txt")
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(final_report)
        print(f"\n[ÚSPĚCH] Analýza uložena do: {report_path}")

if __name__ == "__main__":
    process_files()