import pandas as pd
import numpy as np
import os
import re

# ================= NASTAVENÍ CEST =================
INPUT_DIR = 'data/clean/s3_final_ids'
OUTPUT_DIR = 'data/clean/kontrola_fixnosti'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'kontrola_fixnosti.tsv')
OUTPUT_COLLISIONS_POD_50 = os.path.join(OUTPUT_DIR, 'kolize_zakladnich_id_pod_50.tsv')
OUTPUT_COLLISIONS_NAD_50 = os.path.join(OUTPUT_DIR, 'kolize_zakladnich_id_nad_50.tsv')
OUTPUT_IGNORE_LIST = os.path.join(OUTPUT_DIR, 'ignore_list.tsv')
S4_DIR = 'data/clean/s4_kontrola_fixnosti'

# ================= NASTAVENÍ BĚHU =================
START_YEAR = 2022  # Ignoruje všechny soubory před tímto rokem (např. 2021 bez sazeb)
THRESHOLD_MIL = 50000000
UPPER_THRESHOLD_MIL = 100000000

def analyze_rates():
    # 0. Příprava složek
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Vytvořena složka pro výstupy: {OUTPUT_DIR}")
        
    if not os.path.exists(S4_DIR):
        os.makedirs(S4_DIR)
        print(f"Vytvořena složka pro manuálně zkontrolované soubory: {S4_DIR}")

    files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.tsv')]
    
    if not files:
        print(f"V {INPUT_DIR} nebyly nalezeny žádné soubory k analýze.")
        return

    all_dataframes = []

    print(f"\n1. Načítám soubory od roku {START_YEAR} a extrahuji data...")
    for filename in files:
        match = re.search(r'(20[1-9][0-9])', filename)
        year_str = match.group(1) if match else '0'
        
        # Přeskočení souborů starších než START_YEAR
        if int(year_str) < START_YEAR:
            print(f"  - Ignoruji: {filename} (před rokem {START_YEAR})")
            continue
            
        file_path = os.path.join(INPUT_DIR, filename)
        df = pd.read_csv(file_path, sep='\t', dtype=str)
        
        merge_keys = ['ID', 'DEDUP_SUFFIX']
        available_merge_keys = [k for k in merge_keys if k in df.columns]
        
        year_specific_cols = []
        
        # 1. Datayear (připravíme pro pozdější sloučení)
        if 'DATAYEAR' in df.columns:
            df.rename(columns={'DATAYEAR': f'DATAYEAR_{year_str}'}, inplace=True)
            year_specific_cols.append(f'DATAYEAR_{year_str}')

        # 2. Úroková sazba
        rate_col = next((c for c in df.columns if 'ZC_URSA:ZC_URSA' in c), None)
        if rate_col:
            df.rename(columns={rate_col: f'rate_{year_str}'}, inplace=True)
            year_specific_cols.append(f'rate_{year_str}')
        else:
            df[f'rate_{year_str}'] = np.nan
            year_specific_cols.append(f'rate_{year_str}')

        # 3. Čerpaná výše (ZU_CEUV)
        cerp_col = next((c for c in df.columns if 'ZU_CEUV' in c), None)
        if cerp_col:
            df.rename(columns={cerp_col: f'CERPANA_VYSE_{year_str}'}, inplace=True)
            year_specific_cols.append(f'CERPANA_VYSE_{year_str}')
        else:
            df[f'CERPANA_VYSE_{year_str}'] = np.nan
            year_specific_cols.append(f'CERPANA_VYSE_{year_str}')
            
        # 4. Účel úvěru (UCEL)
        ucel_col = next((c for c in df.columns if 'ZC_UCFP:ZC_UCFP' in c), None)
        if ucel_col:
            df.rename(columns={ucel_col: f'ucel_{year_str}'}, inplace=True)
            year_specific_cols.append(f'ucel_{year_str}')
        else:
            df[f'ucel_{year_str}'] = np.nan
            year_specific_cols.append(f'ucel_{year_str}')
            
        temp = df[available_merge_keys + year_specific_cols].copy()
        all_dataframes.append(temp)

    if not all_dataframes:
        print("Žádná data ke zpracování po aplikování filtru roku.")
        return

    print("2. Spojuji data napříč roky (vytvářím matici)...")
    merged_df = all_dataframes[0]
    for next_df in all_dataframes[1:]:
        available_merge_keys = [k for k in merge_keys if k in merged_df.columns and k in next_df.columns]
        merged_df = pd.merge(merged_df, next_df, on=available_merge_keys, how='outer')

    # Extrakce schválené výše úvěru (LOAN_AMOUNT) ze samotného ID
    merged_df['LOAN_AMOUNT'] = merged_df['ID'].apply(
        lambda x: int(str(x).split('-')[-1]) if pd.notna(x) and str(x).split('-')[-1].isdigit() else 0
    )

    # Identifikace sloupců pro finální uspořádání
    rate_cols = sorted([c for c in merged_df.columns if c.startswith('rate_')])
    cerp_cols = sorted([c for c in merged_df.columns if c.startswith('CERPANA_VYSE_')])
    year_cols = sorted([c for c in merged_df.columns if c.startswith('DATAYEAR_')])
    ucel_cols = sorted([c for c in merged_df.columns if c.startswith('ucel_')])

    # Vytvoření sloupce DATAYEAR_MERGED spojením dostupných roků
    def merge_years(row):
        years = [str(row[col]) for col in year_cols if pd.notna(row[col])]
        return " + ".join(years)
    
    merged_df['DATAYEAR_MERGED'] = merged_df.apply(merge_years, axis=1)

    print("3. Provádím klasifikaci (Empirická variabilita vs. Textová analýza)...")
    def classify_variance(row):
        unique_rates = set()
        for col in rate_cols:
            val = row[col]
            if pd.notna(val) and str(val).strip() != '':
                val_str = re.sub(r'(?i)p\.?\s*a\.?', '', str(val)).strip()
                val_str = val_str.replace(',', '.')
                try:
                    num = float(val_str.replace('%', '').strip())
                    unique_rates.add(str(num))
                except ValueError:
                    unique_rates.add(str(val).lower().strip())
        
        return 'VARIABLE' if len(unique_rates) > 1 else 'FIXED'

    def classify_string(row):
        for col in rate_cols:
            val = row[col]
            if pd.notna(val) and str(val).strip() != '':
                val_clean = re.sub(r'(?i)p\.?\s*a\.?', '', str(val))
                if re.search(r'[^\d\,\.\s\%\-\+]', val_clean):
                    return 'VARIABLE_STRING'
        return 'FIXED_NOSTRING'

    merged_df['FIXED_OR_VARIABLE'] = merged_df.apply(classify_variance, axis=1)
    merged_df['FIXED_NOSTRING_OR_VARIABLE_STRING'] = merged_df.apply(classify_string, axis=1)

    # 4. Sestavení finálních sloupců
    final_cols = ['ID', 'DEDUP_SUFFIX', 'DATAYEAR_MERGED', 'LOAN_AMOUNT'] + cerp_cols + rate_cols + ['FIXED_OR_VARIABLE', 'FIXED_NOSTRING_OR_VARIABLE_STRING'] + ucel_cols
    
    # Pojistka pro existující sloupce
    final_cols = [c for c in final_cols if c in merged_df.columns]
        
    final_df = merged_df[final_cols]
    
    # Odstraníme řádky, kde neznáme sazbu ani v jednom z povolených let
    final_df = final_df.dropna(subset=rate_cols, how='all')

    print(f"4. Ukládám hlavní výsledek k revizi: {OUTPUT_FILE}")
    final_df.to_csv(OUTPUT_FILE, sep='\t', index=False)

    # ================= NOVÁ FUNKCE: KONTROLA KOLIZÍ ID A JEJICH VELIKOSTI =================
    print("\n5. Analyzuji štěpení základních ID a filtruji podle objemu...")
    id_counts = final_df['ID'].value_counts()
    ids_with_multiple_dedups = id_counts[id_counts > 1].index

    if len(ids_with_multiple_dedups) > 0:
        # Všechny kolizní řádky
        collision_df = final_df[final_df['ID'].isin(ids_with_multiple_dedups)].sort_values(by=['ID', 'DEDUP_SUFFIX'])
        
        # Rozdělení na pod 50M a nad 50M
        pod_50_df = collision_df[collision_df['LOAN_AMOUNT'] <= THRESHOLD_MIL]
        nad_50_df = collision_df[collision_df['LOAN_AMOUNT'] > THRESHOLD_MIL]
        nad_100_df = collision_df[collision_df['LOAN_AMOUNT'] > UPPER_THRESHOLD_MIL]
        
        # Unikátní počet kolizních ID (nikoliv řádků) v jednotlivých kategoriích
        unique_pod_50 = pod_50_df['ID'].nunique()
        unique_nad_50 = nad_50_df['ID'].nunique()
        unique_nad_100 = nad_100_df['ID'].nunique()

        print(f"  [!] Nalezeno {len(ids_with_multiple_dedups)} unikátních základních ID, která mají více variant DEDUP_SUFFIX.")
        print(f"      -> {unique_pod_50} úvěrů je s objemem <= 50 mil. CZK (tyto půjdou do ignore listu).")
        print(f"      -> {unique_nad_50} úvěrů je s objemem > 50 mil. CZK (z toho {unique_nad_100} je > 100 mil. CZK).")

        # Uložení rozdělených kolizí
        pod_50_df.to_csv(OUTPUT_COLLISIONS_POD_50, sep='\t', index=False)
        nad_50_df.to_csv(OUTPUT_COLLISIONS_NAD_50, sep='\t', index=False)
        print(f"  [i] Soubor pro zanedbatelné kolize (<= 50M) uložen: {OUTPUT_COLLISIONS_POD_50}")
        print(f"  [i] Soubor pro významné kolize (> 50M) uložen: {OUTPUT_COLLISIONS_NAD_50}")

        # Tvorba ignore listu (pouze ID a DEDUP pro úvěry pod 50 mil.)
        ignore_list_df = pod_50_df[['ID', 'DEDUP_SUFFIX']].drop_duplicates()
        ignore_list_df.to_csv(OUTPUT_IGNORE_LIST, sep='\t', index=False)
        print(f"  [i] Ignore list (ID a DEDUP pro odfiltrování) uložen: {OUTPUT_IGNORE_LIST}")

    else:
        print("  [OK] Nenalezena žádná základní ID s vícero DEDUP_SUFFIXy. Všechna ID jsou stabilní.")
    # ======================================================================================
    
    # Krátká statistika
    print("\n--- STATISTIKA KLASIFIKACÍ ---")
    print("\nEmpirická změna v čase (FIXED vs VARIABLE):")
    print(final_df['FIXED_OR_VARIABLE'].value_counts())
    
    print("\nPřítomnost textu ve stringu (NOSTRING vs STRING):")
    print(final_df['FIXED_NOSTRING_OR_VARIABLE_STRING'].value_counts())
    
    print(f"\n=========================================================")
    print(f"HOTOVO. Výstupní soubory čekají na vaši manuální kontrolu v:")
    print(f"--> {OUTPUT_FILE}")
    if len(ids_with_multiple_dedups) > 0:
        print(f"--> {OUTPUT_COLLISIONS_NAD_50} (K MANUÁLNÍ KONTROLE)")
        print(f"--> {OUTPUT_IGNORE_LIST} (K ODFILTROVÁNÍ V DALŠÍCH KROCÍCH)")
    print(f"\nAž provedete manuální korekce v hlavním souboru a významných kolizích,")
    print(f"uložte výsledný soubor do složky: {S4_DIR}")
    print(f"=========================================================\n")

if __name__ == "__main__":
    analyze_rates()