import pandas as pd
import numpy as np
import os
import re
import sys

# ================= NASTAVENÍ CEST =================
INPUT_DIR = 'data/clean/s3_final_ids'
OUTPUT_DIR = 'data/clean/s4_kontrola_fixnosti_TEMP'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'kontrola_fixnosti.tsv')
S4_DIR = 'data/clean/s4_kontrola_fixnosti'

# ================= NASTAVENÍ BĚHU =================
START_YEAR = 2022  # Ignoruje všechny soubory před tímto rokem (např. 2021 bez sazeb)

def analyze_rates():
    # 0. Příprava složek
    for d in [OUTPUT_DIR, S4_DIR]:
        if not os.path.exists(d):
            os.makedirs(d)
            print(f"Vytvořena složka: {d}")

    files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.tsv')]
    
    if not files:
        print(f"V {INPUT_DIR} nebyly nalezeny žádné soubory k analýze.")
        return

    all_dataframes = []

    print(f"\n1. Načítám soubory od roku {START_YEAR} a provádím validaci čistoty dat...")
    for filename in files:
        match = re.search(r'_(\d{4})\d{3}\.tsv$', filename)
        year_str = match.group(1) if match else '0'
        
        if int(year_str) < START_YEAR:
            print(f"  - Ignoruji: {filename} (před rokem {START_YEAR})")
            continue
            
        file_path = os.path.join(INPUT_DIR, filename)
        df = pd.read_csv(file_path, sep='\t', dtype=str)

        # ================= POVINNÁ VALIDACE (CHECK DEDUP) =================
        if 'DEDUP_SUFFIX' in df.columns:
            has_dedup = df['DEDUP_SUFFIX'].notna() & \
                        (df['DEDUP_SUFFIX'].astype(str).str.strip() != '') & \
                        (df['DEDUP_SUFFIX'].astype(str).str.lower() != 'nan')
            
            if has_dedup.any():
                print(f"\n[KRITICKÁ CHYBA] Bezpečnostní pojistka aktivována!")
                print(f"Soubor {filename} obsahuje {has_dedup.sum()} záznamů s vyplněným DEDUP_SUFFIX.")
                print(f"Do této fáze smí vstoupit pouze čistá data (lokální duplicity měly být vyřazeny ve fázi s3).")
                print("Skript je ukončen.")
                sys.exit(1)
        # ==================================================================

        print(f"  + Načteno a zkontrolováno {filename}: {len(df)} čistých úvěrů.")
        
        year_specific_cols = []
        
        # 1. Datayear
        if 'DATAYEAR' in df.columns:
            df.rename(columns={'DATAYEAR': f'DATAYEAR_{year_str}'}, inplace=True)
            year_specific_cols.append(f'DATAYEAR_{year_str}')

        # 2. Úroková sazba
        rate_col = next((c for c in df.columns if 'ZC_URSA:ZC_URSA' in c), None)
        df[f'rate_{year_str}'] = df[rate_col] if rate_col else np.nan
        year_specific_cols.append(f'rate_{year_str}')

        # 3. Čerpaná výše (ZU_CEUV)
        cerp_col = next((c for c in df.columns if 'ZU_CEUV' in c), None)
        df[f'CERPANA_VYSE_{year_str}'] = df[cerp_col] if cerp_col else np.nan
        year_specific_cols.append(f'CERPANA_VYSE_{year_str}')
            
        # 4. Účel úvěru (UCEL)
        ucel_col = next((c for c in df.columns if 'ZC_UCFP:ZC_UCFP' in c), None)
        df[f'ucel_{year_str}'] = df[ucel_col] if ucel_col else np.nan
        year_specific_cols.append(f'ucel_{year_str}')
            
        # Ponecháváme pouze čisté ID a specifické sloupce pro daný rok
        temp = df[['ID'] + year_specific_cols].copy()
        all_dataframes.append(temp)

    if not all_dataframes:
        print("Žádná data ke zpracování po aplikování filtru roku.")
        return

    print("\n2. Spojuji data napříč roky (vytvářím matici)...")
    merged_df = all_dataframes[0]
    for next_df in all_dataframes[1:]:
        # Mergujeme jednoduše POUZE přes unikátní ID
        merged_df = pd.merge(merged_df, next_df, on=['ID'], how='outer')

    # Extrakce schválené výše úvěru (LOAN_AMOUNT) ze samotného ID
    merged_df['LOAN_AMOUNT'] = merged_df['ID'].apply(
        lambda x: int(str(x).split('-')[-1]) if pd.notna(x) and str(x).split('-')[-1].isdigit() else 0
    )

    rate_cols = sorted([c for c in merged_df.columns if c.startswith('rate_')])
    cerp_cols = sorted([c for c in merged_df.columns if c.startswith('CERPANA_VYSE_')])
    year_cols = sorted([c for c in merged_df.columns if c.startswith('DATAYEAR_')])
    ucel_cols = sorted([c for c in merged_df.columns if c.startswith('ucel_')])

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
        
        return 'CHANGED' if len(unique_rates) > 1 else 'STABLE'

    def classify_string(row):
        for col in rate_cols:
            val = row[col]
            if pd.notna(val) and str(val).strip() != '':
                val_clean = re.sub(r'(?i)p\.?\s*a\.?', '', str(val))
                if re.search(r'[^\d\,\.\s\%\-\+]', val_clean):
                    return 'HAS_STRING'
        return 'NO_STRING'

    merged_df['RATE_CHANGED'] = merged_df.apply(classify_variance, axis=1)
    merged_df['HAS_STRING'] = merged_df.apply(classify_string, axis=1)

    # 4. Sestavení finálních sloupců (DEDUP_SUFFIX z finálního exportu odstraňujeme)
    final_cols = ['ID', 'DATAYEAR_MERGED', 'LOAN_AMOUNT'] + cerp_cols + rate_cols + ['RATE_CHANGED', 'HAS_STRING'] + ucel_cols
    final_cols = [c for c in final_cols if c in merged_df.columns]
        
    final_df = merged_df[final_cols]
    
    # Odstraníme řádky, kde neznáme sazbu ani v jednom z povolených let
    final_df = final_df.dropna(subset=rate_cols, how='all')

    print(f"4. Ukládám hlavní výsledek k revizi: {OUTPUT_FILE}")
    final_df.to_csv(OUTPUT_FILE, sep='\t', index=False)
    
    # Krátká statistika
    print("\n--- STATISTIKA KLASIFIKACÍ ---")
    print("\nEmpirická změna v čase (CHANGED vs STABLE):")
    print(final_df['RATE_CHANGED'].value_counts())
    
    print("\nPřítomnost textu ve stringu (NO_STRING vs HAS_STRING):")
    print(final_df['HAS_STRING'].value_counts())
    
    print(f"\n=========================================================")
    print(f"HOTOVO. Výstupní soubor čeká na vaši manuální kontrolu v:")
    print(f"--> {OUTPUT_FILE}")
    print(f"\nAž provedete manuální korekce sazeb (pokud jsou nutné),")
    print(f"rozdělte data zpět do ročních datasetů a uložte do: {S4_DIR}")
    print(f"=========================================================\n")

if __name__ == "__main__":
    analyze_rates()