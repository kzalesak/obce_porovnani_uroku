import pandas as pd
import numpy as np
import os
import re

# ================= NASTAVENÍ CEST =================
INPUT_DIR = 'data/clean/s3_final_ids'
OUTPUT_FILE = 'data/clean/analyza_urokovych_sazeb.tsv'

def analyze_rates():
    files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.tsv')]
    
    if not files:
        print(f"V {INPUT_DIR} nebyly nalezeny žádné soubory k analýze.")
        return

    all_dataframes = []

    print("1. Načítám soubory a extrahuji sazby...")
    for filename in files:
        # Pokus o vytažení roku z názvu souboru (např. 2021, 2024)
        match = re.search(r'(20[1-9][0-9])', filename)
        year = match.group(1) if match else 'UNKNOWN'
        
        file_path = os.path.join(INPUT_DIR, filename)
        df = pd.read_csv(file_path, sep='\t', dtype=str)
        
        # Očekávané unikátní identifikátory
        keys = ['ID', 'DEDUP_SUFFIX', 'COMPOSITE_ID']
        available_keys = [k for k in keys if k in df.columns]
        
        # Hledáme sloupec úrokové sazby (může jich být víc, nebo v r. 2021 žádný)
        rate_col = next((c for c in df.columns if 'ZC_URSA:ZC_URSA' in c), None)
        
        if rate_col:
            temp = df[available_keys + [rate_col]].copy()
            temp.rename(columns={rate_col: f'rate_{year}'}, inplace=True)
        else:
            # Pro roky, kde sazba chybí (např. 2021)
            temp = df[available_keys].copy()
            temp[f'rate_{year}'] = np.nan
            
        all_dataframes.append(temp)

    print("2. Spojuji data napříč roky (vytvářím matici)...")
    # Postupný Outer Join všech datasetů přes COMPOSITE_ID, ID, DEDUP_SUFFIX
    merged_df = all_dataframes[0]
    for next_df in all_dataframes[1:]:
        # Sloučíme je přes všechny klíče, které jsou k dispozici
        merge_keys = [k for k in ['ID', 'DEDUP_SUFFIX', 'COMPOSITE_ID'] if k in merged_df.columns and k in next_df.columns]
        merged_df = pd.merge(merged_df, next_df, on=merge_keys, how='outer')

    # Identifikace všech sloupců se sazbami (rate_2021, rate_2022...)
    rate_cols = sorted([c for c in merged_df.columns if c.startswith('rate_')])

    print("3. Provádím klasifikaci (Empirická variabilita vs. Textová analýza)...")
    
    # METODA 1: Změnila se reálně sazba napříč roky? (FIXED vs VARIABLE)
    def classify_variance(row):
        unique_rates = set()
        for col in rate_cols:
            val = row[col]
            if pd.notna(val) and str(val).strip() != '':
                val_str = str(val).strip().replace(',', '.')
                # Pokusíme se převést na číslo pro přesnější porovnání (0 vs 0.00)
                try:
                    num = float(val_str.replace('%', '').strip())
                    unique_rates.add(str(num))
                except ValueError:
                    # Pokud je to čistý text (např. PRIBOR), přidáme ho jako text
                    unique_rates.add(val_str.lower())
        
        return 'VARIABLE' if len(unique_rates) > 1 else 'FIXED'

    # METODA 2: Obsahuje verbatim hodnota zakázané znaky indikující text? (NOSTRING vs STRING)
    def classify_string(row):
        for col in rate_cols:
            val = row[col]
            if pd.notna(val) and str(val).strip() != '':
                # Regex vysvětlení: [^...] znamená cokoliv KROMĚ znaků uvnitř.
                # \d = čísla, \, = čárka, \. = tečka, \s = mezery, \% = procento, \-\+ = minus/plus
                if re.search(r'[^\d\,\.\s\%\-\+]', str(val)):
                    return 'VARIABLE_STRING'
        return 'FIXED_NOSTRING'

    # Aplikace klasifikátorů
    merged_df['FIXED_OR_VARIABLE'] = merged_df.apply(classify_variance, axis=1)
    merged_df['FIXED_NOSTRING_OR_VARIABLE_STRING'] = merged_df.apply(classify_string, axis=1)

    # Seřazení sloupců pro přehlednost
    final_cols = ['ID', 'DEDUP_SUFFIX'] + rate_cols + ['FIXED_OR_VARIABLE', 'FIXED_NOSTRING_OR_VARIABLE_STRING']
    # Přidáme i COMPOSITE_ID pokud v datech je
    if 'COMPOSITE_ID' in merged_df.columns:
        final_cols.insert(2, 'COMPOSITE_ID')
        
    final_df = merged_df[final_cols]
    
    # Odstraníme řádky, kde neznáme sazbu v ani jednom z let (vyčistí to matici)
    final_df = final_df.dropna(subset=rate_cols, how='all')

    print(f"4. Ukládám výsledek: {OUTPUT_FILE}")
    final_df.to_csv(OUTPUT_FILE, sep='\t', index=False)
    
    # Krátká statistika
    print("\n--- STATISTIKA KLASIFIKACÍ ---")
    print("\nEmpirická změna v čase (FIXED vs VARIABLE):")
    print(final_df['FIXED_OR_VARIABLE'].value_counts())
    
    print("\nPřítomnost textu ve stringu (NOSTRING vs STRING):")
    print(final_df['FIXED_NOSTRING_OR_VARIABLE_STRING'].value_counts())
    
    # Zajímavý průnik: Kde se metody liší? (Změnilo se to v čase, ale nebyl tam text atd.)
    diff = final_df[
        (final_df['FIXED_OR_VARIABLE'] == 'VARIABLE') & 
        (final_df['FIXED_NOSTRING_OR_VARIABLE_STRING'] == 'FIXED_NOSTRING')
    ]
    print(f"\nUpozornění: Nalezeno {len(diff)} sazeb, které se sice změnily (VARIABLE), ale neobsahují žádný text identifikující variabilitu.")

if __name__ == "__main__":
    analyze_rates()