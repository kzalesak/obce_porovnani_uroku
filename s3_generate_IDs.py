import pandas as pd
import hashlib
import numpy as np
import os
import re  # Nový import pro extrakci roku z názvu souboru

# ================= NASTAVENÍ CEST =================
INPUT_DIR = 'data/clean'
OUTPUT_DIR = 'data/clean/s3_final_ids'

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
    # Ošetření 'nan' hodnot, aby nebyly brány jako text
    s = s.replace('nan', '0')
    return s.str.zfill(pad_len)

def short_hash(val):
    """Vytvoří stabilní krátký hash z textu."""
    clean_val = str(val).strip().lower()
    return hashlib.md5(clean_val.encode('utf-8')).hexdigest()[:8]

def process_files():
    # Vytvoření výstupní složky
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Najdeme všechny TSV soubory (vynecháme ty v podadresářích jako levenshtein)
    files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.tsv') and os.path.isfile(os.path.join(INPUT_DIR, f))]
    
    if not files:
        print(f"V {INPUT_DIR} nebyly nalezeny žádné .tsv soubory.")
        return

    for filename in files:
        file_path = os.path.join(INPUT_DIR, filename)
        print(f"\nZpracovávám: {filename}")
        
        # Extrakce roku z názvu souboru (hledá 4 číslice, např. 2021)
        year_match = re.search(r'\d{4}', filename)
        data_year = year_match.group(0) if year_match else "XXXX"
        
        # Načtení TSV (používáme tabulátor, protože data už jsou vyčištěná)
        df = pd.read_csv(file_path, sep='\t', dtype=str)

        try:
            # Přidání sloupce s rokem datasetu
            df['DATAYEAR'] = data_year

            # Dynamické určení sloupců
            col_ico = get_col(df, 'ZC_ICO:ZC_ICO')
            col_ico_posk = get_col(df, 'ZC_PARTP:ZC_PARTP')
            col_datus = get_col(df, 'ZC_DATUS:ZC_DATUS')
            col_terspl = get_col(df, 'ZC_TERSPL:ZC_TERSPL')
            col_vyse = get_col(df, 'ZU_VYUV')
            col_posk = get_col(df, 'ZC_NAZPOS:ZC_NAZPOS')
            col_ucel = get_col(df, 'ZC_UCFP:ZC_UCFP')

            # 1. Tvorba základního stabilního ID
            p1 = clean_pad(df[col_ico], 8)
            p2 = clean_pad(df[col_ico_posk], 8)
            p3 = df[col_datus].astype(str).str.strip()
            p4 = df[col_terspl].astype(str).str.strip()
            
            # Převod výše na INT (odstranění mezer a desetinných míst)
            amount_numeric = pd.to_numeric(df[col_vyse].astype(str).str.replace(' ', ''), errors='coerce').fillna(0)
            p5 = amount_numeric.astype(int).astype(str)

            df['ID'] = p1 + '-' + p2 + '-' + p3 + '-' + p4 + '-' + p5

            # 2. Inicializace DEDUP_SUFFIX
            df['DEDUP_SUFFIX'] = np.nan

            # Hledání duplicit
            is_duplicate = df.duplicated(subset=['ID'], keep=False)

            if is_duplicate.any():
                # Hashujeme pouze duplicity
                h_posk = df.loc[is_duplicate, col_posk].apply(short_hash)
                h_ucel = df.loc[is_duplicate, col_ucel].apply(short_hash)
                df.loc[is_duplicate, 'DEDUP_SUFFIX'] = h_posk + '-' + h_ucel

            # 3. COMPOSITE_ID pro finální unikátnost (včetně DEDUP_SUFFIX a DATAYEAR)
            df['COMPOSITE_ID'] = df.apply(
                lambda row: f"{row['ID']}-{row['DEDUP_SUFFIX']}-{row['DATAYEAR']}" if pd.notna(row['DEDUP_SUFFIX']) else f"{row['ID']}-{row['DATAYEAR']}", 
                axis=1
            )

            # Statistiky
            rem_dups = df.duplicated(subset=['COMPOSITE_ID'], keep=False).sum()
            print(f"  - Rok datasetu: {data_year}")
            print(f"  - Celkem řádků: {len(df)}")
            print(f"  - Unikátních ID: {df['ID'].nunique()}")
            print(f"  - Počet duplicit vyžadujících suffix: {is_duplicate.sum()}")
            print(f"  - Zbývající absolutní duplicity: {rem_dups}")

            # Uložení
            output_path = os.path.join(OUTPUT_DIR, filename)
            df.to_csv(output_path, sep='\t', index=False)
            print(f"  - Uloženo do: {output_path}")

        except Exception as e:
            print(f"  - Chyba při zpracování souboru {filename}: {e}")

if __name__ == "__main__":
    process_files()