import pandas as pd
import os
import re

def clean_headers_and_save():
    # Definice cest
    input_dir = 'data/raw'
    output_dir = 'data/clean/s1_header'
    output_dir_working = 'data/clean/'
    
    # Vytvoření cílové složky, pokud neexistuje
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Vytvořena složka: {output_dir}")

    # Seznam všech souborů v raw složce
    files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    
    if not files:
        print("V data/raw nebyly nalezeny žádné .csv soubory.")
        return

    for filename in files:
        input_path = os.path.join(input_dir, filename)
        
        # 1. Načtení souboru (předpokládáme středník jako oddělovač)
        try:
            df = pd.read_csv(input_path, sep=';', dtype=str)
        except Exception as e:
            print(f"Chyba při čtení {filename}: {e}")
            continue

        # 2. Normalizace hlaviček
        # Odstraní všechny uvozovky (libovolný počet) a ořeže bílé znaky
        new_columns = []
        for col in df.columns:
            clean_col = col.replace('"', '').strip()
            # Pokud po odstranění uvozovek vzniknou vícenásobné mezery, sjednotíme je
            clean_col = re.sub(r'\s+', ' ', clean_col)
            new_columns.append(clean_col)
        
        df.columns = new_columns

        # 3. Uložení jako TSV
        # Změníme příponu z .csv na .tsv
        output_filename = os.path.splitext(filename)[0] + '.tsv'
        output_path = os.path.join(output_dir, output_filename)
        output_path_working = os.path.join(output_dir_working, output_filename)
        
        df.to_csv(output_path, sep='\t', index=False, encoding='utf-8')
        df.to_csv(output_path_working, sep='\t', index=False, encoding='utf-8')
        print(f"Zpracováno: {filename} -> {output_filename}")

if __name__ == "__main__":
    clean_headers_and_save()