import pandas as pd
import requests
import time
import os
import glob

# Zkratky pro nejběžnější právní formy
LEGAL_FORMS_MAPPING = {
    "101": "f.o.",
    "102": "f.o.",
    "111": "v.o.s.",
    "112": "s.r.o.",
    "121": "a.s.",
    "205": "družstvo",
    "331": "p.o.",
    "332": "státní p.o.",
    "706": "z.s.",
    "771": "s.o.",
    "801": "obec",
    "804": "kraj",
    "811": "MČ / MO"
}

def load_ciselnik(ciselnik_path):
    print(f"Načítám číselník z {ciselnik_path}...")
    df_cis = pd.read_csv(ciselnik_path, dtype={'chodnota1': str})
    kod_to_nazev = dict(zip(df_cis['chodnota1'], df_cis['text1']))
    kod_to_klasifikace = dict(zip(df_cis['chodnota1'], df_cis['text2']))
    return kod_to_nazev, kod_to_klasifikace

def fetch_ares_data(ico, kod_to_nazev, kod_to_klasifikace):
    ico_str = str(ico).strip().split('.')[0]
    ico_str = ico_str.zfill(8)
    
    if len(ico_str) != 8 or ico_str == '00000000' or ico_str.lower() == 'nan':
        return {"ARES_Nazev": "Invalid ICO", "ARES_Pravni_Forma_Kod": ""}

    url = f"https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty/{ico_str}"
    
    try:
        headers = {'Accept': 'application/json'}
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            return {
                "ARES_Nazev": data.get("obchodniJmeno"), 
                "ARES_Pravni_Forma_Kod": str(data.get("pravniForma", ""))
            }
        elif response.status_code == 404:
            return {"ARES_Nazev": "Not Found", "ARES_Pravni_Forma_Kod": ""}
        else:
            return {"ARES_Nazev": f"HTTP Error {response.status_code}", "ARES_Pravni_Forma_Kod": ""}
    except requests.exceptions.RequestException:
        return {"ARES_Nazev": "Connection Error", "ARES_Pravni_Forma_Kod": ""}

def process_dataset(input_file, output_dir, kod_to_nazev, kod_to_klasifikace, ico_column_name, sep='\t'):
    """
    Zpracuje jeden konkrétní soubor a uloží ho do výstupní složky.
    """
    print(f"\n{'-'*50}\nZpracovávám soubor: {os.path.basename(input_file)}")
    
    try:
        df_main = pd.read_csv(input_file, sep=sep, dtype=str)
    except Exception as e:
        print(f"❌ Chyba při čtení souboru {input_file}: {e}")
        return

    if ico_column_name not in df_main.columns:
        print(f"⚠️ Přeskakuji. Sloupec '{ico_column_name}' nebyl v {os.path.basename(input_file)} nalezen.")
        return

    unique_icos = df_main[ico_column_name].dropna().unique()
    total = len(unique_icos)
    print(f"Nalezeno {total} unikátních IČO. Zahajuji stahování z ARES...")
    
    ares_results = []
    for index, ico in enumerate(unique_icos):
        data = fetch_ares_data(ico, kod_to_nazev, kod_to_klasifikace)
        
        form_code = data["ARES_Pravni_Forma_Kod"]
        plny_nazev_formy = kod_to_nazev.get(form_code, "Neznámá forma") if form_code else "N/A"
        klasifikace_formy = kod_to_klasifikace.get(form_code, "Neznámá klasifikace") if form_code else "N/A"
        zkratka = LEGAL_FORMS_MAPPING.get(form_code, "jiná") if form_code else "N/A"
        
        ares_results.append({
            ico_column_name: str(ico),
            "ARES_Nazev_subjektu": data["ARES_Nazev"],
            "ARES_Pravni_Forma_Kod": form_code,
            "ARES_Pravni_Forma_Oficialni": plny_nazev_formy,
            "ARES_Pravni_Forma_Skupina": klasifikace_formy,
            "ARES_Pravni_Forma_Zkratka": zkratka
        })
        
        if (index + 1) % 50 == 0 or (index + 1) == total:
            print(f"  Stahování: {index + 1} / {total} hotovo...")
        time.sleep(0.2) # Ochrana proti limitům ARES

    df_ares = pd.DataFrame(ares_results)
    df_main[ico_column_name] = df_main[ico_column_name].astype(str)
    df_enriched = pd.merge(df_main, df_ares, on=ico_column_name, how='left')

    # Sestavení cest pro výstupy
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    output_enriched = os.path.join(output_dir, f"{base_name}_ENRICHED.tsv")
    output_missing = os.path.join(output_dir, f"{base_name}_MISSING_ICO.tsv")

    # Uložení ENRICHED
    df_enriched.to_csv(output_enriched, sep='\t', index=False)
    print(f"✅ Uloženo obohacené: {output_enriched}")

    # Uložení MISSING pro Levenshteina
    chybna_kriteria = ["Not Found", "Invalid ICO", "Connection Error"]
    chybejici_zaznamy = df_ares[df_ares['ARES_Nazev_subjektu'].isin(chybna_kriteria)].copy()
    
    df_missing_export = pd.DataFrame()
    df_missing_export['Hledane_ICO'] = chybejici_zaznamy[ico_column_name]
    df_missing_export['ARES_Status'] = chybejici_zaznamy['ARES_Nazev_subjektu']
    
    df_missing_export.to_csv(output_missing, sep='\t', index=False)
    print(f"⚠️ Uloženo {len(df_missing_export)} chybějících IČO: {output_missing}")


def process_all_datasets(input_dir, output_dir, ciselnik_path, ico_column_name, sep='\t'):
    """
    Projdede všechny soubory ve vstupní složce a zpracuje je.
    """
    # 1. Kontrola a vytvoření složek
    if not os.path.exists(input_dir):
        print(f"❌ Vstupní složka neexistuje: {input_dir}")
        return
        
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"📁 Vytvořena výstupní složka: {output_dir}")

    # 2. Načtení číselníku (uděláme to jen jednou pro všechny soubory)
    if not os.path.exists(ciselnik_path):
        print(f"❌ Číselník nenalezen na cestě: {ciselnik_path}")
        return
        
    kod_to_nazev, kod_to_klasifikace = load_ciselnik(ciselnik_path)

    # 3. Vyhledání všech TSV souborů ve vstupní složce
    # Změňte '*.tsv' na '*.csv', pokud máte vstupní soubory s čárkou
    search_pattern = os.path.join(input_dir, '*.tsv')
    files_to_process = glob.glob(search_pattern)

    if not files_to_process:
        print(f"⚠️ Ve složce {input_dir} nebyly nalezeny žádné soubory {search_pattern}")
        return

    print(f"Nalezeno {len(files_to_process)} souborů ke zpracování.")

    # 4. Smyčka přes všechny nalezené soubory
    for file_path in files_to_process:
        process_dataset(file_path, output_dir, kod_to_nazev, kod_to_klasifikace, ico_column_name, sep)

    print(f"\n{'='*50}\n🎉 HOTOVO! Všechny soubory byly zpracovány.")


if __name__ == "__main__":
    # ===== KONFIGURACE =====
    # Složka, kde leží původní očištěné hlavičky
    INPUT_DIR = "data/clean/s1_headers" 
    
    # Složka, kam se uloží obohacená data a missing_ico soubory
    OUTPUT_DIR = "data/clean/s2.1_found_ICO" 
    
    # Cesta k číselníku právních forem
    CISELNIK_CSV = "data/sazebniky/VAZ0056_0149_CS.csv"
    
    # Název sloupce, který obsahuje IČO 
    ICO_COLUMN = "IČO poskytovatele/BIC/ZC_PARTP:ZC_PARTP"
    
    # Typ oddělovače ve zdrojových souborech (tsv = '\t', csv = ',')
    SEPARATOR = '\t' 
    # =======================
    
    process_all_datasets(INPUT_DIR, OUTPUT_DIR, CISELNIK_CSV, ICO_COLUMN, SEPARATOR)