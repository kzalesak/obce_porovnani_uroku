"""
PROCES ČIŠTĚNÍ DAT POMOCÍ LEVENSHTEINOVY VZDÁLENOSTI (DÁVKOVÉ ZPRACOVÁNÍ):
1. Skript načte data ze složky 's2.1_found_ICO'.
2. Hledá páry souborů: *_MISSING_ICO.tsv a k nim příslušné *_ENRICHED.tsv.
3. Výsledky hledání překlepů ukládá do 'data/levenshtein_temp/distX'.
4. Do složky 'data/clean/s2.2_levenshtein_clean' pouze umístí prázdný marker soubor, 
   kam máte později ručně nakopírovat vámi opravená data.
"""

import pandas as pd
import Levenshtein
import os
import glob

# ================= NASTAVENÍ PROCESU =================
# Vzdálenost: 1 (překlepy) nebo 2 (větší chyby)
LEV_VZD = 1 

# Vstupní a výstupní cesty
INPUT_DIR = 'data/clean/s2.1_found_ICO'
OUTPUT_BASE_TEMP = 'data/levenshtein_temp'
OUTPUT_DIR_TEMP = os.path.join(OUTPUT_BASE_TEMP, f'dist{LEV_VZD}')

# Složka pro finální manuálně opravená data
OUTPUT_DIR_MANUAL = 'data/clean/s2.2_levenshtein_clean'

# Názvy sloupců po normalizaci (musí odpovídat hlavičkám ve vašich datech)
SLOUPEC_ICO_REF = 'IČO poskytovatele /BIC/ZC_PARTP:ZC_PARTP'
SLOUPEC_NAZEV_REF = 'Název poskytovatele /BIC/ZC_NAZPOS:ZC_NAZPOS'
# =====================================================

def prepare_directories():
    """Vytvoří potřebné složky a záchytný soubor."""
    # Vytvoření složky pro dočasné Levenshtein výsledky
    if not os.path.exists(OUTPUT_DIR_TEMP):
        os.makedirs(OUTPUT_DIR_TEMP)
        print(f"📁 Vytvořena složka pro dočasné výsledky: {OUTPUT_DIR_TEMP}")

    # Vytvoření složky pro finální opravená data
    if not os.path.exists(OUTPUT_DIR_MANUAL):
        os.makedirs(OUTPUT_DIR_MANUAL)
        print(f"📁 Vytvořena cílová složka pro opravená data: {OUTPUT_DIR_MANUAL}")
    
    # Vytvoření prázdného marker souboru
    marker_file_path = os.path.join(OUTPUT_DIR_MANUAL, 'PUT_MANUALLY_CORRECTED_FILES_HERE.txt')
    if not os.path.exists(marker_file_path):
        with open(marker_file_path, 'w', encoding='utf-8') as f:
            f.write("Do této složky uložte data poté, co v Excelu/Spreadsheetu manuálně zkontrolujete "
                    f"a aplikujete opravy z dočasné složky {OUTPUT_BASE_TEMP}.")
        print(f"📌 Vytvořen marker soubor: {marker_file_path}")


def process_dataset_pair(missing_file, enriched_file):
    """Provede Levenshteinovu analýzu pro jeden konkrétní pár datasetů."""
    base_name = os.path.basename(missing_file).replace('_MISSING_ICO.tsv', '')
    print(f"\n{'-'*50}\n🔍 Zpracovávám dataset: {base_name}")
    
    # 1. Načtení hlavního (referenčního) datasetu
    try:
        df_main = pd.read_csv(enriched_file, sep='\t', dtype=str, low_memory=False)
        referencni_data = df_main[[SLOUPEC_ICO_REF, SLOUPEC_NAZEV_REF]].dropna().drop_duplicates()
        mapovani_ico = referencni_data.to_dict('records')
        print(f"  Načteno {len(mapovani_ico)} unikátních referenčních IČO.")
    except Exception as e:
        print(f"  ❌ Chyba při načítání referenčního souboru {enriched_file}: {e}")
        return

    # 2. Načtení chybějících IČO
    try:
        df_missing = pd.read_csv(missing_file, sep='\t', dtype=str)
        if 'Hledane_ICO' not in df_missing.columns:
            print(f"  ❌ Sloupec 'Hledane_ICO' nenalezen v {missing_file}.")
            return
        seznam_chybejicich = df_missing['Hledane_ICO'].dropna().unique()
        print(f"  Nalezeno {len(seznam_chybejicich)} chybějících IČO k dohledání.")
    except Exception as e:
        print(f"  ❌ Chyba při načítání souboru chybějících IČO {missing_file}: {e}")
        return

    # 3. Výpočet vzdáleností
    vysledky_raw = []
    max_shod = 0

    for nezname_ico in seznam_chybejicich:
        shody_pro_ico = []
        
        for ref in mapovani_ico:
            vzdalenost = Levenshtein.distance(str(nezname_ico), str(ref[SLOUPEC_ICO_REF]))
            
            if vzdalenost <= LEV_VZD:
                shody_pro_ico.append({
                    'ico': ref[SLOUPEC_ICO_REF],
                    'nazev': ref[SLOUPEC_NAZEV_REF]
                })
        
        if len(shody_pro_ico) > max_shod:
            max_shod = len(shody_pro_ico)
            
        vysledky_raw.append({'hledane': nezname_ico, 'shody': shody_pro_ico})

    # 4. Sestavení finální tabulky
    finalni_radky = []
    for item in vysledky_raw:
        radka = {
            'Hledane_ICO': item['hledane'],
            'Pocet_potencialnich_shod': len(item['shody'])
        }
        
        for i in range(max_shod):
            idx = i + 1
            if i < len(item['shody']):
                radka[f'Nalezeno_ICO_{idx}'] = item['shody'][i]['ico']
                radka[f'Nazev_subjektu_{idx}'] = item['shody'][i]['nazev']
            else:
                radka[f'Nalezeno_ICO_{idx}'] = ''
                radka[f'Nazev_subjektu_{idx}'] = ''
                
        finalni_radky.append(radka)

    # 5. Uložení výsledku do dočasné (temp) složky
    vystupni_soubor = f"{base_name}_analyza_preklepu_dist{LEV_VZD}.tsv"
    vystupni_cesta = os.path.join(OUTPUT_DIR_TEMP, vystupni_soubor)
    
    df_output = pd.DataFrame(finalni_radky)
    df_output.to_csv(vystupni_cesta, sep='\t', index=False, encoding='utf-8')
    
    print(f"  ✅ Výsledky uloženy do: {vystupni_cesta}")


def main():
    print(f"🚀 Spouštím dávkovou analýzu (Levenshteinova vzdálenost: {LEV_VZD})...")
    
    prepare_directories()

    # Vyhledání všech souborů s chybějícími IČO
    search_pattern = os.path.join(INPUT_DIR, '*_MISSING_ICO.tsv')
    missing_files = glob.glob(search_pattern)

    if not missing_files:
        print(f"⚠️ Ve složce {INPUT_DIR} nebyly nalezeny žádné soubory končící na '_MISSING_ICO.tsv'.")
        return

    # Procházení nalezených souborů a hledání příslušných ENRICHED datasetů
    for missing_file in missing_files:
        # Odvodíme název referenčního souboru (nahradíme koncovku)
        enriched_file = missing_file.replace('_MISSING_ICO.tsv', '_ENRICHED.tsv')
        
        if os.path.exists(enriched_file):
            process_dataset_pair(missing_file, enriched_file)
        else:
            print(f"\n⚠️ Přeskakuji {os.path.basename(missing_file)}: Nenalezen párový soubor {os.path.basename(enriched_file)}.")

    print(f"\n{'='*50}\n🎉 HOTOVO! Všechny dostupné datasety byly zpracovány.")


if __name__ == "__main__":
    main()