"""
PROCES ČIŠTĚNÍ DAT POMOCÍ LEVENSHTEINOVY VZDÁLENOSTI (MANUÁLNÍ KROKY):
1. Všechny normalizované soubory z 'data/clean' jsou připraveny k analýze.
2. Nahrajeme data do Google Spreadsheet pro přehlednou manuální editaci.
3. Spustíme tento skript s LEV_VZD = 1. Skript identifikuje překlepy v IČO.
4. Výstup z 'levenshtein/dist1' manuálně porovnáme v tabulce a opravíme chyby v originálním datasetu.
5. Uložíme opravená data a spustíme skript znovu s LEV_VZD = 2.
6. Opět manuálně zkontrolujeme výstup v 'levenshtein/dist2' a doladíme zbývající nesrovnalosti.
7. Finální opravená data uložíme jako TSV pro další zpracování.
"""

import pandas as pd
import Levenshtein
import os

# ================= NASTAVENÍ PROCESU =================
# Vzdálenost: 1 (překlepy) nebo 2 (větší chyby)
LEV_VZD = 1 

# Cesty
INPUT_DIR = 'data/clean'
OUTPUT_BASE = 'levenshtein'
OUTPUT_DIR = os.path.join(OUTPUT_BASE, f'dist{LEV_VZD}')

OUTPUT_BASE_FINAL = 'data/clean/s2_levenshtein'
OUTPUT_DIR_FINAL = os.path.join(OUTPUT_BASE, f'dist{LEV_VZD}')


# Názvy souborů (upravte dle potřeby)
HLAVNI_SOUBOR = os.path.join(INPUT_DIR, 'FINSZU101_2023012_with_ids.tsv')
CHYBEJICI_SOUBOR = os.path.join(INPUT_DIR, 'chybejici_ICO.tsv')

# Názvy sloupců po normalizaci (odstraněné uvozovky)
SLOUPEC_ICO_REF = 'IČO poskytovatele /BIC/ZC_PARTP:ZC_PARTP'
SLOUPEC_NAZEV_REF = 'Název poskytovatele /BIC/ZC_NAZPOS:ZC_NAZPOS'

def main():
    # Vytvoření výstupní složky
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Vytvořena složka pro výsledky: {OUTPUT_DIR}")

    print(f"Spouštím analýzu (Levenshteinova vzdálenost: {LEV_VZD})...")
    
    # 1. Načtení hlavního datasetu (Reference)
    try:
        # Čteme TSV (tabulátor) z data/clean
        df_main = pd.read_csv(HLAVNI_SOUBOR, sep='\t', dtype=str, low_memory=False)
        
        # Vytvoření unikátního mapování IČO -> Název
        referencni_data = df_main[[SLOUPEC_ICO_REF, SLOUPEC_NAZEV_REF]].drop_duplicates()
        mapovani_ico = referencni_data.to_dict('records')
        print(f"Načteno {len(mapovani_ico)} unikátních referenčních IČO.")
    except Exception as e:
        print(f"Chyba při načítání hlavního souboru: {e}")
        return

    # 2. Načtení souboru s chybějícími IČO (ta, co chceme dohledat)
    try:
        df_missing = pd.read_csv(CHYBEJICI_SOUBOR, sep='\t', dtype=str)
        # Předpokládáme, že v tomto souboru je sloupec 'Hledane_ICO'
        seznam_chybejicich = df_missing['Hledane_ICO'].unique()
    except Exception as e:
        print(f"Chyba při načítání souboru chybějících IČO: {e}")
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

    # 5. Uložení výsledku do TSV
    vystupni_cesta = os.path.join(OUTPUT_DIR, 'analyza_preklepu.tsv')
    df_output = pd.DataFrame(finalni_radky)
    df_output.to_csv(vystupni_cesta, sep='\t', index=False, encoding='utf-8')
    
    print(f"Hotovo. Výsledky uloženy do: {vystupni_cesta}")

if __name__ == "__main__":
    main()