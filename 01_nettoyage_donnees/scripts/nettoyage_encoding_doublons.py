"""
Nettoyage des fichiers CSV M2BPO
- Conversion encoding ISO-8859-1 -> UTF-8
- Suppression des doublons sur ID
- Nettoyage des caractères spéciaux
- Normalisation majuscules / minuscules
"""

import pandas as pd
import re
import os

INPUT_PATH = "data_raw/export_10ans.csv"
OUTPUT_PATH = "data_clean/tenders_clean.csv"

def clean_encoding(df):
    """Corrige les problèmes d'encoding fréquents"""
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.encode('latin1').str.decode('utf-8', errors='ignore')
    return df

def remove_duplicates(df, id_col='ID'):
    """Supprime les doublons basés sur l'ID"""
    before = len(df)
    df = df.drop_duplicates(subset=[id_col], keep='first')
    print(f"Doublons supprimés : {before - len(df)}")
    return df

def clean_special_chars(df):
    """Nettoie les accents et caractères indésirables"""
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.replace(r'[^\w\sÀ-ÿ\-/]', '', regex=True)
        df[col] = df[col].str.upper().str.strip()
    return df

def filter_cancelled(df, text_col='Objet'):
    """Supprime les annonces annulées / sans suite"""
    patterns = r'^(ANNUL|DECLAREE SANS SUITE|MARCHE SANS SUITE|DOUBLON|HORS SUJET)'
    before = len(df)
    df = df[~df[text_col].str.upper().str.contains(patterns, na=False, regex=True)]
    print(f"Annonces invalides supprimées : {before - len(df)}")
    return df

def main():
    print("Chargement du fichier...")
    df = pd.read_csv(INPUT_PATH, encoding='ISO-8859-1', low_memory=False)
    
    print("Nettoyage encodage...")
    df = clean_encoding(df)
    
    print("Suppression doublons...")
    df = remove_duplicates(df)
    
    print("Nettoyage caractères spéciaux...")
    df = clean_special_chars(df)
    
    print("Filtrage annulations...")
    df = filter_cancelled(df)
    
    # Création colonne Pays
    df['Pays'] = df['Département'].apply(lambda x: 'France' if str(x).strip() != 'ZZZ' else 'Étranger')
    
    # Sauvegarde
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False, encoding='utf-8-sig')
    print(f"Fichier nettoyé sauvegardé : {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
