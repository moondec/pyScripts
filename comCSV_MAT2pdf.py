import os
import time
import re
import json
import pandas as pd
import numpy as np
import scipy.io as sio
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_pdf import PdfPages
import logging
from datetime import datetime, timedelta
import multiprocessing

# import słowników config
from config import *

# --- STAŁE KONFIGURACYJNE ---
INTERESUJACE_CZLONY = ["SWIN_1_1_1", "SWin_1_1_1_Avg", "PPFD_1_1_1_Avg", "SWin_1_2_1", "PPFD_1_2_1", "RsUp_Avg", "PPFD_215up_Avg", "PPFDg_Avg", "PPFD_BC_IN_1_1_1", "PPFD_BC_IN_1_1_2"]
ZMIENNE_RADIACYJNE = ["SWIN_1_1_1", "SWin_1_1_1_Avg", "PPFD_1_1_1_Avg", "SWin_1_2_1", "PPFD_1_2_1", "RsUp_Avg", "PPFD_215up_Avg", "PPFDg_Avg", "PPFD_BC_IN_1_1_1", "PPFD_BC_IN_1_1_2"]

RZEDY_SIATKI, KOLUMNY_SIATKI = 3, 4
WYKRESOW_NA_STRONE = RZEDY_SIATKI * KOLUMNY_SIATKI
ROZMIAR_STRONY_A4_POZIOMO = (11.69, 8.27)

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s')

# --- FUNKCJE PRZETWARZANIA DANYCH ---

def apply_column_mapping(df: pd.DataFrame, group_id: str) -> pd.DataFrame:
    """
    Applies column name mapping to the DataFrame based on rules defined in config.py.
    This function links the file group_id to a specific set of mapping rules.
    """
    ruleset_name = STATION_MAPPING_FOR_COLUMNS.get(group_id)
    if not ruleset_name:
        logging.debug(f"No column mapping ruleset defined for group '{group_id}'. Skipping rename.")
        return df

    mapping_dict = COLUMN_MAPPING_RULES.get(ruleset_name)
    if not mapping_dict:
        logging.warning(f"Column mapping ruleset '{ruleset_name}' not found in COLUMN_MAPPING_RULES.")
        return df

    original_columns = df.columns
    df.columns = df.columns.str.strip()
    df.rename(columns=mapping_dict, inplace=True)
    
    renamed_cols_count = sum(1 for col in original_columns if col.strip() in mapping_dict)
    if renamed_cols_count > 0:
        logging.info(f"Applied column mapping '{ruleset_name}' for group '{group_id}', renamed {renamed_cols_count} columns.")
    
    return df

def apply_calibration(df: pd.DataFrame, group_id: str) -> pd.DataFrame:
    """
    Applies calibration to the DataFrame based on rules from config.py.
    It uses the canonical column names (after mapping).
    """
    ruleset_name = STATION_MAPPING_FOR_CALIBRATION.get(group_id)
    if not ruleset_name:
        logging.debug(f"No calibration ruleset defined for group '{group_id}'. Skipping calibration.")
        return df

    calibration_rules = CALIBRATION_RULES_BY_STATION.get(ruleset_name)
    if not calibration_rules:
        logging.warning(f"Calibration ruleset '{ruleset_name}' not found in CALIBRATION_RULES_BY_STATION.")
        return df

    for column, rules in calibration_rules.items():
        if column in df.columns:
            # Ensure column is numeric before applying calibration
            df[column] = pd.to_numeric(df[column], errors='coerce')
            for rule in rules:
                try:
                    start_date = pd.to_datetime(rule['start'])
                    end_date = pd.to_datetime(rule['end'])
                    multiplier = rule.get('multiplier', 1.0)
                    addend = rule.get('addend', 0.0)
                    
                    mask = (df.index >= start_date) & (df.index <= end_date)
                    
                    if mask.any():
                        df.loc[mask, column] = df.loc[mask, column] * multiplier + addend
                        logging.info(f"Applied calibration on '{column}' for group '{group_id}' from {rule['start']} to {rule['end']} (Reason: {rule['reason']})")
                except Exception as e:
                    logging.error(f"Error applying calibration rule for column '{column}': {rule}. Error: {e}")
    return df


# --- KLASA PROFILERA ---
class SimpleProfiler:
    def __init__(self):
        self.timings = {}
        self.start_points = {}
    
    def start(self, name: str):
        self.start_points[name] = time.time()
    
    def stop(self, name: str):
        if name in self.start_points:
            duration = time.time() - self.start_points[name]
            self.timings[name] = self.timings.get(name, 0) + duration
    
    def report(self):
        print("\n" + "="*40 + "\n--- RAPORT CZASU WYKONANIA (PROFILER) ---\n" + "="*40)
        for name, duration in sorted(self.timings.items()): 
            print(f"- {name:<35}: {duration:.4f}s")
        print("="*40)

# --- FUNKCJE POMOCNICZE ---
def czy_interesujaca_kolumna(nazwa_kolumny: str) -> bool:
    return nazwa_kolumny in INTERESUJACE_CZLONY

def potential_radiation(date_POSIX=None, latitude=52.4064, longitude=0):
    if date_POSIX is None:
        date_POSIX = pd.Timestamp.now()
    elif not isinstance(date_POSIX, pd.Timestamp) and not isinstance(date_POSIX, pd.DatetimeIndex):
        date_POSIX = pd.to_datetime(date_POSIX)
    
    if not isinstance(date_POSIX, pd.DatetimeIndex):
        date_POSIX = pd.DatetimeIndex([date_POSIX])
    
    doy = date_POSIX.dayofyear
    time_seconds = date_POSIX.hour * 3600 + date_POSIX.minute * 60 + date_POSIX.second
    tv_hh = time_seconds / 3600
    
    lat_rad = np.deg2rad(latitude)
    long_rad = np.deg2rad(longitude)
    
    declination = np.deg2rad(-23.44) * np.cos(np.deg2rad(360 / 365 * (doy + 10)))
    
    hour_angle = np.pi * (tv_hh - 12) / 12 - long_rad
    
    sin_psi = (np.sin(lat_rad) * np.sin(declination) 
               + np.cos(lat_rad) * np.cos(declination) 
               * np.cos(hour_angle))
    
    transmissivity = 0.6 + 0.2 * sin_psi
    S = 1370
    
    radiation = np.zeros_like(sin_psi, dtype=float)
    mask = sin_psi > 0
    radiation[mask] = S * transmissivity[mask] * sin_psi[mask]
    
    result_df = pd.DataFrame({'Radiation': radiation}, index=date_POSIX)
    
    return result_df

def oblicz_i_zapisz_przesuniecia(df_roczne: pd.DataFrame, sciezka_log: str, nazwa_grupy: str, rok: int):
    logging.info(f"Obliczanie dobowych przesunięć czasowych dla {nazwa_grupy}/{rok}...")
    
    wyniki_offsetu = {}

    for zmienna_ref in ZMIENNE_RADIACYJNE:
        kolumna_ref_csv = zmienna_ref + '_csv'
        if kolumna_ref_csv not in df_roczne.columns or df_roczne[kolumna_ref_csv].dropna().empty:
            logging.debug(f"Brak danych referencyjnych '{zmienna_ref}' w CSV dla {nazwa_grupy}/{rok}.")
            continue

        logging.debug(f"Obliczanie offsetu dla zmiennej: {zmienna_ref}")
        df_ref = df_roczne[[kolumna_ref_csv]].dropna().copy()
        df_ref['potential'] = potential_radiation(df_ref.index)['Radiation']
        
        df_ref_daily = df_ref.groupby(df_ref.index.date)
        
        daily_offsets = {}
        
        for date, group in df_ref_daily:
            sunrise_measured = group[group[kolumna_ref_csv] > 20].index.min()
            sunset_measured = group[group[kolumna_ref_csv] > 20].index.max()
            sunrise_potential = group[group['potential'] > 0].index.min()
            sunset_potential = group[group['potential'] > 0].index.max()
            
            day_offsets_sec = []
            if pd.notna(sunrise_measured) and pd.notna(sunrise_potential):
                day_offsets_sec.append((sunrise_measured - sunrise_potential).total_seconds())
            if pd.notna(sunset_measured) and pd.notna(sunset_potential):
                day_offsets_sec.append((sunset_measured - sunset_potential).total_seconds())
            
            if day_offsets_sec:
                avg_offset_seconds = np.mean(day_offsets_sec)
                daily_offsets[date] = round(avg_offset_seconds / 3600, 2)

        if not daily_offsets:
            logging.warning(f"Nie udało się obliczyć dziennego przesunięcia dla zmiennej '{zmienna_ref}'.")
            continue

        periods = []
        if daily_offsets:
            sorted_dates = sorted(daily_offsets.keys())
            start_date = sorted_dates[0]
            current_offset = daily_offsets[start_date]
            for i in range(1, len(sorted_dates)):
                date = sorted_dates[i]
                prev_date = sorted_dates[i-1]
                if daily_offsets[date] != current_offset or (date - prev_date).days > 1:
                    periods.append({'start': str(start_date), 'end': str(prev_date), 'offset_hours': current_offset})
                    start_date = date
                    current_offset = daily_offsets[date]
            periods.append({'start': str(start_date), 'end': str(sorted_dates[-1]), 'offset_hours': current_offset})
        
        if periods:
            wyniki_offsetu[zmienna_ref] = periods

    if abs(len(wyniki_offsetu)) > 0.8:
        with open(sciezka_log, 'w') as f:
            json.dump(wyniki_offsetu, f, indent=4)
        logging.info(f"Zapisano log przesunięcia z danymi dla {len(wyniki_offsetu)} zmiennych do: {sciezka_log}")
    else:
        logging.warning(f"Nie udało się obliczyć przesunięcia dla żadnej zmiennej w {nazwa_grupy}/{rok}.")


# --- FUNKCJE WCZYTUJĄCE DANE CSV ---
def znajdz_i_grupuj_pliki_csv(sciezka_katalogu: str) -> dict:
    grupy_plikow_csv = {}
    if not os.path.isdir(sciezka_katalogu): 
        logging.warning(f"Katalog CSV nie istnieje: {sciezka_katalogu}")
        return grupy_plikow_csv
    
    for root, _, files in os.walk(sciezka_katalogu):
        for file in files:
            if file.endswith('.csv'):
                nazwa_grupy = os.path.splitext(file)[0]
                sciezka_pliku = os.path.join(root, file)
                grupy_plikow_csv.setdefault(nazwa_grupy, []).append(sciezka_pliku)
    
    return grupy_plikow_csv

def wczytaj_i_polacz_dane_csv(pliki_grupy: list, nazwa_grupy: str) -> pd.DataFrame:
    """
    Loads and concatenates CSV data for a specific group, then applies
    station-specific column name mapping, calibration, and filters for interesting columns.
    """
    if not pliki_grupy: return pd.DataFrame()
    
    lista_df = []
    for sciezka in sorted(pliki_grupy):
        try:
            df = pd.read_csv(sciezka, low_memory=False, encoding_errors='ignore')
            lista_df.append(df)
        except Exception as e:
            logging.error(f"Błąd wczytywania pliku CSV {sciezka}: {e}")
    
    if not lista_df: return pd.DataFrame()
    
    df_laczny = pd.concat(lista_df, ignore_index=True)

    # Convert TIMESTAMP and set as index before any time-based operations
    df_laczny['TIMESTAMP'] = pd.to_datetime(df_laczny['TIMESTAMP'], errors='coerce')
    df_laczny = df_laczny.dropna(subset=['TIMESTAMP'])
    if df_laczny.empty: return pd.DataFrame()
    
    df_laczny.set_index('TIMESTAMP', inplace=True)
    df_laczny.index = df_laczny.index.tz_localize(None)
    df_laczny.sort_index(inplace=True)

    # Apply station-specific column mapping from config.py
    df_laczny = apply_column_mapping(df_laczny, nazwa_grupy)

    # Apply station-specific calibration
    df_laczny = apply_calibration(df_laczny, nazwa_grupy)

    # Filter only for interesting columns AFTER renaming and calibration
    # 'TIMESTAMP' is already the index, so we don't need to keep it as a column
    kolumny_do_zachowania = [col for col in df_laczny.columns if col in INTERESUJACE_CZLONY]
    
    return df_laczny[kolumny_do_zachowania]

# --- FUNKCJE WCZYTUJĄCE DANE MATLAB ---
def znajdz_i_grupuj_pliki_mat(sciezka_katalogu: str) -> dict:
    grupy_plikow_mat = {}
    if not os.path.isdir(sciezka_katalogu): 
        logging.warning(f"Katalog MAT nie istnieje: {sciezka_katalogu}")
        return grupy_plikow_mat
    
    for root, dirs, _ in os.walk(sciezka_katalogu):
        if 'zero_level' in dirs:
            sciezka_zero_level = os.path.join(root, 'zero_level')
            if 'tv.mat' in os.listdir(sciezka_zero_level):
                nazwa_grupy = os.path.basename(root)
                
                pliki_do_ignorowania = ['tv.mat', 'RECORD.mat', 'time_vector.mat']
                sciezki_danych = [os.path.join(sciezka_zero_level, f) for f in os.listdir(sciezka_zero_level) if f.endswith('.mat') and f not in pliki_do_ignorowania]

                if sciezki_danych:
                    logging.debug(f"Znaleziono grupę MAT '{nazwa_grupy}' w katalogu: {root}")
                    wpis = {'sciezka_tv': os.path.join(sciezka_zero_level, 'tv.mat'), 'sciezki_danych': sciezki_