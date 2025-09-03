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
from pathlib import Path

# import słowników config
from config import *

# --- STAŁE KONFIGURACYJNE ---
INTERESUJACE_CZLONY = ["SW_IN_1_1_1", "SW_IN_1_2_1", "PPFD_IN_1_1_1", "PPFD_IN_1_2_1", "PPFD_IN_1_1_3", "PPFD_BC_IN_1_1_1", "PPFD_BC_IN_1_1_2"]
ZMIENNE_RADIACYJNE = ["SW_IN_1_1_1", "SW_IN_1_2_1", "PPFD_IN_1_1_1", "PPFD_IN_1_2_1", "PPFD_IN_1_1_3", "PPFD_BC_IN_1_1_1", "PPFD_BC_IN_1_1_2"]

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
    if not group_id: return df # Return if group_id is None
    
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
    if not group_id: return df # Return if group_id is None

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

    df_laczny['TIMESTAMP'] = pd.to_datetime(df_laczny['TIMESTAMP'], errors='coerce')
    df_laczny = df_laczny.dropna(subset=['TIMESTAMP'])
    if df_laczny.empty: return pd.DataFrame()
    
    df_laczny.set_index('TIMESTAMP', inplace=True)
    df_laczny.index = df_laczny.index.tz_localize(None)
    df_laczny.sort_index(inplace=True)

    df_laczny = apply_column_mapping(df_laczny, nazwa_grupy)
    df_laczny = apply_calibration(df_laczny, nazwa_grupy)

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
                    wpis = {'sciezka_tv': os.path.join(sciezka_zero_level, 'tv.mat'), 'sciezki_danych': sciezki_danych}
                    grupy_plikow_mat.setdefault(nazwa_grupy, []).append(wpis)
    return grupy_plikow_mat

def wczytaj_i_polacz_dane_csv(pliki_grupy: list, nazwa_grupy: str) -> pd.DataFrame:
    """
    Loads and concatenates CSV data. Assumes column names are already canonical.
    Applies calibration and filters for interesting columns.
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

    df_laczny['TIMESTAMP'] = pd.to_datetime(df_laczny['TIMESTAMP'], errors='coerce')
    df_laczny = df_laczny.dropna(subset=['TIMESTAMP'])
    if df_laczny.empty: return pd.DataFrame()
    
    df_laczny.set_index('TIMESTAMP', inplace=True)
    df_laczny.index = df_laczny.index.tz_localize(None)
    df_laczny.sort_index(inplace=True)

    # ### Usunięto apply_column_mapping, ponieważ nazwy w CSV są już poprawne ###

    kolumny_do_zachowania = [col for col in df_laczny.columns if col in INTERESUJACE_CZLONY]
    
    return df_laczny[kolumny_do_zachowania]

def wczytaj_i_polacz_dane_mat(wpisy_grupy: list, nazwa_grupy: str) -> pd.DataFrame: # <--- ZMIANA: Dodano argument nazwa_grupy
    if not wpisy_grupy: return pd.DataFrame()
    lista_df = []
    
    for wpis in sorted(wpisy_grupy, key=lambda x: x['sciezka_tv']):
        try:
            mat_tv = sio.loadmat(wpis['sciezka_tv'], squeeze_me=True)
            klucz_tv = next(k for k in mat_tv.keys() if not k.startswith('__'))
            wektor_czasu_serial = mat_tv[klucz_tv].flatten()
            wektor_czasu = [datetime.fromordinal(int(t)) + timedelta(days=t % 1) - timedelta(days=366) for t in wektor_czasu_serial]
            dane_z_pliku = {'TIMESTAMP': wektor_czasu}
            zmienne_wczytane_count = 0
            
            for sciezka_danych in wpis['sciezki_danych']:
                nazwa_zmiennej = os.path.splitext(os.path.basename(sciezka_danych))[0]
                mat_dane = sio.loadmat(sciezka_danych, squeeze_me=True)
                if nazwa_zmiennej in mat_dane:
                    dane_wektor = mat_dane[nazwa_zmiennej].flatten()
                    if len(dane_wektor) == len(wektor_czasu): 
                        dane_z_pliku[nazwa_zmiennej] = dane_wektor
            
            if len(dane_z_pliku) > 1:
                lista_df.append(pd.DataFrame(dane_z_pliku))
        except Exception as e:
            logging.error(f"Błąd wczytywania MAT: {wpis['sciezka_tv']}: {e}")
    
    if not lista_df: return pd.DataFrame()
    
    df_laczny = pd.concat(lista_df, ignore_index=True)
    df_laczny['TIMESTAMP'] = pd.to_datetime(df_laczny['TIMESTAMP'])
    df_laczny.set_index('TIMESTAMP', inplace=True)
    df_laczny.sort_index(inplace=True)

    # <--- ZMIANA: Zastosowanie mapowania nazw kolumn również dla danych MAT
    df_laczny = apply_column_mapping(df_laczny, nazwa_grupy)
    # <--- ZMIANA: Kalibracja jest teraz stosowana do danych MAT
    df_laczny = apply_calibration(df_laczny, nazwa_grupy)

    # Filter for interesting columns after potential renaming
    kolumny_do_zachowania = [col for col in df_laczny.columns if czy_interesujaca_kolumna(col)]
    df_laczny = df_laczny[kolumny_do_zachowania]

    # # Hardcoded corrections (can be moved to calibration dict in the future)
    # kolumna_korekty = "PPFD_BC_IN_1_1_1"
    # if kolumna_korekty in df_laczny.columns:
        # data_korekty = pd.to_datetime("2018-11-16 11:09:00")
        # wspolczynnik = 3397.547
        # maska = df_laczny.index > data_korekty
        # df_laczny.loc[maska, kolumna_korekty] = df_laczny.loc[maska, kolumna_korekty] * wspolczynnik
        # logging.info(f"Zastosowano stałą korektę do kolumny '{kolumna_korekty}' dla danych MAT po dacie {data_korekty}.")
        
    # kolumna_korekty = "PPFD_BC_IN_1_1_2"
    # if kolumna_korekty in df_laczny.columns:
        # data_korekty = pd.to_datetime("2018-11-16 11:09:00")
        # wspolczynnik = 3288.716
        # maska = df_laczny.index > data_korekty
        # df_laczny.loc[maska, kolumna_korekty] = df_laczny.loc[maska, kolumna_korekty] * wspolczynnik
        # logging.info(f"Zastosowano stałą korektę do kolumny '{kolumna_korekty}' dla danych MAT po dacie {data_korekty}.")
    
    return df_laczny
    
def mapuj_grupy(grupy_csv: dict, grupy_mat: dict) -> dict:
    zmapowane_grupy = {}
    mat_dopasowane = set()

    for csv_nazwa, csv_pliki in grupy_csv.items():
        best_match = None
        for mat_nazwa in grupy_mat:
            if mat_nazwa.replace("_", "").lower() in csv_nazwa.replace("_", "").lower():
                best_match = mat_nazwa
                break

        if best_match:
            kanoniczna_nazwa = csv_nazwa
            zmapowane_grupy[kanoniczna_nazwa] = {'csv': csv_nazwa, 'mat': best_match}
            mat_dopasowane.add(best_match)
        else:
            zmapowane_grupy[csv_nazwa] = {'csv': csv_nazwa, 'mat': None}

    for mat_nazwa in grupy_mat:
        if mat_nazwa not in mat_dopasowane:
            zmapowane_grupy[mat_nazwa] = {'csv': None, 'mat': mat_nazwa}
            
    return zmapowane_grupy

# --- FUNKCJA GENERUJĄCA RAPORT PORÓWNAWCZY ---
def generuj_raport_porownawczy(df_roczne: pd.DataFrame, sciezka_pdf: str, nazwa_grupy: str, rok: int):
    if df_roczne.empty:
        logging.warning(f"Brak danych dla {nazwa_grupy}/{rok}. Pomijam generowanie PDF.")
        return
        
    kolumny_csv = {k.replace('_csv', '') for k in df_roczne.columns if k.endswith('_csv')}
    kolumny_mat = {k.replace('_mat', '') for k in df_roczne.columns if k.endswith('_mat')}
    wszystkie_kolumny = sorted(list(kolumny_csv.union(kolumny_mat)))
    
    kolumny_do_rysowania = [kol for kol in wszystkie_kolumny if czy_interesujaca_kolumna(kol)]
    
    if not kolumny_do_rysowania:
        logging.warning(f"Brak interesujących zmiennych dla {nazwa_grupy}/{rok} po filtracji.")
        return
    
    logging.info(f"  > Generowanie raportu dla {len(kolumny_do_rysowania)} odfiltrowanych zmiennych: {kolumny_do_rysowania}")
    
    liczba_stron_w_pdf = 0
    with PdfPages(sciezka_pdf) as pdf:
        for kolumna_bazowa in kolumny_do_rysowania:
            kolumna_csv = kolumna_bazowa + '_csv'
            kolumna_mat = kolumna_bazowa + '_mat'
            df_kolumny = df_roczne[[c for c in [kolumna_csv, kolumna_mat] if c in df_roczne.columns]].dropna(how='all')
            if df_kolumny.empty: continue

            if kolumna_bazowa in ZMIENNE_RADIACYJNE:
                df_kolumny['potential'] = potential_radiation(df_kolumny.index)['Radiation']

            grupy_dni = df_kolumny.groupby(df_kolumny.index.date)
            lista_dni = list(grupy_dni)
            
            for i in range(0, len(lista_dni), WYKRESOW_NA_STRONE):
                fig, axes = plt.subplots(RZEDY_SIATKI, KOLUMNY_SIATKI, figsize=ROZMIAR_STRONY_A4_POZIOMO, constrained_layout=True)
                fig.suptitle(f"{nazwa_grupy} - {rok} - {kolumna_bazowa}", fontsize=14)
                fig.text(0.5, 0.95, "Niebieski=CSV, Czerwony=MATLAB, Zielony=Teoretyczne", ha='center', va='top', fontsize=10)
                ax_list = axes.flatten()
                fragment_dni = lista_dni[i:i + WYKRESOW_NA_STRONE]

                for j, (data_dnia, grupa_dnia) in enumerate(fragment_dni):
                    ax = ax_list[j]
                    dzien_start = datetime.combine(data_dnia, datetime.min.time())
                    dzien_koniec = datetime.combine(data_dnia, datetime.max.time())
                    
                    if kolumna_csv in grupa_dnia.columns and not grupa_dnia[kolumna_csv].isna().all():
                        ax.plot(grupa_dnia.index, grupa_dnia[kolumna_csv], 'b-', label='CSV', linewidth=2.0)
                    
                    if kolumna_mat in grupa_dnia.columns and not grupa_dnia[kolumna_mat].isna().all():
                        ax.plot(grupa_dnia.index, grupa_dnia[kolumna_mat], 'r-', label='MATLAB', linewidth=1.0)

                    if 'potential' in grupa_dnia.columns and not grupa_dnia['potential'].isna().all():
                        ax.plot(grupa_dnia.index, grupa_dnia['potential'], 'g--', label='Potencjalne', linewidth=1.0)
                    
                    ax.set_title(data_dnia.strftime("%Y-%m-%d"), fontsize=9)
                    ax.set_xlim(dzien_start, dzien_koniec)
                    ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                    ax.grid(True, alpha=0.3)
                    ax.tick_params(axis='x', labelsize=7, rotation=45)
                    ax.tick_params(axis='y', labelsize=7)
                    if j == 0:
                        ax.legend(fontsize=7, loc='best')

                for k in range(len(fragment_dni), WYKRESOW_NA_STRONE):
                    ax_list[k].set_visible(False)
                
                pdf.savefig(fig, dpi=100)
                plt.close(fig)
                liczba_stron_w_pdf += 1
    
    if liczba_stron_w_pdf > 0:
        logging.info(f"Zapisano raport ({liczba_stron_w_pdf} stron): {sciezka_pdf}")
    else:
        logging.warning(f"Nie wygenerowano żadnej strony dla raportu: {sciezka_pdf}")

# --- FUNKCJA ROBOCZA DLA WIELOPROCESOWOŚCI ---
def process_group(args):
    """
    Worker function to process a single canonical group.
    This function is designed to be called by a multiprocessing pool.
    """
    kanoniczna_nazwa, mapowanie, grupy_csv, grupy_mat, katalog_wyjsciowy = args
    
    logging.info(f"\n{'='*50}\nPRZETWARZANIE GRUPY KANONICZNEJ: {kanoniczna_nazwa}\n{'='*50}")
    
    nazwa_grupy_csv = mapowanie.get('csv')
    nazwa_grupy_mat = mapowanie.get('mat')

    df_csv = pd.DataFrame()
    if nazwa_grupy_csv:
        df_csv = wczytaj_i_polacz_dane_csv(grupy_csv.get(nazwa_grupy_csv, []), nazwa_grupy_csv)

    df_mat = pd.DataFrame()
    if nazwa_grupy_mat:
        # <--- POPRAWKA: Używamy nazwy grupy CSV jako klucza do znalezienia reguł dla MAT
        # To zapewnia, że obie grupy (CSV i MAT) używają tego samego zestawu reguł mapowania.
        df_mat = wczytaj_i_polacz_dane_mat(grupy_mat.get(nazwa_grupy_mat, []), nazwa_grupy_csv) 
        
    logging.info(f"Wynik wczytywania dla '{kanoniczna_nazwa}': CSV={df_csv.shape}, MAT={df_mat.shape}")

    if df_csv.empty and df_mat.empty:
        logging.warning(f"Brak danych dla {kanoniczna_nazwa} po wczytaniu. Pomijam.")
        return f"{kanoniczna_nazwa}: Skipped"
    
    if not df_csv.empty: 
        df_csv = df_csv.add_suffix('_csv')
    if not df_mat.empty: 
        df_mat = df_mat.add_suffix('_mat')
    
    df_porownawczy = pd.merge(df_csv, df_mat, left_index=True, right_index=True, how='outer')
    
    for col in df_porownawczy.columns:
        if col.endswith('_csv') or col.endswith('_mat'):
            if not pd.api.types.is_numeric_dtype(df_porownawczy[col]):
                df_porownawczy[col] = pd.to_numeric(df_porownawczy[col], errors='coerce')
    
    logging.info(f"Dane scalone: {df_porownawczy.shape[0]} rekordów, {df_porownawczy.shape[1]} kolumn")
    
    lata = sorted(df_porownawczy.index.year.unique())
    logging.info(f"Dostępne lata: {lata}")
    
    for rok in lata:
        df_roczny = df_porownawczy[df_porownawczy.index.year == rok].copy()
        
        if df_roczny.empty: 
            logging.warning(f"Brak danych dla roku {rok}")
            continue

        sciezka_log_offsetu = os.path.join(katalog_wyjsciowy, f"Offset_{kanoniczna_nazwa}_{rok}.json")
        oblicz_i_zapisz_przesuniecia(df_roczny, sciezka_log_offsetu, kanoniczna_nazwa, rok)
            
        sciezka_pdf = os.path.join(katalog_wyjsciowy, f"Porownanie_{kanoniczna_nazwa}_{rok}.pdf")
        generuj_raport_porownawczy(df_roczny, sciezka_pdf, kanoniczna_nazwa, rok)
        
    return f"{kanoniczna_nazwa}: Success"
    
# --- GŁÓWNA CZĘŚĆ SKRYPTU ---
if __name__ == '__main__':
    BASE_DIR = Path(__file__).parent
    LOGS_DIR = BASE_DIR / 'logs_mat'
    os.makedirs(LOGS_DIR, exist_ok=True)
    
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    root_logger = logging.getLogger()
    
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # Handler do pliku
    file_handler = logging.FileHandler(LOGS_DIR / "script_run.log")
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)

    # Handler do konsoli
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)
    
    root_logger.setLevel(logging.INFO)
    
    profiler = SimpleProfiler()
    profiler.start('Całkowity czas wykonania')
    
    sciezka_danych_csv = 'E:\\test_split\\ME'
    sciezka_danych_mat = 'D:\\sites\\ME\\met-data_ME'
    katalog_wyjsciowy = 'E:\\pdfy\\Porownanie_CSV_vs_MATLAB\\ME'
    
    if not all([INTERESUJACE_CZLONY]):
        logging.error("Lista INTERESUJACE_CZLONY nie może być pusta!")
        exit(1)
    
    os.makedirs(katalog_wyjsciowy, exist_ok=True)
    logging.info(f"Katalog wyjściowy: {katalog_wyjsciowy}")
    
    profiler.start('Wyszukiwanie plików')
    grupy_csv = znajdz_i_grupuj_pliki_csv(sciezka_danych_csv)
    grupy_mat = znajdz_i_grupuj_pliki_mat(sciezka_danych_mat)
    profiler.stop('Wyszukiwanie plików')

    zmapowane_grupy = mapuj_grupy(grupy_csv, grupy_mat)

    logging.info(f"\n--- DIAGNOSTYKA ZMAPOWANYCH GRUP ---")
    for kanoniczna, mapowanie in zmapowane_grupy.items():
        logging.info(f"  > Grupa '{kanoniczna}': CSV='{mapowanie['csv']}', MAT='{mapowanie['mat']}'")
    logging.info(f"-----------------------------------\n")

    if not zmapowane_grupy:
        logging.error("Nie znaleziono żadnych grup danych do przetworzenia!")
        exit(1)
    
    tasks = [
        (kanoniczna, mapowanie, grupy_csv, grupy_mat, katalog_wyjsciowy)
        for kanoniczna, mapowanie in zmapowane_grupy.items()
    ]
    
    logging.info(f"Uruchamianie przetwarzania równoległego dla {len(tasks)} grup...")
    with multiprocessing.Pool() as pool:
        results = pool.map(process_group, tasks)
        
    logging.info("--- WYNIKI PRZETWARZANIA RÓWNOLEGŁEGO ---")
    for result in results:
        logging.info(f" - {result}")
    logging.info("----------------------------------------")

    profiler.stop('Całkowity czas wykonania')
    profiler.report()
    
    logging.info("\n" + "="*50 + "\nPRZETWARZANIE ZAKOŃCZONE POMYŚLNIE!\n" + "="*50)