import os
import time
import re
import pandas as pd
import numpy as np
import scipy.io as sio
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_pdf import PdfPages
import logging
from datetime import datetime, timedelta

# --- STAŁE KONFIGURACYJNE ---
INTERESUJACE_CZLONY = ["TA_1_1_1", "SWin_1_2_1", "PPFD_BC_IN_1_1_1", "TS_6_1_1", "PPFD_1_2_1"]
RZEDY_SIATKI, KOLUMNY_SIATKI = 3, 4
WYKRESOW_NA_STRONE = RZEDY_SIATKI * KOLUMNY_SIATKI
ROZMIAR_STRONY_A4_POZIOMO = (11.69, 8.27)

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    """
    Sprawdza, czy nazwa kolumny DOKŁADNIE pasuje do jednej z pozycji na liście.
    """
    return nazwa_kolumny in INTERESUJACE_CZLONY

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

def wczytaj_i_polacz_dane_csv(pliki_grupy: list) -> pd.DataFrame:
    if not pliki_grupy: return pd.DataFrame()
    lista_df = []
    
    for sciezka in sorted(pliki_grupy):
        try:
            naglowki_oryginalne = pd.read_csv(sciezka, nrows=0, encoding_errors='ignore').columns.tolist()
            mapowanie_naglowkow = {oryg: oryg.strip() for oryg in naglowki_oryginalne}
            kolumny_do_wczytania = [oryg for oryg, wyczyszczony in mapowanie_naglowkow.items() 
                                   if wyczyszczony == 'TIMESTAMP' or czy_interesujaca_kolumna(wyczyszczony)]
            
            if len(kolumny_do_wczytania) > 1:
                logging.debug(f"Plik {os.path.basename(sciezka)}: wczytywanie {len(kolumny_do_wczytania)-1} interesujących kolumn.")
                df = pd.read_csv(sciezka, usecols=kolumny_do_wczytania, low_memory=False)
                df.rename(columns=mapowanie_naglowkow, inplace=True)
                lista_df.append(df)
            else:
                 logging.debug(f"W pliku {os.path.basename(sciezka)} nie znaleziono żadnych interesujących kolumn.")

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
    if 'Record' in df_laczny.columns:
        df_laczny = df_laczny.drop(columns=['Record'])
    
    return df_laczny

# --- FUNKCJE WCZYTUJĄCE DANE MATLAB ---
def znajdz_i_grupuj_pliki_mat(sciezka_katalogu: str) -> dict:
    grupy_plikow_mat = {}
    if not os.path.isdir(sciezka_katalogu): 
        logging.warning(f"Katalog MAT nie istnieje: {sciezka_katalogu}")
        return grupy_plikow_mat
    
    for root, _, files in os.walk(sciezka_katalogu):
        if 'tv.mat' in files and 'zero_level' in root:
            parent_dir = os.path.dirname(root)
            grandparent_dir = os.path.dirname(parent_dir)
            potential_year = os.path.basename(parent_dir)
            
            if potential_year.isdigit() and len(potential_year) == 4:
                nazwa_grupy = os.path.basename(grandparent_dir)
            else:
                nazwa_grupy = os.path.basename(parent_dir)
            
            if not nazwa_grupy: continue

            pliki_do_ignorowania = ['tv.mat', 'RECORD.mat', 'time_vector.mat']
            sciezki_danych = [os.path.join(root, f) for f in files if f.endswith('.mat') and f not in pliki_do_ignorowania]
            
            if sciezki_danych:
                logging.debug(f"Znaleziono grupę MAT '{nazwa_grupy}' w katalogu: {root}")
                wpis = {'sciezka_tv': os.path.join(root, 'tv.mat'), 'sciezki_danych': sciezki_danych}
                grupy_plikow_mat.setdefault(nazwa_grupy, []).append(wpis)
                
    return grupy_plikow_mat

def wczytaj_i_polacz_dane_mat(wpisy_grupy: list) -> pd.DataFrame:
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
                if not czy_interesujaca_kolumna(nazwa_zmiennej): continue
                    
                mat_dane = sio.loadmat(sciezka_danych, squeeze_me=True)
                if nazwa_zmiennej in mat_dane:
                    dane_wektor = mat_dane[nazwa_zmiennej].flatten()
                    if len(dane_wektor) == len(wektor_czasu): 
                        dane_z_pliku[nazwa_zmiennej] = dane_wektor
                        zmienne_wczytane_count += 1
            
            logging.debug(f"Katalog {os.path.basename(os.path.dirname(wpis['sciezka_tv']))}: wczytano {zmienne_wczytane_count} interesujących zmiennych")
            if len(dane_z_pliku) > 1:
                lista_df.append(pd.DataFrame(dane_z_pliku))
        except Exception as e:
            logging.error(f"Błąd wczytywania MAT: {wpis['sciezka_tv']}: {e}")
    
    if not lista_df: return pd.DataFrame()
    
    df_laczny = pd.concat(lista_df, ignore_index=True)
    df_laczny['TIMESTAMP'] = pd.to_datetime(df_laczny['TIMESTAMP'])
    df_laczny.set_index('TIMESTAMP', inplace=True)
    df_laczny.sort_index(inplace=True)
    return df_laczny

# --- FUNKCJA MAPUJĄCA GRUPY ---
def mapuj_grupy(grupy_csv: dict, grupy_mat: dict) -> dict:
    zmapowane_grupy = {}
    mat_dopasowane = set()

    for csv_nazwa, csv_pliki in grupy_csv.items():
        znaleziono_dopasowanie = False
        for mat_nazwa, mat_wpisy in grupy_mat.items():
            if csv_nazwa.lower() in mat_nazwa.lower() or mat_nazwa.lower() in csv_nazwa.lower():
                zmapowane_grupy[csv_nazwa] = {'csv': csv_nazwa, 'mat': mat_nazwa}
                mat_dopasowane.add(mat_nazwa)
                znaleziono_dopasowanie = True
                break
        if not znaleziono_dopasowanie:
            zmapowane_grupy[csv_nazwa] = {'csv': csv_nazwa, 'mat': None}

    for mat_nazwa, _ in grupy_mat.items():
        if mat_nazwa not in mat_dopasowane:
            zmapowane_grupy[mat_nazwa] = {'csv': None, 'mat': mat_nazwa}
            
    return zmapowane_grupy

# --- FUNKCJA GENERUJĄCA RAPORT PORÓWNAWCZY ---
def generuj_raport_porownawczy(df_roczne: pd.DataFrame, sciezka_pdf: str, nazwa_grupy: str, rok: int):
    if df_roczne.empty:
        logging.warning(f"Brak danych dla {nazwa_grupy}/{rok}. Pomijam generowanie PDF.")
        return
        
    # ZMIANA: Jawna konwersja wszystkich kolumn na typ numeryczny
    for col in df_roczne.columns:
        if col.endswith('_csv') or col.endswith('_mat'):
            df_roczne[col] = pd.to_numeric(df_roczne[col], errors='coerce')
    
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

            grupy_dni = df_kolumny.groupby(df_kolumny.index.date)
            lista_dni = list(grupy_dni)
            
            for i in range(0, len(lista_dni), WYKRESOW_NA_STRONE):
                fig, axes = plt.subplots(RZEDY_SIATKI, KOLUMNY_SIATKI, figsize=ROZMIAR_STRONY_A4_POZIOMO, constrained_layout=True)
                fig.suptitle(f"{nazwa_grupy} - {rok} - {kolumna_bazowa}", fontsize=14)
                fig.text(0.5, 0.95, "Niebieski = dane CSV, Czerwony = dane MATLAB", ha='center', va='top', fontsize=10)
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
                    
                    ax.set_title(data_dnia.strftime("%Y-%m-%d"), fontsize=9)
                    ax.set_xlim(dzien_start, dzien_koniec)
                    ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                    ax.grid(True, alpha=0.3)
                    ax.tick_params(axis='x', labelsize=7, rotation=45)
                    ax.tick_params(axis='y', labelsize=7)
                    if j == 0 and (kolumna_csv in grupa_dnia.columns or kolumna_mat in grupa_dnia.columns):
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

# --- GŁÓWNA CZĘŚĆ SKRYPTU ---
if __name__ == '__main__':
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
    
    for kanoniczna_nazwa, mapowanie in zmapowane_grupy.items():
        nazwa_grupy_csv = mapowanie.get('csv')
        nazwa_grupy_mat = mapowanie.get('mat')

        logging.info(f"\n{'='*50}\nPRZETWARZANIE GRUPY KANONICZNEJ: {kanoniczna_nazwa}\n{'='*50}")
        profiler.start(f'Przetwarzanie {kanoniczna_nazwa}')
        
        df_csv = wczytaj_i_polacz_dane_csv(grupy_csv.get(nazwa_grupy_csv, [])) if nazwa_grupy_csv else pd.DataFrame()
        df_mat = wczytaj_i_polacz_dane_mat(grupy_mat.get(nazwa_grupy_mat, [])) if nazwa_grupy_mat else pd.DataFrame()
        
        logging.info(f"Wynik wczytywania dla '{kanoniczna_nazwa}': CSV={df_csv.shape}, MAT={df_mat.shape}")

        if df_csv.empty and df_mat.empty:
            logging.warning(f"Brak danych dla {kanoniczna_nazwa} po wczytaniu. Pomijam.")
            profiler.stop(f'Przetwarzanie {kanoniczna_nazwa}')
            continue
        
        if not df_csv.empty: 
            df_csv = df_csv.add_suffix('_csv')
        if not df_mat.empty: 
            df_mat = df_mat.add_suffix('_mat')
        
        profiler.start(f'Scalanie danych {kanoniczna_nazwa}')
        df_porownawczy = pd.merge(df_csv, df_mat, left_index=True, right_index=True, how='outer')
        profiler.stop(f'Scalanie danych {kanoniczna_nazwa}')
        
        logging.info(f"Dane scalone: {df_porownawczy.shape[0]} rekordów, {df_porownawczy.shape[1]} kolumn")
        
        lata = sorted(df_porownawczy.index.year.unique())
        logging.info(f"Dostępne lata: {lata}")
        
        for rok in lata:
            profiler.start(f'Raport {kanoniczna_nazwa} {rok}')
            df_roczny = df_porownawczy[df_porownawczy.index.year == rok]
            
            if df_roczny.empty: 
                logging.warning(f"Brak danych dla roku {rok}")
                profiler.stop(f'Raport {kanoniczna_nazwa} {rok}')
                continue
                
            sciezka_pdf = os.path.join(katalog_wyjsciowy, f"Porownanie_{kanoniczna_nazwa}_{rok}.pdf")
            generuj_raport_porownawczy(df_roczny, sciezka_pdf, kanoniczna_nazwa, rok)
            profiler.stop(f'Raport {kanoniczna_nazwa} {rok}')
        
        profiler.stop(f'Przetwarzanie {kanoniczna_nazwa}')
    
    profiler.stop('Całkowity czas wykonania')
    profiler.report()
    
    logging.info("\n" + "="*50 + "\nPRZETWARZANIE ZAKOŃCZONE POMYŚLNIE!\n" + "="*50)
