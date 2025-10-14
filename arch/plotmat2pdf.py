import os
import time
import pandas as pd
import numpy as np
import scipy.io as sio  # Biblioteka do wczytywania plików .mat
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_pdf import PdfPages
import logging
from datetime import datetime, timedelta

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- KLASA PROFILERA (taka sama jak w poprzednim skrypcie) ---
class SimpleProfiler:
    def __init__(self):
        self.timings = {}
        self.start_points = {}
    def start(self, name: str):
        self.start_points[name] = time.time()
        logging.info(f"[PROFILER] -> Rozpoczynam pomiar: '{name}'")
    def stop(self, name: str):
        if name in self.start_points:
            duration = time.time() - self.start_points[name]
            self.timings[name] = self.timings.get(name, 0) + duration
            logging.info(f"[PROFILER] <- Zakończono pomiar: '{name}' (Czas: {duration:.4f}s)")
            del self.start_points[name]
    def report(self):
        print("\n" + "="*40 + "\n--- RAPORT CZASU WYKONANIA (PROFILER) ---\n" + "="*40)
        if not self.timings: print("Brak zarejestrowanych pomiarów.")
        else:
            for name, duration in sorted(self.timings.items()): print(f"- {name:<35}: {duration:.4f}s")
        print("="*40)
# -------------------------

def znajdz_i_grupuj_pliki_mat(sciezka_katalogu: str, nazwa_pliku_czasu: str = 'tv.mat') -> dict:
    """
    Przeszukuje katalogi, znajduje pliki .mat i grupuje je na podstawie nadrzędnego katalogu.
    Zakłada, że w każdym podfolderze (np. /2023/, /2024/) znajduje się plik czasu i pliki danych.
    """
    grupy_plikow = {}
    logging.info(f"Rozpoczynam przeszukiwanie katalogu w poszukiwaniu plików .mat: {sciezka_katalogu}")

    for root, _, files in os.walk(sciezka_katalogu):
        if nazwa_pliku_czasu in files:
            try:
                nazwa_grupy = os.path.basename(os.path.dirname(root))
                if not nazwa_grupy:
                    nazwa_grupy = os.path.basename(root)

                sciezka_tv = os.path.join(root, nazwa_pliku_czasu)
                
                pliki_do_ignorowania = [nazwa_pliku_czasu, 'RECORD.mat', 'time_vector.mat']
                
                sciezki_danych = [os.path.join(root, f) for f in files if f.endswith('.mat') and f not in pliki_do_ignorowania]

                if not sciezki_danych:
                    logging.warning(f"W folderze {root} znaleziono plik czasu, ale brak plików danych (po odfiltrowaniu). Pomijam.")
                    continue

                wpis_grupy = {
                    'sciezka_tv': sciezka_tv,
                    'sciezki_danych': sciezki_danych
                }
                
                grupy_plikow.setdefault(nazwa_grupy, []).append(wpis_grupy)

            except Exception as e:
                logging.error(f"Błąd podczas grupowania plików w folderze {root}: {e}")
                
    logging.info(f"Zakończono grupowanie. Znaleziono {len(grupy_plikow)} unikalnych grup.")
    return grupy_plikow

def wczytaj_i_polacz_dane_mat(wpisy_grupy: list) -> pd.DataFrame:
    """Wczytuje dane z plików .mat dla danej grupy i łączy je w jeden DataFrame."""
    lista_df = []
    
    for wpis in sorted(wpisy_grupy, key=lambda x: x['sciezka_tv']):
        try:
            mat_tv = sio.loadmat(wpis['sciezka_tv'])
            klucz_tv = [k for k in mat_tv.keys() if not k.startswith('__')][0]
            wektor_czasu_serial = mat_tv[klucz_tv].flatten()
            wektor_czasu = [datetime.fromordinal(int(t)) + timedelta(days=t % 1) - timedelta(days=366) for t in wektor_czasu_serial]

            dane_z_pliku = {'TIMESTAMP': wektor_czasu}

            for sciezka_danych in wpis['sciezki_danych']:
                nazwa_zmiennej = os.path.splitext(os.path.basename(sciezka_danych))[0]
                mat_dane = sio.loadmat(sciezka_danych)
                if nazwa_zmiennej in mat_dane:
                    dane_wektor = mat_dane[nazwa_zmiennej].flatten()
                    if len(dane_wektor) == len(wektor_czasu):
                        dane_z_pliku[nazwa_zmiennej] = dane_wektor
                    else:
                        logging.warning(f"Niezgodność liczby próbek dla '{nazwa_zmiennej}' w {sciezka_danych} ({len(dane_wektor)} vs {len(wektor_czasu)}). Pomijam zmienną.")
                else:
                    klucze_danych = [k for k in mat_dane.keys() if not k.startswith('__')]
                    if len(klucze_danych) == 1:
                        klucz_danych = klucze_danych[0]
                        dane_wektor = mat_dane[klucz_danych].flatten()
                        if len(dane_wektor) == len(wektor_czasu):
                            dane_z_pliku[klucz_danych] = dane_wektor
                        else:
                            logging.warning(f"Niezgodność liczby próbek dla klucza '{klucz_danych}' w {sciezka_danych}. Pomijam.")

            lista_df.append(pd.DataFrame(dane_z_pliku))
        except Exception as e:
            logging.error(f"Nie udało się przetworzyć danych z {wpis['sciezka_tv']}: {e}")

    if not lista_df:
        return pd.DataFrame()

    df_laczny = pd.concat(lista_df, ignore_index=True)
    df_laczny.set_index('TIMESTAMP', inplace=True)
    df_laczny.sort_index(inplace=True)
    return df_laczny

def generuj_raport_z_df(df: pd.DataFrame, sciezka_pdf: str, nazwa_grupy: str):
    """Generuje raport PDF z danymi zawartymi w DataFrame (logika rysowania jest identyczna)."""
    if df.empty:
        logging.warning(f"Brak danych dla grupy '{nazwa_grupy}' do wygenerowania raportu.")
        return
        
    RZEDY_SIATKI, KOLUMNY_SIATKI = 3, 4
    WYKRESOW_NA_STRONE = RZEDY_SIATKI * KOLUMNY_SIATKI
    ROZMIAR_STRONY_A4_POZIOMO = (11.69, 8.27)
    
    kolumny_numeryczne = [kol for kol in df.columns if 'record' not in str(kol).lower()]

    with PdfPages(sciezka_pdf) as pdf:
        for kolumna in kolumny_numeryczne:
            # Grupujemy przed usunięciem NaN, aby mieć wszystkie dni
            grupy_dni = df[kolumna].groupby(df.index.date)
            lista_dni, licznik_wykresow_globalny = list(grupy_dni), 0

            while licznik_wykresow_globalny < len(lista_dni):
                fig, axes = plt.subplots(nrows=RZEDY_SIATKI, ncols=KOLUMNY_SIATKI, figsize=ROZMIAR_STRONY_A4_POZIOMO, constrained_layout=True)
                fig.suptitle(f"Raport dobowy dla: {nazwa_grupy} - Kolumna: {kolumna}", fontsize=16)
                
                for i in range(WYKRESOW_NA_STRONE):
                    ax = axes.flatten()[i]
                    if licznik_wykresow_globalny < len(lista_dni):
                        data_dnia, grupa_dnia_all = lista_dni[licznik_wykresow_globalny]
                        grupa_dnia = grupa_dnia_all.dropna()
                        
                        ax.set_title(f'Dzień: {data_dnia.strftime("%Y-%m-%d")}', fontsize=9)
                        
                        # ZMIANA: Ustawienie sztywnych granic osi X na pełną dobę
                        dzien_start = datetime.combine(data_dnia, datetime.min.time())
                        dzien_koniec = datetime.combine(data_dnia, datetime.max.time())
                        ax.set_xlim(dzien_start, dzien_koniec)
                        
                        ax.grid(True, which='major', linestyle='-')
                        ax.grid(True, which='minor', linestyle=':', alpha=0.6)

                        # ZMIANA: Warunek sprawdzający, czy są dane do narysowania
                        if not grupa_dnia.empty:
                            ax.plot(grupa_dnia.index, grupa_dnia.values)
                        else:
                            # Jeśli nie ma danych, wyświetl komunikat
                            ax.text(0.5, 0.5, "Brak danych", ha='center', va='center', transform=ax.transAxes, fontsize=12, color='red')
                        
                        ax.xaxis.set_major_locator(mdates.HourLocator(byhour=[0, 6, 12, 18]))
                        ax.xaxis.set_minor_locator(mdates.HourLocator(byhour=range(0, 24, 3)))
                        
                        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                        
                        plt.setp(ax.get_xticklabels(), rotation=45, ha="right", fontsize=7)
                        plt.setp(ax.get_yticklabels(), fontsize=7)
                    else:
                        ax.set_visible(False)
                    licznik_wykresow_globalny += 1
                
                pdf.savefig(fig)
                plt.close(fig)

if __name__ == '__main__':
    profiler = SimpleProfiler()
    profiler.start('Całkowity czas wykonania')

    # # --- KONFIGURACJA ---
    # sciezka_wejsciowa = 'D:\\sites\\SA\\met-data_SA' 
    # katalog_wyjsciowy = 'E:\\pdfy\\SApdfPlot_matlab'
    # # --------------------
    # --- KONFIGURACJA ---
    sciezka_wejsciowa = 'D:\\sites\\ME\\met-data_ME' 
    katalog_wyjsciowy = 'E:\\pdfy\\MEpdfPlot_matlab'
    # --------------------

    if not os.path.isdir(sciezka_wejsciowa):
        logging.error(f"Katalog wejściowy nie istnieje: {sciezka_wejsciowa}")
    else:
        os.makedirs(katalog_wyjsciowy, exist_ok=True)
        
        profiler.start('1. Wyszukiwanie i grupowanie plików .mat')
        pogrupowane_pliki = znajdz_i_grupuj_pliki_mat(sciezka_wejsciowa)
        profiler.stop('1. Wyszukiwanie i grupowanie plików .mat')

        for nazwa_grupy, wpisy_grupy in pogrupowane_pliki.items():
            profiler.start(f'2. Przetwarzanie grupy: {nazwa_grupy}')
            
            df_danych = wczytaj_i_polacz_dane_mat(wpisy_grupy)
            
            nazwa_pliku_pdf = f"raport_matlab_{nazwa_grupy}.pdf"
            sciezka_do_pdf = os.path.join(katalog_wyjsciowy, nazwa_pliku_pdf)
            
            generuj_raport_z_df(df_danych, sciezka_do_pdf, nazwa_grupy)
            
            profiler.stop(f'2. Przetwarzanie grupy: {nazwa_grupy}')
            logging.info(f"Zakończono raport dla grupy '{nazwa_grupy}'. Plik zapisano w: {sciezka_do_pdf}")

    profiler.stop('Całkowity czas wykonania')
    profiler.report()