import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates # Dodano import do formatowania dat
from matplotlib.backends.backend_pdf import PdfPages
import logging

# Konfiguracja logowania, aby widzieć postępy i ewentualne błędy
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def znajdz_pliki_csv(sciezka_katalogu: str) -> list:
    """Rekurencyjnie przeszukuje podany katalog i zwraca listę ścieżek do wszystkich plików .csv."""
    pliki_csv = []
    logging.info(f"Rozpoczynam przeszukiwanie katalogu: {sciezka_katalogu}")
    for root, _, files in os.walk(sciezka_katalogu):
        for file in files:
            if file.endswith('.csv'):
                pelna_sciezka = os.path.join(root, file)
                pliki_csv.append(pelna_sciezka)
    logging.info(f"Zakończono przeszukiwanie. Znaleziono {len(pliki_csv)} plików CSV.")
    return pliki_csv

def grupuj_pliki_po_nazwie(lista_plikow_csv: list) -> dict:
    """Grupuje listę ścieżek do plików CSV na podstawie ich nazwy bazowej (bez rozszerzenia)."""
    grupy_plikow = {}
    for sciezka_pliku in lista_plikow_csv:
        nazwa_pliku_z_rozszerzeniem = os.path.basename(sciezka_pliku)
        nazwa_grupy = os.path.splitext(nazwa_pliku_z_rozszerzeniem)[0]
        grupy_plikow.setdefault(nazwa_grupy, []).append(sciezka_pliku)
    logging.info(f"Pogrupowano pliki w {len(grupy_plikow)} unikalnych grup.")
    return grupy_plikow

def generuj_raport_dla_grupy(lista_plikow_w_grupie: list, sciezka_pdf: str, kolumna_czasu: str = 'TIMESTAMP'):
    """
    Łączy dane z grupy, a następnie tworzy raport PDF z siatką wykresów (12 na stronę),
    grupując strony według kolumn danych.
    """
    # --- Konfiguracja siatki wykresów ---
    RZEDY_SIATKI = 3
    KOLUMNY_SIATKI = 4
    WYKRESOW_NA_STRONE = RZEDY_SIATKI * KOLUMNY_SIATKI
    # Rozmiar A4 w calach (poziomo)
    ROZMIAR_STRONY_A4_POZIOMO = (11.69, 8.27)
    # ------------------------------------

    # Krok 1: Łączenie wszystkich plików z grupy w jeden DataFrame
    logging.info("  -> Łączenie plików w jedną tabelę...")
    lista_df = []
    for sciezka_pliku in sorted(lista_plikow_w_grupie):
        try:
            df_temp = pd.read_csv(sciezka_pliku)
            if kolumna_czasu in df_temp.columns:
                lista_df.append(df_temp)
            else:
                logging.warning(f"    W pliku {sciezka_pliku} brakuje kolumny '{kolumna_czasu}'. Pomijam.")
        except Exception as e:
            logging.error(f"    Nie udało się wczytać pliku {sciezka_pliku}: {e}")
    
    if not lista_df:
        logging.error("  -> Nie wczytano żadnych danych dla tej grupy. Przechodzę do następnej.")
        return

    df_laczny = pd.concat(lista_df, ignore_index=True)
    
    # Krok 2: Przygotowanie danych (konwersja czasu, sortowanie)
    df_laczny[kolumna_czasu] = pd.to_datetime(df_laczny[kolumna_czasu], errors='coerce')
    df_laczny.dropna(subset=[kolumna_czasu], inplace=True)
    df_laczny.set_index(kolumna_czasu, inplace=True)
    df_laczny.sort_index(inplace=True) 
    
    kolumny_numeryczne_wszystkie = df_laczny.select_dtypes(include=['number']).columns
    kolumny_numeryczne = [kol for kol in kolumny_numeryczne_wszystkie if 'record' not in kol.lower()]
    
    logging.info(f"  -> Znaleziono następujące kolumny do narysowania (po odfiltrowaniu 'Record'): {kolumny_numeryczne}")

    with PdfPages(sciezka_pdf) as pdf:
        # Krok 3: Główna pętla po każdej kolumnie numerycznej
        for kolumna in kolumny_numeryczne:
            logging.info(f"    -> Generowanie wykresów dla kolumny: '{kolumna}'")
            dane_kolumny = df_laczny[kolumna].dropna()
            if dane_kolumny.empty:
                logging.warning(f"    Kolumna '{kolumna}' nie zawiera danych do narysowania. Pomijam.")
                continue

            grupy_dni = dane_kolumny.groupby(dane_kolumny.index.date)
            
            lista_dni = list(grupy_dni)
            licznik_wykresow_globalny = 0

            while licznik_wykresow_globalny < len(lista_dni):
                fig, axes = plt.subplots(
                    nrows=RZEDY_SIATKI, 
                    ncols=KOLUMNY_SIATKI, 
                    figsize=ROZMIAR_STRONY_A4_POZIOMO
                )
                fig.suptitle(f"Raport dobowy dla kolumny: {kolumna}", fontsize=16)
                
                for i in range(WYKRESOW_NA_STRONE):
                    idx_ax = i % KOLUMNY_SIATKI
                    idy_ax = i // KOLUMNY_SIATKI
                    ax = axes[idy_ax, idx_ax]

                    if licznik_wykresow_globalny < len(lista_dni):
                        data, grupa_dnia = lista_dni[licznik_wykresow_globalny]
                        
                        ax.plot(grupa_dnia.index, grupa_dnia.values)
                        ax.set_title(f'Dzień: {data.strftime("%Y-%m-%d")}', fontsize=9)
                        ax.grid(True)
                        
                        # ZMIANA: Ustaw główne znaczniki na osi X co 3 godziny
                        ax.xaxis.set_major_locator(mdates.HourLocator(interval=3))
                        # Sformatuj oś X, aby pokazywała tylko godzinę i minutę
                        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                        
                        for label in ax.get_xticklabels():
                            label.set_rotation(45)
                            label.set_fontsize(7)
                        for label in ax.get_yticklabels():
                            label.set_fontsize(7)
                    else:
                        ax.set_visible(False)
                    
                    licznik_wykresow_globalny += 1
                
                plt.tight_layout(rect=[0, 0, 1, 0.96])
                pdf.savefig(fig)
                plt.close(fig)

if __name__ == '__main__':
    # --- KONFIGURACJA ---
    # Podaj ścieżkę do katalogu, który chcesz przeszukać
    sciezka_wejsciowa = 'E:\\test_split\\SA' 
    
    # Podaj ścieżkę do katalogu, gdzie zapiszą się gotowe raporty PDF
    katalog_wyjsciowy = 'E:\\pdfy'
    # --------------------

    if not os.path.isdir(sciezka_wejsciowa):
        logging.error(f"Katalog wejściowy nie istnieje: {sciezka_wejsciowa}")
    else:
        os.makedirs(katalog_wyjsciowy, exist_ok=True)
        wszystkie_pliki = znajdz_pliki_csv(sciezka_wejsciowa)

        if not wszystkie_pliki:
            logging.warning("Nie znaleziono żadnych plików CSV w podanej lokalizacji.")
        else:
            pogrupowane_pliki = grupuj_pliki_po_nazwie(wszystkie_pliki)

            for nazwa_grupy, lista_plikow_w_grupie in pogrupowane_pliki.items():
                logging.info(f"Rozpoczynam generowanie raportu dla grupy: '{nazwa_grupy}'")
                nazwa_pliku_pdf = f"raport_{nazwa_grupy}.pdf"
                sciezka_do_pdf = os.path.join(katalog_wyjsciowy, nazwa_pliku_pdf)
                
                generuj_raport_dla_grupy(lista_plikow_w_grupie, sciezka_do_pdf, kolumna_czasu='TIMESTAMP')
                
                logging.info(f"Zakończono raport dla grupy '{nazwa_grupy}'. Plik zapisano w: {sciezka_do_pdf}")

            logging.info("Wszystkie raporty zostały wygenerowane.")