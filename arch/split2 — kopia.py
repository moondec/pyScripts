# -*- coding: utf-8 -*-

"""
================================================================================
                    Skrypt do Agregacji Danych Pomiarowych
================================================================================

Opis:
    Ten skrypt został zaprojektowany do automatyzacji procesu przetwarzania,
    czyszczenia i agregacji danych szeregów czasowych pochodzących z różnych
    systemów rejestrujących, w szczególności z urządzeń firmy Campbell Scientific.
    Głównym celem jest połączenie danych z wielu plików źródłowych (o różnych
    formatach) w spójne, roczne pliki wynikowe w formacie CSV.

    Skrypt jest wysoce konfigurowalny i potrafi radzić sobie z wieloma
    typowymi problemami występującymi w danych pomiarowych, takimi jak
    błędy formatu, uszkodzone pliki, niespójności w strefach czasowych
    (czas letni/zimowy) oraz niestandardowe formaty danych.

Główne funkcjonalności:
    - Obsługa wielu formatów wejściowych: TOB1 (binarny z nagłówkiem ASCII),
      TOA5 (tekstowy z nagłówkiem ASCII) oraz prosty format CSV.
    - Dekodowanie specjalistycznych formatów binarnych, w tym FP2 ('csiFs2ToFloat').
    - Elastyczne przetwarzanie interwałów czasowych:
        - Automatyczna detekcja interwału na podstawie próbki danych.
        - Możliwość wymuszenia konkretnego interwału (np. '30min', '5S').
    - Zaawansowane zarządzanie strefami czasowymi:
        - Ujednolicanie danych do wspólnej strefy czasowej (docelowo UTC+1).
        - Możliwość zdefiniowania reguł korekty dla danych sprzed i po
          zmianie ustawień rejestratora (np. przejście z CEST na UTC).
        - Możliwość zdefiniowania ręcznych, okresowych przesunięć czasu
          (np. dodanie/odjęcie godziny w konkretnym przedziale dat).
    - Agregacja i scalanie plików:
        - Możliwość zdefiniowania grup, które łączą dane z wielu plików
          o różnych identyfikatorach w jeden wspólny plik wynikowy.
    - Wydajność i inteligencja:
        - Zoptymalizowany, jednofazowy tryb działania, gdy interwał jest znany.
        - Mechanizm cache (.splitdata), który zapamiętuje stan przetworzonych
          plików (na podstawie rozmiaru i daty modyfikacji) i pomija je
          przy kolejnych uruchomieniach, znacznie przyspieszając pracę.
        - Opcja --overwrite do świadomego ponownego przetworzenia plików.
    - Diagnostyka i raportowanie błędów:
        - Wykrywanie i raportowanie uszkodzonych rekordów w plikach binarnych.
        - Wykrywanie, raportowanie i automatyczne korygowanie błędów
          chronologii w plikach tekstowych.
        - Tworzenie trwałego logu błędów chronologii w pliku .chronoerror.

Wymagania:
    - Python 3.8+
    - Biblioteka pandas: Należy ją zainstalować za pomocą polecenia:
      pip install pandas

Konfiguracja:
    Skrypt jest konfigurowany za pomocą trzech słowników na początku kodu:
    1. FILE_ID_MERGE_GROUPS:
       - Cel: Definiowanie grup plików, które mają być scalone w jeden plik wynikowy.
       - Struktura: Klucz to nazwa grupy (używana w opcji --file_id), a wartość
         to słownik zawierający listę identyfikatorów źródłowych ('source_ids')
         oraz sztywno zdefiniowany interwał ('interval') dla tej grupy.
       - Przykład:
         'TU_STATION_COMBINED': {
             'source_ids': ['CR5000_flux', 'CR5000_meteo'],
             'interval': '30min'
         }

    2. TIMEZONE_CORRECTIONS:
       - Cel: Definiowanie reguł korekty dla danych z problemami ze strefą
         czasową (np. czas letni/zimowy).
       - Struktura: Kluczem jest `file_id` lub nazwa grupy. Wartość może być
         aliasem (stringiem wskazującym na inną konfigurację) lub słownikiem
         z parametrami: 'source_tz', 'correction_end_date', 'post_correction_tz',
         'target_tz'.

    3. MANUAL_TIME_SHIFTS:
       - Cel: Definiowanie ręcznych przesunięć czasowych dla konkretnych okresów.
       - Struktura: Kluczem jest `file_id` lub nazwa grupy. Wartością jest lista
         słowników-reguł, gdzie każda reguła zawiera 'start', 'end' i 'offset_hours'.

Uruchamianie:
    Skrypt należy uruchamiać z wiersza poleceń (Terminal na macOS/Linux, CMD/PowerShell na Windows).

    Składnia podstawowa:
    python nazwa_skryptu.py -i <katalog_wejsciowy> -o <katalog_wyjsciowy> [opcje]

    Argumenty:
      -i, --input_dir       (Wymagany) Ścieżka do katalogu z danymi wejściowymi.
      -o, --output_dir      (Wymagany) Ścieżka do nadrzędnego katalogu, gdzie
                            będą tworzone pliki wynikowe.
      -fid, --file_id       (Opcjonalny) Filtr nazw plików i identyfikator. Może
                            być to ciąg znaków (np. 'CR5000_flux') lub nazwa grupy
                            zdefiniowanej w FILE_ID_MERGE_GROUPS.
      -int, --interval      (Opcjonalny) Wymusza określony interwał przetwarzania
                            (np. '30min', '1H', '5S'). Jest ignorowany, jeśli
                            dla danej grupy zdefiniowano interwał w słowniku.
      -ow, --overwrite      (Opcjonalny) Flaga, która wymusza ponowne przetworzenie
                            wszystkich plików pasujących do kryteriów, ignorując
                            historię zapisaną w pliku .splitdata.

    Przykłady użycia:
      # 1. Przetworzenie wszystkich plików z katalogu z automatyczną detekcją interwału
      python nazwa_skryptu.py -i /dane/stacja_rzecin -o /wyniki/rzecin

      # 2. Przetworzenie zdefiniowanej grupy 'TU_STATION_COMBINED' (z interwałem '30min' z definicji)
      python nazwa_skryptu.py -i /dane/stacja_tuczno -o /wyniki/tuczno -fid TU_STATION_COMBINED

      # 3. Przetworzenie tylko plików z 'CR1000_LI840' z wymuszonym interwałem 5-sekundowym i nadpisaniem cache
      python nazwa_skryptu.py -i /dane/stacja_tuczno -o /wyniki/tuczno -fid CR1000_LI840 -int 5S -ow

Struktura Plików Wynikowych:
    - Skrypt tworzy w katalogu wyjściowym podkatalogi dla każdego roku (np. '2011', '2012').
    - Pliki wynikowe CSV są zapisywane w odpowiednich katalogach rocznych.
    - Nazwa pliku jest tworzona wg schematu:
      <identyfikator>_<interwał>_<ROK>.csv
      np. TU_STATION_COMBINED_30min_2011.csv

Pliki Pomocnicze i Logi:
    W katalogu, w którym znajduje się skrypt, tworzone są dwa pliki z kropką na początku:
    - .splitdata: Plik JSON przechowujący cache (historię) przetworzonych plików.
      Pozwala na znaczne przyspieszenie kolejnych uruchomień.
    - .chronoerror: Plik tekstowy, do którego dopisywane są logi o wykrytych
      błędach chronologii w plikach źródłowych.

--------------------------------------------------------------------------------
    Autor: Marek Urbaniak
    Skrypt powstał w wyniku współpracy z Pomocnikiem w programowaniu Gemini.
    Wersja: 4.0
    Data ostatniej modyfikacji: 24.06.2025
--------------------------------------------------------------------------------
"""

import os
import pandas as pd
import struct
import re
import argparse
import math
import json
from datetime import datetime, timedelta
from collections import defaultdict

# --- Globalne definicje i słowniki konfiguracyjne ---
CAMPBELL_EPOCH = pd.Timestamp('1990-01-01 00:00:00', tz='UTC')
STRUCT_FORMAT_MAP = {'ULONG':'L', 'IEEE4':'f', 'IEEE8':'d', 'LONG':'l', 'BOOL':'?', 'SHORT':'h', 'USHORT':'H', 'BYTE':'b'}
CACHE_FILENAME = ".splitdata"
CACHE_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), CACHE_FILENAME)
CHRONO_ERROR_FILENAME = ".chronoerror"
CHRONO_ERROR_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), CHRONO_ERROR_FILENAME)

# === NOWY, ZAAWANSOWANY SŁOWNIK DO SCALANIA GRUP FILE_ID ===
FILE_ID_MERGE_GROUPS = {
    # Rzecin
    'RZ_MET30min': { 'source_ids': ['Meteo_32', 'meteo_Meteo_32', 'Meteo_30', 'CR1000_Meteo_30', '_LiCor'], 'interval': '30min' },
    'RZ_MET1min': { 'source_ids': ['sky_SKYE_1min_', 'CR1000_2_meteo_SKYE_1min', 'CR1000_sky_SKYE_1min', '3_SKYE', 'sky_SKYE', 'CR1000_2_meteo_Meteo_1', 'CR1000_Meteo_1', 'CR1000_2_meteo_Meteo_2', 'CR1000_2_Meteo_2', 'CR1000_Methan_1', 'CR1000_Methan_2', 'Parsival', 'NetRadiometers', 'SWD'], 'interval': '1min' },
    'RZ_MET30sec': { 'source_ids': ['CR1000_wet_RadTaRH', 'CR1000_wet_TEMP_PRT', 'CR1000_wet_Ts_P'], 'interval': '30S' }, # Zmieniono '30sec' na '30S' dla spójności
    # Tuczno
    'TU_MET30min': { 'source_ids': ['CR5000_flux', 'Rad_tu', '_Bole_temp', '_meteo_Spec_idx', '_meteo_EneBal', '_meteo_Prec_Top', 'CR200Series_Table1', '_meteo_WXTmet', '_Results', '_profil_comp_integ', '_soil_Temperatury', '_soil_Soil_HFP01', '_garden_RainGauge', '_garden_T107_gar'], 'interval': '30min' },
    'TU_MET10min': { 'source_ids': ['_garden_CS_616', '_studnia_1_CS616S1'], 'interval': '10min' },
    'TU_MET2min': { 'source_ids': ['CR1000_LI840_stor'], 'interval': '2min' },
    'TU_MET1min': { 'source_ids': ['_Rad_1min'], 'interval': '1min' },
    'TU_MET30sec': { 'source_ids': ['CR1000_soil2_SoTemS13'], 'interval': '30S' }, # Zmieniono '30sec' na '30S'
    'TU_MET5sec': { 'source_ids': ['_soil_PPFD_under_tree', 'CR1000_meteo_GlobalRad', '_soil_Temperatury_5', 'CR1000_meteo_GlobalRad'], 'interval': '5S' }, # Zmieniono '5sec' na '5S'
    'TU_MET1sec': { 'source_ids': ['CR1000_profil_LI840', 'CR1000_LI840', 'CR1000_soil2_LI840'], 'interval': '1S' }, # Zmieniono '1sec' na '1S'
    # Brody
    'BR_MET30min': { 'source_ids': ['CR3000_Barometr', 'CR3000_CR3000', 'CR3000_Multiplekser_dat', 'CR3000_Rain', 'CR3000_Spec_Veg_Ind'], 'interval': '30min' },
    'BR_MET1min': { 'source_ids': ['CR3000_SpectralData'], 'interval': '1min' },
    # Tlen1
    'TL1_MET30min': { 'source_ids': [ 'Rad_TR_30min', 'Soil_TR_30min' ], 'interval': '30min' },
    'TL1_MET1min': { 'source_ids': [ 'Rad_TR_1min' ], 'interval': '1min' },
    # Tlen1a
    'TL1a_MET30min': { 'source_ids': [ 'pom30m_', 'Tlen_1a_Biomet', 'Tlen_1a_cnf4_data', '_Soil_30min', 'Tlen1A_biomet_Biomet', 'Tlen1A_biomet_cnf4_data'], 'interval': '30min' },
    'TL1a_PREC30min': { 'source_ids': [ 'deszcz_d_'], 'interval': '30min' },
    'TL1a_MET1min': { 'source_ids': [ 'pom1m_', 'Tlen1A_biomet_Meteo_1min' ], 'interval': '1min' },
    'TL1a_CAL1min': { 'source_ids': [ 'plytki_calib_' ], 'interval': '1min' },
    # Tlen2
    'TL2_MET30min': { 'source_ids': [ 'pom30m_', 'TL2_30min', 'Tlen2_biomet_Biomet', 'Tlen_2_Soil_TL2_30min' , 'Tlen2_biomet_cnf4_data', 'Tlen2_biomet_Soil_30min'], 'interval': '30min' },
    'TL2_MET1min': { 'source_ids': [ 'pom1m_', 'Tlen_2_Soil_moist_1min', 'Tlen2_biomet_Meteo_1min' ], 'interval': '1min' },
    'TL2_CAL1min': { 'source_ids': [ 'plytki_calib_' ], 'interval': '1min' },
    # Mezyk
    'ME_TOP_MET_30min': { 'source_ids': [ 'pom30m_'], 'interval': '30min' },
    'ME_DOWN_MET_30min': { 'source_ids': [ 'pom30m_'], 'interval': '30min' },
    'Rain_down': { 'source_ids': [ 'deszcz_d_'], 'interval': '30min' },
    'Rain_top': { 'source_ids': [ 'deszcz_' ], 'interval': '30min' },
    'ME_MET_10m': { 'source_ids': [ 'pom10m_' ], 'interval': '10min' },
    'ME_TOP_MET_1min': { 'source_ids': [ 'pom1m_' ], 'interval': '1min' },
    'ME_DOWN_MET_1min': { 'source_ids': [ 'pom1m_' ], 'interval': '1min' },
    'CalPlates': { 'source_ids': [ 'plytki_calib_' ], 'interval': '1min' },
    # Sarbia
    'SA_MET_30min': { 'source_ids': [ 'SA_biomet_Biomet', '_cnf4_data'], 'interval': '30min' },
    'SA_MET_1min': { 'source_ids': [ 'SA_biomet_Meteo_1min'], 'interval': '1min' }
}

# TIMEZONE_CORRECTIONS = {
#     'STACJA_TU_COMMON': { 'source_tz':'Europe/Warsaw', 'correction_end_date':'2011-05-27 09:00:00', 'post_correction_tz':'UTC', 'target_tz':'Etc/GMT-1'},
#     'TU_MET30min': 'STACJA_TU_COMMON','TU_MET10min': 'STACJA_TU_COMMON','TU_MET1min': 'STACJA_TU_COMMON',
#     'TU_MET2min': 'STACJA_TU_COMMON', 'TU_MET30sec': 'STACJA_TU_COMMON', 'TU_MET5sec': 'STACJA_TU_COMMON', 'TU_MET1sec': 'STACJA_TU_COMMON',
#     'INNA_STACJA_ID': { 'source_tz':'Europe/Warsaw', 'correction_end_date':'2020-05-10 12:00:00', 'post_correction_tz':'UTC', 'target_tz':'Etc/GMT-1' }
# }

TIMEZONE_CORRECTIONS = {
    # 1. Definicja "konfiguracji-matki" dla stacji TU
    'STACJA_TU_COMMON': { 
        'source_tz': 'Europe/Warsaw', # Nazwa strefy czasowej, w której rejestrator zapisywał dane przed datą poprawki (np. 'Europe/Warsaw'). Używanie nazw z bazy IANA (jak Europe/Warsaw) jest kluczowe, ponieważ automatycznie obsługują one zarówno czas zimowy (CET, UTC+1), jak i letni (CEST, UTC+2).
        'correction_end_date': '2011-05-27 09:00:00', # Data i godzina, po której dane są już zapisywane poprawnie. Skrypt zastosuje specjalną korektę tylko do danych z timestampami wcześniejszymi lub równymi tej dacie.
        'post_correction_tz': 'UTC', # Strefa czasowa "poprawnych" danych (tych po correction_end_date). 
        'target_tz': 'Etc/GMT-1' # Docelowa, jednolita strefa czasowa dla wszystkich danych w plikach wynikowych. ("UTC/GMT +1 godzina"), użyjemy 'Etc/GMT-1'. (Uwaga: notacja Etc/GMT ma odwrócony znak, więc Etc/GMT-1 oznacza UTC+1).
    },
    
   # 2. Poszczególne file_id, które wskazują na wspólną konfigurację
    'TU_MET30min': 'STACJA_TU_COMMON',
    'TU_MET10min': 'STACJA_TU_COMMON',
    'TU_MET1min': 'STACJA_TU_COMMON',
    'TU_MET2min': 'STACJA_TU_COMMON',
    'TU_MET30sec': 'STACJA_TU_COMMON',
    'TU_MET5sec': 'STACJA_TU_COMMON',
    'TU_MET1sec': 'STACJA_TU_COMMON',

    # # 3. Definicja "konfiguracji-matki" dla stacji TL1 (oryginalne dane są w czasie zimowym górowanie słońca o ok. 12:00, brak konieczności systemowej zmiany czasu zima/lato)
    # 'STACJA_TL1_COMMON': {
        # 'source_tz': 'Europe/Warsaw',
        # 'correction_end_date': '2020-05-10 12:00:00',
        # 'post_correction_tz': 'Etc/GMT-1',
        # 'target_tz': 'Etc/GMT-1'
    # },
    
    # 'TL1_MET30min' : 'STACJA_TL1_COMMON',
    # 'TL1_MET1min' : 'STACJA_TL1_COMMON',
    
    # 4. Definicja "konfiguracji-matki" dla stacji TL1a 
    'STACJA_TL1a_COMMON': {
        'source_tz': 'Europe/Warsaw',
        'correction_end_date': '2050-05-10 12:00:00', # znajdź datę po której jest CET
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    
    'TL1a_MET30min' : 'STACJA_TL1a_COMMON',
    'TL1a_MET1min' : 'STACJA_TL1a_COMMON',
    'TL1a_PREC30min' : 'STACJA_TL1a_COMMON',
    'TL1a_CAL1min' : 'STACJA_TL1a_COMMON'
    
    # # 5. Definicja "konfiguracji-matki" dla stacji TL2 (oryginalne dane są w czasie zimowym górowanie słońca o ok. 12:00, brak konieczności systemowej zmiany czasu zima/lato)
    # 'STACJA_TL2_COMMON': {
        # 'source_tz': 'Europe/Warsaw',
        # 'correction_end_date': '2050-05-10 12:00:00',
        # 'post_correction_tz': 'Etc/GMT-1',
        # 'target_tz': 'Etc/GMT-1'
    # },   
    
    # 'TL2_MET30min' : 'STACJA_TL2_COMMON',
    # 'TL2_MET1min' : 'STACJA_TL2_COMMON',
    # 'TL2_CAL1min' : 'STACJA_TL2_COMMON',
    
    # # 6. Definicja "konfiguracji-matki" dla stacji ME (oryginalne dane są w czasie zimowym górowanie słońca o ok. 12:00, brak konieczności systemowej zmiany czasu zima/lato)
    # 'STACJA_ME_COMMON': {
        # 'source_tz': 'Europe/Warsaw',
        # 'correction_end_date': '2050-05-10 12:00:00',
        # 'post_correction_tz': 'Etc/GMT-1',
        # 'target_tz': 'Etc/GMT-1'
    # },
    
     # 'STACJA_ME_COMMON' : 'ME_TOP_MET_30min',   
     # 'STACJA_ME_COMMON' : 'ME_DOWN_MET_30min',
     # 'STACJA_ME_COMMON' : 'Rain_down',
     # 'STACJA_ME_COMMON' : 'ME_DOWN_MET_1min',
     # 'STACJA_ME_COMMON' : 'ME_DOWN_MET_30min',
     # 'STACJA_ME_COMMON' : 'Rain_top',
     # 'STACJA_ME_COMMON' : 'CalPlates',
     # 'STACJA_ME_COMMON' : 'ME_MET_10m',
    
     # # 7. Definicja "konfiguracji-matki" dla stacji SA (oryginalne dane są w czasie zimowym górowanie słońca o ok. 12:00, brak konieczności systemowej zmiany czasu zima/lato)
    # 'STACJA_SA_COMMON': {
        # 'source_tz': 'Europe/Warsaw',
        # 'correction_end_date': '2022-01-14 11:00:00',
        # 'post_correction_tz': 'Etc/GMT-1',
        # 'target_tz': 'Etc/GMT-1'
    # },
    
     # 'STACJA_SA_COMMON' : 'SA_MET_30min',   
     # 'STACJA_SA_COMMON' : 'SA_MET_1min'
}

# === POPRAWIONA SKŁADNIA SŁOWNIKA ===
MANUAL_TIME_SHIFTS = {
    # --- GRUPA TUCZNO ---
    'STACJA_TU_COMMON': [
        {
            'start': '2008-01-01 00:00:00',
            'end': '2008-03-31 00:00:00',
            'offset_hours': 1
        },
        {
            'start': '2011-03-27 00:00:00',
            'end': '2011-05-27 09:00:00',
            'offset_hours': 1
        },
    ],
    'TU_MET30min': 'STACJA_TU_COMMON',
    'TU_MET10min': 'STACJA_TU_COMMON',
    'TU_MET1min':  'STACJA_TU_COMMON', 
    'TU_MET2min':  'STACJA_TU_COMMON',
    'TU_MET30sec': 'STACJA_TU_COMMON',
    'TU_MET5sec':  'STACJA_TU_COMMON',
    'TU_MET1sec':  'STACJA_TU_COMMON',
    
    # # --- GRUPA TL1a ---
#     'STACJA_TL1a_COMMON': [
#         {
#             'start': '2025-06-01 00:00:00',
#             'end': '2025-06-18 21:30:00',
#             'offset_hours': -1
#         },
#     'TL1a_MET30min' : 'STACJA_TL1a_COMMON',
#     'TL1a_MET1min' : 'STACJA_TL1a_COMMON',
#     
#     # --- GRUPA TL2 ---
#     'STACJA_TL2_COMMON': [
#         {
#             'start': '2025-06-01 00:00:00',
#             'end': '2025-06-18 21:30:00',
#             'offset_hours': -1
#         },
#     'TL2_MET30min' : 'STACJA_TL2_COMMON',
#     'TL2_MET1min' : 'STACJA_TL2_COMMON',
#     
# --- GRUPA SARBIA ---
    # Poprawna struktura: najpierw definicja reguł, potem osobne definicje aliasów
    # 'STACJA_SA_COMMON': [
        # {
            # 'start': '2021-06-01 00:00:00',
            # 'end': '2055-06-18 21:30:00',
            # 'offset_hours': -1
        # },
    # ],
    # 'SA_MET_30min': 'STACJA_SA_COMMON',
    # 'SA_MET_1min': 'STACJA_SA_COMMON',
    
#     # --- GRUPA TL1_OLD ---
#     'TL_OLD_STATION_COMBINED': [ # Zastosować do całych grup
#     # ------- pojedyncze przesunięcie w czasie 1
#         {
#             'start': '2013-01-01 00:00:00',
#             'end': '2018-06-14 02:30:00',
#             'offset_hours': -1  # W tym okresie ODEJMIJ 1 godzinę w wektorze czasu danych EC
#         }, 
#     # ------- koniec przesunięcia 1
#     # ------- pojedyncze przesunięcie w czasie 2
#         {
#             'start': '2018-06-14 02:30:00',
#             'end': '2018-06-19 10:00:00',
#             'offset_hours': 1 # W tym okresie DODAJ 1 godzinę
#         }, 
#     # ------- koniec przesunięcia 2
#     
#     # ------- pojedyncze przesunięcie w czasie 3
#         {
#             'start': '2018-06-19 10:30:00',
#             'end': '2019-08-23 09:00:00',
#             'offset_hours': 0 # W tym okresie NIC NIE RÓB!
#         },
#     # ------- koniec przesunięcia 3
# 
# # ------- pojedyncze przesunięcie w czasie 4
#         {
#             'start': '2019-08-23 09:30:00',
#             'end': '2019-12-31 23:59:00',
#             'offset_hours': -2 # W tym okresie ODEJMIJ 2 godzinY
#         },
#     # ------- koniec przesunięcia 4
# 
#    # ------- pojedyncze przesunięcie w czasie 5
#         {
#             'start': '2019-12-31 23:59:00',
#             'end': NOW,
#             'offset_hours': -2 # W tym okresie ODEJMIJ 2 godzinY
#         }
#     # ------- koniec przesunięcia 5      
#         
#     ],
#     'TL1_MET30min' : 'TL_OLD_STATION_COMBINED',
#     'TL1_MET1min' : 'TL_OLD_STATION_COMBINED',
#    # --- GRUPA ME_TOP ---
#     # 1. Definicja reguł dla grupy
#     'ME_TOP_STATION_COMBINED': [
#         {
#             'start': '2021-04-05 16:24:00',
#             'end': '2021-12-06 12:30:00',
#             'offset_hours': -1
#         },
#         {
#             'start': '2014-01-20 05:00:00',
#             'end': '2014-02-18 12:00:00',
#             'offset_hours': -1
#         },
#     ],
#    # 2. Definicja aliasów wskazujących na grupę (jako osobne wpisy w głównym słowniku)
#    'ME_TOP_MET_30min' : 'ME_TOP_STATION_COMBINED',
#    'ME_TOP_MET_1min' : 'ME_TOP_STATION_COMBINED',
#    'Rain_top' : 'ME_TOP_STATION_COMBINED',

    # --- GRUPA ME_DOWN ---
    # 1. Definicja reguł dla grupy
    # 'ME_DOWN_STATION_COMBINED': [
        # {
            # "start": "2018-05-15",
            # "end": "2018-07-05",
            # "offset_hours": -1
        # },
        # {
            # "start": "2018-06-21",
            # "end": "2018-06-21",
            # "offset_hours": 3
        # },
        # {
            # "start": "2018-07-06",
            # "end": "2018-07-06",
            # "offset_hours": 2
        # },
        # {
            # "start": "2018-07-07",
            # "end": "2018-12-31",
            # "offset_hours": -1
        # },
        # {
            # "start": "2018-07-26",
            # "end": "2018-07-26",
            # "offset_hours": 3
        # },
        # {
            # "start": "2019-07-05 02:30:00",
            # "end": "2019-07-08 22:20:00",
            # "offset_hours": 6.25
        # },
        # {
            # "start": "2019-11-23 06:25:00",
            # "end": "2019-11-28 08:59:00",
            # "offset_hours": 5.68
        # },
        # {
            # "start": "2019-07-09",
            # "end": "2019-07-09",
            # "offset_hours": 2
        # },
        # {
            # "start": "2019-07-17",
            # "end": "2019-07-17",
            # "offset_hours": 1
        # },
        # {
            # "start": "2019-01-01",
            # "end": "2019-12-10",
            # "offset_hours": -1
        # },
        # {
            # "start": "2019-12-11",
            # "end": "2020-01-05",
            # "offset_hours": 2
        # },
        # {
            # "start": "2020-01-06",
            # "end": "2020-01-12",
            # "offset_hours": -1
        # },
        # {
            # "start": "2020-01-13",
            # "end": "2020-01-19",
            # "offset_hours": -1
        # },
        # {
            # 'start': '2024-08-07 01:00:00',
            # 'end': '2024-08-08 02:00:00',
            # 'offset_hours': -1
        # },
        # {
            # 'start': '2024-07-23 01:00:00',
            # 'end': '2024-07-29 01:00:00',
            # 'offset_hours': -1
        # },
        # {
            # 'start': '2022-10-24 00:00:00',
            # 'end': '2022-10-25 01:00:00',
            # 'offset_hours': 9
        # },
    # ],
    # 2. Definicja aliasów wskazujących na grupę
    # 'ME_DOWN_MET_30min' : 'ME_DOWN_STATION_COMBINED',
    # 'ME_DOWN_MET_1min' : 'ME_DOWN_STATION_COMBINED',
    # 'Rain_down' : 'ME_DOWN_STATION_COMBINED',
    # 'CalPlates' : 'ME_DOWN_STATION_COMBINED',
}

CALIBRATION_RULES_BY_STATION = {
    'MEZYK_DOWN': {
        # Wszystkie reguły dla JEDNEJ kolumny muszą być w JEDNEJ liście
        'PPFD_BC_IN_1_1_1': [
            {'start': '2018-11-13 23:00:00', 'end': '2058-11-13 23:00:00', 'multiplier': 3397.547, 'addend': 0, 'reason': 'LQA3027, (data in umol/m2/s1)'},
            {'start': '2019-09-01 19:30:00', 'end': '2019-09-13 06:00:00', 'multiplier': 1, 'addend': -230, 'reason': 'LQA3028 - korekta'}
        ],
        'PPFD_BC_IN_1_1_2': [
            {'start': '2018-08-08 13:30:00', 'end': '2018-11-17 12:00:00', 'multiplier': 1, 'addend': -650, 'reason': 'LQA3028 - stara korekta'},
            {'start': '2018-11-13 23:00:00', 'end': '2058-11-13 23:00:00', 'multiplier': 3288.716, 'addend': 0, 'reason': 'LQA3028, (data in umol/m2/s1)'},
            {'start': '2019-09-01 19:30:00', 'end': '2019-09-13 06:00:00', 'multiplier': 1, 'addend': -230, 'reason': 'LQA3028 - korekta'}
        ]
    },
    'MEZYK_TOP': {
        'PPFD_1_2_1': [
            {'start': '2018-01-01 00:00:00', 'end': '2018-11-15 12:00:00', 'multiplier': 199.601, 'addend': 0, 'reason': 'CNR4 (data in W/m2)'}
        ],
        'PPFDr_1_2_1': [
            {'start': '2018-01-01 00:00:00', 'end': '2018-11-15 12:00:00', 'multiplier': 201.613, 'addend': 0, 'reason': 'CNR4 (data in W/m2)'}
        ],
        'SWin_1_2_1': [
            {'start': '2018-08-09 04:30:00', 'end': '2018-11-15 12:00:00', 'multiplier': 74.85, 'addend': 0, 'reason': 'CNR4 (data in W/m2)'}
        ],
        'SWout_1_2_1': [
            {'start': '2018-08-09 04:30:00', 'end': '2018-11-15 12:00:00', 'multiplier': 81.5, 'addend': 0, 'reason': 'CNR4 (data in W/m2)'}
        ]
    }
}

STATION_MAPPING_FOR_CALIBRATION = {
    # 'ME_TOP_MET_30min': 'MEZYK_TOP',
    'ME_TOP_MET1_min' : 'MEZYK_TOP',
    # 'Rain_top': 'MEZYK_TOP',

    # 'ME_DOWN_MET_30min' : 'MEZYK_DOWN',
    'ME_DOWN_MET_1min' : 'MEZYK_DOWN',
    # 'Rain_down' : 'MEZYK_DOWN',
    # 'CalPlates' : 'MEZYK_DOWN',
    
    # Dodaj tutaj mapowania dla innych stacji, np.
    # 'TU_STATION_COMBINED': 'TUCZNO',
}

# === NOWY SŁOWNIK DO FILTROWANIA ZAKRESÓW DAT ===
DATE_RANGES_TO_EXCLUDE = {
    # Przykład, dostosuj do swoich potrzeb
    'ME_DOWN_STATION_COMBINED': [
        {
            'start': '2018-06-01 10:19:00',
            'end': '2018-06-03 14:04:00',
            'reason': 'Awaria rejestratora'
        },
    ],
    'ME_DOWN_MET_30min' : 'ME_DOWN_STATION_COMBINED',
    'ME_DOWN_MET_1min' : 'ME_DOWN_STATION_COMBINED',
    'Rain_down' : 'ME_DOWN_STATION_COMBINED',
    'CalPlates' : 'ME_DOWN_STATION_COMBINED',
}

# === KONFIGURACJA UŻYTKOWNIKA ===
# (Poniżej przykładowe konfiguracje, dostosuj do swoich potrzeb)
# FILE_ID_MERGE_GROUPS = { 'TU_STATION_COMBINED': { 'source_ids': ['CR5000_flux'], 'interval': '30min' } }
# TIMEZONE_CORRECTIONS = { 'TU_STATION_COMBINED': { 'source_tz':'Europe/Warsaw', 'correction_end_date':'2018-01-01 00:00:00', 'post_correction_tz':'UTC', 'target_tz':'Etc/GMT-1'} }
# MANUAL_TIME_SHIFTS = { 'CR5000_flux': [{'start': '2012-08-15 00:00:00', 'end': '2012-11-05 10:00:00', 'offset_hours': 1}] }
# === KONIEC KONFIGURACJI UŻYTKOWNIKA ===

# --- Funkcje pomocnicze ---
def load_cache(cache_path):
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r', encoding='utf-8') as f: return json.load(f)
        except (json.JSONDecodeError, IOError) as e: print(f"Ostrzeżenie: Nie można wczytać cache '{cache_path}': {e}.")
    return {}
def save_cache(data, cache_path):
    try:
        with open(cache_path, 'w', encoding='utf-8') as f: json.dump(data, f, indent=4)
    except IOError as e: print(f"BŁĄD: Nie można zapisać cache do '{cache_path}': {e}")
def log_chronology_error(file_path, problem_indices):
    try:
        log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"{log_time} - FILE: {os.path.abspath(file_path)} - ORIGINAL_ROW_INDICES: {problem_indices}\n"
        with open(CHRONO_ERROR_FILE_PATH, 'a', encoding='utf-8') as f: f.write(log_message)
    except Exception as e: print(f"Ostrzeżenie: Nie można zapisać błędu chronologii: {e}")
def parse_header_line(line): return [field.strip() for field in line.strip('"').split('","')]
def decode_csi_fs2_float(raw_short_int_le: int) -> float:
    uint16_val_le=raw_short_int_le&0xFFFF;byte_stream_0=uint16_val_le&0xFF;byte_stream_1=(uint16_val_le>>8)&0xFF;fs_word=(byte_stream_0<<8)|byte_stream_1
    POS_INFINITY_FS2,NEG_INFINITY_FS2,NOT_A_NUMBER_FS2=0x1FFF,0x9FFF,0x9FFE
    if fs_word==POS_INFINITY_FS2:return float('inf')
    elif fs_word==NEG_INFINITY_FS2:return float('-inf')
    elif fs_word==NOT_A_NUMBER_FS2:return float('nan')
    is_negative=(fs_word&0x8000)!=0;mantissa_val=fs_word&0x1FFF;exponent_val=(fs_word&0x6000)>>13;rtn=float(mantissa_val)
    if mantissa_val!=0:
        for _ in range(exponent_val):rtn/=10.0
    else:rtn=0.0
    if is_negative and mantissa_val!=0:rtn*=-1.0
    return rtn
def get_tob1_metadata(file_path):
    try:
        with open(file_path,'r',encoding='latin-1')as f:header_lines=[f.readline().strip()for _ in range(5)]
        if not header_lines[0].startswith('"TOB1"'):return None
        col_names_from_header=parse_header_line(header_lines[1]);binary_formats_str_from_header=parse_header_line(header_lines[4])
        struct_pattern_prefix='<';actual_col_names_to_use,actual_struct_pattern_parts,fp2_column_names=[],[],[]
        for i,original_fmt_str in enumerate(binary_formats_str_from_header):
            fmt_str_upper=original_fmt_str.upper();base_col_name=col_names_from_header[i]if i<len(col_names_from_header)else f"DataCol_idx{i}";fmt_code=None
            if fmt_str_upper.startswith("ASCII("):
                try:
                    length_str=fmt_str_upper[len("ASCII("):-1]
                    if length_str.isdigit():length=int(length_str)
                    if length>0:fmt_code=f"{length}s"
                except ValueError:pass
            elif fmt_str_upper=="FP2":fmt_code='h';fp2_column_names.append(base_col_name)
            elif fmt_str_upper=="SECNANO":fmt_code=None
            else:fmt_code=STRUCT_FORMAT_MAP.get(fmt_str_upper)
            if fmt_code:actual_struct_pattern_parts.append(fmt_code);actual_col_names_to_use.append(base_col_name)
        if not actual_struct_pattern_parts:return None
        final_struct_pattern=struct_pattern_prefix+"".join(actual_struct_pattern_parts)
        if len(actual_col_names_to_use)!=len(actual_struct_pattern_parts):return None
        return actual_col_names_to_use,final_struct_pattern,5,fp2_column_names
    except Exception:return None
def read_tob1_data(file_path,metadata):
    col_names,struct_pattern,num_header_lines,fp2_cols=metadata;records=[]
    try:
        if not col_names:return pd.DataFrame()
        record_size=struct.calcsize(struct_pattern)
        if record_size==0:return pd.DataFrame(columns=col_names)
        with open(file_path,'rb')as f:
            for i in range(num_header_lines):
                if not f.readline():return pd.DataFrame(columns=col_names)
            processed_records_count=0
            while True:
                chunk=f.read(record_size)
                if not chunk:break
                if len(chunk)<record_size:break
                try:records.append(struct.unpack(struct_pattern,chunk));processed_records_count+=1
                except struct.error as e:print(f"BŁĄD ROZPAKOWYWANIA TOB1 w {file_path} (rekord ~{processed_records_count+1}): {e}");break
        if not records:return pd.DataFrame(columns=col_names)
        try:df=pd.DataFrame(records,columns=col_names)
        except ValueError as ve:print(f"Błąd DataFrame TOB1 {os.path.basename(file_path)}: {ve}");return pd.DataFrame()
        if not df.empty and fp2_cols:
            for fp2_col_name in fp2_cols:
                if fp2_col_name in df.columns:
                    try:
                        if not pd.api.types.is_numeric_dtype(df[fp2_col_name]):df[fp2_col_name]=pd.to_numeric(df[fp2_col_name],errors='coerce')
                        df[fp2_col_name]=df[fp2_col_name].apply(decode_csi_fs2_float)
                    except Exception as e_fp2:print(f"  OSTRZEŻENIE: Błąd dekodowania FP2 dla '{fp2_col_name}': {e_fp2}")
        timestamp_created_successfully=False
        if'SECONDS'in df.columns and'NANOSECONDS'in df.columns:
            df['SECONDS']=pd.to_numeric(df['SECONDS'],errors='coerce');df['NANOSECONDS']=pd.to_numeric(df['NANOSECONDS'],errors='coerce')
            valid_ts_data_indices=df.dropna(subset=['SECONDS','NANOSECONDS']).index
            if not valid_ts_data_indices.empty:
                try:
                    secs_s=df.loc[valid_ts_data_indices,'SECONDS'];nanos_s=df.loc[valid_ts_data_indices,'NANOSECONDS']
                    df.loc[valid_ts_data_indices,'TIMESTAMP']=CAMPBELL_EPOCH+pd.to_timedelta(secs_s,unit='s')+pd.to_timedelta(nanos_s,unit='ns')
                    timestamp_created_successfully=True
                except Exception as e_ts:print(f"Błąd tworzenia TIMESTAMP (TOB1) dla {file_path}: {e_ts}")
        if not timestamp_created_successfully and'TIMESTAMP'in df.columns and df['TIMESTAMP'].isnull().all():del df['TIMESTAMP']
        return df
    except Exception as e:print(f"Krytyczny błąd odczytu TOB1 {file_path}: {e}");return pd.DataFrame()
def get_toa5_metadata(file_path):
    try:
        with open(file_path,'r',encoding='latin-1')as f:header_lines=[f.readline().strip()for _ in range(4)]
        if not header_lines[0].startswith('"TOA5"'):return None
        return parse_header_line(header_lines[1]),4
    except Exception:return None
    
def read_toa5_data(file_path,metadata):
    col_names,num_header_lines=metadata
    try:
        df=pd.read_csv(file_path,skiprows=num_header_lines,header=None,names=col_names,na_values=['"NAN"','NAN','"INF"','""',''],quotechar='"',escapechar='\\',on_bad_lines='warn',encoding='latin-1')
        if'TIMESTAMP'not in df.columns:return pd.DataFrame()
        df['TIMESTAMP']=pd.to_datetime(df['TIMESTAMP'],errors='coerce');df.dropna(subset=['TIMESTAMP'],inplace=True)
        return df
    except Exception:return pd.DataFrame()
def read_simple_csv_data(file_path):
    try:
        df=pd.read_csv(file_path,header=0,low_memory=False,encoding='latin-1')
        if'Timestamp'not in df.columns:return pd.DataFrame()
        df.rename(columns={'Timestamp':'TIMESTAMP'},inplace=True)
        df['TIMESTAMP']=pd.to_datetime(df['TIMESTAMP'],errors='coerce')
        if df['TIMESTAMP'].isnull().any():df.dropna(subset=['TIMESTAMP'],inplace=True)
        return df
    except Exception as e:print(f"Krytyczny błąd odczytu SimpleCSV z {file_path}: {e}");return pd.DataFrame()

# === OSTATECZNA WERSJA `apply_timezone_correction` ===
def apply_timezone_correction(ts_series: pd.Series, file_id: str):
    if ts_series.empty or ts_series.isnull().all(): return ts_series
    
    # Krok 1: Znajdź konfigurację
    config_entry = TIMEZONE_CORRECTIONS.get(file_id); final_config = None
    if isinstance(config_entry, str):
        alias_name = config_entry
        if alias_name in TIMEZONE_CORRECTIONS and isinstance(TIMEZONE_CORRECTIONS[alias_name], dict): final_config = TIMEZONE_CORRECTIONS[alias_name]
    elif isinstance(config_entry, dict): final_config = config_entry
    
    # Krok 2: Upewnij się, że seria jest typu datetime i jest świadoma UTC
    # To jest kluczowy krok ujednolicający
    corrected_series = pd.to_datetime(ts_series, errors='coerce').dropna()
    if corrected_series.empty: return corrected_series
    
    if corrected_series.dt.tz is None:
        corrected_series = corrected_series.dt.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
    else:
        corrected_series = corrected_series.dt.tz_convert('UTC')
    corrected_series = corrected_series.dropna()
    if corrected_series.empty: return corrected_series
    
    # Krok 3: Zastosuj reguły (jeśli istnieją) lub domyślną konwersję
    target_tz = final_config['target_tz'] if final_config else 'Etc/GMT-1'
    if final_config:
        source_tz = final_config['source_tz']
        correction_end_date = pd.Timestamp(final_config['correction_end_date'], tz=source_tz).tz_convert('UTC')
        pre_fix_mask = corrected_series <= correction_end_date
        
        if pre_fix_mask.any():
            # "Taniec stref czasowych" dla okresu do korekty
            # UTC -> naiwny -> zlokalizuj w poprawnej strefie źródłowej -> skonwertuj z powrotem do UTC
            pre_fix_timestamps = corrected_series[pre_fix_mask]
            corrected_pre_fix = pre_fix_timestamps.dt.tz_localize(None).dt.tz_localize(source_tz, ambiguous='NaT', nonexistent='NaT').dt.tz_convert('UTC')
            # Zaktualizuj główną serię
            corrected_series.update(corrected_pre_fix)
    
    # Krok 4: Ostateczna konwersja całej, spójnej serii UTC do docelowej strefy czasowej
    return corrected_series.dt.tz_convert(target_tz)

# === WERSJA DIAGNOSTYCZNA funkcji apply_manual_time_shifts ===
# Zastąp nią całą poprzednią wersję tej funkcji.

def apply_manual_time_shifts(df, file_id, timestamp_col='TIMESTAMP'):
    """
    DIAGNOSTYCZNA WERSJA: Stosuje ręczne przesunięcia czasowe, drukując stan na każdym etapie.
    """
    # print("\n" + "="*20 + f" DIAGNOSTYKA apply_manual_time_shifts dla file_id: '{file_id}' " + "="*20)
    
    # Krok 1: Sprawdzenie, czy w ogóle mamy co robić
    if file_id not in MANUAL_TIME_SHIFTS:
        # print("  -> Info: Brak reguł w MANUAL_TIME_SHIFTS dla tego file_id. Funkcja kończy działanie.")
        # print("="*80 + "\n")
        return df

    # Krok 2: Znalezienie i walidacja konfiguracji
    config_entry = MANUAL_TIME_SHIFTS.get(file_id)
    rules = None
    if isinstance(config_entry, str):
        alias_name = config_entry
        # print(f"  -> Info: Znaleziono alias '{alias_name}' dla file_id '{file_id}'.")
        if alias_name in MANUAL_TIME_SHIFTS and isinstance(MANUAL_TIME_SHIFTS.get(alias_name), list):
            rules = MANUAL_TIME_SHIFTS[alias_name]
            # print(f"  -> Info: Pomyślnie odnaleziono wspólną konfigurację reguł pod kluczem '{alias_name}'.")
        # else:
            # print(f"  -> BŁĄD: Alias '{alias_name}' nie wskazuje na poprawną listę reguł. Funkcja kończy działanie.")
    elif isinstance(config_entry, list):
        rules = config_entry
        # print(f"  -> Info: Znaleziono bezpośrednią listę reguł dla file_id '{file_id}'.")
    
    if not rules:
        # print("  -> BŁĄD: Nie udało się załadować reguł. Funkcja kończy działanie.")
        # print("="*80 + "\n")
        return df

    # Krok 3: Sprawdzenie warunków wstępnych ramki danych
    if df.empty or df[timestamp_col].dt.tz is None:
        # print(f"  -> Ostrzeżenie: DataFrame jest pusty lub kolumna TIMESTAMP nie ma strefy czasowej (jest '{df[timestamp_col].dt.tz}'). Pomijam ręczną korektę.")
        # print("="*80 + "\n")
        return df
    
    df_out = df.copy()
    current_tz = df_out[timestamp_col].dt.tz
    # print(f"  -> Info: Strefa czasowa danych do korekty: {current_tz}")
    # print(f"  -> Info: Znaleziono {len(rules)} reguł(y) do zastosowania.")

    # Krok 4: Pętla przez każdą regułę
    for i, rule in enumerate(rules):
        # print(f"\n  --- Przetwarzanie reguły #{i+1} ---")
        try:
            start_str = rule.get('start')
            end_str = rule.get('end')
            offset_val = rule.get('offset_hours')
            # print(f"    -> Reguła z pliku: start='{start_str}', end='{end_str}', offset={offset_val}h")

            if not all([start_str, end_str, isinstance(offset_val, int)]):
                print("    -> BŁĄD: Reguła ma niekompletne dane (brak 'start', 'end' lub 'offset_hours').")
                continue

            start_ts = pd.Timestamp(start_str, tz=current_tz)
            end_ts = pd.Timestamp(end_str, tz=current_tz)
            # print(f"    -> Parsowane daty: start={start_ts}, end={end_ts}")
            
            # Sprawdzenie zakresu dat w danych
            min_data_ts = df_out[timestamp_col].min()
            max_data_ts = df_out[timestamp_col].max()
            # print(f"    -> Zakres dat w przetwarzanym pliku: od {min_data_ts} do {max_data_ts}")

            # Zastosowanie maski
            mask = (df_out[timestamp_col] >= start_ts) & (df_out[timestamp_col] <= end_ts)
            num_affected = mask.sum()
            # print(f"    -> WYNIK: Liczba wierszy w zdefiniowanym zakresie dat: {num_affected}")

            if num_affected > 0:
                offset = pd.Timedelta(hours=offset_val)
                df_out.loc[mask, timestamp_col] = df_out.loc[mask, timestamp_col] + offset
                # print(f"    -> SUKCES: Zastosowano przesunięcie {offset_val}h.")
            # else:
                # print(f"    -> INFO: Pominięto przesunięcie (brak pasujących danych w zakresie).")
        except Exception as e:
            print(f"    -> BŁĄD podczas przetwarzania reguły: {e}")
            
    # print("--- DIAGNOSTYKA: Zakończono `apply_manual_time_shifts` ---" + "\n")
    return df_out
    
# === NOWA FUNKCJA DO FILTROWANIA DANYCH ===

def filter_data_by_date_ranges(df, file_id, timestamp_col='TIMESTAMP'):
    """Filtruje DataFrame, usuwając wiersze z zdefiniowanych zakresów dat."""
    if file_id not in DATE_RANGES_TO_EXCLUDE:
        return df # Zwróć bez zmian, jeśli nie ma reguł

    rules = DATE_RANGES_TO_EXCLUDE.get(file_id)
    if not isinstance(rules, list):
        # Ta część obsługuje aliasy, tak jak w innych funkcjach
        alias = DATE_RANGES_TO_EXCLUDE.get(file_id)
        if isinstance(alias, str):
            rules = DATE_RANGES_TO_EXCLUDE.get(alias)
        if not isinstance(rules, list):
            return df
    
    if df.empty or df[timestamp_col].dt.tz is None:
        return df

    df_filtered = df.copy()
    current_tz = df_filtered[timestamp_col].dt.tz

    for i, rule in enumerate(rules):
        try:
            start_ts = pd.Timestamp(rule['start'], tz=current_tz)
            end_ts = pd.Timestamp(rule['end'], tz=current_tz)
            reason = rule.get('reason', 'brak opisu')

            # Stwórz maskę dla wierszy DO USUNIĘCIA
            exclusion_mask = (df_filtered[timestamp_col] >= start_ts) & (df_filtered[timestamp_col] <= end_ts)
            
            num_to_exclude = exclusion_mask.sum()
            if num_to_exclude > 0:
                print(f"  Info (Filtr): Stosowanie reguły '{reason}'. Oznaczono do usunięcia {num_to_exclude} wierszy.")
                # Zastosuj maskę logiczną, aby usunąć wiersze
                df_filtered = df_filtered[~exclusion_mask]

        except KeyError as ke:
            print(f"  BŁĄD w regule filtrowania #{i+1} dla '{file_id}': Brak klucza {ke}. Reguła pominięta.")
        except Exception as e:
            print(f"  BŁĄD podczas stosowania reguły filtrowania #{i+1} dla '{file_id}': {e}. Reguła pominięta.")
            
    return df_filtered
    
def detect_interval(df, timestamp_col='TIMESTAMP', sample_rows=100):
    if df is None or df.empty or timestamp_col not in df.columns or len(df) < 2: return None
    df_sorted_sample = df.sort_values(by=timestamp_col, kind='mergesort').head(sample_rows)
    if len(df_sorted_sample) < 2: return None
    time_diffs = pd.Series(df_sorted_sample[timestamp_col]).diff().dropna()
    if not time_diffs.empty: return time_diffs.median()
    return None
def correct_and_report_chronology(df: pd.DataFrame, file_path: str, timestamp_col='TIMESTAMP'):
    df_corrected = df.copy() # Pracuj na kopii, aby uniknąć modyfikacji oryginału
    if df_corrected[timestamp_col].is_monotonic_increasing: return df_corrected
    known_interval = detect_interval(df_corrected, timestamp_col)
    if known_interval is None or known_interval <= pd.Timedelta(0): return df_corrected.sort_values(by=timestamp_col, kind='mergesort').reset_index(drop=True)
    original_timestamps = df_corrected[timestamp_col].tolist(); corrected_timestamps = []; problem_indices = []
    is_in_correction_mode = False; last_valid_ts = None
    resync_tolerance = known_interval / 2
    for i, current_ts in enumerate(original_timestamps):
        if last_valid_ts is None: corrected_timestamps.append(current_ts); last_valid_ts = current_ts; continue
        expected_next_ts = last_valid_ts + known_interval
        time_jump_detected = current_ts < last_valid_ts
        if is_in_correction_mode:
            if abs(current_ts - expected_next_ts) < resync_tolerance:
                is_in_correction_mode = False; corrected_timestamps.append(current_ts); last_valid_ts = current_ts
                print(f"  -> Info: Wykryto resynchronizację w wierszu {i} pliku {os.path.basename(file_path)}.")
            else: corrected_timestamps.append(expected_next_ts); last_valid_ts = expected_next_ts
        elif time_jump_detected:
            is_in_correction_mode = True; problem_indices.append(i)
            corrected_ts = expected_next_ts; corrected_timestamps.append(corrected_ts); last_valid_ts = corrected_ts
            log_message = (f"\n{'#'*80}\nBŁĄD CHRONOLOGII DANYCH: Wykryto i rozpoczęto korektę cofnięcia czasu w pliku:\n"
                         f"  -> Plik: {file_path}\n  -> Problem w wierszu (indeks): {i}\n"
                         f"  -> Czas cofnął się z: {original_timestamps[i-1].strftime('%Y-%m-%d %H:%M:%S')} do: {current_ts.strftime('%Y-%m-%d %H:%M:%S')}\n"
                         f"  -> Info: Rozpoczęto tryb autokorekty z interwałem {known_interval}.\n" + "#"*80 + "\n")
            print(log_message); log_chronology_error(file_path, [i])
        else: corrected_timestamps.append(current_ts); last_valid_ts = current_ts
    if problem_indices: df_corrected[timestamp_col] = pd.Series(corrected_timestamps, index=df_corrected.index, dtype=df_corrected[timestamp_col].dtype)
    return df_corrected
def align_timestamp(df, timestamp_col='TIMESTAMP', sample_rows_for_interval_detection=100, force_interval=None):
    if df is None or df.empty or timestamp_col not in df.columns or df[timestamp_col].isnull().all(): return pd.DataFrame(columns=df.columns if df is not None else []), "no_timestamp_data"
    df_processed = df.copy();
    if not pd.api.types.is_datetime64_any_dtype(df_processed[timestamp_col]): df_processed[timestamp_col] = pd.to_datetime(df_processed[timestamp_col], errors='coerce')
    df_processed.dropna(subset=[timestamp_col], inplace=True)
    if df_processed.empty: return df_processed, "no_valid_timestamp"
    if force_interval:
        VALID_FREQ_MAP = {'subsecond': None, '1S': 's', '2S': '2s', '5S': '5s', '10S': '10s', '30S': '30s', '1min': 'min', '2min': '2min', '5min': '5min', '10min': '10min', '15min': '15min', '30min': '30min', '1H': 'H', '1D': 'D'}
        if force_interval not in VALID_FREQ_MAP: print(f"OSTRZEŻENIE: Nieprawidłowy wymuszony interwał '{force_interval}'."); return df_processed, "invalid_forced_interval"
        actual_freq_for_pandas = VALID_FREQ_MAP[force_interval]
        if actual_freq_for_pandas is None: return df_processed, "subsecond"
        try: df_processed.loc[:, timestamp_col] = df_processed[timestamp_col].dt.round(actual_freq_for_pandas); return df_processed, force_interval
        except Exception as e_round: print(f"BŁĄD: Nie można zastosować wymuszonego interwału '{actual_freq_for_pandas}': {e_round}"); return df_processed, f"error_forcing_interval"
    known_interval = detect_interval(df_processed, timestamp_col)
    if known_interval is None: interval_label = "lessthan2sample_or_const_ts_1min"; actual_freq_for_pandas = 'min'
    elif known_interval < pd.Timedelta(seconds=1): return df_processed, "subsecond"
    else:
        interval_map = [(pd.Timedelta(seconds=1.5),'S','1S'),(pd.Timedelta(seconds=62),'min','1min'),(pd.Timedelta(minutes=5.5),'5min','5min'),(pd.Timedelta(minutes=10.5),'10min','10min'),(pd.Timedelta(minutes=15.5),'15min','15min'),(pd.Timedelta(minutes=31),'30min','30min'),(pd.Timedelta(minutes=61),'H','1H'),(pd.Timedelta(days=1.1),'D','1D')]
        found_match = False
        for upper_bound, freq, label in interval_map:
            if known_interval <= upper_bound: actual_freq_for_pandas, interval_label = freq, label; found_match = True; break
        if not found_match: actual_freq_for_pandas, interval_label = ('S', 'unknown_S') if known_interval < pd.Timedelta(minutes=1) else ('min', 'unknown_min')
    try: df_processed.loc[:, timestamp_col] = df_processed[timestamp_col].dt.round(actual_freq_for_pandas)
    except Exception: df_processed.loc[:, timestamp_col] = df_processed[timestamp_col].dt.round('min'); interval_label = "fallback_1min"
    return df_processed, interval_label

# === ZMODYFIKOWANA funkcja apply_calibration z obsługą stacji ===

def apply_calibration(df, file_id):
    """Przelicza wartości, używając reguł kalibracyjnych przypisanych do stacji."""
    if df.empty:
        return df

    # Krok 1: Znajdź nazwę stacji dla danego file_id za pomocą mapowania
    station_name = STATION_MAPPING_FOR_CALIBRATION.get(file_id)
        
    if not station_name:
        return df # Zwróć bez zmian, jeśli nie ma mapowania dla tego file_id

    # Krok 2: Pobierz reguły dla znalezionej stacji
    if station_name not in CALIBRATION_RULES_BY_STATION:
        return df
        
    column_rules = CALIBRATION_RULES_BY_STATION[station_name]
    if not isinstance(column_rules, dict):
        print(f"Ostrzeżenie: Konfiguracja w CALIBRATION_RULES_BY_STATION dla '{station_name}' powinna być słownikiem.")
        return df

    print(f"Info: Stosowanie reguł kalibracji dla stacji: '{station_name}' (na podstawie file_id: '{file_id}')")
    df_calibrated = df.copy()
    
    # Krok 3: Zastosuj reguły
    for col_name, rules_list in column_rules.items():
        if col_name not in df_calibrated.columns:
            continue
        
        df_calibrated[col_name] = pd.to_numeric(df_calibrated[col_name], errors='coerce')
        
        for i, rule in enumerate(rules_list):
            try:
                # Upewnij się, że timestampy do porównania mają tę samą strefę czasową
                current_tz = df_calibrated['TIMESTAMP'].dt.tz
                start_ts = pd.Timestamp(rule['start']).tz_localize(current_tz)
                end_ts = pd.Timestamp(rule['end']).tz_localize(current_tz)
                
                multiplier = float(rule['multiplier'])
                addend = float(rule['addend'])
                mask = (df_calibrated['TIMESTAMP'] >= start_ts) & (df_calibrated['TIMESTAMP'] <= end_ts)
                
                if mask.any():
                    df_calibrated.loc[mask, col_name] = (df_calibrated.loc[mask, col_name] * multiplier) + addend

            except KeyError as ke:
                print(f"  BŁĄD w regule kalibracji #{i+1} dla '{col_name}': Brak klucza {ke}. Reguła pominięta.")
            except Exception as e:
                print(f"  BŁĄD podczas stosowania reguły kalibracji #{i+1} dla '{col_name}': {e}. Reguła pominięta.")

    return df_calibrated
    
# # === ZMODYFIKOWANA funkcja process_and_save_aggregated_data ===
# # z logiką scalania na poziomie komórek

# def process_and_save_aggregated_data(data_by_year, output_dir, source_id, interval_label, overwrite=False):
    # """Agreguje dane roczne, inteligentnie scala komórki z duplikatów i zapisuje do CSV."""
    # for year, list_of_dfs in data_by_year.items():
        # # Krok 1: Połącz wszystkie dane z bieżącego uruchomienia dla danego roku
        # master_df_for_year = pd.concat(list_of_dfs, ignore_index=True)
        # if master_df_for_year.empty or 'TIMESTAMP' not in master_df_for_year.columns:
            # continue
        
        # # Krok 2: Wczytaj istniejący plik wynikowy, jeśli istnieje i nie ma flagi --overwrite
        # if os.path.exists(os.path.join(output_dir, str(year), f"{source_id}_{interval_label}_{year}.csv")) and not overwrite:
            # try:
                # existing_df = pd.read_csv(os.path.join(output_dir, str(year), f"{source_id}_{interval_label}_{year}.csv"), parse_dates=['TIMESTAMP'], low_memory=False)
                # # Upewnij się, że strefy czasowe są zgodne przed połączeniem
                # if 'TIMESTAMP' in existing_df.columns:
                    # if existing_df['TIMESTAMP'].dt.tz is None:
                        # try: existing_df.loc[:, 'TIMESTAMP'] = existing_df['TIMESTAMP'].dt.tz_localize('UTC')
                        # except Exception: pass
                    # if str(existing_df['TIMESTAMP'].dt.tz) != str(master_df_for_year['TIMESTAMP'].dt.tz):
                        # try: existing_df.loc[:, 'TIMESTAMP'] = existing_df['TIMESTAMP'].dt.tz_convert(master_df_for_year['TIMESTAMP'].dt.tz)
                        # except Exception: pass
                
                # # Połącz istniejące dane z nowymi danymi z bieżącego uruchomienia
                # master_df_for_year = pd.concat([existing_df, master_df_for_year], ignore_index=True)
            # except Exception as e:
                # print(f"  Ostrzeżenie: Nie można wczytać istniejącego pliku. Zostanie on nadpisany. Błąd: {e}")

        # # === NOWA, UPROSZCZONA I POTĘŻNIEJSZA LOGIKA SCALANIA ===
        # # 1. Posortuj po TIMESTAMP, aby ustalić priorytet dla .first() (stare dane przed nowymi)
        # master_df_for_year.sort_values(by='TIMESTAMP', kind='mergesort', inplace=True)
        
        # # 2. Zgrupuj po TIMESTAMP i dla każdej grupy wybierz PIERWSZĄ niepustą wartość w każdej kolumnie.
        # # To jest serce inteligentnego scalania komórek.
        # final_df = master_df_for_year.groupby('TIMESTAMP').first()
        # # === KONIEC NOWEJ LOGIKI SCALANIA ===

        # if final_df.empty:
            # continue
            
        # df_to_save = final_df.copy().reset_index()

        # # Krok 3: Zapisz finalny, w pełni scalony plik
        # year_dir = os.path.join(output_dir, str(year))
        # if not os.path.exists(year_dir):
            # try: os.makedirs(year_dir)
            # except OSError as e: print(f"  BŁĄD: Nie można utworzyć '{year_dir}': {e}"); continue
        
# #         output_filename = f"{source_id}_{interval_label}_{year}.csv"
        # output_filename = f"{source_id}.csv"
        # output_filepath = os.path.join(year_dir, output_filename)
        
        # if os.path.exists(output_filepath) and overwrite:
            # print(f"  Info: Nadpisywanie istniejącego pliku {os.path.basename(output_filepath)} z powodu opcji --overwrite.")
        
        # df_to_save.to_csv(output_filepath, index=False)
        # # print(f"  Zapisano/zaktualizowano plik {output_filepath}")
        
 # === ZMODYFIKOWANA funkcja process_and_save_aggregated_data ===
# z dwustopniową logiką deduplikacji

# def process_and_save_aggregated_data(data_by_year, output_dir, source_id, interval_label, overwrite=False):
    # """Agreguje dane roczne, stosując dwustopniową deduplikację i zapisuje do CSV."""
    # for year, list_of_dfs in data_by_year.items():
        # # Krok 1: Połącz wszystkie dane z bieżącego uruchomienia dla danego roku
        # master_df_for_year = pd.concat(list_of_dfs, ignore_index=True)
        # if master_df_for_year.empty or 'TIMESTAMP' not in master_df_for_year.columns:
            # continue
        
        # # Krok 2: Wczytaj istniejący plik wynikowy, jeśli istnieje i nie ma flagi --overwrite
        # if os.path.exists(os.path.join(output_dir, str(year), f"{source_id}_{interval_label}_{year}.csv")) and not overwrite:
            # try:
                # existing_df = pd.read_csv(os.path.join(output_dir, str(year), f"{source_id}_{interval_label}_{year}.csv"), parse_dates=['TIMESTAMP'], low_memory=False)
                # # Upewnij się, że strefy czasowe są zgodne przed połączeniem
                # if 'TIMESTAMP' in existing_df.columns:
                    # if existing_df['TIMESTAMP'].dt.tz is None:
                        # try: existing_df.loc[:, 'TIMESTAMP'] = existing_df['TIMESTAMP'].dt.tz_localize('UTC')
                        # except Exception: pass
                    # if str(existing_df['TIMESTAMP'].dt.tz) != str(master_df_for_year['TIMESTAMP'].dt.tz):
                        # try: existing_df.loc[:, 'TIMESTAMP'] = existing_df['TIMESTAMP'].dt.tz_convert(master_df_for_year['TIMESTAMP'].dt.tz)
                        # except Exception: pass
                
                # # Połącz istniejące dane z nowymi danymi z bieżącego uruchomienia
                # master_df_for_year = pd.concat([existing_df, master_df_for_year], ignore_index=True)
            # except Exception as e:
                # print(f"  Ostrzeżenie: Nie można wczytać istniejącego pliku. Zostanie on nadpisany. Błąd: {e}")

        # # === NOWA, DWUSTOPNIOWA LOGIKA SCALANIA I DEDUPLIKACJI ===
        
        # # 1. Usuń wiersze, które są w 100% identyczne
        # # Sprawdzamy, czy istnieją jakiekolwiek kolumny poza TIMESTAMP i source_file do porównania
        # data_cols = [col for col in master_df_for_year.columns if col not in ['TIMESTAMP', 'source_file']]
        # if data_cols: # Unikaj błędu, jeśli mamy tylko kolumny TIMESTAMP i source_file
            # master_df_for_year.drop_duplicates(inplace=True)

        # # 2. Inteligentnie scal wiersze z tym samym TIMESTAMP, ale różnymi danymi
        # # Grupowanie po TIMESTAMP i wybieranie pierwszej niepustej wartości w każdej kolumnie
        # # final_df = master_df_for_year.groupby('TIMESTAMP').first()
        # final_df = master_df_for_year
        # # === KONIEC NOWEJ LOGIKI SCALANIA ===

        # if final_df.empty:
            # continue
            
        # df_to_save = final_df.copy().reset_index()
        # # Sortowanie na końcu dla pewności, że plik jest chronologiczny
        # df_to_save.sort_values(by='TIMESTAMP', inplace=True)

        # # Krok 3: Zapisz finalny, w pełni scalony plik
        # year_dir = os.path.join(output_dir, str(year))
        # if not os.path.exists(year_dir):
            # try: os.makedirs(year_dir)
            # except OSError as e: print(f"  BŁĄD: Nie można utworzyć '{year_dir}': {e}"); continue
        
        # output_filename = f"{source_id}.csv"
        # output_filepath = os.path.join(year_dir, output_filename)
        
        # if os.path.exists(output_filepath) and overwrite:
            # print(f"  Info: Nadpisywanie istniejącego pliku {os.path.basename(output_filepath)} z powodu opcji --overwrite.")
        
        # df_to_save.to_csv(output_filepath, index=False)
        # # print(f"  Zapisano/zaktualizowano plik {output_filepath}")
            
# === OSTATECZNA WERSJA `process_and_save_aggregated_data` Z POPRAWIONĄ DEDUPLIKACJĄ ===

def process_and_save_aggregated_data(data_by_year, output_dir, source_id, interval_label, overwrite=False):
    """Agreguje dane roczne, stosuje deduplikację na podstawie wartości i zapisuje do CSV."""
    for year, list_of_dfs in data_by_year.items():
        master_df_for_year = pd.concat(list_of_dfs, ignore_index=True)
        if master_df_for_year.empty or 'TIMESTAMP' not in master_df_for_year.columns: continue
        
        output_filename = f"{source_id}.csv"
        year_dir = os.path.join(output_dir, str(year))
        output_filepath = os.path.join(year_dir, output_filename)

        if os.path.exists(output_filepath) and not overwrite:
            try:
                existing_df = pd.read_csv(output_filepath, parse_dates=['TIMESTAMP'], low_memory=False)
                if 'TIMESTAMP' in existing_df.columns and not existing_df.empty:
                    master_df_for_year = pd.concat([existing_df, master_df_for_year], ignore_index=True)
            except Exception as e:
                print(f"  Ostrzeżenie: Nie można wczytać istn. pliku {os.path.basename(output_filepath)}. Zostanie nadpisany. Błąd: {e}")

        # === NOWA LOGIKA DEDUPLIKACJI ===
        # 1. Zdefiniuj kolumny, które mają być porównywane (wszystkie oprócz `TIMESTAMP` i `source_file`)
        if not master_df_for_year.empty:
            cols_to_check = [col for col in master_df_for_year.columns if col not in ['TIMESTAMP', 'source_file']]
            initial_rows = len(master_df_for_year)

            # 2. Usuń duplikaty na podstawie wartości w kolumnach z danymi, zachowując PIERWSZE wystąpienie
            # i sortując najpierw po czasie, aby zapewnić spójność.
            if cols_to_check: # Wykonaj tylko jeśli są kolumny z danymi
                 master_df_for_year.sort_values(by='TIMESTAMP', kind='mergesort', inplace=True)
                 master_df_for_year.drop_duplicates(subset=cols_to_check, keep='first', inplace=True)
            
            final_rows = len(master_df_for_year)
            if initial_rows > final_rows:
                print(f"  Info (Deduplikacja Danych dla ROKU {year}): Usunięto {initial_rows - final_rows} wierszy o zduplikowanej treści pomiarowej.")
        # === KONIEC NOWEJ LOGIKI DEDUPLIKACJI ===
        
        if master_df_for_year.empty: continue
            
        # Ostateczne sortowanie i zapis
        df_to_save = master_df_for_year.sort_values(by='TIMESTAMP').reset_index(drop=True)

        if not os.path.exists(year_dir):
            try: os.makedirs(year_dir)
            except OSError as e: print(f"  BŁĄD: Nie można utworzyć '{year_dir}': {e}"); continue
        
        if os.path.exists(output_filepath) and overwrite:
            print(f"  Info: Nadpisywanie istniejącego pliku {os.path.basename(output_filepath)}.")
        
        try:
            df_to_save.to_csv(output_filepath, index=False)
        except Exception as e:
            print(f"  Krytyczny błąd podczas zapisu do pliku {output_filepath}: {e}")

            
# def read_and_process_one_file(file_path, force_interval, active_file_id):
    # current_df, is_simple_csv = pd.DataFrame(), False
    # try:
        # with open(file_path, 'r', encoding='latin-1') as f: first_line = f.readline().strip()
    # except Exception: return pd.DataFrame(), None
    # if first_line.startswith('"TOB1"'):
        # metadata = get_tob1_metadata(file_path)
        # if metadata: current_df = read_tob1_data(file_path, metadata)
    # elif first_line.startswith('"TOA5"'):
        # metadata = get_toa5_metadata(file_path)
        # if metadata: current_df = read_toa5_data(file_path, metadata)
    # elif first_line.startswith('"Timestamp"'):
        # is_simple_csv = True
        # current_df = read_simple_csv_data(file_path)
    # else: return pd.DataFrame(), None
    # if current_df.empty or 'TIMESTAMP' not in current_df.columns or current_df['TIMESTAMP'].dropna().empty: return pd.DataFrame(), None
    
    # # --- NOWA, BEZPIECZNA LOGIKA KOREKT ---
    # timestamps_corrected = apply_timezone_correction(current_df['TIMESTAMP'], active_file_id)
    # if timestamps_corrected.empty: return pd.DataFrame(), None
    # current_df = current_df.loc[timestamps_corrected.index].copy()
    # current_df['TIMESTAMP'] = timestamps_corrected
    
    # if is_simple_csv: current_df = correct_and_report_chronology(current_df, file_path)
    # current_df = apply_manual_time_shifts(current_df, active_file_id)
    # if current_df.empty: return pd.DataFrame(), None
    
    # aligned_df, interval_label = align_timestamp(current_df, force_interval=force_interval)
    # return aligned_df, interval_label
    
# === ZMODYFIKOWANA funkcja read_and_process_one_file z poprawną kolejnością operacji ===

def read_and_process_one_file(file_path, force_interval, active_file_id):
    """
    Orkiestruje pełny potok przetwarzania dla jednego pliku wejściowego.
    """
    current_df, is_simple_csv = pd.DataFrame(), False
    try:
        with open(file_path, 'r', encoding='latin-1') as f: first_line = f.readline().strip()
    except Exception: return pd.DataFrame(), None

    # Logika wczytywania danych
    if first_line.startswith('"TOB1"'):
        metadata = get_tob1_metadata(file_path)
        if metadata: current_df = read_tob1_data(file_path, metadata)
    elif first_line.startswith('"TOA5"'):
        metadata = get_toa5_metadata(file_path)
        if metadata: current_df = read_toa5_data(file_path, metadata)
    elif first_line.startswith('"Timestamp"'):
        is_simple_csv = True
        current_df = read_simple_csv_data(file_path)
    else: return pd.DataFrame(), None

    if current_df.empty or 'TIMESTAMP' not in current_df.columns or current_df['TIMESTAMP'].dropna().empty:
        return pd.DataFrame(), None
    
    # --- Potok Przetwarzania ---
    
    # KROK 1: Korekta surowej chronologii (jeśli dotyczy)
    if is_simple_csv:
        current_df = correct_and_report_chronology(current_df, file_path)
    
    # KROK 2: Korekta stref czasowych
    # === KLUCZOWA ZMIANA: Dodajemy .copy(), aby przekazać niezależną kopię danych ===
    timestamps_corrected = apply_timezone_correction(current_df['TIMESTAMP'].copy(), active_file_id)
    if timestamps_corrected.empty:
        return pd.DataFrame(), None
    
    # Bezpiecznie zaktualizuj ramkę danych
    current_df = current_df.loc[timestamps_corrected.index].copy()
    current_df['TIMESTAMP'] = timestamps_corrected
    
    # KROK 3: Ręczne, okresowe przesunięcia czasu
    current_df = apply_manual_time_shifts(current_df, active_file_id)
    if current_df.empty: return pd.DataFrame(), None
    
    # KROK 4: Zastosowanie kalibracji
    current_df = apply_calibration(current_df, active_file_id)
    
    # KROK 5: Finalne wyrównanie (zaokrąglenie) czasu
    aligned_df, interval_label = align_timestamp(current_df, force_interval=force_interval)
    
    return aligned_df, interval_label

def main(input_dirs_arg, output_dir_arg, file_id_filter_arg, force_interval_arg, overwrite_arg):
    # ... (początek funkcji main, definicje zmiennych - bez zmian) ...
    output_directory_base = output_dir_arg
    if not os.path.exists(output_directory_base):
        try: os.makedirs(output_directory_base); print(f"Utworzono: {output_directory_base}")
        except OSError as e: print(f"BŁĄD: Nie można utworzyć '{output_directory_base}': {e}"); return
    processed_files_cache = load_cache(CACHE_FILE_PATH)
    if overwrite_arg: print("Info: Użyto --overwrite. Pliki pasujące do kryteriów zostaną przetworzone ponownie.")
    current_run_cache = {}
    source_ids_to_process, source_id_for_filename, effective_force_interval = [], "", force_interval_arg
    if file_id_filter_arg:
        if file_id_filter_arg in FILE_ID_MERGE_GROUPS:
            group_config = FILE_ID_MERGE_GROUPS[file_id_filter_arg]
            source_ids_to_process = group_config.get('source_ids', [])
            source_id_for_filename = file_id_filter_arg 
            predefined_interval = group_config.get('interval')
            if predefined_interval:
                if force_interval_arg and force_interval_arg != predefined_interval:
                    print(f"Ostrzeżenie: Podano --interval, ale grupa '{file_id_filter_arg}' ma predefiniowany interwał '{predefined_interval}'. Używam interwału z grupy.")
                effective_force_interval = predefined_interval
        else:
            source_ids_to_process = [file_id_filter_arg]; source_id_for_filename = file_id_filter_arg
    else:
        if len(input_dirs_arg) > 1: source_id_for_filename = "aggregated_dataset"
        else:
            norm_input_path = os.path.normpath(input_dirs_arg[0]); basename_input = os.path.basename(norm_input_path)
            source_id_for_filename = "dataset" if basename_input in [".", "..", ""] else basename_input
        source_ids_to_process = None 
    source_id_for_filename = re.sub(r'[^\w\-_\.]', '_', source_id_for_filename)
    if not source_id_for_filename: source_id_for_filename = "dataset"
    print(f"\nPrzetwarzanie danych z katalogów: {input_dirs_arg}")
    if file_id_filter_arg: print(f"Filtr/Grupa file_id: '{file_id_filter_arg}'")
    if effective_force_interval: print(f"Efektywny interwał przetwarzania: '{effective_force_interval}'")
    print(f"Identyfikator dla plików wynikowych: {source_id_for_filename}")
    active_timezone_id = file_id_filter_arg if file_id_filter_arg else source_id_for_filename
    
    files_to_process = []
    print("\nKrok 1: Wyszukiwanie plików wejściowych...")
    for input_dir in input_dirs_arg:
        if not os.path.isdir(input_dir):
            print(f"Ostrzeżenie: Podana ścieżka wejściowa nie jest katalogiem i zostanie pominięta: {input_dir}")
            continue
        for root, _, files in os.walk(input_dir):
            for filename in files:
                if filename.startswith('.') or (source_ids_to_process and not any(sid in filename for sid in source_ids_to_process)): continue
                file_path = os.path.abspath(os.path.join(root, filename))
                
                # --- NOWA LOGIKA CACHE ---
                # Sprawdź typ pliku, aby zdecydować, czy używać cache
                is_simple_csv = False
                try:
                    with open(file_path, 'r', encoding='latin-1') as f:
                        first_line = f.readline().strip()
                        if first_line.startswith('"Timestamp"'):
                            is_simple_csv = True
                except Exception:
                    continue # Pomiń pliki, których nie da się odczytać

                # Ignoruj cache dla plików SimpleCSV
                use_cache = not is_simple_csv

                file_mtime, file_size = os.path.getmtime(file_path), os.path.getsize(file_path)
                if use_cache and not overwrite_arg and file_path in processed_files_cache and processed_files_cache[file_path].get('mtime') == file_mtime and processed_files_cache[file_path].get('size') == file_size:
                    continue
                
                files_to_process.append({'path': file_path, 'mtime': file_mtime, 'size': file_size})

    if not files_to_process: print("\nZakończono. Brak nowych lub zmienionych plików do przetworzenia."); return
    print(f"\nZnaleziono {len(files_to_process)} plików do przetworzenia (nowych, zmienionych lub typu SimpleCSV).")
    
    if effective_force_interval:
        data_by_year = defaultdict(list)
        for file_info in files_to_process:
            aligned_df, _ = read_and_process_one_file(file_info['path'], effective_force_interval, active_timezone_id)
            if not aligned_df.empty:
                # === NOWY KROK: Dodanie kolumny z nazwą pliku źródłowego ===
                aligned_df['source_file'] = os.path.basename(file_info['path'])
                
                for year, year_group in aligned_df.groupby(aligned_df['TIMESTAMP'].dt.year):
                    data_by_year[year].append(year_group)
                current_run_cache[file_info['path']] = {'mtime': file_info['mtime'], 'size': file_info['size'], 'interval_label': effective_force_interval}

        if data_by_year:
            process_and_save_aggregated_data(data_by_year, output_directory_base, source_id_for_filename, effective_force_interval, overwrite=overwrite_arg)
        else:
            print("Brak nowych danych do zapisania.")
    else:
        file_scan_results, unique_interval_labels = [], set()
        print("\nFaza 1: Skanowanie i kategoryzacja plików...")
        for file_info in files_to_process:
            # W Fazie 1 tylko kategoryzujemy, więc nie dodajemy jeszcze kolumny source_file
            current_df, interval_label = read_and_process_one_file(file_info['path'], None, active_timezone_id)
            if interval_label and interval_label not in ["no_timestamp_data", "no_valid_timestamp", "unknown_interval", "invalid_forced_interval"]:
                file_scan_results.append({'path': file_info['path'], 'interval_label': interval_label})
                unique_interval_labels.add(interval_label)
                current_run_cache[file_info['path']] = {'mtime': file_info['mtime'], 'size': file_info['size'], 'interval_label': interval_label}

        if not file_scan_results: print("Zakończono Faze 1. Nie znaleziono poprawnych danych do dalszego przetwarzania.")
        else:
            print(f"Info: Wykryto unikalne interwały: {sorted(list(unique_interval_labels))}")
            for target_interval_label in sorted(list(unique_interval_labels)):
                print(f"\nPrzetwarzanie dla interwału: '{target_interval_label}'")
                paths_for_interval = [item['path'] for item in file_scan_results if item['interval_label'] == target_interval_label]
                list_of_dfs = []
                for file_path in paths_for_interval:
                    df, _ = read_and_process_one_file(file_path, target_interval_label, active_timezone_id)
                    if not df.empty:
                        # === NOWY KROK: Dodanie kolumny z nazwą pliku źródłowego ===
                        df['source_file'] = os.path.basename(file_path)
                        list_of_dfs.append(df)
                if list_of_dfs:
                    data_by_year = defaultdict(list)
                    for df in list_of_dfs:
                        for year, year_group in df.groupby(df['TIMESTAMP'].dt.year):
                            data_by_year[year].append(year_group)
                    process_and_save_aggregated_data(data_by_year, output_directory_base, source_id_for_filename, target_interval_label, overwrite=overwrite_arg)
    
    processed_files_cache.update(current_run_cache)
    save_cache(processed_files_cache, CACHE_FILE_PATH)
    print(f"\nZakończono. Dodano/zaktualizowano {len(current_run_cache)} plików w historii przetwarzania.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Przetwarza i scala pliki danych Campbell Scientific.")
    parser.add_argument("-i", "--input_dir", required=True, nargs='+', help="Jedna lub więcej ścieżek do katalogów z danymi wejściowymi.")
    parser.add_argument("-o", "--output_dir", required=True, help="Ścieżka do nadrzędnego katalogu wyjściowego.")
    parser.add_argument("-fid", "--file_id", default=None, help="Filtr/identyfikator. Może być kluczem grupy z FILE_ID_MERGE_GROUPS.")
    parser.add_argument("-int", "--interval", default=None, help="Wymusza interwał (np. '30min'). Może być nadpisany przez definicję grupy w file_id.")
    parser.add_argument("-ow", "--overwrite", action='store_true', default=False, help="Wymusza ponowne przetworzenie plików pasujących do kryteriów.")
    args = parser.parse_args()
    main(args.input_dir, args.output_dir, args.file_id, args.interval, args.overwrite)