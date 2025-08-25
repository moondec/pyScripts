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
    Wersja: 5.0
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

# 1. SŁOWNIK GRUPUJĄCY PLIKI ŹRÓDŁOWE
FILE_ID_MERGE_GROUPS = {
    # Rzecin
    'RZ_MET_30min': { 'source_ids': ['Meteo_32', 'meteo_Meteo_32', 'Meteo_30', 'CR1000_Meteo_30', '_LiCor'], 'interval': '30min' },
    'RZ_MET_1min': { 'source_ids': ['sky_SKYE_1min_', 'CR1000_2_meteo_SKYE_1min', 'CR1000_sky_SKYE_1min', '3_SKYE', 'sky_SKYE', 'CR1000_2_meteo_Meteo_1', 'CR1000_Meteo_1', 'CR1000_2_meteo_Meteo_2', 'CR1000_2_Meteo_2', 'CR1000_Methan_1', 'CR1000_Methan_2', 'Parsival', 'NetRadiometers', 'SWD'], 'interval': '1min' },
    'RZ_MET_30sec': { 'source_ids': ['CR1000_wet_RadTaRH', 'CR1000_wet_TEMP_PRT', 'CR1000_wet_Ts_P'], 'interval': '30S' }, # Zmieniono '30sec' na '30S' dla spójności
    # Tuczno
    'TU_MET_30min': { 'source_ids': ['CR5000_flux', 'Rad_tu', '_Bole_temp', '_meteo_Spec_idx', '_meteo_EneBal', '_meteo_Prec_Top', 'CR200Series_Table1', '_meteo_WXTmet', '_Results', '_profil_comp_integ', '_soil_Temperatury', '_soil_Soil_HFP01', '_garden_RainGauge', '_garden_T107_gar'], 'interval': '30min' },
    'TU_MET_10min': { 'source_ids': ['_garden_CS_616', '_studnia_1_CS616S1'], 'interval': '10min' },
    'TU_MET_2min': { 'source_ids': ['CR1000_LI840_stor'], 'interval': '2min' },
    'TU_MET_1min': { 'source_ids': ['_Rad_1min'], 'interval': '1min' },
    'TU_MET_30sec': { 'source_ids': ['CR1000_soil2_SoTemS13'], 'interval': '30S' }, # Zmieniono '30sec' na '30S'
    'TU_MET_5sec': { 'source_ids': ['_soil_PPFD_under_tree', 'CR1000_meteo_GlobalRad', '_soil_Temperatury_5', 'CR1000_meteo_GlobalRad'], 'interval': '5S' }, # Zmieniono '5sec' na '5S'
    'TU_MET_1sec': { 'source_ids': ['CR1000_profil_LI840', 'CR1000_LI840', 'CR1000_soil2_LI840'], 'interval': '1S' }, # Zmieniono '1sec' na '1S'
    # Brody
    'BR_MET_30min': { 'source_ids': ['CR3000_Barometr', 'CR3000_CR3000', 'CR3000_Multiplekser_dat', 'CR3000_Rain', 'CR3000_Spec_Veg_Ind'], 'interval': '30min' },
    'BR_MET_1min': { 'source_ids': ['CR3000_SpectralData'], 'interval': '1min' },
    # Tlen1
    'TL1_MET_30min': { 'source_ids': [ 'Rad_TR_30min', 'Soil_TR_30min' ], 'interval': '30min' },
    'TL1_MET_1min': { 'source_ids': [ 'Rad_TR_1min' ], 'interval': '1min' },
    # Tlen1a
    'TL1a_MET_30min': { 'source_ids': [ 'pom30m_', 'Tlen_1a_Biomet', 'Tlen_1a_cnf4_data', '_Soil_30min', 'Tlen1A_biomet_Biomet', 'Tlen1A_biomet_cnf4_data'], 'interval': '30min' },
    'TL1a_PREC_30min': { 'source_ids': [ 'deszcz_d_'], 'interval': '30min' },
    'TL1a_MET_1min': { 'source_ids': [ 'pom1m_', 'Tlen1A_biomet_Meteo_1min' ], 'interval': '1min' },
    'TL1a_CAL_1min': { 'source_ids': [ 'plytki_calib_' ], 'interval': '1min' },
    # Tlen2
    'TL2_MET_30min': { 'source_ids': [ 'pom30m_', 'TL2_30min', 'Tlen2_biomet_Biomet', 'Tlen_2_Soil_TL2_30min' , 'Tlen2_biomet_cnf4_data', 'Tlen2_biomet_Soil_30min'], 'interval': '30min' },
    'TL2_MET_1min': { 'source_ids': [ 'pom1m_', 'Tlen_2_Soil_moist_1min', 'Tlen2_biomet_Meteo_1min' ], 'interval': '1min' },
    'TL2_CAL_1min': { 'source_ids': [ 'plytki_calib_' ], 'interval': '1min' },
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

# 2. SŁOWNIK KOREKTY STREF CZASOWYCH
TIMEZONE_CORRECTIONS = {
    # 1.1. Definicja "konfiguracji-matki" dla stacji TU
    'STACJA_TU_COMMON': { 
        'source_tz': 'Europe/Warsaw', # Nazwa strefy czasowej, w której rejestrator zapisywał dane przed datą poprawki (np. 'Europe/Warsaw'). Używanie nazw z bazy IANA (jak Europe/Warsaw) jest kluczowe, ponieważ automatycznie obsługują one zarówno czas zimowy (CET, UTC+1), jak i letni (CEST, UTC+2).
        'correction_end_date': '2011-05-27 09:00:00', # Data i godzina, po której dane są już zapisywane poprawnie. Skrypt zastosuje specjalną korektę tylko do danych z timestampami wcześniejszymi lub równymi tej dacie.
        'post_correction_tz': 'UTC', # Strefa czasowa "poprawnych" danych (tych po correction_end_date). 
        'target_tz': 'GMT' # Docelowa, jednolita strefa czasowa dla wszystkich danych w plikach wynikowych. ("UTC/GMT +1 godzina"), użyjemy 'Etc/GMT-1'. (Uwaga: notacja Etc/GMT ma odwrócony znak, więc Etc/GMT-1 oznacza UTC+1).
    },
    
   # 1.2. Poszczególne file_id, które wskazują na wspólną konfigurację
    'TU_MET_30min': 'STACJA_TU_COMMON',
    'TU_MET_10min': 'STACJA_TU_COMMON',
    'TU_MET_1min': 'STACJA_TU_COMMON',
    'TU_MET_2min': 'STACJA_TU_COMMON',
    'TU_MET_30sec': 'STACJA_TU_COMMON',
    'TU_MET_5sec': 'STACJA_TU_COMMON',
    'TU_MET_1sec': 'STACJA_TU_COMMON',

    # 2.1. Definicja "konfiguracji-matki" dla stacji TL1
    'STACJA_TL1_COMMON': {
        'source_tz': 'Europe/Warsaw',
        'correction_end_date': '2020-05-10 12:00:00',
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'GMT'
    },
    
    'TL1_MET30min' : 'STACJA_TL1_COMMON',
    'TL1_MET1min' : 'STACJA_TL1_COMMON',
    
    # 3.1. Definicja "konfiguracji-matki" dla stacji TL1a 
    'STACJA_TL1a_COMMON': {
        'source_tz': 'Europe/Warsaw',
        'correction_end_date': '2050-05-10 12:00:00', # znajdź datę po której jest CET
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'GMT'
    },
    # # 3.2. Definicja "konfiguracji-matki" dla stacji TL1a (oryginalne dane są w czasie zimowym górowanie słońca o ok. 12:00, brak konieczności systemowej zmiany czasu zima/lato)
    # 'TL1a_MET_30min' : 'STACJA_TL1a_COMMON',
    # 'TL1a_MET_1min' : 'STACJA_TL1a_COMMON',
    # 'TL1a_PREC_30min' : 'STACJA_TL1a_COMMON',
    # 'TL1a_CAL_1min' : 'STACJA_TL1a_COMMON'
    
    # # 4.1. Definicja "konfiguracji-matki" dla stacji TL2 (oryginalne dane są w czasie zimowym górowanie słońca o ok. 12:00, brak konieczności systemowej zmiany czasu zima/lato)
    # 'STACJA_TL2_COMMON': {
        # 'source_tz': 'Europe/Warsaw',
        # 'correction_end_date': '2050-05-10 12:00:00',
        # 'post_correction_tz': 'Etc/GMT-1',
        # 'target_tz': 'Etc/GMT-1'
    # },   
    # # 4.2. Definicja "konfiguracji-matki" dla stacji TL2 (oryginalne dane są w czasie zimowym górowanie słońca o ok. 12:00, brak konieczności systemowej zmiany czasu zima/lato)
    # 'TL2_MET_30min' : 'STACJA_TL2_COMMON',
    # 'TL2_MET_1min' : 'STACJA_TL2_COMMON',
    # 'TL2_CAL_1min' : 'STACJA_TL2_COMMON',
    
    # # 5.1 Definicja "konfiguracji-matki" dla stacji ME (oryginalne dane są w czasie zimowym górowanie słońca o ok. 12:00, brak konieczności systemowej zmiany czasu zima/lato)
    # 'STACJA_ME_COMMON': {
        # 'source_tz': 'Europe/Warsaw',
        # 'correction_end_date': '2050-05-10 12:00:00',
        # 'post_correction_tz': 'Etc/GMT-1',
        # 'target_tz': 'Etc/GMT-1'
    # },
    ## 5.2. Definicja "konfiguracji-matki" dla stacji ME (oryginalne dane są w czasie zimowym górowanie słońca o ok. 12:00, brak konieczności systemowej zmiany czasu zima/lato)
     # 'STACJA_ME_COMMON' : 'ME_TOP_MET_30min',   
     # 'STACJA_ME_COMMON' : 'ME_DOWN_MET_30min',
     # 'STACJA_ME_COMMON' : 'Rain_down',
     # 'STACJA_ME_COMMON' : 'ME_DOWN_MET_1min',
     # 'STACJA_ME_COMMON' : 'ME_DOWN_MET_30min',
     # 'STACJA_ME_COMMON' : 'Rain_top',
     # 'STACJA_ME_COMMON' : 'CalPlates',
     # 'STACJA_ME_COMMON' : 'ME_MET_10m',
    
     # # 6.1. Definicja "konfiguracji-matki" dla stacji SA (oryginalne dane są w czasie zimowym górowanie słońca o ok. 12:00, brak konieczności systemowej zmiany czasu zima/lato)
    # 'STACJA_SA_COMMON': {
        # 'source_tz': 'Europe/Warsaw',
        # 'correction_end_date': '2022-01-14 11:00:00',
        # 'post_correction_tz': 'Etc/GMT-1',
        # 'target_tz': 'Etc/GMT-1'
    # },
    # # 3.2. Definicja "konfiguracji-matki" dla stacji SA (oryginalne dane są w czasie zimowym górowanie słońca o ok. 12:00, brak konieczności systemowej zmiany czasu zima/lato)
     # 'STACJA_SA_COMMON' : 'SA_MET_30min',   
     # 'STACJA_SA_COMMON' : 'SA_MET_1min'
}

# 3. SŁOWNIK RĘCZNYCH PRZESUNIĘĆ CZASOWYCH
MANUAL_TIME_SHIFTS = {
    'STACJA_TU_COMMON': [
        {'start': '2008-01-01 00:00:00', 'end': '2008-03-31 00:00:00', 'offset_hours': 1 },
        {'start': '2011-03-27 00:00:00', 'end': '2011-05-27 09:00:00', 'offset_hours': 1 },
    ],
    'TU_MET_30min': 'STACJA_TU_COMMON', 'TU_MET_10min': 'STACJA_TU_COMMON', 'TU_MET_1min': 'STACJA_TU_COMMON',
    'TU_MET_2min': 'STACJA_TU_COMMON', 'TU_MET_30S': 'STACJA_TU_COMMON', 'TU_MET5S': 'STACJA_TU_COMMON', 'TU_MET_1S': 'STACJA_TU_COMMON',

    'ME_DOWN_STATION_COMBINED': [
        {"start": "2018-01-11 08:07:00", "end": "2018-01-15 05:19:00", "offset_hours": 4.204861 },
        {"start": "2018-05-30 16:03:00", "end": "2018-06-02 21:40:00", "offset_hours": 59 }, 
        {"start": "2019-07-04 23:46:00", "end": "2019-07-08 20:21:00", "offset_hours": 18.43 },
        {"start": "2019-11-22 16:04:00", "end": "2019-11-28 06:59:00", "offset_hours": 5.7 },
        {"start": "2019-12-09 11:51:00", "end": "2019-12-31 23:59:00", "offset_hours": 2.62 },
        {"start": "2020-01-01 00:00:00", "end": "2020-01-06 11:59:00", "offset_hours": 2.62 },
        {"start": "2020-01-11 08:07:00", "end": "2020-01-15 05:19:00", "offset_hours": 4 },
        {"start": "2021-06-29 03:17:00", "end": "2021-06-30 08:17:00", "offset_hours": 557.76 },
        {"start": "2021-08-01 04:12:00", "end": "2021-08-04 08:59:00", "offset_hours": 37 },
        {"start": "2021-08-07 19:30:00", "end": "2021-08-08 19:42:00", "offset_hours": 111.45 },
        {"start": "2021-11-10 10:58:00", "end": "2021-11-12 00:42:00", "offset_hours": 14 },
        {"start": "2022-06-11 06:42:00", "end": "2022-06-13 02:03:00", "offset_hours": 1.805556 },
        {"start": "2022-05-30 08:21:00", "end": "2022-06-03 22:04:00", "offset_hours": 220 },
        {"start": "2023-02-06 12:17:00", "end": "2023-02-06 13:18:00", "offset_hours": 2.75 },
        {"start": "2023-02-12 10:13:00", "end": "2023-02-12 11:12:00", "offset_hours": 6.4666667 },
        {'start': '2024-11-19 13:41:00', 'end': '2024-11-20 07:35:00', "offset_hours": 60.15 },
        {'start': '2025-01-08 02:47:00', 'end': '2025-01-08 11:50:00', "offset_hours": 45.4 },
        {'start': '2025-01-30 16:57:00', 'end': '2025-02-17 10:18:00', "offset_hours": 25.2 },
        {'start': '2025-05-15 05:26:00', 'end': '2025-05-18 14:52:00', "offset_hours": 18 },
    ],
    'ME_DOWN_MET_30min' : 'ME_DOWN_STATION_COMBINED', 'ME_DOWN_MET_1min' : 'ME_DOWN_STATION_COMBINED',
    'ME_Rain_down' : 'ME_DOWN_STATION_COMBINED', 'ME_CalPlates' : 'ME_DOWN_STATION_COMBINED'
}

# 4. SŁOWNIK WYKLUCZEŃ
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
    'FILTRY_DLA_MEZYKA': [
        {
            'start': '2018-06-01 10:19:00',
            'end': '2018-06-03 14:04:00',
            'reason': 'Awaria rejestratora1'
        },
        {
            'start': '2020-01-10 06:57:00',
            'end': '2018-01-11 08:06:00',
            'reason': 'Awaria rejestratora2'
        },
        {
            'start': '2021-07-31 20:48:00',
            'end': '2021-08-01 04:11:00',
            'reason': 'Awaria rejestratora3'
        },
        {
            'start': '2021-09-22 12:51:00',
            'end': '2021-09-22 23:50:00',
            'reason': 'Awaria rejestratora3'
        },
        # {
            # 'start': "2018-11-17 14:21",
            # 'end': "2018-11-17 14:24",
            # 'reason': 'Awaria BC'
        # },        
        
            
    ],
    'ME_DOWN_MET_30min' : 'FILTRY_DLA_MEZYKA',
    'ME_DOWN_MET_1min' : 'FILTRY_DLA_MEZYKA',
    'Rain_down' : 'FILTRY_DLA_MEZYKA',
    'CalPlates' : 'FILTRY_DLA_MEZYKA',
}

# === KONIEC KONFIGURACJI UŻYTKOWNIKA ===
# --- Funkcje Pomocnicze (bez zmian) ---
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
def apply_timezone_correction(df, file_id, timestamp_col='TIMESTAMP'):
    if df.empty or timestamp_col not in df.columns or df[timestamp_col].isnull().all(): return df
    config_entry = TIMEZONE_CORRECTIONS.get(file_id); final_config = None
    if isinstance(config_entry, str):
        alias_name = config_entry
        if alias_name in TIMEZONE_CORRECTIONS and isinstance(TIMEZONE_CORRECTIONS[alias_name], dict): final_config = TIMEZONE_CORRECTIONS[alias_name]
    elif isinstance(config_entry, dict): final_config = config_entry
    df_out = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df_out[timestamp_col]):
        df_out[timestamp_col] = pd.to_datetime(df_out[timestamp_col], errors='coerce')
    df_out.dropna(subset=[timestamp_col], inplace=True)
    if df_out.empty: return df_out
    target_tz = final_config['target_tz'] if final_config and 'target_tz' in final_config else 'Etc/GMT-1'
    if df_out[timestamp_col].dt.tz is None:
        df_out.loc[:, timestamp_col] = df_out[timestamp_col].dt.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
    else:
        df_out.loc[:, timestamp_col] = df_out[timestamp_col].dt.tz_convert('UTC')
    df_out.dropna(subset=[timestamp_col], inplace=True)
    if df_out.empty: return df_out
    if final_config:
        source_tz = final_config['source_tz']
        correction_end_date = pd.Timestamp(final_config['correction_end_date'], tz=source_tz).tz_convert('UTC')
        pre_fix_mask = df_out[timestamp_col] <= correction_end_date
        if pre_fix_mask.any():
            pre_fix_series = df_out.loc[pre_fix_mask, timestamp_col].dt.tz_localize(None).dt.tz_localize(source_tz, ambiguous='NaT', nonexistent='NaT')
            df_out.loc[pre_fix_mask, timestamp_col] = pre_fix_series.dt.tz_convert('UTC')
    if not df_out.empty:
        df_out.loc[:, timestamp_col] = df_out[timestamp_col].dt.tz_convert(target_tz)
    return df_out.dropna(subset=[timestamp_col])
def apply_manual_time_shifts(df, file_id, timestamp_col='TIMESTAMP'):
    if file_id not in MANUAL_TIME_SHIFTS: return df
    rules = MANUAL_TIME_SHIFTS.get(file_id)
    if not isinstance(rules, list): 
        alias = MANUAL_TIME_SHIFTS.get(file_id)
        if isinstance(alias, str): rules = MANUAL_TIME_SHIFTS.get(alias)
        if not isinstance(rules, list): return df
    if df.empty or df[timestamp_col].dt.tz is None: return df
    df_out = df.copy() 
    current_tz = df_out[timestamp_col].dt.tz
    for i, rule in enumerate(rules):
        try:
            start_ts, end_ts = pd.Timestamp(rule['start'], tz=current_tz), pd.Timestamp(rule['end'], tz=current_tz)
            offset = pd.Timedelta(hours=rule['offset_hours'])
            mask = (df_out[timestamp_col] >= start_ts) & (df_out[timestamp_col] <= end_ts)
            if mask.any(): df_out.loc[mask, timestamp_col] = df_out.loc[mask, timestamp_col] + offset
        except Exception as e: print(f"  BŁĄD reguły manualnej #{i+1} dla '{file_id}': {e}.")
    return df_out
def apply_calibration(df, file_id):
    if df.empty: return df
    station_name = STATION_MAPPING_FOR_CALIBRATION.get(file_id)
    if not station_name: return df
    if station_name not in CALIBRATION_RULES_BY_STATION: return df
    column_rules = CALIBRATION_RULES_BY_STATION[station_name]
    if not isinstance(column_rules, dict): return df
    df_calibrated = df.copy()
    for col_name, rules_list in column_rules.items():
        if col_name not in df_calibrated.columns: continue
        df_calibrated[col_name] = pd.to_numeric(df_calibrated[col_name], errors='coerce')
        for i, rule in enumerate(rules_list):
            try:
                start_ts = pd.Timestamp(rule['start'], tz=df_calibrated['TIMESTAMP'].dt.tz)
                end_ts = pd.Timestamp(rule['end'], tz=df_calibrated['TIMESTAMP'].dt.tz)
                multiplier = float(rule.get('multiplier', 1.0)); addend = float(rule.get('addend', 0.0))
                mask = (df_calibrated['TIMESTAMP'] >= start_ts) & (df_calibrated['TIMESTAMP'] <= end_ts)
                if mask.any(): df_calibrated.loc[mask, col_name] = (df_calibrated.loc[mask, col_name] * multiplier) + addend
            except Exception as e: print(f"  BŁĄD podczas stosowania reguły kalibracji #{i+1} dla '{col_name}': {e}.")
    return df_calibrated
def filter_by_date_ranges(df, file_id, timestamp_col='TIMESTAMP'):
    if file_id not in DATE_RANGES_TO_EXCLUDE: return df
    rules = DATE_RANGES_TO_EXCLUDE.get(file_id)
    if not isinstance(rules, list):
        alias = DATE_RANGES_TO_EXCLUDE.get(file_id)
        if isinstance(alias, str): rules = DATE_RANGES_TO_EXCLUDE.get(alias)
        if not isinstance(rules, list): return df
    if df.empty or df[timestamp_col].dt.tz is None: return df
    df_filtered = df.copy()
    initial_rows = len(df_filtered)
    for i, rule in enumerate(rules):
        try:
            start_ts = pd.Timestamp(rule['start'], tz=df_filtered[timestamp_col].dt.tz)
            end_ts = pd.Timestamp(rule['end'], tz=df_filtered[timestamp_col].dt.tz)
            exclusion_mask = (df_filtered[timestamp_col] >= start_ts) & (df_filtered[timestamp_col] <= end_ts)
            if exclusion_mask.any(): df_filtered = df_filtered[~exclusion_mask]
        except Exception as e: print(f"  BŁĄD podczas stosowania reguły filtrowania #{i+1}: {e}.")
    rows_removed = initial_rows - len(df_filtered)
    if rows_removed > 0: print(f"  Info (Filtr): Usunięto {rows_removed} wierszy na podstawie zdefiniowanych okresów.")
    return df_filtered
def align_timestamp(df, timestamp_col='TIMESTAMP', force_interval=None):
    if df is None or df.empty or timestamp_col not in df.columns or df[timestamp_col].isnull().all(): return pd.DataFrame(columns=df.columns if df is not None else []), "no_timestamp_data"
    df_processed = df.copy();
    if not pd.api.types.is_datetime64_any_dtype(df_processed[timestamp_col]): df_processed[timestamp_col] = pd.to_datetime(df_processed[timestamp_col], errors='coerce')
    df_processed.dropna(subset=[timestamp_col], inplace=True)
    if df_processed.empty: return df_processed, "no_valid_timestamp"
    if not force_interval: return df_processed, "no_interval_forced"
    
    cleaned_interval = force_interval.strip()
    VALID_FREQ_MAP = {'subsecond': None, '1S': 'S', '2S': '2S', '5S': '5S', '10S': '10S', '30S': '30S', '1min': 'min', '2min': '2min', '5min': '5min', '10min': '10min', '15min': '15min', '30min': '30min', '1H': 'H', '1D': 'D'}
    if cleaned_interval not in VALID_FREQ_MAP: print(f"OSTRZEŻENIE: Nieprawidłowy wymuszony interwał '{cleaned_interval}'."); return df_processed, "invalid_forced_interval"
    actual_freq_for_pandas = VALID_FREQ_MAP[cleaned_interval]
    if actual_freq_for_pandas is None: return df_processed, "subsecond"
        
    try:
        df_processed.loc[:, timestamp_col] = df_processed[timestamp_col].dt.round(actual_freq_for_pandas)
        return df_processed, cleaned_interval
    except Exception as e_round: print(f"BŁĄD: Nie można zastosować wymuszonego interwału '{actual_freq_for_pandas}': {e_round}"); return df_processed, "error_forcing_interval"

# === GŁÓWNA, PRZEBUDOWANA FUNKCJA `main` ===
def main(input_dirs_arg, output_dir_arg, file_id_filter_arg, overwrite_arg):
    output_directory_base = output_dir_arg
    if not os.path.exists(output_directory_base):
        try: os.makedirs(output_directory_base); print(f"Utworzono katalog wyjściowy: {output_directory_base}")
        except OSError as e: print(f"BŁĄD: Nie można utworzyć '{output_directory_base}': {e}"); return
    
    if not file_id_filter_arg:
        print("BŁĄD: Podanie argumentu --file_id jest wymagane w tej architekturze skryptu.")
        return
    
    group_config = FILE_ID_MERGE_GROUPS.get(file_id_filter_arg)
    if not group_config:
        print(f"BŁĄD: Nie znaleziono definicji dla grupy '{file_id_filter_arg}' w FILE_ID_MERGE_GROUPS.")
        return
        
    source_ids_to_process = group_config.get('source_ids', [])
    effective_force_interval = group_config.get('interval')
    if not source_ids_to_process or not effective_force_interval:
        print(f"BŁĄD: Definicja dla grupy '{file_id_filter_arg}' jest niekompletna (wymagane 'source_ids' i 'interval').")
        return
    
    source_id_for_filename = file_id_filter_arg
    print(f"\nPrzetwarzanie dla grupy: '{source_id_for_filename}' z interwałem '{effective_force_interval}'")
    
    # Krok 1: Wyszukiwanie i sortowanie plików
    print("\nKrok 1: Wyszukiwanie i sortowanie plików źródłowych...")
    all_file_paths = []
    for input_dir in input_dirs_arg:
        if not os.path.isdir(input_dir):
            print(f"Ostrzeżenie: Ścieżka '{input_dir}' nie jest katalogiem i zostanie pominięta.")
            continue
        for root, _, files in os.walk(input_dir):
            for filename in files:
                if filename.startswith('.') or not any(sid in filename for sid in source_ids_to_process):
                    continue
                all_file_paths.append(os.path.abspath(os.path.join(root, filename)))
    all_file_paths.sort() 
    if not all_file_paths: print("Zakończono. Nie znaleziono pasujących plików do przetworzenia."); return
        
    print(f"Znaleziono {len(all_file_paths)} pasujących plików. Kolejność przetwarzania:")
    for path in all_file_paths: print(f"  -> {os.path.basename(path)}")

    # Krok 2: Wczytywanie
    print("\nKrok 2: Wczytywanie danych...")
    list_of_dfs = []
    for file_path in all_file_paths:
        df = pd.DataFrame()
        try:
            with open(file_path, 'r', encoding='latin-1') as f: first_line = f.readline().strip()
            if first_line.startswith('"TOB1"'):
                metadata = get_tob1_metadata(file_path)
                if metadata: df = read_tob1_data(file_path, metadata)
            elif first_line.startswith('"TOA5"'):
                metadata = get_toa5_metadata(file_path)
                if metadata: df = read_toa5_data(file_path, metadata)
            elif first_line.startswith('"Timestamp"'):
                df = read_simple_csv_data(file_path)
            
            if not df.empty and 'TIMESTAMP' in df.columns:
                df['source_file'] = os.path.basename(file_path)
                list_of_dfs.append(df)
        except Exception as e:
            print(f"Ostrzeżenie: Nie można wczytać pliku {os.path.basename(file_path)}. Błąd: {e}")
            
    if not list_of_dfs: print("Zakończono. Nie udało się wczytać danych z żadnego pliku."); return
    
    master_df = pd.concat(list_of_dfs, ignore_index=True)
    del list_of_dfs 
    print(f"Wczytano łącznie {len(master_df)} wierszy ze wszystkich plików.")
    
    # Krok 3: Globalne czyszczenie i przetwarzanie
    print("\nKrok 3: Stosowanie globalnych korekt i deduplikacji...")

    # a) Ujednolicenie i czyszczenie typów
    master_df['TIMESTAMP'] = pd.to_datetime(master_df['TIMESTAMP'], errors='coerce')
    master_df.dropna(subset=['TIMESTAMP'], inplace=True)
    for col in master_df.columns:
        if col not in ['TIMESTAMP', 'source_file']:
            master_df[col] = pd.to_numeric(master_df[col], errors='coerce')

    # b) Wstępna deduplikacja na podstawie DANYCH i CZASU
    initial_rows = len(master_df)
    cols_to_check = [col for col in master_df.columns if col != 'source_file']
    if cols_to_check:
        master_df.drop_duplicates(subset=cols_to_check, keep='first', inplace=True)
    if len(master_df) < initial_rows: print(f"  -> Usunięto {initial_rows - len(master_df)} wierszy będących idealnymi duplikatami.")

    # c) Korekty czasu i wartości
    master_df = apply_timezone_correction(master_df, file_id_filter_arg)
    master_df = apply_manual_time_shifts(master_df, file_id_filter_arg)
    master_df = filter_by_date_ranges(master_df, file_id_filter_arg)
    master_df = apply_calibration(master_df, file_id_filter_arg)
    
    # d) Wyrównanie czasu
    master_df, final_interval = align_timestamp(master_df, force_interval=effective_force_interval)
    if master_df.empty: print("Zakończono. Brak danych po korektach."); return
    
    # e) Ostateczna deduplikacja na podstawie czasu (inteligentne scalanie)
    initial_rows = len(master_df)
    final_df = master_df.groupby('TIMESTAMP').first().reset_index()
    if len(final_df) < initial_rows: print(f"  -> Scalono {initial_rows - len(final_df)} konfliktów czasowych po zaokrągleniu.")
            
    if final_df.empty: print("Zakończono. Brak danych po finalnej deduplikacji."); return
            
    # Krok 4: Zapis
    print(f"\nKrok 4: Zapisywanie wyników z interwałem '{final_interval}'...")
    final_df.sort_values(by='TIMESTAMP', inplace=True)
    
    for year, year_group in final_df.groupby(final_df['TIMESTAMP'].dt.year):
        year_dir = os.path.join(output_directory_base, str(year))
        if not os.path.exists(year_dir):
            try: os.makedirs(year_dir)
            except OSError as e: print(f"  BŁĄD: Nie można utworzyć '{year_dir}': {e}"); continue
            
        output_filename = f"{source_id_for_filename}.csv"
        output_filepath = os.path.join(year_dir, output_filename)
        
        print(f"  -> Zapisywanie pliku: {output_filename} (zawiera {len(year_group)} wierszy)")
        try:
            year_group.to_csv(output_filepath, index=False)
        except Exception as e:
            print(f"  Krytyczny błąd podczas zapisu do pliku {output_filepath}: {e}")

    print("\nZakończono pomyślnie.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Przetwarza i scala pliki danych Campbell Scientific.")
    parser.add_argument("-i", "--input_dir", required=True, nargs='+', help="Jedna lub więcej ścieżek do katalogów z danymi wejściowymi.")
    parser.add_argument("-o", "--output_dir", required=True, help="Ścieżka do nadrzędnego katalogu wyjściowego.")
    parser.add_argument("-fid", "--file_id", required=True, help="Wymagany identyfikator grupy zdefiniowanej w FILE_ID_MERGE_GROUPS.")
    parser.add_argument("-ow", "--overwrite", action='store_true', default=False, help="Zawsze nadpisuje pliki wynikowe.")
    args = parser.parse_args()
    main(args.input_dir, args.output_dir, args.file_id, args.overwrite)