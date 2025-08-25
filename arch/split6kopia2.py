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
    Wersja: 10 - Hybrydowa
    Data ostatniej modyfikacji: 09.06.2025
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
    'TU_MET_5sec': { 'source_ids': ['_soil_PPFD_under_tree', 'CR1000_meteo_GlobalRad', '_soil_Temperatury_5$', 'CR1000_meteo_GlobalRad'], 'interval': '5S' }, # Zmieniono '5sec' na '5S'
    'TU_MET_1sec': { 'source_ids': ['CR1000_profil_LI840', 'CR1000_LI840', 'CR1000_soil2_LI840'], 'interval': '1S' }, # Zmieniono '1sec' na '1S'
    # Brody
    'BR_MET_30min': { 'source_ids': ['CR3000_Barometr', 'CR3000_CR3000', 'CR3000_Multiplekser_dat', 'CR3000_Rain', 'CR3000_Spec_Veg_Ind'], 'interval': '30min' },
    'BR_MET_1min': { 'source_ids': ['CR3000_SpectralData'], 'interval': '1min' },
    # Tlen1
    'TL1_RAD_30min': { 'source_ids': [ 'TR_30min', 'TR_30min' ], 'interval': '30min' },
    'TL1_RAD_1min': { 'source_ids': [ 'TR_1min' ], 'interval': '1min' },
    # Tlen1a
    'TL1a_MET_30_dT': { 'source_ids': [ 'pom30m_'], 'interval': '30min' },
    'TL1a_Rain_down_30min': { 'source_ids': [ 'deszcz_d_'], 'interval': '30min' },
    'TL1a_MET_1_dT': { 'source_ids': [ 'pom1m_' ], 'interval': '1min' },
    'TL1a_CalPlates_1min': { 'source_ids': [ 'plytki_calib_' ], 'interval': '1min' },
    'TL1a_MET_30_csi': { 'source_ids': ['Tlen_1a_Biomet', 'Tlen_1a_cnf4_data', '_Soil_30min', 'Tlen1A_biomet_Biomet', 'Tlen1A_biomet_cnf4_data'], 'interval': '30min' },
    'TL1a_MET_1_csi': { 'source_ids': ['Tlen1A_biomet_Meteo_1min' ], 'interval': '1min' },
    # Tlen2
    # 'TL2_MET_30min': { 'source_ids': [ 'pom30m_', 'TL2_30min', 'Tlen2_biomet_Biomet', 'Tlen_2_Soil_TL2_30min' , 'Tlen2_biomet_cnf4_data', 'Tlen2_biomet_Soil_30min'], 'interval': '30min' },
    # 'TL2_MET_1min': { 'source_ids': [ 'pom1m_', 'Tlen_2_Soil_moist_1min', 'Tlen2_biomet_Meteo_1min' ], 'interval': '1min' },
    'TL2_CalPlates_dT': { 'source_ids': [ 'plytki_calib_' ], 'interval': '1min' },
    'TL2_MET_1_csi': { 'source_ids': [ 'Tlen_2_Soil_moist_1min', 'Tlen2_biomet_Meteo_1min' ], 'interval': '1min' },
    'TL2_MET_1_dT': { 'source_ids': [ 'pom1m_'], 'interval': '1min' },
    'TL2_MET_1m': { 'source_ids': [ 'Tlen_2_Soil_moist_1min'], 'interval': '1min' },
    'TL2_MET_30_csi': { 'source_ids': [ 'Tlen2_biomet_Biomet', 'Tlen2_biomet_cnf4_data', 'Tlen2_biomet_Soil_30min'], 'interval': '30min' },
    'TL2_MET_30_dT': { 'source_ids': [ 'pom30m_'], 'interval': '30min' },
    'TL2_MET_30m': { 'source_ids': [ 'TL2_30min'], 'interval': '30min' },
    # Mezyk
    'ME_TOP_MET_30min': { 'source_ids': [ 'pom30m_'], 'interval': '30min' },
    'ME_DOWN_MET_30min': { 'source_ids': [ 'pom30m_'], 'interval': '30min' },
    'ME_Rain_down': { 'source_ids': [ 'deszcz_d_'], 'interval': '30min' },
    'ME_Rain_top': { 'source_ids': [ 'deszcz_' ], 'interval': '30min' },
    'ME_MET_10m': { 'source_ids': [ 'pom10m_' ], 'interval': '10min' },
    'ME_TOP_MET_1min': { 'source_ids': [ 'pom1m_' ], 'interval': '1min' },
    'ME_DOWN_MET_1min': { 'source_ids': [ 'pom1m_' ], 'interval': '1min' },
    'ME_CalPlates': { 'source_ids': [ 'plytki_calib_' ], 'interval': '1min' },
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
        'post_correction_tz': 'Etc/GMT-1', # Strefa czasowa "poprawnych" danych (tych po correction_end_date). 
        'target_tz': 'Etc/GMT-1' # Docelowa, jednolita strefa czasowa dla wszystkich danych w plikach wynikowych. ("UTC/GMT +1 godzina"), użyjemy 'Etc/GMT-1'. (Uwaga: notacja Etc/GMT ma odwrócony znak, więc Etc/GMT-1 oznacza UTC+1).
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
        'source_tz': 'Etc/GMT',
        'correction_end_date': '2016-01-02 00:00:00',
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    
    'TL1_RAD_30min' : 'STACJA_TL1_COMMON',
    'TL1_RAD_1min' : 'STACJA_TL1_COMMON',
    
    # 3.1. Definicja "konfiguracji-matki" dla stacji TL1a 
    'STACJA_TL1a_COMMON': {
        'source_tz': 'Etc/GMT-2',
        'correction_end_date': '2050-05-10 12:00:00', 
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    # 3.2. Definicja "konfiguracji-matki" dla stacji TL1a 
    'TL1a_MET_30_dT' : 'STACJA_TL1a_COMMON',
    'TL1a_Rain_down_30min' : 'STACJA_TL1a_COMMON',
    'TL1a_MET_1_dT' : 'STACJA_TL1a_COMMON',
    'TL1a_CalPlates_1min' : 'STACJA_TL1a_COMMON',
    
    'STACJA_TL1anew_COMMON': {
        'source_tz': 'Etc/GMT-1',
        'correction_end_date': '2050-05-10 12:00:00', # znajdź datę po której jest CET
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    # 3.2. Definicja "konfiguracji-matki" dla stacji TL1a (oryginalne dane są w czasie zimowym górowanie słońca o ok. 12:00, brak konieczności systemowej zmiany czasu zima/lato)
    'TL1a_MET_30_csi' : 'STACJA_TL1anew_COMMON',
    'TL1a_MET_1_csi' : 'STACJA_TL1anew_COMMON',
    
    # 4.1. Definicja "konfiguracji-matki" dla stacji TL2 
    'STACJA_TL2_COMMON': {
        'source_tz': 'Etc/GMT-2',
        'correction_end_date': '2051-05-1 12:00:00',
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },   
    # 4.2. Definicja "konfiguracji-matki" dla stacji TL2 
    'TL2_CalPlates_dT' : 'STACJA_TL2_COMMON',   
    'TL2_MET_1_dT' : 'STACJA_TL2_COMMON',
    'TL2_MET_30_dT' : 'STACJA_TL2_COMMON',
    
    'STACJA_TL2_COMMON2': {
        'source_tz': 'Europe/Warsaw',
        'correction_end_date': '2014-10-26 1:00:00',
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },   
    # 4.2. Definicja "konfiguracji-matki" dla stacji TL2 
    'TL2_MET_30m' : 'STACJA_TL2_COMMON2',
    'TL2_MET_1m' : 'STACJA_TL2_COMMON2',
    
    'STACJA_TL2_COMMON_CSI': { #OK
        'source_tz': 'Etc/GMT-1',
        'correction_end_date': '2051-05-1 12:00:00',
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    'TL2_MET_1_csi' : 'STACJA_TL2_COMMON_CSI',
    'TL2_MET_30_csi' : 'STACJA_TL2_COMMON_CSI',
    # 'TL2_MET_30m' : 'STACJA_TL2_COMMON_CSI',
    # 'TL2_MET_1m' : 'STACJA_TL2_COMMON_CSI',
    
    # 5.1 Definicja "konfiguracji-matki" dla stacji ME 
    'STACJA_ME_COMMON': {
        'source_tz': 'GMT',
        'correction_end_date': '2050-05-10 12:00:00',
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    # 5.2. Definicja "konfiguracji-matki" dla stacji ME 
     'STACJA_ME_COMMON' : 'ME_TOP_MET_30min',   
     'STACJA_ME_COMMON' : 'ME_DOWN_MET_30min',
     'STACJA_ME_COMMON' : 'ME_Rain_down',
     'STACJA_ME_COMMON' : 'ME_DOWN_MET_1min',
     'STACJA_ME_COMMON' : 'ME_DOWN_MET_30min',
     'STACJA_ME_COMMON' : 'ME_Rain_top',
     'STACJA_ME_COMMON' : 'ME_CalPlates',
     'STACJA_ME_COMMON' : 'ME_ME_MET_10m',
    
     # 6.1. Definicja "konfiguracji-matki" dla stacji SA
    'STACJA_SA_COMMON': {
        'source_tz': 'Europe/Warsaw',
        'correction_end_date': '2022-01-14 11:00:00',
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    # 3.2. Definicja "konfiguracji-matki" dla stacji SA 
     'STACJA_SA_COMMON' : 'SA_MET_30min',   
     'STACJA_SA_COMMON' : 'SA_MET_1min'
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
        {"start": "2020-01-11 11:14:00", "end": "2020-01-15 15:19:00", "offset_hours": 100.91666 }, #100.95 109.8167
        # {"start": "2020-01-15 01:19:00", "end": "2020-01-15 05:19:00", "offset_hours": 8.866667 },
        {"start": "2021-06-29 03:17:00", "end": "2021-06-30 08:17:00", "offset_hours": 557.76 },
        {"start": "2021-08-01 04:12:00", "end": "2021-08-04 08:59:00", "offset_hours": 37 },
        {"start": "2021-08-07 19:30:00", "end": "2021-08-08 19:42:00", "offset_hours": 111.45 },
        {"start": "2021-11-10 10:58:00", "end": "2021-11-12 00:42:00", "offset_hours": 14 },
        {"start": "2022-05-30 08:21:00", "end": "2022-06-03 22:04:00", "offset_hours": 220 },
        {"start": "2022-06-11 08:42:00", "end": "2022-06-13 00:03:00", "offset_hours": 7.5 }, #
        {"start": "2023-02-06 12:17:00", "end": "2023-02-06 13:18:00", "offset_hours": 2.75 },
        {"start": "2023-02-12 10:13:00", "end": "2023-02-12 11:12:00", "offset_hours": 6.4666667 },
        {'start': '2024-11-19 13:41:00', 'end': '2024-11-20 07:35:00', "offset_hours": 60.15 },
        {'start': '2025-01-08 02:47:00', 'end': '2025-01-08 11:50:00', "offset_hours": 45.4 },
        {'start': '2025-01-30 16:57:00', 'end': '2025-02-17 10:18:00', "offset_hours": 25.2 },
        {'start': '2025-05-15 05:26:00', 'end': '2025-05-18 14:52:00', "offset_hours": 18 },
    ],
    'ME_DOWN_MET_30min' : 'ME_DOWN_STATION_COMBINED', 'ME_DOWN_MET_1min' : 'ME_DOWN_STATION_COMBINED',
    'ME_Rain_down' : 'ME_DOWN_STATION_COMBINED', 'ME_CalPlates' : 'ME_DOWN_STATION_COMBINED',
    
    'STACJA_TL2_old': [
        { "start": "2014-10-26 02:00:00", "end": "2015-03-19 11:49", "offset_hours": -2},
        { "start": "2015-03-19 11:50:00", "end": "2051-09-16", "offset_hours": -1},
    ],
    'TL2_MET_30m' : 'STACJA_TL2_old', 'TL2_MET_1m' : 'STACJA_TL2_old',
    
    'STACJA_TL1_old': [
        { "start": "2016-01-02 01:00:00", "end": "2055-03-19 11:49", "offset_hours": -1},
       
    ],
    'TL1_RAD_30min' : 'STACJA_TL1_old', 'TL1_RAD_1min' : 'STACJA_TL1_old',
    
    'STACJA_TL1_dT': [
        { "start": "2021-09-19 01:00:00", "end": "2055-03-19 11:49", "offset_hours": -1}, # godzina bez - znaczenia braki w danych
       
    ],
    'TL1a_MET_30_dT' : 'STACJA_TL1_dT', 'TL1a_MET_1_dT' : 'STACJA_TL1_dT', 'TL1a_Rain_down_30min' : 'STACJA_TL1_dT', 'TL1a_CalPlates_1min' : 'STACJA_TL1_dT'
}

# 4. SŁOWNIK KALIBRACJI
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
    },
    'TL1dT': {
        'SWin_1_1_1' : [
            {'start':  '2014-07-08 09:30:00', 'end' : '2018-11-13 9:30:00', 'multiplier': 77.101, 'addend': 0, 'reason': 'NR01 (data in W/m2)'},
        ],
        'PPFD_1_1_1' : [
            {'start':  '2014-07-08 09:30:00', 'end' : '2018-11-13 9:30:00', 'multiplier': 210.97, 'addend': 0, 'reason': 'PQ1 (data in umol/m2/s)'}
        ]
    },
    'TL2dT': {
        # 'G_2_1_1_Avg' : [
            # {'start':  '2014-07-10 09:30:00', 'end' : '2014-07-18 01:30:00', 'multiplier': 0.06246877, 'addend': 0, 'reason': 'Soil plates (data in W/m2)'}
        # ],
        # 'G_3_1_1_Avg' : [
            # {'start':  '2014-07-10 09:30:00', 'end' : '2014-07-18 01:30:00', 'multiplier': 16.008, 'addend': 0, 'reason': 'Soil plates (data in W/m2)'}
        # ],
        'SWin_1_2_1' : [
            {'start':  '2014-07-08 09:30:00', 'end' : '2018-11-16 12:30:00', 'multiplier': 69.638, 'addend': 0, 'reason': 'NR01 (data in W/m2)'},
        ],
        'PPFD_1_2_1' : [
            {'start':  '2014-07-08 09:30:00', 'end' : '2018-11-16 12:30:00', 'multiplier': 213.675, 'addend': 0, 'reason': 'PQ1 (data in umol/m2/s)'}
        ]
    }
}

STATION_MAPPING_FOR_CALIBRATION = {
    # 'ME_TOP_MET_30min': 'MEZYK_TOP',
    # 'ME_TOP_MET_1min' : 'MEZYK_TOP',
    # 'ME_Rain_top': 'MEZYK_TOP',

    # 'ME_DOWN_MET_30min' : 'MEZYK_DOWN',
    # 'ME_DOWN_MET_1min' : 'MEZYK_DOWN',
    # 'ME_Rain_down' : 'MEZYK_DOWN',
    # 'ME_CalPlates' : 'MEZYK_DOWN',
    
    # Dodaj tutaj mapowania dla innych stacji, np.
    # 'TU_STATION_COMBINED': 'TUCZNO',
    
    'TL2_MET_1_dT' : 'TL2dT',
    'TL2_MET_1m' : 'TL2dT',
    'TL2_MET_30m': 'TL2dT',
    
    'TL1a_MET_30_dT' : 'TL2dT',
    'TL1a_MET_1_dT' : 'TL2dT',

    # TL2_MET_30_dT
}

# 4. SŁOWNIK WYKLUCZEŃ
DATE_RANGES_TO_EXCLUDE = {
    # Przykład, dostosuj do swoich potrzeb
    'FILTRY_DLA_MEZYKA_DOWN': [
        {
            'start': '2018-06-01 10:19:00',
            'end': '2018-06-03 14:04:00',
            'reason': 'Awaria rejestratora1'
        },
        {
            'start': '2020-01-10 06:57:00', #'start': '2020-01-10 06:57:00',
            'end': '2020-01-11 16:14:00', # 'end': '2020-01-11 11:14:00',
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
            'reason': 'Awaria rejestratora4'
        },
        {
            'start': '2021-11-24 11:30:00',
            'end': '2021-11-24 13:30:00',
            'reason': 'Awaria rejestratora5'
        },
        # {
            # 'start': "2018-11-17 14:21",
            # 'end': "2018-11-17 14:24",
            # 'reason': 'Awaria BC'
        # },                   
    ],
    'ME_DOWN_MET_30min' : 'FILTRY_DLA_MEZYKA_DOWN',
    'ME_DOWN_MET_1min' : 'FILTRY_DLA_MEZYKA_DOWN',
    'ME_Rain_down' : 'FILTRY_DLA_MEZYKA_DOWN',
    'ME_CalPlates' : 'FILTRY_DLA_MEZYKA_DOWN',
    
    # 'FILTRY_DLA_TLEN2_dT': [
        # {
        # 'start': '2021-05-15 12:00:00',
        # 'end': '2021-11-25 13:30:00',
        # 'reason': 'Awaria rejestratora TL2'
        # }
    # ],
    
    # 'TL2_MET_1_csi' : 'FILTRY_DLA_TLEN2_dT',
}

# === KONIEC KONFIGURACJI UŻYTKOWNIKA ===
def load_cache(cache_path):
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r', encoding='utf-8') as f: return json.load(f)
        except (json.JSONDecodeError, IOError): pass
    return {}
def save_cache(data, cache_path):
    try:
        with open(cache_path, 'w', encoding='utf-8') as f: json.dump(data, f, indent=4)
    except IOError as e: print(f"BŁĄD: Nie można zapisać cache: {e}")
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
        # validate_record_column(df, file_path)
        df_cleaned = clean_and_validate_record_column(df, file_path)
        return df_cleaned
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
        # validate_record_column(df, file_path)
        df_cleaned = clean_and_validate_record_column(df, file_path)
        return df_cleaned
    except Exception:return pd.DataFrame()
def validate_record_column(df, file_path, timestamp_col='TIMESTAMP'):
    """Sprawdza spójność kolumny RECORD i raportuje anomalie."""
    if 'RECORD' not in df.columns:
        return

    # Upewnij się, że kolumna jest numeryczna, konwertując błędy na NaN, a następnie usuwając je
    records = pd.to_numeric(df['RECORD'], errors='coerce')
    valid_records = records.dropna()
    
    if valid_records.empty:
        return

    # Sprawdzenie 1: Wartości ujemne
    negative_indices = valid_records[valid_records < 0].index.tolist()

    # Sprawdzenie 2: Błędy w sekwencji (powinno być +1)
    diffs = valid_records.diff().dropna()
    # Ignorujemy poprawne przyrosty (+1) oraz prawdopodobne resety licznika (duże ujemne wartości)
    # Szukamy wszystkich innych przypadków
    non_sequential_indices = diffs[(diffs != 1) & (diffs > -1000)].index.tolist()

    # Jeśli znaleziono jakiekolwiek problemy, zaraportuj je
    if negative_indices or non_sequential_indices:
        print("\n" + "#"*30 + " OSTRZEŻENIE: Niespójna kolumna RECORD " + "#"*30)
        # print(f"  -> W pliku: {os.path.basename(file_path)}")
        print(f"  -> W pliku: {file_path}")
        if negative_indices:
            print(f"     - Wykryto wartości ujemne w wierszach (indeksy): {negative_indices}")
        if non_sequential_indices:
            print(f"     - Wykryto błędy sekwencji (skok inny niż +1) w wierszach (indeksy): {non_sequential_indices}")
        print("#"*95 + "\n")
def clean_and_validate_record_column(df: pd.DataFrame, file_path: str, timestamp_col='TIMESTAMP'):
    """
    Sprawdza spójność kolumny RECORD i filtruje wiersze, które nie pasują
    do chronologicznej sekwencji, zachowując te, które wracają do normy.
    """
    if 'RECORD' not in df.columns or df.empty:
        return df

    df_cleaned = df.copy()
    initial_rows = len(df_cleaned)

    # Krok 1: Wstępne czyszczenie (usuwanie zer i wartości nienumerycznych)
    df_cleaned['RECORD'] = pd.to_numeric(df_cleaned['RECORD'], errors='coerce')
    df_cleaned.dropna(subset=['RECORD'], inplace=True)
    df_cleaned['RECORD'] = df_cleaned['RECORD'].astype(int)
    # df_cleaned = df_cleaned[df_cleaned['RECORD'] > 0]
    
    if df_cleaned.empty:
        return df_cleaned

    # Krok 2: Inteligentne filtrowanie sekwencji
    good_indices = []
    last_valid_record = None

    for index, current_record in df_cleaned['RECORD'].items():
        if last_valid_record is None: # Pierwszy poprawny rekord zawsze jest dodawany
            good_indices.append(index)
            last_valid_record = current_record
            continue
            
        diff = current_record - last_valid_record
        
        # Warunek prawidłowy: przyrost o 1 LUB duży spadek (reset licznika)
        is_correct_step = (diff == 1) or (diff < -1000)
        
        if is_correct_step:
            good_indices.append(index)
            last_valid_record = current_record # Aktualizuj ostatni poprawny rekord
        else:
            # Ten wiersz jest błędem sekwencji - jest ignorowany.
            # `last_valid_record` pozostaje bez zmian, czekając na kolejny poprawny krok.
            pass

    rows_removed = initial_rows - len(good_indices)
    if rows_removed > 0:
        # print(f"  Info (Walidacja RECORD): Usunięto {rows_removed} wierszy z powodu błędów w sekwencji lub zerowych wartości w pliku {os.path.basename(file_path)}.")
        print(f"  Info (Walidacja RECORD): Usunięto {rows_removed} wierszy z powodu błędów w sekwencji lub zerowych wartości w pliku {file_path}.")
    return df.loc[good_indices]
def filter_by_realistic_date_range(df: pd.DataFrame, file_path: str, file_mtime: float, timestamp_col='TIMESTAMP'):
    """
    Usuwa wiersze z nierealistycznymi znacznikami czasu, bazując na
    dacie ostatniej modyfikacji pliku jako punkcie odniesienia.
    """
    if df.empty or timestamp_col not in df.columns:
        return df

    try:
        # Użyj daty modyfikacji pliku do ustalenia "prawidłowego" roku
        anchor_dt = datetime.fromtimestamp(file_mtime)
        anchor_year = anchor_dt.year
        
        # Utwórz dynamiczne okno czasowe: rok modyfikacji pliku +/- 1 rok
        # Tworzymy je jako świadome strefy UTC dla bezpiecznego porównywania
        start_date = pd.Timestamp(f'{anchor_year - 1}-01-01', tz='UTC')
        end_date = pd.Timestamp(f'{anchor_year + 1}-12-31', tz='UTC')

        # --- NOWA, BEZPIECZNA LOGIKA ---
        # Pracujemy na kopii oryginalnej kolumny
        timestamps_to_check = df[timestamp_col].copy()
        
        # Krok 1: Upewnij się, że dane są typu datetime i usuń błędy
        if not pd.api.types.is_datetime64_any_dtype(timestamps_to_check):
            timestamps_to_check = pd.to_datetime(timestamps_to_check, errors='coerce')
        
        timestamps_to_check.dropna(inplace=True)
        if timestamps_to_check.empty:
            return pd.DataFrame(columns=df.columns) # Zwróć pustą ramkę, jeśli nie ma poprawnych dat

        # Krok 2: Sprowadź do spójnego formatu UTC
        if timestamps_to_check.dt.tz is None:
            timestamps_utc = timestamps_to_check.dt.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
        else:
            timestamps_utc = timestamps_to_check.dt.tz_convert('UTC')
        
        timestamps_utc.dropna(inplace=True)
        if timestamps_utc.empty:
            return pd.DataFrame(columns=df.columns)

        # Krok 3: Stwórz maskę i bezpiecznie filtruj
        valid_range_mask = (timestamps_utc >= start_date) & (timestamps_utc <= end_date)
        
        # Pobierz indeksy poprawnych wierszy i na ich podstawie odfiltruj oryginalną ramkę
        valid_indices = timestamps_utc[valid_range_mask].index
        df_filtered = df.loc[valid_indices]
        
        rows_removed = len(df) - len(df_filtered)
        if rows_removed > 0:
            print(f"  Info (Filtr Dat): Usunięto {rows_removed} wierszy z nierealistyczną datą w pliku {os.path.basename(file_path)} (rok odniesienia: {anchor_year}).")
            
        return df_filtered
        
    except Exception as e:
        print(f"Ostrzeżenie: Błąd podczas filtrowania po dacie dla pliku {os.path.basename(file_path)}: {e}")
        return df # Zwróć oryginalny df w razie nieoczekiwanego błędu
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
    
    config_entry = TIMEZONE_CORRECTIONS.get(file_id)
    final_config = None
    if isinstance(config_entry, str):
        alias_name = config_entry
        if alias_name in TIMEZONE_CORRECTIONS and isinstance(TIMEZONE_CORRECTIONS[alias_name], dict): final_config = TIMEZONE_CORRECTIONS[alias_name]
    elif isinstance(config_entry, dict): final_config = config_entry
    
    corrected_series = pd.to_datetime(ts_series, errors='coerce').dropna()
    if corrected_series.empty: return corrected_series

    target_tz = final_config['target_tz'] if final_config and 'target_tz' in final_config else 'GMT'

    if corrected_series.dt.tz is None:
        corrected_series = corrected_series.dt.tz_localize('GMT', ambiguous='NaT', nonexistent='NaT')
    else:
        corrected_series = corrected_series.dt.tz_convert('GMT')
    
    corrected_series = corrected_series.dropna()
    if corrected_series.empty: return corrected_series
    
    if final_config:
        source_tz = final_config['source_tz']
        correction_end_date = pd.Timestamp(final_config['correction_end_date'], tz=source_tz).tz_convert('GMT')
        pre_fix_mask = corrected_series <= correction_end_date
        
        if pre_fix_mask.any():
            pre_fix_timestamps = corrected_series[pre_fix_mask]
            # Wykonaj "taniec stref" na kopii, aby uniknąć ostrzeżeń
            corrected_pre_fix = pre_fix_timestamps.copy().dt.tz_localize(None).dt.tz_localize(source_tz, ambiguous='NaT', nonexistent='NaT').dt.tz_convert('UTC')
            # Zaktualizuj główną serię
            corrected_series.update(corrected_pre_fix)
            
    return corrected_series.dt.tz_convert(target_tz)

def apply_manual_time_shifts(df, file_id, timestamp_col='TIMESTAMP'):
    if file_id not in MANUAL_TIME_SHIFTS: return df
    config_entry = MANUAL_TIME_SHIFTS.get(file_id); rules = None
    if isinstance(config_entry, str):
        alias_name = config_entry
        if alias_name in MANUAL_TIME_SHIFTS and isinstance(MANUAL_TIME_SHIFTS.get(alias_name), list): rules = MANUAL_TIME_SHIFTS[alias_name]
    elif isinstance(config_entry, list): rules = config_entry
    if not rules or df.empty or df[timestamp_col].dt.tz is None: return df
    
    df_out = df.copy(); current_tz = df_out[timestamp_col].dt.tz
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
    """Filtruje DataFrame, usuwając wiersze z zdefiniowanych zakresów dat."""
    if file_id not in DATE_RANGES_TO_EXCLUDE:
        return df

    config_entry = DATE_RANGES_TO_EXCLUDE.get(file_id)
    rules = None
    if isinstance(config_entry, str):
        alias_name = config_entry
        if alias_name in DATE_RANGES_TO_EXCLUDE and isinstance(DATE_RANGES_TO_EXCLUDE.get(alias_name), list):
            rules = DATE_RANGES_TO_EXCLUDE[alias_name]
    elif isinstance(config_entry, list):
        rules = config_entry
    
    if not rules or df.empty or df[timestamp_col].dt.tz is None:
        return df

    df_filtered = df.copy()
    initial_rows = len(df_filtered)
    current_tz = df_filtered[timestamp_col].dt.tz

    for i, rule in enumerate(rules):
        try:
            start_ts = pd.Timestamp(rule['start'], tz=current_tz)
            end_ts = pd.Timestamp(rule['end'], tz=current_tz)
            reason = rule.get('reason', 'brak opisu')

            exclusion_mask = (df_filtered[timestamp_col] >= start_ts) & (df_filtered[timestamp_col] <= end_ts)
            
            if exclusion_mask.any():
                print(f"  Info (Filtr): Stosowanie reguły '{reason}'. Usunięto {exclusion_mask.sum()} wierszy.")
                df_filtered = df_filtered[~exclusion_mask]

        except Exception as e:
            print(f"  BŁĄD podczas stosowania reguły filtrowania #{i+1}: {e}. Reguła pominięta.")
            
    return df_filtered
def align_timestamp(df, timestamp_col='TIMESTAMP', force_interval=None):
    if df is None or df.empty or timestamp_col not in df.columns or df[timestamp_col].isnull().all(): return pd.DataFrame(columns=df.columns if df is not None else []), "no_timestamp_data"
    df_processed = df.copy();
    if not pd.api.types.is_datetime64_any_dtype(df_processed[timestamp_col]): df_processed[timestamp_col] = pd.to_datetime(df_processed[timestamp_col], errors='coerce')
    df_processed.dropna(subset=[timestamp_col], inplace=True)
    if df_processed.empty: return df_processed, "no_valid_timestamp"
    if not force_interval: return df_processed, "no_interval_forced"
    
    cleaned_interval = force_interval.strip()
    VALID_FREQ_MAP = {'subsecond': None, '1S': 's', '2S': '2s', '5S': '5s', '10S': '10s', '30S': '30s', '1min': 'min', '2min': '2min', '5min': '5min', '10min': '10min', '15min': '15min', '30min': '30min', '1H': 'H', '1D': 'D'}
    if cleaned_interval not in VALID_FREQ_MAP: print(f"OSTRZEŻENIE: Nieprawidłowy wymuszony interwał '{cleaned_interval}'."); return df_processed, "invalid_forced_interval"
    actual_freq_for_pandas = VALID_FREQ_MAP[cleaned_interval]
    if actual_freq_for_pandas is None: return df_processed, "subsecond"
        
    try:
        df_processed.loc[:, timestamp_col] = df_processed[timestamp_col].dt.round(actual_freq_for_pandas)
        return df_processed, cleaned_interval
    except Exception as e_round: print(f"BŁĄD: Nie można zastosować wymuszonego interwału '{actual_freq_for_pandas}': {e_round}"); return df_processed, "error_forcing_interval"

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

def read_and_process_one_file(file_path: str, file_mtime: float, force_interval: str, active_file_id: str):
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
    
    if not current_df.empty:
        # current_df['source_file'] = os.path.basename(file_path)
        current_df['source_file'] = file_path

    if current_df.empty or 'TIMESTAMP' not in current_df.columns or current_df['TIMESTAMP'].dropna().empty:
        return pd.DataFrame(), None
    
    # --- Potok Przetwarzania ---
    
    # KROK 1: Korekta surowej chronologii (jeśli dotyczy)
    if is_simple_csv:
        current_df = correct_and_report_chronology(current_df, file_path)
    
    if not is_simple_csv:
        # Ten krok tylko dla TOB1/TOA5
        current_df = clean_and_validate_record_column(current_df, file_path)
        if current_df.empty: return pd.DataFrame(), None

        # Krok 2: Filtrowanie po realistycznej dacie, bazując na danych z kroku 1
        current_df = filter_by_realistic_date_range(current_df, file_path, file_mtime)
        if current_df.empty: return pd.DataFrame(), None
    
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
# === Główny potok przetwarzania dla jednego pliku ===
def process_single_file(file_path, file_mtime, active_file_id, force_interval):
    # 1. Wczytaj dane
    df = pd.DataFrame()
    with open(file_path, 'r', encoding='latin-1') as f: first_line = f.readline().strip()
    if first_line.startswith('"TOB1"'):
        metadata = get_tob1_metadata(file_path)
        if metadata: df = read_tob1_data(file_path, metadata)
    elif first_line.startswith('"TOA5"'):
        metadata = get_toa5_metadata(file_path)
        if metadata: df = read_toa5_data(file_path, metadata)
    else: return pd.DataFrame()
    if df.empty or 'TIMESTAMP' not in df.columns: return pd.DataFrame()

    # 2. Zastosuj pełen potok czyszczenia i korekt
    df = clean_and_validate_record_column(df, file_path)
    if df.empty: return df
    df = filter_by_realistic_date_range(df, file_path, file_mtime)
    if df.empty: return df
    df = apply_timezone_correction(df, active_file_id)
    if df.empty: return df
    df = apply_manual_time_shifts(df, active_file_id)
    df = apply_calibration(df, active_file_id)
    df, _ = align_timestamp(df, force_interval=force_interval)
    return df

# === Funkcja do przyrostowego zapisu i deduplikacji ===
def update_and_save_yearly_file(df_new, output_path, interval_label):
    if df_new.empty: return
    for year, year_group in df_new.groupby(df_new['TIMESTAMP'].dt.year):
        year_dir = os.path.join(output_path, str(year))
        if not os.path.exists(year_dir): os.makedirs(year_dir)
        output_filepath = os.path.join(year_dir, f"{os.path.basename(output_path)}_{interval_label}_{year}.csv")
        
        try:
            if os.path.exists(output_filepath):
                existing_df = pd.read_csv(output_filepath, parse_dates=['TIMESTAMP'], low_memory=False)
                if not existing_df.empty:
                    # Doprowadź strefy czasowe do spójności przed połączeniem
                    target_tz = year_group['TIMESTAMP'].dt.tz
                    if existing_df['TIMESTAMP'].dt.tz is None:
                        existing_df['TIMESTAMP'] = existing_df['TIMESTAMP'].dt.tz_localize('UTC').dt.tz_convert(target_tz)
                    elif str(existing_df['TIMESTAMP'].dt.tz) != str(target_tz):
                        existing_df['TIMESTAMP'] = existing_df['TIMESTAMP'].dt.tz_convert(target_tz)
                    combined_df = pd.concat([existing_df, year_group], ignore_index=True)
                else:
                    combined_df = year_group
            else:
                combined_df = year_group

            # Ostateczna, inteligentna deduplikacja
            final_df = combined_df.groupby('TIMESTAMP').first().reset_index()
            final_df.sort_values(by='TIMESTAMP', inplace=True)
            final_df.to_csv(output_filepath, index=False)
        except Exception as e:
            print(f"BŁĄD podczas aktualizacji pliku {output_filepath}: {e}")

# === OSTATECZNA ARCHITEKTURA `main` ===
def main(input_dirs_arg, output_dir_arg, file_id_filter_arg, overwrite_arg):
    output_directory_base = output_dir_arg
    if not os.path.exists(output_directory_base): os.makedirs(output_directory_base)
    if not file_id_filter_arg: print("BŁĄD: Podanie --file_id jest wymagane."); return
    
    group_config = FILE_ID_MERGE_GROUPS.get(file_id_filter_arg)
    if not group_config: print(f"BŁĄD: Nie znaleziono definicji dla grupy '{file_id_filter_arg}'."); return
    source_ids_to_process, effective_force_interval = group_config.get('source_ids', []), group_config.get('interval')
    if not source_ids_to_process or not effective_force_interval: print(f"BŁĄD: Definicja dla grupy '{file_id_filter_arg}' jest niekompletna."); return
        
    print(f"\nPrzetwarzanie dla grupy: '{file_id_filter_arg}' z interwałem '{effective_force_interval}'")
    
    # Krok 1: Wyszukiwanie i kategoryzacja plików
    print("\nKrok 1: Wyszukiwanie i kategoryzacja plików...")
    csv_files, other_files = [], []
    for input_dir in input_dirs_arg:
        if not os.path.isdir(input_dir):
            print(f"Ostrzeżenie: Ścieżka '{input_dir}' nie jest katalogiem."); continue
        for root, _, files in os.walk(input_dir):
            for filename in files:
                if filename.startswith('.'):
                    continue

                # === OTO POPRAWIONA I POPRAWNIE WSTAWIONA LOGIKA FILTROWANIA ===
                file_stem = os.path.splitext(filename)[0]
                match_found = False
                for sid in source_ids_to_process:
                    if sid.endswith('$'):
                        # Tryb precyzyjny: sprawdź, czy nazwa pliku KOŃCZY SIĘ na identyfikator
                        if file_stem.endswith(sid.rstrip('$')):
                            match_found = True
                            break
                    else:
                        # Tryb domyślny: sprawdź, czy nazwa pliku ZAWIERA identyfikator
                        if sid in filename:
                            match_found = True
                            break
                
                if not match_found:
                    continue
                # === KONIEC POPRAWIONEJ LOGIKI ===

                file_path = os.path.abspath(os.path.join(root, filename))
                try:
                    with open(file_path, 'r', encoding='latin-1') as f_check:
                        if f_check.readline().strip().startswith('"Timestamp"'):
                            csv_files.append(file_path)
                        else:
                            other_files.append(file_path)
                except Exception:
                    other_files.append(file_path)
    
    # Krok 2: Przetwarzanie globalne dla plików CSV
    processed_csv_df = pd.DataFrame()
    if csv_files:
        print(f"\nKrok 2a: Globalne przetwarzanie {len(csv_files)} plików CSV...")
        csv_files.sort()
        list_of_csv_dfs = [read_simple_csv_data(fp) for fp in csv_files]
        master_csv_df = pd.concat(list_of_csv_dfs, ignore_index=True)
        # Zastosuj wszystkie korekty do połączonego zbioru CSV
        master_csv_df = apply_timezone_correction(master_csv_df, file_id_filter_arg)
        master_csv_df = apply_manual_time_shifts(master_csv_df, file_id_filter_arg)
        master_csv_df = apply_calibration(master_csv_df, file_id_filter_arg)
        processed_csv_df, _ = align_timestamp(master_csv_df, force_interval=effective_force_interval)

    # Krok 3: Przetwarzanie strumieniowe dla TOA5/TOB1 z cache
    if other_files:
        print(f"\nKrok 2b: Strumieniowe przetwarzanie {len(other_files)} plików TOA5/TOB1...")
        processed_files_cache = load_cache(CACHE_FILE_PATH)
        current_run_cache = {}
        if overwrite_arg: print("Info: Użyto --overwrite. Cache zostanie zignorowany.")
        other_files.sort()
        for file_path in other_files:
            try:
                file_mtime, file_size = os.path.getmtime(file_path), os.path.getsize(file_path)
                if not overwrite_arg and file_path in processed_files_cache and processed_files_cache.get(file_path, {}).get('mtime') == file_mtime: continue
                print(f"  -> Przetwarzam: {os.path.basename(file_path)}")
                processed_df = process_single_file(file_path, file_mtime, file_id_filter_arg, effective_force_interval)
                if not processed_df.empty:
                    update_and_save_yearly_file(processed_df, output_directory_base, effective_force_interval)
                    current_run_cache[file_path] = {'mtime': file_mtime, 'size': file_size}
            except Exception as e: print(f"Ostrzeżenie: Nie można przetworzyć pliku {os.path.basename(file_path)}. Błąd: {e}")
        processed_files_cache.update(current_run_cache)
        save_cache(processed_files_cache, CACHE_FILE_PATH)

    # Krok 4: Zapisz globalnie przetworzone dane CSV
    if not processed_csv_df.empty:
        print("\nKrok 3: Scalanie danych CSV z istniejącymi plikami wynikowymi...")
        update_and_save_yearly_file(processed_csv_df, output_directory_base, effective_force_interval)

    print("\nZakończono pomyślnie.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Przetwarza i scala pliki danych Campbell Scientific.")
    parser.add_argument("-i", "--input_dir", required=True, nargs='+', help="Jedna lub więcej ścieżek do katalogów z danymi wejściowymi.")
    parser.add_argument("-o", "--output_dir", required=True, help="Ścieżka do nadrzędnego katalogu wyjściowego.")
    parser.add_argument("-fid", "--file_id", required=True, help="Wymagany identyfikator grupy zdefiniowanej w FILE_ID_MERGE_GROUPS.")
    parser.add_argument("-ow", "--overwrite", action='store_true', default=False, help="Ignoruje cache dla plików TOA5/TOB1.")
    args = parser.parse_args()
    main(args.input_dir, args.output_dir, args.file_id, args.overwrite)