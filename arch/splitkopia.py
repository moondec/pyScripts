# -*- coding: utf-8 -*-

"""
================================================================================
            Skrypt do Agregacji Danych Pomiarowych (Wersja 2.0)
================================================================================

Opis:
    Ten skrypt został zaprojektowany do automatyzacji procesu przetwarzania,
    czyszczenia i agregacji danych szeregów czasowych pochodzących z różnych
    systemów rejestrujących, w szczególności z urządzeń firmy Campbell Scientific.
    Głównym celem jest połączenie danych z wielu plików źródłowych (o różnych
    formatach) w spójne, roczne pliki wynikowe w formacie CSV.

    Wersja 2.0 wprowadza znaczące ulepszenia w architekturze, w tym:
    - Przetwarzanie równoległe (multiprocessing) w celu przyspieszenia pracy.
    - Profesjonalny system logowania z kontrolą poziomu szczegółowości.
    - Zoptymalizowane operacje na danych przy użyciu biblioteki pandas.
    - Pełną dokumentację kodu oraz testy jednostkowe.

Główne funkcjonalności:
    - Obsługa wielu formatów wejściowych: TOB1, TOA5, oraz prosty format CSV.
    - Dekodowanie specjalistycznych formatów binarnych, w tym FP2.
    - Elastyczne przetwarzanie interwałów czasowych.
    - Zaawansowane zarządzanie strefami czasowymi z możliwością korekt.
    - Agregacja i scalanie plików w ramach zdefiniowanych grup.
    - Mechanizm cache przyspieszający kolejne uruchomienia.
    - Diagnostyka i raportowanie błędów chronologii.
    - Równoległe przetwarzanie plików w celu maksymalnego wykorzystania zasobów CPU.

Wymagania:
    - Python 3.10+
    - Biblioteki: pandas, pytest (do testowania)
      pip install pandas pytest

Konfiguracja:
    Skrypt jest konfigurowany za pomocą słowników na początku kodu:
    1. FILE_ID_MERGE_GROUPS: Grupowanie plików źródłowych.
    2. TIMEZONE_CORRECTIONS: Reguły korekty stref czasowych.
    3. MANUAL_TIME_SHIFTS: Ręczne przesunięcia czasowe.
    4. CALIBRATION_RULES_BY_STATION: Reguły kalibracji sensorów.
    5. DATE_RANGES_TO_EXCLUDE: Okresy do wykluczenia z analizy.

Uruchamianie:
    Skrypt należy uruchamiać z wiersza poleceń.

    Składnia podstawowa:
    python split.py -i <katalog_wejsciowy> -o <katalog_wyjsciowy> -fid <id_grupy> [opcje]

    Nowe i zaktualizowane argumenty:
      -i, --input_dir       (Wymagany) Ścieżka do katalogu z danymi wejściowymi.
      -o, --output_dir      (Wymagany) Ścieżka do katalogu wyjściowego.
      -fid, --file_id       (Wymagany) Identyfikator grupy do przetworzenia.
      -j, --jobs            (Opcjonalny) Liczba równoległych procesów do użycia.
                            Domyślnie: liczba rdzeni procesora.
      --log-level           (Opcjonalny) Poziom logowania: DEBUG, INFO, WARNING, ERROR.
                            Domyślnie: INFO.
      --no-cache            (Opcjonalny) Flaga wyłączająca użycie cache.
                            Wymusza ponowne przetworzenie wszystkich plików.

    Przykłady użycia:
      # Przetworzenie grupy 'TU_MET_30min' z użyciem wszystkich rdzeni CPU
      python split.py -i /dane/tuczno -o /wyniki/tuczno -fid TU_MET_30min

      # Przetworzenie z użyciem 4 procesów i bardziej szczegółowym logowaniem
      python split.py -i /dane/rzecin -o /wyniki/rzecin -fid RZ_MET_30min --jobs 4 --log-level DEBUG

      # Przetworzenie z pominięciem cache
      python split.py -i /dane/brody -o /wyniki/brody -fid BR_MET_30min --no-cache

--------------------------------------------------------------------------------
    Autor: Marek Urbaniak
    Wersja: 2.0 - Zrefaktoryzowana, Równoległa
    Data ostatniej modyfikacji: 10.07.2025
--------------------------------------------------------------------------------
"""

import os
import pandas as pd
import struct
import re
import argparse
import math
import json
import logging
from logging.handlers import RotatingFileHandler
import multiprocessing
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from typing import List, Dict, Any, Tuple, Optional, Union

# --- Globalne definicje i słowniki konfiguracyjne ---
# CAMPBELL_EPOCH = pd.Timestamp('1990-01-01 00:00:00', tz='UTC')
CAMPBELL_EPOCH = pd.Timestamp('1990-01-01 00:00:00')
STRUCT_FORMAT_MAP = {'ULONG':'L', 'IEEE4':'f', 'IEEE8':'d', 'LONG':'l', 'BOOL':'?', 'SHORT':'h', 'USHORT':'H', 'BYTE':'b'}
CACHE_FILENAME = ".splitdata_cache.json"
CACHE_FILE_PATH = Path(__file__).parent / CACHE_FILENAME
LOG_FILENAME = "split_data.log"
LOG_FILE_PATH = Path(__file__).parent / LOG_FILENAME

# --- POCZĄTEK SEKCJI KONFIGURACJI ---

# 1. SŁOWNIK GRUPUJĄCY PLIKI ŹRÓDŁOWE
FILE_ID_MERGE_GROUPS = {
    # Rzecin
    'RZ_MET_30min': { 'source_ids': ['Meteo_32', 'meteo_Meteo_32', 'Meteo_30', 'CR1000_Meteo_30', '_LiCor'], 'interval': '30min' },
    'RZ_MET_1min': { 'source_ids': ['sky_SKYE_1min_', 'CR1000_2_meteo_SKYE_1min', 'CR1000_sky_SKYE_1min', '3_SKYE', 'sky_SKYE', 'CR1000_2_meteo_Meteo_1', 'CR1000_Meteo_1', 'CR1000_2_meteo_Meteo_2', 'CR1000_2_Meteo_2', 'CR1000_Methan_1', 'CR1000_Methan_2', 'Parsival', 'NetRadiometers', 'SWD'], 'interval': '1min' },
    'RZ_MET_30sec': { 'source_ids': ['CR1000_wet_RadTaRH', 'CR1000_wet_TEMP_PRT', 'CR1000_wet_Ts_P'], 'interval': '30s' }, # Zmieniono '30sec' na '30S' dla spójności
    # Tuczno
    'TU_MET_30min': { 'source_ids': ['CR5000_flux', 'Rad_tu', '_Bole_temp', '_meteo_Spec_idx', '_meteo_EneBal', '_meteo_Prec_Top', 'CR200Series_Table1', '_meteo_WXTmet', '_Results', '_profil_comp_integ', '_soil_Temperatury$', '_soil_Soil_HFP01', '_garden_RainGauge', '_garden_T107_gar'], 'interval': '30min' },
    'TU_MET_10min': { 'source_ids': ['_garden_CS_616', '_studnia_1_CS616S1'], 'interval': '10min' },
    'TU_MET_2min': { 'source_ids': ['CR1000_LI840_stor'], 'interval': '2min' },
    'TU_MET_1min': { 'source_ids': ['_Rad_1min'], 'interval': '1min' },
    'TU_MET_30sec': { 'source_ids': ['CR1000_soil2_SoTemS13'], 'interval': '30s' }, # Zmieniono '30sec' na '30S'
    'TU_MET_5sec': { 'source_ids': ['_soil_PPFD_under_tree', 'CR1000_meteo_GlobalRad', '_soil_Temperatury_5$', 'CR1000_meteo_GlobalRad'], 'interval': '5s' }, # Zmieniono '5sec' na '5S'
    'TU_MET_1sec': { 'source_ids': ['CR1000_profil_LI840', 'CR1000_LI840', 'CR1000_soil2_LI840'], 'interval': '1s' }, # Zmieniono '1sec' na '1S'
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
     'ME_TOP_MET_30min' : 'STACJA_ME_COMMON',   
     'ME_DOWN_MET_30min' : 'STACJA_ME_COMMON',
     'ME_Rain_down' : 'STACJA_ME_COMMON',
     'ME_DOWN_MET_1min' : 'STACJA_ME_COMMON',
     'ME_DOWN_MET_30min' : 'STACJA_ME_COMMON',
     'ME_Rain_top' : 'STACJA_ME_COMMON',
     'ME_CalPlates' : 'STACJA_ME_COMMON',
     'ME_ME_MET_10m' : 'STACJA_ME_COMMON',
    
     # 6.1. Definicja "konfiguracji-matki" dla stacji SA
    'STACJA_SA_COMMON': {
        'source_tz': 'Europe/Warsaw',
        'correction_end_date': '2022-01-14 11:00:00',
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    # 3.2. Definicja "konfiguracji-matki" dla stacji SA 
     'SA_MET_30min' : 'STACJA_SA_COMMON',   
     'SA_MET_1min' : 'STACJA_SA_COMMON'
}

# 3. SŁOWNIK RĘCZNYCH PRZESUNIĘĆ CZASOWYCH
MANUAL_TIME_SHIFTS = {
    'STACJA_TU_COMMON': [
        {'start': '2008-01-01 00:00:00', 'end': '2008-03-31 00:00:00', 'offset_hours': 1 },
        {'start': '2011-03-27 00:00:00', 'end': '2011-05-27 09:00:00', 'offset_hours': 1 },
    ],
    'TU_MET_30min': 'STACJA_TU_COMMON', 'TU_MET_10min': 'STACJA_TU_COMMON', 'TU_MET_1min': 'STACJA_TU_COMMON',
    'TU_MET_2min': 'STACJA_TU_COMMON', 'TU_MET_30sec': 'STACJA_TU_COMMON', 'TU_MET_5sec': 'STACJA_TU_COMMON', 'TU_MET_1sec': 'STACJA_TU_COMMON',

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
# --- KONIEC SEKCJI KONFIGURACJI ---


# --- MODUŁY POMOCNICZE I LOGOWANIA ---

def setup_logging(level: str = 'INFO'):
    """Konfiguruje system logowania."""
    log_level = getattr(logging, level.upper(), logging.INFO)
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    file_handler = RotatingFileHandler(LOG_FILE_PATH, maxBytes=5*1024*1024, backupCount=2, encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        root_logger.setLevel(log_level)
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)

def load_cache() -> Dict[str, Any]:
    """Wczytuje cache przetworzonych plików."""
    if CACHE_FILE_PATH.exists():
        try:
            with open(CACHE_FILE_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {}

def save_cache(data: Dict[str, Any]):
    """Zapisuje cache przetworzonych plików."""
    try:
        with open(CACHE_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
    except IOError as e:
        logging.error(f"Nie można zapisać pliku cache: {e}")

def is_file_in_cache(file_path: Path, cache: dict) -> bool:
    """Sprawdza, czy plik jest w cache i jest aktualny."""
    file_key = str(file_path.resolve())
    if file_key not in cache:
        return False
    try:
        file_stat = file_path.stat()
        if cache[file_key].get('mtime') == file_stat.st_mtime and cache[file_key].get('size') == file_stat.st_size:
            return True
    except FileNotFoundError:
        return False
    return False

def update_cache(processed_files: list, cache: dict):
    """Aktualizuje cache o informacje o przetworzonych plikach."""
    for file_path in processed_files:
        try:
            file_stat = file_path.stat()
            cache[str(file_path.resolve())] = {'mtime': file_stat.st_mtime, 'size': file_stat.st_size}
        except FileNotFoundError:
            continue

def parse_header_line(line): return [field.strip() for field in line.strip('"').split('","')]

# ======================================================================
# === POCZĄTEK BLOKU FUNKCJI POMOCNICZYCH ===
# ======================================================================

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
        # df = clean_and_validate_record_column(df, file_path)
        return df
    except Exception as e:print(f"Krytyczny błąd odczytu TOB1 {file_path}: {e}");return pd.DataFrame()

def get_toa5_metadata(file_path: Path) -> tuple[list[str], int] | None:
    """Parses a TOA5 file header."""
    try:
        with open(file_path, 'r', encoding='latin-1') as f:
            header_lines = [f.readline().strip() for _ in range(4)]
        if not header_lines[0].startswith('"TOA5"'): return None
        return parse_header_line(header_lines[1]), 4
    except Exception as e:
        logging.error(f"Błąd parsowania nagłówka TOA5 w {file_path.name}: {e}")
        return None

def read_toa5_data(file_path: Path, metadata: tuple) -> pd.DataFrame:
    """
    (Wersja 2.1) Wczytuje dane TOA5 w porcjach (chunks), aby oszczędzać pamięć
    przy bardzo dużych plikach.
    """
    col_names, num_header_lines = metadata
    all_chunks = []
    try:
        # Użyj chunksize, aby wczytywać plik porcjami po 100 000 wierszy
        chunk_iterator = pd.read_csv(
            file_path, skiprows=num_header_lines, header=None, names=col_names,
            na_values=['"NAN"', 'NAN', '"INF"', '""', ''], quotechar='"',
            encoding='latin-1', on_bad_lines='warn',
            chunksize=100_000
        )

        for chunk_df in chunk_iterator:
            if 'TIMESTAMP' in chunk_df.columns:
                # Wykonaj czyszczenie dat dla każdej porcji
                timestamps_str = chunk_df['TIMESTAMP'].astype(str)
                cleaned_timestamps_str = timestamps_str.str.replace('.0-', '-', regex=False)
                chunk_df['TIMESTAMP'] = pd.to_datetime(cleaned_timestamps_str, errors='coerce')
                all_chunks.append(chunk_df)
        
        if not all_chunks:
            return pd.DataFrame()

        # Połącz wszystkie przetworzone porcje w jedną ramkę
        final_df = pd.concat(all_chunks, ignore_index=True)
        final_df.dropna(subset=['TIMESTAMP'], inplace=True)
        return final_df
    except Exception as e:
        logging.error(f"Błąd odczytu danych TOA5 z {file_path.name}: {e}")
        return pd.DataFrame()

def read_simple_csv_data(file_path: Path) -> pd.DataFrame:
    """
    (Wersja 2.1) Wczytuje dane CSV w porcjach (chunks), aby oszczędzać pamięć.
    """
    all_chunks = []
    try:
        chunk_iterator = pd.read_csv(file_path, header=0, low_memory=False, 
                                     encoding='latin-1', on_bad_lines='warn',
                                     chunksize=100_000)
        
        for chunk_df in chunk_iterator:
            if 'Timestamp' not in chunk_df.columns:
                continue # Pomiń porcję bez kolumny czasu
            
            chunk_df.rename(columns={'Timestamp': 'TIMESTAMP'}, inplace=True)
            timestamps_str = chunk_df['TIMESTAMP'].astype(str)
            cleaned_timestamps_str = timestamps_str.str.replace('.0-', '-', regex=False)
            chunk_df['TIMESTAMP'] = pd.to_datetime(cleaned_timestamps_str, errors='coerce')
            all_chunks.append(chunk_df)

        if not all_chunks:
            return pd.DataFrame()
            
        final_df = pd.concat(all_chunks, ignore_index=True)
        final_df.dropna(subset=['TIMESTAMP'], inplace=True)
        return final_df
    except Exception as e:
        logging.error(f"Krytyczny błąd odczytu SimpleCSV z {file_path.name}: {e}")
        return pd.DataFrame()

def apply_timezone_correction(ts_series_naive: pd.Series, file_id: str) -> pd.Series:
    """
    (WERSJA OSTATECZNA 3.2) Stosuje korekty stref czasowych, dodając zabezpieczenie
    przed utratą typu 'datetime' po operacji concat.
    """
    if ts_series_naive.empty:
        return ts_series_naive
    
    config_entry = TIMEZONE_CORRECTIONS.get(file_id)
    final_config = None
    if isinstance(config_entry, str):
        final_config = TIMEZONE_CORRECTIONS.get(config_entry)
    elif isinstance(config_entry, dict):
        final_config = config_entry

    if not final_config:
        return ts_series_naive.dt.tz_localize('GMT', ambiguous='NaT', nonexistent='NaT')

    source_tz = final_config['source_tz']
    post_correction_tz = final_config['post_correction_tz']
    target_tz = final_config['target_tz']
    correction_end_date = pd.to_datetime(final_config['correction_end_date'])

    pre_mask = ts_series_naive <= correction_end_date
    post_mask = ~pre_mask

    pre_series = ts_series_naive[pre_mask].dt.tz_localize(source_tz, ambiguous='NaT', nonexistent='NaT')
    post_series = ts_series_naive[post_mask].dt.tz_localize(post_correction_tz, ambiguous='NaT', nonexistent='NaT')

    series_to_concat = []
    if not pre_series.empty:
        series_to_concat.append(pre_series)
    if not post_series.empty:
        series_to_concat.append(post_series)
    
    if not series_to_concat:
        return pd.Series(dtype=f'datetime64[ns, {target_tz}]')

    combined_series = pd.concat(series_to_concat).sort_index()

    # === POCZĄTEK KLUCZOWEJ ZMIANY ===
    # Dodajemy zabezpieczenie, wymuszając ponowną konwersję na typ datetime.
    # Zapobiega to błędowi, gdy `concat` zwraca serię o typie 'object'.
    combined_series = pd.to_datetime(combined_series, errors='coerce', utc=True)
    # === KONIEC KLUCZOWEJ ZMIANY ===

    # Usuwamy puste wartości, które mogły powstać po powyższej konwersji
    combined_series.dropna(inplace=True)
    if combined_series.empty:
        return combined_series

    return combined_series.dt.tz_convert(target_tz)

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
    
def apply_calibration(df: pd.DataFrame, file_id: str) -> pd.DataFrame:
    """Applies calibration rules to data columns."""
    station_name = STATION_MAPPING_FOR_CALIBRATION.get(file_id)
    if not station_name or station_name not in CALIBRATION_RULES_BY_STATION: return df
    
    column_rules = CALIBRATION_RULES_BY_STATION[station_name]
    df_calibrated = df.copy()
    for col_name, rules_list in column_rules.items():
        if col_name not in df_calibrated.columns: continue
        df_calibrated[col_name] = pd.to_numeric(df_calibrated[col_name], errors='coerce')
        for rule in rules_list:
            try:
                start_ts = pd.Timestamp(rule['start'], tz=df_calibrated['TIMESTAMP'].dt.tz)
                end_ts = pd.Timestamp(rule['end'], tz=df_calibrated['TIMESTAMP'].dt.tz)
                multiplier = float(rule.get('multiplier', 1.0))
                addend = float(rule.get('addend', 0.0))
                mask = (df_calibrated['TIMESTAMP'] >= start_ts) & (df_calibrated['TIMESTAMP'] <= end_ts)
                if mask.any():
                    df_calibrated.loc[mask, col_name] = (df_calibrated.loc[mask, col_name] * multiplier) + addend
            except Exception as e:
                logging.warning(f"Błąd reguły kalibracji dla '{col_name}': {e}.")
    return df_calibrated

def filter_by_date_ranges(df: pd.DataFrame, file_id: str) -> pd.DataFrame:
    """
    Filtruje DataFrame, usuwając wiersze z zdefiniowanych w konfiguracji
    zakresów dat (np. okresy awarii).

    Args:
        df: Ramka danych do przefiltrowania.
        file_id: Identyfikator grupy, używany do znalezienia reguł w DATE_RANGES_TO_EXCLUDE.

    Returns:
        Przefiltrowana ramka danych.
    """
    config_entry = DATE_RANGES_TO_EXCLUDE.get(file_id)
    rules = None

    # Rozwiązuje aliasy (np. 'ME_DOWN_MET_30min' -> 'FILTRY_DLA_MEZYKA_DOWN')
    if isinstance(config_entry, str):
        rules = DATE_RANGES_TO_EXCLUDE.get(config_entry)
    elif isinstance(config_entry, list):
        rules = config_entry
    
    if not rules or df.empty or 'TIMESTAMP' not in df.columns or df['TIMESTAMP'].dt.tz is None:
        return df

    df_filtered = df.copy()
    current_tz = df_filtered['TIMESTAMP'].dt.tz

    for i, rule in enumerate(rules):
        try:
            start_ts = pd.Timestamp(rule['start'], tz=current_tz)
            end_ts = pd.Timestamp(rule['end'], tz=current_tz)
            reason = rule.get('reason', 'brak opisu')

            # Maska wierszy do USUNIĘCIA
            exclusion_mask = (df_filtered['TIMESTAMP'] >= start_ts) & (df_filtered['TIMESTAMP'] <= end_ts)
            
            rows_to_remove = exclusion_mask.sum()
            if rows_to_remove > 0:
                logging.info(f"Filtr wykluczeń '{reason}': Usuwanie {rows_to_remove} wierszy.")
                df_filtered = df_filtered[~exclusion_mask]

        except Exception as e:
            logging.warning(f"Błąd podczas stosowania reguły filtrowania #{i+1} ({rule.get('reason', 'N/A')}): {e}.")
            
    return df_filtered

def align_timestamp(df: pd.DataFrame, force_interval: str) -> tuple[pd.DataFrame, str]:
    """Rounds timestamps to a specified frequency."""
    if df.empty: return df, "empty"
    
    try:
        df.loc[:, 'TIMESTAMP'] = df['TIMESTAMP'].dt.round(freq=force_interval)
        return df, force_interval
    except Exception as e:
        logging.error(f"Błąd podczas wyrównywania czasu do interwału '{force_interval}': {e}")
        return df, "error"
        
def filter_by_realistic_date_range(df: pd.DataFrame, file_path: Path) -> pd.DataFrame:
    """
    (WERSJA DOCELOWA 2.2) Usuwa wiersze z nierealistycznymi znacznikami czasu.
    Ujednolica strefy czasowe przed porównaniem, aby uniknąć błędów.
    """
    if df.empty or 'TIMESTAMP' not in df.columns:
        return df

    try:
        # Krok 1: Wstępne czyszczenie i konwersja (bez zmian)
        timestamps_str = df['TIMESTAMP'].astype(str)
        cleaned_timestamps_str = timestamps_str.str.replace('.0-', '-', regex=False)
        timestamps_series = pd.to_datetime(cleaned_timestamps_str, errors='coerce')

        # Krok 2: Wstępne filtrowanie na podstawie poprawności dat
        valid_mask = timestamps_series.notna()
        df = df[valid_mask]
        timestamps = timestamps_series[valid_mask]

        if timestamps.empty:
            return df

        # === POCZĄTEK KLUCZOWEJ ZMIANY ===

        # Krok 3: Ujednolicenie danych do strefy UTC na potrzeby porównania
        if timestamps.dt.tz is None:
            # Jeśli dane są "naiwne", przypisujemy im strefę UTC
            timestamps_utc = timestamps.dt.tz_localize('UTC')
        else:
            # Jeśli dane już mają strefę, konwertujemy je do UTC
            timestamps_utc = timestamps.dt.tz_convert('UTC')

        # Krok 4: Wyznaczenie mediany z danych w UTC
        median_year = timestamps_utc.dt.year.median()

        # Krok 5: Stworzenie dat granicznych, RÓWNIEŻ w strefie UTC
        start_date_utc = pd.Timestamp(f'{int(median_year) - 2}-01-01', tz='UTC')
        end_date_utc = pd.Timestamp(f'{int(median_year) + 2}-12-31', tz='UTC')

        # Krok 6: Wykonanie bezpiecznego porównania (obie strony są w UTC)
        final_mask = (timestamps_utc >= start_date_utc) & (timestamps_utc <= end_date_utc)
        
        # === KONIEC KLUCZOWEJ ZMIANY ===

        # Zastosuj ostateczną maskę do przefiltrowanego już DataFrame
        filtered_df = df.loc[final_mask]

        rows_removed = len(df) - len(filtered_df)
        if rows_removed > 0:
             logging.info(f"Filtr dat (mediana): Usunięto {rows_removed} wierszy spoza zakresu {int(median_year)}±2 lata w pliku {file_path.name}.")

        return filtered_df
        
    except Exception as e:
        logging.warning(f"Błąd podczas filtrowania po dacie (mediana) dla pliku {file_path.name}: {e}")
        return df

def correct_and_report_chronology(df: pd.DataFrame, file_path: Path) -> pd.DataFrame:
    """Sprawdza i koryguje błędy chronologii w plikach CSV."""
    if 'TIMESTAMP' not in df.columns or df['TIMESTAMP'].is_monotonic_increasing:
        return df
    logging.warning(f"Wykryto błąd chronologii w {file_path.name}. Próba sortowania.")
    return df.sort_values(by='TIMESTAMP', kind='mergesort').reset_index(drop=True)

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
    
# ======================================================================
# === KONIEC BLOKU FUNKCJI POMOCNICZYCH ===
# ======================================================================
    
# --- NOWA ARCHITEKTURA PRZETWARZANIA ---

def scan_for_files(input_dirs: List[str], source_ids: List[str]) -> List[Path]:
    """
    Scans input directories for files matching the source IDs.

    Args:
        input_dirs: A list of directories to scan.
        source_ids: A list of substrings to match in filenames. Supports '$' for end-of-string matching.

    Returns:
        A sorted list of absolute paths to the found files.
    """
    all_file_paths = []
    for input_dir in input_dirs:
        if not Path(input_dir).is_dir():
            logging.warning(f"Ścieżka '{input_dir}' nie jest katalogiem i zostanie pominięta.")
            continue
        for p in Path(input_dir).rglob('*'):
            if p.is_file():
                file_stem = p.stem
                for sid in source_ids:
                    if (sid.endswith('$') and file_stem.endswith(sid.rstrip('$'))) or (sid in p.name):
                        all_file_paths.append(p.resolve())
                        break
    return sorted(all_file_paths)
    
# ======================================================================
# === POCZĄTEK BLOKU Z KOMPLETNYM, POPRAWIONYM KODEM ===
# ======================================================================
def process_single_file(args: tuple) -> Optional[pd.DataFrame]:
    """
    Kompletny potok przetwarzania dla jednego pliku - ta funkcja jest uruchamiana równolegle.
    Odtwarza logikę z oryginalnego `read_and_process_one_file`.
    """
    file_path, config = args
    active_file_id = config['file_id']
    force_interval = config['interval']
    
    try:
        with open(file_path, 'r', encoding='latin-1') as f:
            first_line = f.readline().strip()
        
        df = pd.DataFrame()
        is_binary = False
        
        if first_line.startswith('"TOB1"'):
            is_binary = True
            metadata = get_tob1_metadata(file_path)
            if metadata: df = read_tob1_data(file_path, metadata)
        elif first_line.startswith('"TOA5"'):
            is_binary = True
            metadata = get_toa5_metadata(file_path)
            if metadata: df = read_toa5_data(file_path, metadata)
        elif first_line.startswith('"Timestamp"'):
            df = read_simple_csv_data(file_path)
            if not df.empty: df = correct_and_report_chronology(df, file_path)
        else:
            logging.warning(f"Nierozpoznany format pliku: {file_path.name}")
            return None

        if df.empty: return None

        # --- Kompletny potok korekt (zgodny z oryginałem) ---
        if is_binary:
            df = filter_by_realistic_date_range(df, file_path)
        
        if df.empty: return None

        df['TIMESTAMP'] = apply_timezone_correction(df['TIMESTAMP'], active_file_id)
        df.dropna(subset=['TIMESTAMP'], inplace=True)
        if df.empty: return None

        df = apply_manual_time_shifts(df, active_file_id)
        df = apply_calibration(df, active_file_id)
        df = filter_by_date_ranges(df, active_file_id)
        df, _ = align_timestamp(df, force_interval=force_interval)

        df['source_file'] = str(file_path.resolve())
        return df

    except Exception as e:
        logging.error(f"Krytyczny błąd podczas przetwarzania pliku {file_path.name}: {e}", exc_info=True)
        return None
        
def is_file_in_cache(file_path: Path, cache: dict[str, any]) -> bool:
    """Sprawdza, czy plik jest w cache i czy jego metadane się zgadzają."""
    file_key = str(file_path)
    if file_key not in cache:
        return False
    try:
        file_stat = file_path.stat()
        cached_info = cache[file_key]
        if cached_info.get('mtime') == file_stat.st_mtime and cached_info.get('size') == file_stat.st_size:
            return True
    except FileNotFoundError:
        return False
    return False

# def save_yearly_results(processed_dfs: List[pd.DataFrame], output_dir: str, file_id: str):
    # """
    # (WERSJA 3.2) Otrzymuje listę ramek danych, łączy je, agreguje przez scalanie,
    # porządkuje kolumny i zapisuje wyniki do rocznych plików CSV.
    # """
    # # === POCZĄTEK ZMIANY: Poprawna obsługa listy wejściowej ===

    # # 1. Sprawdzamy, czy lista wejściowa jest pusta.
    # if not processed_dfs:
        # logging.warning("Brak przetworzonych danych do zapisu.")
        # return

    # # 2. Łączymy listę ramek w jeden, duży DataFrame.
    # logging.info(f"Łączenie {len(processed_dfs)} ramek danych w jeden zbiór...")
    # final_df = pd.concat(processed_dfs, ignore_index=True)

    # if final_df.empty:
        # logging.warning("Po połączeniu ramek danych zbiór jest pusty. Nic nie zostanie zapisane.")
        # return

    # # === KONIEC ZMIANY ===

    # # Dalsza część funkcji działa już poprawnie na pojedynczej ramce 'final_df'
    # logging.info(f"Scalanie {len(final_df)} wierszy przez grupowanie po znaczniku czasu...")
    # final_df.sort_values(by=['TIMESTAMP', 'source_file'], kind='mergesort', inplace=True)
    # merged_df = final_df.groupby('TIMESTAMP').first().reset_index()
    # logging.info(f"Po scaleniu pozostało {len(merged_df)} unikalnych wierszy czasowych.")

    # output_path = Path(output_dir)
    # for year, year_group in merged_df.groupby(merged_df['TIMESTAMP'].dt.year):
        # try:
            # year_dir = output_path / str(year)
            # year_dir.mkdir(parents=True, exist_ok=True)
            # output_filename = f"{file_id}.csv"
            # output_filepath = year_dir / output_filename

            # all_columns = list(year_group.columns)
            # start_col = ['TIMESTAMP']
            # end_col = ['source_file'] if 'source_file' in all_columns else []
            # middle_cols = sorted([c for c in all_columns if c not in start_col + end_col])
            # final_order = start_col + middle_cols + end_col
            # year_group = year_group[final_order]

            # logging.info(f"Zapisywanie {len(year_group)} wierszy do pliku: {output_filepath}")
            # year_group.to_csv(output_filepath, index=False, date_format='%Y-%m-%d %H:%M:%S')

        # except Exception as e:
            # logging.error(f"Nie można zapisać danych dla roku {year} do pliku {output_filepath}: {e}")

def save_yearly_results(processed_dfs: List[pd.DataFrame], output_dir: str, file_id: str):
    """
    (WERSJA OSTATECZNA 4.0) W pełni zoptymalizowana pamięciowo. Przetwarza dane
    rok po roku, aby uniknąć tworzenia jednej, dużej ramki danych w pamięci.
    """
    if not processed_dfs:
        logging.warning("Brak przetworzonych danych do zapisu.")
        return

    # Krok 1: Grupujemy LISTĘ ramek danych po latach, nie łącząc ich jeszcze w jedną całość.
    logging.info("Grupowanie przetworzonych danych według lat...")
    data_by_year = defaultdict(list)
    for df in processed_dfs:
        if not df.empty:
            # Grupujemy każdą ramkę z osobna, na wypadek gdyby jeden plik zawierał dane z przełomu lat
            for year, year_group in df.groupby(df['TIMESTAMP'].dt.year):
                data_by_year[year].append(year_group)

    logging.info(f"Znaleziono dane dla {len(data_by_year)} lat. Rozpoczynanie zapisu rocznego...")
    
    # Krok 2: Przetwarzamy i zapisujemy dane w pętli, ROK PO ROKU.
    output_path = Path(output_dir)
    for year, new_dfs_list in sorted(data_by_year.items()):
        try:
            logging.info(f"--- Przetwarzanie roku: {year} ---")
            output_filepath = output_path / str(year) / f"{file_id}.csv"
            output_filepath.parent.mkdir(parents=True, exist_ok=True)
            
            # Krok 3: Wczytujemy istniejący plik TYLKO dla bieżącego roku
            try:
                existing_df = pd.read_csv(output_filepath, parse_dates=['TIMESTAMP'], low_memory=False)
                all_dfs_for_this_year = [existing_df] + new_dfs_list
            except FileNotFoundError:
                all_dfs_for_this_year = new_dfs_list
            
            # Krok 4: Łączymy dane TYLKO dla bieżącego roku. To jest mała i bezpieczna operacja.
            master_df_for_year = pd.concat(all_dfs_for_this_year, ignore_index=True)
            
            # Krok 5: Scalamy wiersze i sortujemy (logika z poprzednich kroków)
            master_df_for_year.sort_values(by=['TIMESTAMP', 'source_file'], kind='mergesort', inplace=True)
            final_year_df = master_df_for_year.groupby('TIMESTAMP').first().reset_index()

            # Krok 6: Porządkujemy kolumny
            all_columns = list(final_year_df.columns)
            start_col = ['TIMESTAMP']
            end_col = ['source_file'] if 'source_file' in all_columns else []
            middle_cols = sorted([c for c in all_columns if c not in start_col + end_col])
            final_order = start_col + middle_cols + end_col
            final_year_df = final_year_df[final_order]

            # Krok 7: Zapisujemy finalny plik dla tego roku
            logging.info(f"Zapisywanie {len(final_year_df)} wierszy do pliku: {output_filepath}")
            final_year_df.to_csv(output_filepath, index=False, date_format='%Y-%m-%d %H:%M:%S')

        except Exception as e:
            logging.error(f"Nie można zapisać danych dla roku {year} do pliku {output_filepath}: {e}")
            
def update_cache(processed_files: list[Path], cache: dict[str, any]):
    """Aktualizuje słownik cache o informacje o przetworzonych plikach."""
    for file_path in processed_files:
        try:
            file_stat = file_path.stat()
            cache[str(file_path)] = {
                'mtime': file_stat.st_mtime,
                'size': file_stat.st_size,
                'processed_at': datetime.now().isoformat()
            }
        except FileNotFoundError:
            continue

def aggregate_and_save_results(final_df: pd.DataFrame, output_dir: str, file_id: str):
    """Agreguje dane przez SCALANIE wierszy, a następnie zapisuje wyniki."""
    if final_df.empty:
        logging.warning("Otrzymano pustą ramkę danych do zapisu. Nic nie zostanie zapisane.")
        return

    logging.info(f"Scalanie {len(final_df)} wierszy przez grupowanie po znaczniku czasu...")
    final_df.sort_values(by=['TIMESTAMP', 'source_file'], kind='mergesort', inplace=True)
    merged_df = final_df.groupby('TIMESTAMP').first().reset_index()
    logging.info(f"Po scaleniu pozostało {len(merged_df)} unikalnych wierszy czasowych.")
    
    output_path = Path(output_dir)
    for year, year_group in merged_df.groupby(merged_df['TIMESTAMP'].dt.year):
        try:
            year_dir = output_path / str(year)
            year_dir.mkdir(parents=True, exist_ok=True)
            output_filename = f"{file_id}.csv"
            output_filepath = year_dir / output_filename
            logging.info(f"Zapisywanie {len(year_group)} wierszy do pliku: {output_filepath}")
            if 'source_file' in year_group.columns:
                year_group = year_group.drop(columns=['source_file'])
            year_group.to_csv(output_filepath, index=False, date_format='%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logging.error(f"Nie można zapisać danych dla roku {year} do pliku {output_filepath}: {e}")

def process_file_wrapper(args: tuple[Path, dict[str, any]]) -> pd.DataFrame | None:
    """Wrapper do wczytywania plików i stosowania WSTĘPNEJ, specyficznej dla formatu obróbki."""
    file_path, _ = args
    try:
        with open(file_path, 'r', encoding='latin-1') as f:
            first_line = f.readline().strip()

        df = pd.DataFrame()
        
        if first_line.startswith('"TOB1"') or first_line.startswith('"TOA5"'):
            metadata = None
            if first_line.startswith('"TOB1"'):
                metadata = get_tob1_metadata(file_path)
                if metadata: df = read_tob1_data(file_path, metadata)
            else:
                metadata = get_toa5_metadata(file_path)
                if metadata: df = read_toa5_data(file_path, metadata)
            
            if not df.empty:
                # Ten filtr jest teraz poprawny i powinien działać
                df = filter_by_realistic_date_range(df, file_path)
        elif first_line.startswith('"Timestamp"'):
            df = read_simple_csv_data(file_path)
            if not df.empty:
                df = correct_and_report_chronology(df, file_path)
        else:
            logging.warning(f"Nierozpoznany format pliku: {file_path.name}")
            return None

        if df.empty:
            return None
        
        df['source_file'] = str(file_path)
        return df
    except Exception as e:
        logging.error(f"Krytyczny błąd podczas przetwarzania pliku {file_path.name}: {e}", exc_info=True)
        return None

def main():
    """Główna funkcja orkiestrująca cały proces."""
    parser = argparse.ArgumentParser(description="Przetwarza i scala pliki danych Campbell Scientific.")
    parser.add_argument("-i", "--input_dir", required=True, nargs='+', help="Katalogi wejściowe.")
    parser.add_argument("-o", "--output_dir", required=True, help="Katalog wyjściowy.")
    parser.add_argument("-fid", "--file_id", required=True, help="Identyfikator grupy.")
    parser.add_argument("-j", "--jobs", type=int, default=os.cpu_count(), help="Liczba procesów.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Poziom logowania.")
    parser.add_argument("--no-cache", action='store_true', help="Wyłącza cache.")
    args = parser.parse_args()
    
    setup_logging(args.log_level)
    logging.info(f"{'='*20} Rozpoczęto przetwarzanie dla grupy: '{args.file_id}' {'='*20}")
    
    group_config = FILE_ID_MERGE_GROUPS.get(args.file_id)
    if not group_config:
        logging.error(f"Nie znaleziono definicji dla grupy '{args.file_id}'.")
        return
        
    group_config['file_id'] = args.file_id    

    processed_files_cache = load_cache() if not args.no_cache else {}
    all_files = scan_for_files(args.input_dir, group_config.get('source_ids', []))
    
    files_to_process = [p for p in all_files if not is_file_in_cache(p, processed_files_cache)]
    if not files_to_process:
        logging.info("Brak nowych lub zmodyfikowanych plików do przetworzenia. Zakończono.")
        return
        
    logging.info(f"Znaleziono {len(files_to_process)} nowych/zmienionych plików do przetworzenia.")

    # Etap 1: Przetwarzanie równoległe
    logging.info("Etap 1: Rozpoczynanie równoległego wczytywania i przetwarzania plików...")
    processing_args = [(path, group_config) for path in files_to_process]
    
    with multiprocessing.Pool(processes=args.jobs) as pool:
        results = pool.map(process_single_file, processing_args)
    
    successful_results = [df for df in results if df is not None and not df.empty]
    logging.info(f"Pomyślnie przetworzono {len(successful_results)} z {len(files_to_process)} plików.")

    # Etap 2: Zapis roczny z blokadą
    if successful_results:
        logging.info("Etap 2: Rozpoczynanie zapisu danych rok po roku...")
        save_yearly_results(successful_results, args.output_dir, args.file_id)
            
    # Etap 3: Aktualizacja cache
    if not args.no_cache:
        update_cache(files_to_process, processed_files_cache)
        save_cache(processed_files_cache)
        logging.info("Cache został zaktualizowany.")

    logging.info(f"{'='*20} Zakończono przetwarzanie dla grupy: '{args.file_id}' {'='*20}\n")

if __name__ == '__main__':
    # Ustawienie metody startowej jest ważne dla Windows/macOS
    if os.name == 'nt' or os.name == 'posix':
         multiprocessing.set_start_method('spawn', force=True)
    main()

# ======================================================================
# === KONIEC BLOKU Z KOMPLETNYM, POPRAWIONYM KODEM ===
# ======================================================================

if __name__ == '__main__':
    # Ustawienie metody startowej dla multiprocessing, ważne dla macOS i Windows
    multiprocessing.set_start_method('spawn', force=True)
    main()