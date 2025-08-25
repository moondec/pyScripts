# -*- coding: utf-8 -*-

"""
================================================================================
      Skrypt do Agregacji Danych (Wersja 6.0 - Baza Danych SQLite)
================================================================================
Opis:
    Wersja 6.0 porzuca zapis do plików CSV na rzecz centralnej bazy danych SQLite.
    Zapewnia to znacznie szybszy dostęp do danych i zaawansowane możliwości
    filtrowania za pomocą zapytań SQL.
    Pobierz program do wygodnej edycji bazy danych: https://sqlitebrowser.org/dl/

Architektura:
    - Skrypt przy pierwszym uruchomieniu tworzy plik bazy danych SQLite.
    - Schemat tabeli jest zarządzany dynamicznie: nowe kolumny (parametry) są
      automatycznie dodawane do bazy w miarę ich napotykania w danych.
    - Zastosowano logikę "UPSERT" (wstaw lub zamień), aby dane były zawsze
      aktualne, bez duplikatów.
    - Zachowano architekturę hybrydową: pliki binarne są przetwarzane równolegle,
      a pliki CSV sekwencyjnie w partiach.
      
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

import argparse
import json
import logging
import math
import multiprocessing
import os
import sqlite3
from collections import defaultdict
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm import tqdm
from scipy.io import loadmat

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
STATION_COORDINATES = {
    # Tuczno
    'TU_MET_30min': {'lat': 53.193, 'lon': 16.0974},
    'TU_MET_10min': {'lat': 53.193, 'lon': 16.0974},
    'TU_MET_2min': {'lat': 53.193, 'lon': 16.0974},
    'TU_MET_1min': {'lat': 53.193, 'lon': 16.0974},
    'TU_MET_30sec': {'lat': 53.193, 'lon': 16.0974},
    'TU_MET_5sec': {'lat': 53.193, 'lon': 16.0974},
    'TU_MET_1sec': {'lat': 53.193, 'lon': 16.0974},

    # Mezyk
    'ME_TOP_MET_30min': {'lat': 52.836980, 'lon': 16.252285},
    'ME_DOWN_MET_30min': {'lat': 52.836980, 'lon': 16.252285},
    'ME_Rain_down': {'lat': 52.836980, 'lon': 16.252285},
    'ME_Rain_top': {'lat': 52.836980, 'lon': 16.252285},
    'ME_MET_10m': {'lat': 52.836980, 'lon': 16.252285},
    'ME_TOP_MET_1min': {'lat': 52.836980, 'lon': 16.252285},
    'ME_DOWN_MET_1min': {'lat': 52.836980, 'lon': 16.252285},
    'ME_CalPlates': {'lat': 52.836980, 'lon': 16.252285},

    # Tlen - precyzyjne rozróżnienie
    'TL1_RAD_30min': {'lat': 53.634836, 'lon': 18.257957},      # Tlen1
    'TL1_RAD_1min': {'lat': 53.634836, 'lon': 18.257957},       # Tlen1
    'TL1a_MET_30_dT': {'lat': 53.634, 'lon': 18.2561},          # Tlen1a
    'TL1a_Rain_down_30min': {'lat': 53.634, 'lon': 18.2561},    # Tlen1a
    'TL1a_MET_1_dT': {'lat': 53.634, 'lon': 18.2561},           # Tlen1a
    'TL1a_CalPlates_1min': {'lat': 53.634, 'lon': 18.2561},      # Tlen1a
    'TL1a_MET_30_csi': {'lat': 53.634, 'lon': 18.2561},         # Tlen1a
    'TL1a_MET_1_csi': {'lat': 53.634, 'lon': 18.2561},          # Tlen1a
    'TL2_CalPlates_dT': {'lat': 53.6438, 'lon': 18.2864},       # Tlen2
    'TL2_MET_1_csi': {'lat': 53.6438, 'lon': 18.2864},          # Tlen2
    'TL2_MET_1_dT': {'lat': 53.6438, 'lon': 18.2864},           # Tlen2
    'TL2_MET_1m': {'lat': 53.6438, 'lon': 18.2864},             # Tlen2
    'TL2_MET_30_csi': {'lat': 53.6438, 'lon': 18.2864},         # Tlen2
    'TL2_MET_30_dT': {'lat': 53.6438, 'lon': 18.2864},          # Tlen2
    'TL2_MET_30m': {'lat': 53.6438, 'lon': 18.2864},            # Tlen2

    # Chlewiska - użyj kluczy zgodnych z Twoim FILE_ID_MERGE_GROUPS
    # Poniżej przykładowe nazwy, dostosuj je w razie potrzeby
    'CH_RZEPAK_2002': {'lat': 52.523366, 'lon': 16.620834},
    'CH_KUKURYDZA_2003': {'lat': 52.528773, 'lon': 16.617576},

    # Rzecin
    'RZ_MET_30min': {'lat': 52.762274, 'lon': 16.309501},
    'RZ_MET_1min': {'lat': 52.762274, 'lon': 16.309501},
    'RZ_MET_30sec': {'lat': 52.762274, 'lon': 16.309501},

    # Brody
    'BR_MET_30min': {'lat': 52.434198, 'lon': 16.299358},
    'BR_MET_1min': {'lat': 52.434198, 'lon': 16.299358},

    # Sarbia
    'SA_MET_30min': {'lat': 52.9756, 'lon': 16.7873},
    'SA_MET_1min': {'lat': 52.9756, 'lon': 16.7873}
}

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
    'TL1_RAD_30min': { 'source_ids': [ 'TR_30min' ], 'interval': '30min' },
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
    'TU_TZSHIFT': { 
        'source_tz': 'Europe/Warsaw', # Nazwa strefy czasowej, w której rejestrator zapisywał dane przed datą poprawki (np. 'Europe/Warsaw'). Używanie nazw z bazy IANA (jak Europe/Warsaw) jest kluczowe, ponieważ automatycznie obsługują one zarówno czas zimowy (CET, UTC+1), jak i letni (CEST, UTC+2).
        'correction_end_date': '2011-05-27 09:00:00', # Data i godzina, po której dane są już zapisywane poprawnie. Skrypt zastosuje specjalną korektę tylko do danych z timestampami wcześniejszymi lub równymi tej dacie.
        'post_correction_tz': 'Etc/GMT-1', # Strefa czasowa "poprawnych" danych (tych po correction_end_date). 
        'target_tz': 'Etc/GMT-1' # Docelowa, jednolita strefa czasowa dla wszystkich danych w plikach wynikowych. ("UTC/GMT +1 godzina"), użyjemy 'Etc/GMT-1'. (Uwaga: notacja Etc/GMT ma odwrócony znak, więc Etc/GMT-1 oznacza UTC+1).
    },
    
   # 1.2. Poszczególne file_id, które wskazują na wspólną konfigurację
    'TU_MET_30min': 'TU_TZSHIFT',
    'TU_MET_10min': 'TU_TZSHIFT',
    'TU_MET_1min': 'TU_TZSHIFT',
    'TU_MET_2min': 'TU_TZSHIFT',
    'TU_MET_30sec': 'TU_TZSHIFT',
    'TU_MET_5sec': 'TU_TZSHIFT',
    'TU_MET_1sec': 'TU_TZSHIFT',

    # 2.1. Definicja "konfiguracji-matki" dla stacji TL1
    'TL1_TZSHIFT': {
        'source_tz': 'Etc/GMT',
        'correction_end_date': '2012-01-02 00:00:00', # '2016-01-02 00:00:00'
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    
    'TL1_RAD_30min' : 'TL1_TZSHIFT',
    'TL1_RAD_1min' : 'TL1_TZSHIFT',
    
    # 3.1. Definicja "konfiguracji-matki" dla stacji TL1a 
    'TL1a_TZSHIFT': {
        'source_tz': 'Etc/GMT-2',
        'correction_end_date': '2050-05-10 12:00:00', 
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    # 3.2. Definicja "konfiguracji-matki" dla stacji TL1a 
    'TL1a_MET_30_dT' : 'TL1a_TZSHIFT',
    'TL1a_Rain_down_30min' : 'TL1a_TZSHIFT',
    'TL1a_MET_1_dT' : 'TL1a_TZSHIFT',
    'TL1a_CalPlates_1min' : 'TL1a_TZSHIFT',
    
    'TL1a_NEW_TZSHIFT': {
        'source_tz': 'Etc/GMT-1',
        'correction_end_date': '2050-05-10 12:00:00', # znajdź datę po której jest CET
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    # 3.2. Definicja "konfiguracji-matki" dla stacji TL1a (oryginalne dane są w czasie zimowym górowanie słońca o ok. 12:00, brak konieczności systemowej zmiany czasu zima/lato)
    'TL1a_MET_30_csi' : 'TL1a_NEW_TZSHIFT',
    'TL1a_MET_1_csi' : 'TL1a_NEW_TZSHIFT',
    
    # 4.1. Definicja "konfiguracji-matki" dla stacji TL2 
    'TL2_TZSHIFT': {
        'source_tz': 'Etc/GMT-2',
        'correction_end_date': '2051-05-1 12:00:00',
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },   
    # 4.2. Definicja "konfiguracji-matki" dla stacji TL2 
    'TL2_CalPlates_dT' : 'TL2_TZSHIFT',   
    'TL2_MET_1_dT' : 'TL2_TZSHIFT',
    'TL2_MET_30_dT' : 'TL2_TZSHIFT',
    
    'TL2_TZSHIFT2': {
        'source_tz': 'Europe/Warsaw',
        'correction_end_date': '2014-10-26 1:00:00',
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },   
    # 4.2. Definicja "konfiguracji-matki" dla stacji TL2 
    'TL2_MET_30m' : 'TL2_TZSHIFT2',
    'TL2_MET_1m' : 'TL2_TZSHIFT2',
    
    'TL2_TZSHIFT_CSI': { #OK
        'source_tz': 'Etc/GMT-1',
        'correction_end_date': '2051-05-1 12:00:00',
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    'TL2_MET_1_csi' : 'TL2_TZSHIFT_CSI',
    'TL2_MET_30_csi' : 'TL2_TZSHIFT_CSI',
    # 'TL2_MET_30m' : 'TL2_TZSHIFT_CSI',
    # 'TL2_MET_1m' : 'TL2_TZSHIFT_CSI',
    
    # 5.1 Definicja "konfiguracji-matki" dla stacji ME 
    'ME_TZSHIFT': {
        'source_tz': 'Etc/GMT-1', # GMT
        'correction_end_date': '2050-05-10 12:00:00',
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    # 5.2. Definicja "konfiguracji-matki" dla stacji ME 
     'ME_TOP_MET_30min' : 'ME_TZSHIFT',   
     'ME_DOWN_MET_30min' : 'ME_TZSHIFT',
     'ME_Rain_down' : 'ME_TZSHIFT',
     'ME_DOWN_MET_1min' : 'ME_TZSHIFT',
     'ME_DOWN_MET_30min' : 'ME_TZSHIFT',
     'ME_Rain_top' : 'ME_TZSHIFT',
     'ME_CalPlates' : 'ME_TZSHIFT',
     'ME_ME_MET_10m' : 'ME_TZSHIFT',
    
     # 6.1. Definicja "konfiguracji-matki" dla stacji SA
    'SA_TZSHIFT': {
        'source_tz': 'Europe/Warsaw',
        'correction_end_date': '2022-01-14 11:00:00',
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    # 3.2. Definicja "konfiguracji-matki" dla stacji SA 
     'SA_MET_30min' : 'SA_TZSHIFT',   
     'SA_MET_1min' : 'SA_TZSHIFT'
}

# 3. SŁOWNIK RĘCZNYCH PRZESUNIĘĆ CZASOWYCH
MANUAL_TIME_SHIFTS = {
    'TU_MTSHIFT': [
        {'start': '2008-01-01 00:00:00', 'end': '2008-03-31 00:00:00', 'offset_hours': 1 },
        {'start': '2011-03-27 00:00:00', 'end': '2011-05-27 09:00:00', 'offset_hours': 1 },
    ],
    'TU_MET_30min': 'TU_MTSHIFT', 'TU_MET_10min': 'TU_MTSHIFT', 'TU_MET_1min': 'TU_MTSHIFT',
    'TU_MET_2min': 'TU_MTSHIFT', 'TU_MET_30sec': 'TU_MTSHIFT', 'TU_MET_5sec': 'TU_MTSHIFT', 'TU_MET_1sec': 'TU_MTSHIFT',

    'ME_MTSHIFT': [
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
    'ME_DOWN_MET_30min' : 'ME_MTSHIFT', 'ME_DOWN_MET_1min' : 'ME_MTSHIFT',
    'ME_Rain_down' : 'ME_MTSHIFT', 'ME_CalPlates' : 'ME_MTSHIFT',
    
    'TL2_MTSHIFT': [
        { "start": "2014-10-26 02:00:00", "end": "2015-03-19 11:49", "offset_hours": -2},
        { "start": "2015-03-19 11:50:00", "end": "2051-09-16", "offset_hours": -1},
    ],
    'TL2_MET_30m' : 'TL2_MTSHIFT', 'TL2_MET_1m' : 'TL2_MTSHIFT',
    
    'TL1_MTSHIFT': [
        { "start": "2013-01-01 00:00:00", "end": "2055-08-12 14:00", "offset_hours": -2},
        { "start": "2015-08-12 15:00:00", "end": "2016-01-02 01:00:00", "offset_hours": 1},
        # { "start": "2016-01-02 01:00:00", "end": "2055-03-19 11:49", "offset_hours": -2}
    ],
    'TL1_RAD_30min' : 'TL1_MTSHIFT', 'TL1_RAD_1min' : 'TL1_MTSHIFT',
    
    'TL1_dT_MTSHIFT': [
        { "start": "2021-09-19 01:00:00", "end": "2055-03-19 11:49", "offset_hours": -1},
        { "start": "2021-09-19 01:00:00", "end": "2055-03-19 11:49", "offset_hours": -1}, # godzina bez - znaczenia braki w danych
       
    ],
    'TL1a_MET_30_dT' : 'TL1_dT_MTSHIFT', 'TL1a_MET_1_dT' : 'TL1_dT_MTSHIFT', 'TL1a_Rain_down_30min' : 'TL1_dT_MTSHIFT', 'TL1a_CalPlates_1min' : 'TL1_dT_MTSHIFT'
}

# 4. SŁOWNIK KALIBRACJI
CALIBRATION_RULES_BY_STATION = {
    'TUCZNO_CAL': {
        # Wszystkie reguły dla JEDNEJ kolumny muszą być w JEDNEJ liście
        'PPFD_IN_1_1_1': [
            {'start': '2008-01-01 00:00:00', 'end': '2011-01-19 16:00:00', 'multiplier': 0.6040033, 'addend': 0, 'reason': 'SKP215, (data in umol/m2/s1)'},
            {'start': '2011-03-08 07:30:00', 'end': '2011-04-20 17:00:00', 'multiplier': 0.6040033, 'addend': 0, 'reason': 'SKP215, (data in umol/m2/s1)'},
            # {'start': '2011-03-08 07:30:00', 'end': '2011-03-08 17:30:00', 'multiplier': 0.6040033, 'addend': 0, 'reason': 'SKP215, (data in umol/m2/s1)'},
            {'start': '2011-02-12 05:30:00', 'end': '2011-02-15 13:00:00', 'multiplier': 0.6040033, 'addend': 0, 'reason': 'SKP215 - korekta'}
        ],
        'PPFD_BC_IN_1_1_2': [
            {'start': '2018-08-08 13:30:00', 'end': '2018-11-17 12:00:00', 'multiplier': 1, 'addend': -650, 'reason': 'LQA3028 - stara korekta'},
            {'start': '2018-11-13 23:00:00', 'end': '2058-11-13 23:00:00', 'multiplier': 3288.716, 'addend': 0, 'reason': 'LQA3028, (data in umol/m2/s1)'},
            {'start': '2019-09-01 19:30:00', 'end': '2019-09-13 06:00:00', 'multiplier': 1, 'addend': -230, 'reason': 'LQA3028 - korekta'}
        ]
    },
    'MEZYK_DOWN_CAL': {
        # Wszystkie reguły dla JEDNEJ kolumny muszą być w JEDNEJ liście
        'PPFD_BC_IN_1_1_1': [
            {'start': '2018-11-13 23:00:00', 'end': '2058-11-13 23:00:00', 'multiplier': 3397.547, 'addend': 0, 'reason': 'LQA3027, (data in umol/m2/s1)'},
            {'start': '2019-09-01 19:30:00', 'end': '2019-09-13 06:00:00', 'multiplier': 1, 'addend': -230, 'reason': 'LQA3028 - korekta'}
        ],
        'PPFD_BC_IN_1_1_2': [
            {'start': '2018-08-08 13:30:00', 'end': '2018-11-17 12:00:00', 'multiplier': 1, 'addend': -650, 'reason': 'LQA3028 - stara korekta'},
            {'start': '2018-11-13 23:00:00', 'end': '2058-11-13 23:00:00', 'multiplier': 3288.716, 'addend': 0, 'reason': 'LQA3028, (data in umol/m2/s1)'},
            {'start': '2019-09-01 19:30:00', 'end': '2019-09-13 06:00:00', 'multiplier': 1, 'addend': -230, 'reason': 'LQA3028 - korekta'}
        ],
        'G_1_1_1': [
            {'start': '2018-06-16 14:00:00', 'end': '2018-11-14', 'multiplier': 15.865, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
            {'start': '2018-06-16 14:00:00', 'end': '2018-07-16 19:00:00', 'multiplier': 0.1, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'}
        ],
        'G_2_1_1': [
            {'start': '2018-06-16 14:00:00', 'end': '2018-11-14', 'multiplier': 15.830, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
            {'start': '2018-06-16 14:00:00', 'end': '2018-07-16 19:00:00', 'multiplier': 0.1, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'}
        ],
        'G_3_1_1': [
            {'start': '2018-06-16 14:00:00', 'end': '2018-11-14', 'multiplier': 15.110, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
            {'start': '2018-06-16 14:00:00', 'end': '2018-07-16 19:00:00', 'multiplier': 0.1, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'}
        ],
        'G_4_1_1': [
              {'start': '2018-06-16 14:00:00', 'end': '2018-11-14', 'multiplier': 16.168, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
              {'start': '2018-06-16 14:00:00', 'end': '2018-07-16 19:00:00', 'multiplier': 0.1, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'}              
        ],
        'G_5_1_1': [
            {'start': '2018-06-16 14:00:00', 'end': '2018-11-14', 'multiplier': 15.681, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
            {'start': '2018-06-16 14:00:00', 'end': '2018-07-16 19:00:00', 'multiplier': 0.1, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'}            
        ],
        'G_6_1_1': [
             {'start': '2018-06-16 14:00:00', 'end': '2018-11-14', 'multiplier': 15.530, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
             {'start': '2018-06-16 14:00:00', 'end': '2018-07-16 19:00:00', 'multiplier': 0.1, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'}
        ],
        'G_7_1_1': [
            {'start': '2018-06-16 14:00:00', 'end': '2018-11-14', 'multiplier': 15.681, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
            {'start': '2018-06-16 14:00:00', 'end': '2018-07-16 19:00:00', 'multiplier': 0.1, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'}
        ],
        'G_8_1_1': [
            {'start': '2018-06-16 14:00:00', 'end': '2018-11-14', 'multiplier': 15.929, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
            {'start': '2018-06-16 14:00:00', 'end': '2018-07-16 19:00:00', 'multiplier': 0.1, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'}            
        ],
        'G_9_1_1': [
             {'start': '2018-06-16 14:00:00', 'end': '2018-11-14', 'multiplier': 15.743, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
             {'start': '2018-06-16 14:00:00', 'end': '2018-07-16 19:00:00', 'multiplier': 0.1, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'}             
        ],
        'G_10_1_1': [
             {'start': '2018-06-16 14:00:00', 'end': '2018-11-14', 'multiplier': 15.718, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
             {'start': '2018-06-16 14:00:00', 'end': '2018-07-16 19:00:00', 'multiplier': 0.1, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'}             
        ],
    },
    'MEZYK_TOP_CAL': {
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
    'TL1dT_CAL': {
        'SWin_1_1_1' : [
            {'start':  '2014-07-08 09:30:00', 'end' : '2018-11-13 9:30:00', 'multiplier': 77.101, 'addend': 0, 'reason': 'NR01 (data in W/m2)'},
        ],
        'PPFD_1_1_1' : [
            {'start':  '2014-07-08 09:30:00', 'end' : '2018-11-13 9:30:00', 'multiplier': 210.97, 'addend': 0, 'reason': 'PQ1 (data in umol/m2/s)'}
        ]
    },
    'TL2dT_CAL': {
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
    'ME_TOP_MET_30min': 'MEZYK_TOP_CAL',
    'ME_TOP_MET_1min' : 'MEZYK_TOP_CAL',
    'ME_Rain_top': 'MEZYK_TOP_CAL',

    'ME_DOWN_MET_30min' : 'MEZYK_DOWN_CAL',
    'ME_DOWN_MET_1min' : 'MEZYK_DOWN_CAL',
    'ME_Rain_down' : 'MEZYK_DOWN_CAL',
    'ME_CalPlates' : 'MEZYK_DOWN_CAL',
    
    # Dodaj tutaj mapowania dla innych stacji, np.
    # 'TU_STATION_COMBINED': 'TUCZNO',
    
    'TL2_MET_1_dT' : 'TL2dT_CAL',
    'TL2_MET_1m' : 'TL2dT_CAL',
    'TL2_MET_30m': 'TL2dT_CAL',
    
    'TL1a_MET_30_dT' : 'TL2dT_CAL',
    'TL1a_MET_1_dT' : 'TL2dT_CAL',

    # TL2_MET_30_dT
}

# 4. SŁOWNIK FLAG JAKOŚCI
# flag_value to liczba całkowita, 3 = brak czujnika, zdemontowany, 2 = błąd krytyczny, 1 = wartość podejrzana, 0 = dane dobre.
QUALITY_FLAGS = {
    # Kluczem jest grupa zdefiniowana w słowniku FILE_ID_MERGE_GROUPS
    'ME_DOWN_QF': {
        # Kluczem jest nazwa kolumny (parametru)
        '*': [
            {'start': '2018-06-01 10:19:00', 'end': '2018-06-03 14:04:00', 'flag_value': 2, 'reason': 'Awaria rejestratora1'},
            {'start': '2020-01-10 06:57:00', 'end': '2020-01-11 16:14:00', 'flag_value': 2, 'reason': 'Awaria rejestratora2'},
            {'start': '2021-07-31 20:48:00', 'end': '2021-08-01 04:11:00', 'flag_value': 2, 'reason': 'Awaria rejestratora3'},
            {'start': '2021-09-22 12:51:00', 'end': '2021-09-22 23:50:00', 'flag_value': 2, 'reason': 'Awaria rejestratora4'},
            {'start': '2021-11-24 11:30:00', 'end': '2021-11-24 13:30:00', 'flag_value': 2, 'reason': 'Awaria rejestratora5'},
            {'start': '2020-01-10 06:57:00', 'end': '2020-01-11 16:14:00', 'flag_value': 2, 'reason': 'Awaria rejestratora2'}
        ],
        'G_1_1_1': [
            {'start': '2019-09-01 16:00:00', 'end': '2019-09-13 00:00:00', 'flag_value': 2, 'reason': 'Nierealne wartości strumienia G'}
        ],
        'G_2_1_1': [
            {'start': '2019-09-01 16:00:00', 'end': '2019-09-13 00:00:00', 'flag_value': 2, 'reason': 'Nierealne wartości strumienia G'}
        ],
        'G_3_1_1': [
            {'start': '2019-09-01 16:00:00', 'end': '2019-09-13 00:00:00', 'flag_value': 2, 'reason': 'Nierealne wartości strumienia G'}
        ],
        'G_5_1_1': [
            {'start': '2019-09-01 16:00:00', 'end': '2019-09-13 00:00:00', 'flag_value': 2, 'reason': 'Nierealne wartości strumienia G'}
        ],
        'G_6_1_1': [
            {'start': '2019-09-01 16:00:00', 'end': '2019-09-13 00:00:00', 'flag_value': 2, 'reason': 'Nierealne wartości strumienia G'}
        ],
        'G_7_1_1': [
            {'start': '2019-09-01 16:00:00', 'end': '2019-09-13 00:00:00', 'flag_value': 2, 'reason': 'Nierealne wartości strumienia G'}
        ],
        'PPFD_BC_IN_1_1_1': [
            {'start': '2018-01-01 00:00:00', 'end': '2018-05-14 10:30:00', 'flag_value': 3, 'reason': 'brak sensora'}
        ],
        'PPFD_BC_IN_1_1_2': [
            {'start': '2018-01-01 00:00:00', 'end': '2018-05-14 10:30:00', 'flag_value': 3, 'reason': 'brak sensora'}
        ],
        'PPFD_IN_1_2_1': [
            {'start': '2018-01-01 00:00:00', 'end': '2018-05-14 10:30:00', 'flag_value': 3, 'reason': 'brak sensora'}
        ],
        'PPFD_OUT_1_2_1': [
            {'start': '2018-01-01 00:00:00', 'end': '2018-05-14 10:30:00', 'flag_value': 3, 'reason': 'brak sensora'}
        ]
    },
    'ME_TOP_QF': {
        'PPFD_IN_1_2_1':[
            {'start': '2019-10-15 13:30:00', 'end': '2019-10-16', 'flag_value': 3, 'reason': 'brak sensora'},
            {'start': '2019-09-01 18:00:00', 'end': '2019-10-15 13:30:00', 'flag_value': 3, 'reason': 'brak sensora'},
        ],
        'PPFD_OUT_1_2_1':[
            {'start': '2019-10-15 13:30:00', 'end': '2019-10-16', 'flag_value': 3, 'reason': 'brak sensora'},
            {'start': '2019-09-01 18:00:00', 'end': '2019-10-15 13:30:00', 'flag_value': 3, 'reason': 'brak sensora'},
        ],
        'PPFD_IN_1_1_1':[
            {'start': '2019-10-15', 'end': '2019-10-18 03:30:00', 'flag_value': 3, 'reason': 'brak sensora'}
        ],
        'PPFD_DIF_1_1_1':[
            {'start': '2019-10-15', 'end': '2019-10-18 03:30:00', 'flag_value': 3, 'reason': 'brak sensora'}
        ],
        'SW_IN_1_2_1':[
            {'start': '2017-10-15', 'end': '2018-08-09 04:30:00', 'flag_value': 3, 'reason': 'brak sensora - zanim instalcja'}
        ],
        'SW_OUT_1_2_1':[
            {'start': '2017-10-15', 'end': '2018-08-09 04:30:00', 'flag_value': 3, 'reason': 'brak sensora - zanim instalcja'}
        ],
        'LW_IN_1_2_1':[
            {'start': '2017-10-15', 'end': '2018-08-09 04:30:00', 'flag_value': 3, 'reason': 'brak sensora - zanim instalcja'}
        ],
        'LW_OUT_1_2_1':[
            {'start': '2017-10-15', 'end': '2018-08-09 04:30:00', 'flag_value': 3, 'reason': 'brak sensora - zanim instalcja'}
        ]
    },
    'TU_QF': {
        'PPFD_IN_1_1_1': [
            {'start': '2008-01-01', 'end': '2008-03-30 09:30:00', 'flag_value': 3, 'reason': 'Brak czujnika SKP215'}
        ],
        'PPFD_IN_1_1_2': [
            {'start': '2017-03-05', 'end': '2017-11-21 13:30:00', 'flag_value': 2, 'reason': 'Awaria czujnika BF3H/BF5'}
        ],
        'PPFD_DIF_Avg': [
            {'start': '2017-03-05', 'end': '2017-11-21 13:30:00', 'flag_value': 2, 'reason': 'Awaria czujnika BF3H/BF5'}
        ],
        'SunStat_Tot': [
            {'start': '2017-03-05', 'end': '2017-11-21 13:30:00', 'flag_value': 2, 'reason': 'Awaria czujnika BF3H/BF5'}
        ],
        'TS_4_3_1': [
            {'start': '2012-12-15 21:00:00', 'end': '2012-12-15 22:00:00', 'flag_value': 2, 'reason': 'Awaria czujnika t107'}
        ],
        'TS_4_1_1': [
            {'start': '2012-01-23 23:30:00', 'end': '2012-12-15 22:00:00', 'flag_value': 2, 'reason': 'Awaria czujnika t107'}
        ],
        # Wildcard '*' TYLKO dla plików zawierających w nazwie "_meteo_WXTmet"
        '*': [
            {'start': '2011-11-15 13:29:00', 'end': '2011-11-15 13:31:00', 'flag_value': 2, 'reason': 'Awaria tylko czujnika WXT', 'filename_contains': '_meteo_WXTmet'},
            {'start': '2018-11-18 12:30:00', 'end': '2019-03-04 10:30:00', 'flag_value': 3, 'reason': 'Zdemontowany WXT do upgrade', 'filename_contains': '_meteo_WXTmet'}
        ],
        '*': [
            {'start': '2014-02-05 15:30:00', 'end': '2014-08-12 15:30:00', 'flag_value': 2, 'reason': 'Awaria tylko czujnika Multispc', 'filename_contains': '_meteo_Spec_idx'},
            {'start': '2014-08-12 13:30:00', 'end': '2015-08-27 13:30:00', 'flag_value': 3, 'reason': 'Multispc w kalibracji', 'filename_contains': '_meteo_Spec_idx'},
            {'start': '2018-06-18 15:10:00', 'end': '2050-06-18 15:10:00', 'flag_value': 3, 'reason': 'Multispc zdemontowane', 'filename_contains': '_meteo_Spec_idx'}
        ]
    }
    # ... i tak dalej dla innych stacji
}

STATION_MAPPING_FOR_QC = {
    # Tuczno
    'TU_MET_30min': 'TU_QF', 
    'TU_MET_1min': 'TU_QF',
    # Mezyk
    'ME_DOWN_MET_30min': 'ME_DOWN_QF', 
    'ME_DOWN_MET_1min': 'ME_DOWN_QF', 
    'ME_Rain_down': 'ME_DOWN_QF', 
    'ME_CalPlates': 'ME_DOWN_QF',
    # Tlen1
    # Tlen2
    # Sarbia
}
# Właściwy opis kodowania zmiennych zgodny z fluxnet: https://fluxnet.org/data/aboutdata/data-variables/
# ZMIENNA_H_V_R, 1 - najwyższy poziom, 
COLUMN_MAPPING_RULES = {
    'TUCZNO_MAP': {
        # 'Fc_wpl': 'FC_1_1_1',
        # 'Hs': 'H_1_1_1',
        # 'LE_wpl': 'LE_1_1_1',
        # Czujniki na najwyższej platformie wieży WXT510
        'press_Avg': 'PA_1_1_1', # WXT510
        'Air_temp_Avg': 'TA_1_1_1', # WXT510
        'Ta_Avg': 'TA_1_1_1', # WXT510
        'Bar_Pres_Avg': 'PA_1_1_2', # WXT510
        'Press_Avg': 'PA_1_1_2', # WXT510
        'Humidity_Avg': 'RH_1_1_1', # WXT510
        'Rh_Avg': 'RH_1_1_1', # WXT510
        'Rain_tot': 'P_1_1_1', # WXT510
        'P_rain_1_2_1_Tot': 'P_1_1_1', # WXT510
        'Rain_Top_Tot': 'P_1_1_2', # korytkowy A-ster
        # 'Hail_tot': '',
        'Wind_dir_Avg': 'WD_1_1_1', # WXT510
        'Wind_spe_Avg': 'WS_1_1_1', # WXT510
        'Ts_Avg': 'T_SONIC', # CSAT3
        'DENDRO_1_Avg': 'DBH_1_1_1',
        'DENDRO_2_Avg': 'DBH_2_1_1',
        'DENDRO_3_Avg': 'DBH_3_1_1',
        'PAR_Den_Avg': 'PPFD_IN_1_1_1', # SKP215
        'PAR_up_1': 'PPFD_BC_IN_1_1_1', # nowe LI191 2 projekt
        'PAR_up_2': 'PPFD_BC_IN_1_1_2', # nowe LI191 2 projekt
        'PPFD_215down_Avg': 'PPFD_OUT_1_1_1', # nowe SKP215 2 projekt
        'PPFD_215up_Avg': 'PPFD_IN_1_1_3', # nowe SKP215 2 projekt
        'PPFDd_Avg': 'PPFD_DIF_1_1_2', # BF3H/BF5
        'PPFDg_Avg': 'PPFD_IN_1_1_2', # BF3H/BF5
        'Albedo_Avg': 'ALB_1_1_1',
        'NetTot_Avg': 'RN_1_1_1',
        'NR01TC_Avg': 'TA_1_1_2', # NR01
        'RlDnCo_Avg': 'LW_OUT_1_1_1',
        'RlUpCo_Avg': 'LW_IN_1_1_1',
        'RsDn_Avg': 'SW_OUT_1_1_1',
        'RsUp_Avg': 'SW_IN_1_1_1', 
        # Czujniki multispektralne
        'MSR_Avg': 'MSR_1_1_1', # SKR
        'NDVI_Avg': 'NDVI_1_1_1',
        'OSAVI_Avg': 'OSAVI_1_1_1',
        'PRI_Avg': 'PRI_1_1_1',
        'SAVI_Avg': 'SAVI_1_1_1',
        'SR_Avg': 'SR_Avg',
        # Deszczomierze w krzyżu pod koronami drzew
        'R_Gauge_Tot1': 'P_1_2_1', # A-ster indeks V = 2 bo opad mierzony jest też na górze
        'R_Gauge_Tot2': 'P_1_2_2', # indeks R się zmienia ponieważ uznałem, że opady powinny być uśredniane
        'R_Gauge_Tot3': 'P_1_2_3',
        'R_Gauge_Tot4': 'P_1_2_4',
        'R_Gauge_Tot5': 'P_1_2_5',
        'R_Gauge_Tot6': 'P_1_2_6',
        'R_Gauge_Tot7': 'P_1_2_7',
        'R_Gauge_Tot8': 'P_1_2_8',
        'R_Gauge_Tot9': 'P_1_2_9',
        'R_Gauge_Tot10': 'P_1_2_10',
        'R_Gauge_Tot11': 'P_1_2_11',
        'R_Gauge_Tot12': 'P_1_2_12',
        'R_Gauge_Tot13': 'P_1_2_13',
        # Temp. gleby
        # profil 1
        'T109_C_2_Avg': 'TS_1_1_1',
        'T109_C_3_Avg':	'TS_1_2_1',
        'T109_C_Avg': 'TS_1_3_1',
        # profil 2
        'Gard_Ts_1_Avg': 'TS_2_1_1',
        'Gard_Ts_2_Avg': 'TS_2_2_1',
        'Gard_Ts_3_Avg': 'TS_2_3_1',
        'Gard_Ts_4_Avg': 'TS_2_4_1',
        'Gard_Ts_5_Avg': 'TS_2_5_1',
        # profil 3
        'T_soil_2cm_Avg': 'TS_3_1_1',
        'T_soil_5cm_Avg': 'TS_3_2_1',
        'T_soil_10cm_Avg': 'TS_3_3_1',
        # profil 4
        'T_soil_2cm': 'TS_4_1_1',
        'T_soil_5cm': 'TS_4_2_1',
        'T_soil_10cm': 'TS_4_3_1',
        # Temperatury w kolarach do pociągu?
        'T107_C1': 'TS_5_1_1',
        'T107_C2': 'TS_6_2_1',
        'T107_C3': 'TS_7_3_1',
        'T107_C4': 'TS_8_4_1',
        'T107_C5': 'TS_9_5_1',
        'T107_C6': 'TS_10_6_1',
        'T107_C7': 'TS_11_7_1',
        'T107_C8': 'TS_12_8_1',
        'T107_C9': 'TS_13_9_1',
        # Ciepło glebowe
        'SoilP_1_Avg': 'G_1_1_1',
        'SoilP_2_Avg': 'G_2_1_1',
        'SoilP_3_Avg': 'G_3_1_1',
        'SoilP_4_Avg': 'G_4_1_1',
        # Wilgotność gleby, 2 profile
        'VW_Avg1': 'SWC_1_1_1',
        'VW_Avg2': 'SWC_1_2_1',
        'VW_Avg3': 'SWC_1_3_1',
        'VW_Avg4': 'SWC_1_4_1',
        'VW_Avg5': 'SWC_1_5_1',
        'VW_Avg6': 'SWC_2_1_1',
        'VW_Avg7': 'SWC_2_2_1',
        'VW_Avg8': 'SWC_2_3_1',
        'VW_Avg9': 'SWC_2_4_1',
        'VW_Avg10': 'SWC_2_5_1'
    },
    'MEZYK_MAP': {
        'PPFD_1_2_1': 'PPFD_IN_1_1_1', # BF3H/BF5
        'PPFDr_1_2_1': 'PPFD_OUT_1_1_1', # PQS
        'PPFD_1_1_1': 'PPFD_IN_1_1_2', # PQS
        'PPFDd_1_1_1': 'PPFD_DIF_1_1_1', # BF3H/BF5
        'SWin_1_2_1': 'SW_IN_1_1_1', # CNR4
        'SWout_1_2_1': 'SW_OUT_1_1_1', # CNR4
        'LWin_1_2_1': 'LW_IN_1_1_1', # CNR4
        'LWOUT_1_2_1': 'LW_OUT_1_1_1' # CNR4
    },
    'RZECIN_MAP': {
        'T_1_1_Avg': 'TA_1_1_1' 
    }
}

STATION_MAPPING_FOR_COLUMNS = {
    'TU_MET_30min': 'TUCZNO_MAP',
    'TU_MET_1sec': 'TUCZNO_MAP',
    'ME_TOP_MET_1min': 'MEZYK_MAP',
    'RZ_MET_30min': 'RZECIN_MAP'
    # ... i tak dalej
}

VALUE_RANGE_FLAGS = {
    # Ta reguła zostanie zastosowana do wszystkich kolumn zaczynających się na 'TA'
    # np. TA_1_1_1, TA_1_1_2, TA_2_1_1 itd.
    'TA_': {'min': -40, 'max': 45},
    'air_temperature': {'min': 233, 'max': 313},
    'RH_': {'min': 10, 'max': 105},
    'TS_': {'min': -30, 'max': 60},
    'T107_': {'min': -30, 'max': 60},
    'SW_IN_': {'min': 0, 'max': 1000},
    'SW_OUT_': {'min': 0, 'max': 250},
    'LW_IN_': {'min': 150, 'max': 600},
    'LW_OUT_': {'min': 150, 'max': 600},
    'RN_': {'min': -200, 'max': 900},
    'PPFD_IN_': {'min': 0, 'max': 3000},
    'PPFD_BC_IN_': {'min': 0, 'max': 2000},
    'PPFDBC_IN_': {'min': 0, 'max': 2000},
    'PPFDd': {'min': 0, 'max': 2000},
    'PPFDd_': {'min': 0, 'max': 2000},
    'PPFD_DIF': {'min': 0, 'max': 2000},
    'PPFD_OUT_': {'min': 0, 'max': 500},
    'PPFDr_': {'min': 0, 'max': 500},
    'G_': {'min': -50, 'max': 200},
    'SHF_': {'min': -50, 'max': 200},
    'G_': {'min': -50, 'max': 200},
    'SWC_': {'min': 0, 'max': 50},
    'P_': {'min': -0.0001, 'max': 10},
    'FC_': {'min': -30, 'max': 20},
    'Fc_wpl': {'min': -3, 'max': 3},
    'co2_flux': {'min': -30, 'max': 30},
    'co2_strg': {'min': -60, 'max': 60},
    'co2_molar_density': {'min': -10, 'max': 40},
    'LE_wpl': {'min': -50, 'max': 600},
    'LE': {'min': -50, 'max': 600},
    'LE_strg': {'min': -100, 'max': 100},
    'h2o_strg': {'min': -60, 'max': 60},
    'Tau': {'min': -10, 'max': 10},
    'H': {'min': -300, 'max': 650},
    'H_strg': {'min': -100, 'max': 100},
    'ch4_strg': {'min': -60, 'max': 60},
    'none_strg': {'min': -60, 'max': 60}
}

MATLAB_DATA_PATHS = {
    'TU_MET_30min': r'D:\sites\TU\met-data_TU',
    'TU_MET_1sec': r'D:\sites\TU\met-data_TU',
    'ME_TOP_MET_1min': r'D:\sites\ME\met-data_ME',
    # ... i tak dalej dla każdej grupy, z której chcesz wczytywać dane .mat
}

# --- KONIEC SEKCJI KONFIGURACJI ---
# --- MODUŁY POMOCNICZE I LOGOWANIA ---
# --- NOWE FUNKCJE DO OBSŁUGI BAZY DANYCH SQLITE ---
def initialize_database(db_path: Path):
    """Tworzy bazę danych. Klucz główny to kombinacja `group_id` i `TIMESTAMP`."""
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data (
                group_id TEXT NOT NULL,
                TIMESTAMP DATETIME NOT NULL,
                source_file TEXT,
                interval TEXT,
                latitude REAL,
                longitude REAL,
                PRIMARY KEY (group_id, TIMESTAMP)
            )
        """)
        conn.commit()
        conn.close()
        logging.info(f"Baza danych zainicjalizowana w: {db_path}")
    except Exception as e:
        logging.error(f"Nie można zainicjalizować bazy danych: {e}")
        raise

def add_missing_columns(df: pd.DataFrame, conn: sqlite3.Connection):
    """Dynamicznie dodaje brakujące kolumny do tabeli w bazie danych."""
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(data)")
    existing_cols = {row[1] for row in cursor.fetchall()}
    df_cols = set(df.columns)
    missing_cols = df_cols - existing_cols

    for col in missing_cols:
        # Prosta inferencja typu SQL na podstawie typu pandas
        if pd.api.types.is_integer_dtype(df[col]):
            sql_type = "INTEGER"
        elif pd.api.types.is_numeric_dtype(df[col]):
            sql_type = "REAL"
        else:
            sql_type = "TEXT"
        
        try:
            cursor.execute(f'ALTER TABLE data ADD COLUMN "{col}" {sql_type}')
            logging.info(f"Dodano nową kolumnę do bazy danych: {col} ({sql_type})")
        except sqlite3.OperationalError as e:
            logging.warning(f"Nie można dodać kolumny '{col}'. Może już istnieć. Błąd: {e}")
    conn.commit()

# def save_dataframe_to_sqlite(df: pd.DataFrame, config: dict, lock: multiprocessing.Lock):
    # """
    # (Wersja 7.0 - OSTATECZNA) Zapisuje dane do SQLite, implementując logikę
    # "wypełniania braków" (Read-Merge-Write) z użyciem poprawnego `group_id`.
    # """
    # if df.empty:
        # return

    # db_path = Path(config['db_path'])
    # group_id = config['file_id']
    
    # # Dodaj metadane
    # df['group_id'] = group_id
    # df['interval'] = config.get('interval', 'N/A')
    # # coords = STATION_COORDINATES.get(group_id, {'lat': None, 'lon': None})
    # # df['latitude'] = coords['lat']
    # # df['longitude'] = coords['lon']

    # df.set_index(['group_id', 'TIMESTAMP'], inplace=True)

    # with lock:
        # try:
            # conn = sqlite3.connect(db_path, timeout=10)
            # add_missing_columns(df.reset_index(), conn)
            
            # keys_to_fetch = df.index.to_list()
            # if keys_to_fetch:
                # query = f"SELECT * FROM data WHERE (group_id, TIMESTAMP) IN ({','.join(['(?,?)']*len(keys_to_fetch))})"
                # params = [item for sublist in keys_to_fetch for item in sublist]
                
                # existing_df = pd.read_sql_query(query, conn, params=params, parse_dates=['TIMESTAMP'])
                
                # if not existing_df.empty:
                    # # Zakładamy, że obie ramki (nowa i wczytana) mają naiwny czas
                    # existing_df.set_index(['group_id', 'TIMESTAMP'], inplace=True)
                    
                    # combined = pd.concat([df, existing_df])
                    # # .first() inteligentnie wypełni luki w starych danych nowymi wartościami
                    # df_final = combined.groupby(level=['group_id', 'TIMESTAMP']).first()
                # else:
                    # df_final = df
            # else:
                 # df_final = df
                 
            # if df_final.empty:
                # conn.close()
                # return

            # df_to_save = df_final.reset_index()

            # columns_str = ", ".join([f'"{c}"' for c in df_to_save.columns])
            # placeholders = ", ".join(["?"] * len(df_to_save.columns))
            # sql = f"INSERT OR REPLACE INTO data ({columns_str}) VALUES ({placeholders})"
            
            # cursor = conn.cursor()
            # cursor.executemany(sql, df_to_save.to_records(index=False))
            # conn.commit()
            # conn.close()

        # except Exception as e:
            # logging.error(f"Krytyczny błąd zapisu do bazy danych: {e}", exc_info=True)

def save_dataframe_to_sqlite(df: pd.DataFrame, config: dict, lock: multiprocessing.Lock):
    """
    (WERSJA OSTATECZNA 6.5) Łączy dane z głównego potoku, dane z MATLABa oraz
    dane już istniejące w bazie, a następnie zapisuje finalny, scalony wynik.
    """
    if df.empty:
        return

    db_path = Path(config['db_path'])
    group_id = config['file_id']
    
    # Dodajemy metadane, które dotyczą całej ramki danych
    df['group_id'] = group_id
    df['interval'] = config.get('interval', 'N/A')
    # coords = STATION_COORDINATES.get(group_id, {'lat': None, 'lon': None})
    # df['latitude'] = coords['lat']
    # df['longitude'] = coords['lon']

    # Grupujemy dane wejściowe po latach, aby przetwarzać każdy rok osobno
    for year, new_data_from_pipeline in df.groupby(df['TIMESTAMP'].dt.year):
        logging.info(f"--- Rozpoczynanie zapisu dla roku: {year} | Grupa: {group_id} ---")
        
        # --- Krok 1: Wzbogać dane z potoku o dane z MATLABa ---
        matlab_df = load_matlab_data(year, config)
        
        final_new_data = new_data_from_pipeline
        if not matlab_df.empty:
            logging.debug(f"Uzupełnianie danych dla roku {year} o dane z plików MATLAB...")
            matlab_df['TIMESTAMP'] = pd.to_datetime(matlab_df['TIMESTAMP'])
            
            # Scal dane. `how='left'` zachowuje wszystkie dane z głównego potoku.
            final_new_data = pd.merge(
                new_data_from_pipeline, 
                matlab_df, 
                on='TIMESTAMP', 
                how='left',
                suffixes=('', '_mat') # Unikamy konfliktu nazw kolumn
            )
            logging.debug(f"Scalono {len(final_new_data)} wierszy z danymi z MATLABa dla roku {year}.")

        # `final_new_data` to teraz nasze kompletne "nowe" dane (z potoku + z MATLABa)
        final_new_data.set_index(['group_id', 'TIMESTAMP'], inplace=True)
        
        # --- Krok 2: Wykonaj cykl "Wczytaj-Scal-Zapisz" z bazą danych ---
        with lock:
            try:
                conn = sqlite3.connect(db_path, timeout=10)
                add_missing_columns(final_new_data.reset_index(), conn)
                
                keys_to_fetch = final_new_data.index.to_list()
                if keys_to_fetch:
                    query = f"SELECT * FROM data WHERE (group_id, TIMESTAMP) IN ({','.join(['(?,?)']*len(keys_to_fetch))})"
                    params = [item for sublist in keys_to_fetch for item in sublist]
                    existing_df_from_db = pd.read_sql_query(query, conn, params=params, parse_dates=['TIMESTAMP'])
                    
                    if not existing_df_from_db.empty:
                        existing_df_from_db.set_index(['group_id', 'TIMESTAMP'], inplace=True)
                        
                        # Łączymy "nowe" (już z MATLABem) dane z tymi z bazy
                        combined_for_upsert = pd.concat([final_new_data, existing_df_from_db])
                        # Scalamy, inteligentnie wypełniając luki
                        df_to_save_final = combined_for_upsert.groupby(level=['group_id', 'TIMESTAMP']).first()
                    else:
                        df_to_save_final = final_new_data
                else:
                     df_to_save_final = final_new_data

                if not df_to_save_final.empty:
                    df_to_save = df_to_save_final.reset_index()
                    
                    # Zapis do bazy za pomocą INSERT OR REPLACE
                    columns_str = ", ".join([f'"{c}"' for c in df_to_save.columns])
                    placeholders = ", ".join(["?"] * len(df_to_save.columns))
                    sql = f"INSERT OR REPLACE INTO data ({columns_str}) VALUES ({placeholders})"
                    
                    cursor = conn.cursor()
                    cursor.executemany(sql, df_to_save.to_records(index=False))
                    conn.commit()
                    logging.info(f"Zakończono zapis {len(df_to_save)} wierszy dla roku {year} do bazy danych.")
                
                conn.close()

            except Exception as e:
                logging.error(f"Krytyczny błąd zapisu do bazy danych dla roku {year}: {e}", exc_info=True)
                
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

def read_tob1_data(file_path: Path, metadata: tuple) -> pd.DataFrame:
    """
    (Wersja 2.0) Wczytuje dane z pliku binarnego TOB1 w porcjach (chunks),
    aby zapewnić niskie zużycie pamięci nawet przy bardzo dużych plikach.
    """
    col_names, struct_pattern, num_header_lines, fp2_cols = metadata
    all_chunks = []
    try:
        record_size = struct.calcsize(struct_pattern)
        if record_size == 0: return pd.DataFrame()

        with open(file_path, 'rb') as f:
            # Pomiń nagłówek
            for _ in range(num_header_lines):
                f.readline()
            
            chunk_size_rows = 100_000  # Liczba rekordów do przetworzenia w jednej porcji

            while True:
                records_in_chunk = []
                # Wewnętrzna pętla wczytująca jedną porcję rekordów
                for _ in range(chunk_size_rows):
                    chunk_bytes = f.read(record_size)
                    if not chunk_bytes or len(chunk_bytes) < record_size:
                        break  # Koniec pliku lub niekompletny rekord
                    records_in_chunk.append(struct.unpack(struct_pattern, chunk_bytes))

                if not records_in_chunk:
                    break  # Brak rekordów do wczytania, wyjdź z głównej pętli

                # Konwertuj bieżącą porcję rekordów na DataFrame
                chunk_df = pd.DataFrame(records_in_chunk, columns=col_names)
                
                # Zastosuj logikę dekodowania FP2 i tworzenia TIMESTAMP dla porcji
                if not chunk_df.empty and fp2_cols:
                    for fp2_col_name in fp2_cols:
                        if fp2_col_name in chunk_df.columns:
                            chunk_df[fp2_col_name] = pd.to_numeric(chunk_df[fp2_col_name], errors='coerce').apply(decode_csi_fs2_float)
                
                if 'SECONDS' in chunk_df.columns and 'NANOSECONDS' in chunk_df.columns:
                    secs = pd.to_numeric(chunk_df['SECONDS'], errors='coerce')
                    nanos = pd.to_numeric(chunk_df['NANOSECONDS'], errors='coerce')
                    chunk_df['TIMESTAMP'] = CAMPBELL_EPOCH + pd.to_timedelta(secs, unit='s') + pd.to_timedelta(nanos, unit='ns')
                
                all_chunks.append(chunk_df)

                # Jeśli ostatnia wczytana porcja była mniejsza niż zakładano, to znaczy, że to koniec pliku
                if len(records_in_chunk) < chunk_size_rows:
                    break

        if not all_chunks: return pd.DataFrame()
        
        final_df = pd.concat(all_chunks, ignore_index=True)
        final_df['source_file'] = str(file_path.resolve())
        return final_df

    except Exception as e:
        logging.error(f"Krytyczny błąd odczytu TOB1 z {file_path.name}: {e}", exc_info=True)
        return pd.DataFrame()

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
        final_df['source_file'] = str(file_path.resolve())
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
        final_df['source_file'] = str(file_path.resolve())
        return final_df
    except Exception as e:
        logging.error(f"Krytyczny błąd odczytu SimpleCSV z {file_path.name}: {e}")
        return pd.DataFrame()

def matlab_to_datetime(matlab_datenum: float) -> datetime:
    """Konwertuje numer seryjny daty z MATLABa na obiekt datetime Pythona."""
    return datetime.fromordinal(int(matlab_datenum)) + timedelta(days=matlab_datenum % 1) - timedelta(days=366)
    
def load_matlab_data(year: int, config: dict) -> pd.DataFrame:
    """
    Wczytuje i scala dane z plików .mat dla danego roku i grupy.
    Automatycznie obsługuje pliki roczne i miesięczne.
    """
    group_id = config['file_id']
    interval = config.get('interval', '')
    
    base_path_str = MATLAB_DATA_PATHS.get(group_id)
    if not base_path_str:
        return pd.DataFrame() # Brak konfiguracji dla tej grupy

    # Normalizacja interwału (np. 30m -> 30min) - dostosuj wg potrzeb
    interval_folder = interval.replace('m', 'min').replace('s', 'sec')
    
    data_path = Path(base_path_str) / str(year) / f"MET_{interval_folder}" / "zero_level"

    if not data_path.exists():
        logging.debug(f"Katalog z danymi MATLABa nie istnieje: {data_path}")
        return pd.DataFrame()

    # Sprawdzenie, czy dane są miesięczne (interwał < 5s)
    is_monthly = 'sec' in interval_folder and int(re.sub(r'\D', '', interval_folder)) < 5

    monthly_dfs = []
    months_to_check = range(1, 13) if is_monthly else [None] # Pętla po miesiącach lub jedna iteracja dla danych rocznych

    for month in months_to_check:
        try:
            # Ustal nazwy plików czasu i zmiennych
            time_file_name = f"tv_{month:02d}.mat" if month else "tv.mat"
            time_file_path = data_path / time_file_name
            
            if not time_file_path.exists():
                continue

            # Wczytaj wektor czasu
            time_vector_raw = loadmat(time_file_path)['tv'].flatten()
            timestamps = [matlab_to_datetime(t) for t in time_vector_raw]
            
            # Wczytaj wszystkie pliki ze zmiennymi
            var_files = [f for f in data_path.glob('*.mat') if not f.name.startswith('tv')]
            
            matlab_data = {'TIMESTAMP': timestamps}
            
            for var_file in var_files:
                var_name = var_file.stem
                # Dopasuj pliki miesięczne
                if month and not var_name.endswith(f"_{month:02d}"):
                    continue
                
                clean_var_name = var_name.rsplit('_', 1)[0] if month else var_name
                
                data = loadmat(var_file)[var_name].flatten()
                matlab_data[clean_var_name] = data

            monthly_dfs.append(pd.DataFrame(matlab_data))

        except Exception as e:
            logging.warning(f"Błąd podczas wczytywania danych MATLABa dla {year}/{month}: {e}")
            continue

    if not monthly_dfs:
        return pd.DataFrame()

    # Zwróć połączoną ramkę danych
    return pd.concat(monthly_dfs, ignore_index=True)

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

    # return combined_series.dt.tz_convert(target_tz)
    return combined_series.dt.tz_convert(target_tz).dt.tz_localize(None)

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

def apply_value_range_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    (Wersja 2.0) Sprawdza wartości w kolumnach, których nazwy ZACZYNAJĄ SIĘ
    od kluczy zdefiniowanych w VALUE_RANGE_FLAGS i flaguje wartości spoza zakresu.
    """
    if df.empty or not VALUE_RANGE_FLAGS:
        return df

    df_out = df.copy()

    # Iterujemy po regułach (np. prefix='TA', range_dict={'min': -40, 'max': 50})
    for prefix, range_dict in VALUE_RANGE_FLAGS.items():
        # Znajdujemy wszystkie kolumny w ramce danych, które pasują do prefiksu
        target_cols = [col for col in df_out.columns if str(col).startswith(prefix)]

        # Dla każdej znalezionej kolumny stosujemy tę samą regułę zakresu
        for col_name in target_cols:
            numeric_col = pd.to_numeric(df_out[col_name], errors='coerce')
            
            min_val = range_dict.get('min', -float('inf'))
            max_val = range_dict.get('max', float('inf'))
            
            out_of_range_mask = (numeric_col < min_val) | (numeric_col > max_val)
            
            if out_of_range_mask.any():
                flag_col_name = f"{col_name}_flag"
                if flag_col_name not in df_out.columns:
                    df_out[flag_col_name] = 0
                
                df_out[flag_col_name] = pd.to_numeric(df_out[flag_col_name], errors='coerce').fillna(0).astype(int)
                
                logging.info(f"Oznaczanie flagą '4' {out_of_range_mask.sum()} wierszy w kolumnie '{col_name}' (wartości poza zakresem).")
                df_out.loc[out_of_range_mask, flag_col_name] = 4
            
    return df_out
    
def apply_quality_flags(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    (Wersja 3.0) Dodaje flagi jakości, używając dwuetapowego systemu słowników
    (mapowanie file_id -> zestaw reguł -> reguły). Obsługuje wildcard '*'
    oraz opcjonalny filtr na nazwę pliku.
    """
    group_id = config.get('file_id')
    if not group_id or df.empty:
        return df

    # === NOWA LOGIKA: DWUETAPOWE WYSZUKIWANIE REGUŁ ===
    
    # Krok 1: Znajdź nazwę zestawu reguł dla danej grupy
    ruleset_name = STATION_MAPPING_FOR_QC.get(group_id)
    if not ruleset_name:
        return df # Brak zdefiniowanego mapowania dla tej grupy, zwracamy oryginał

    # Krok 2: Pobierz właściwy słownik z regułami
    station_rules = STATION_MAPPING_FOR_QC.get(ruleset_name)
    if not station_rules:
        logging.warning(f"Nie znaleziono definicji reguł '{ruleset_name}' w STATION_MAPPING_FOR_QC dla grupy '{group_id}'.")
        return df

    # === KONIEC NOWEJ LOGIKI WYSZUKIWANIA ===

    df_out = df.copy()

    # Reszta funkcji (obsługa wildcarda, pętle, flagowanie) pozostaje bez zmian
    for col_to_flag, rules_list in station_rules.items():
        if col_to_flag == '*':
            target_cols = [c for c in df_out.select_dtypes(include='number').columns if not c.endswith('_flag')]
        elif col_to_flag in df_out.columns:
            target_cols = [col_to_flag]
        else:
            continue

        for rule in rules_list:
            try:
                start_ts = pd.Timestamp(rule['start'], tz=df_out['TIMESTAMP'].dt.tz)
                end_ts = pd.Timestamp(rule['end'], tz=df_out['TIMESTAMP'].dt.tz)
                final_mask = (df_out['TIMESTAMP'] >= start_ts) & (df_out['TIMESTAMP'] <= end_ts)

                filename_filter = rule.get('filename_contains')
                if filename_filter:
                    if 'source_file' in df_out.columns:
                        file_mask = df_out['source_file'].str.contains(filename_filter, na=False, regex=False)
                        final_mask &= file_mask
                    else:
                        continue
                
                if final_mask.any():
                    for col_name in target_cols:
                        flag_col_name = f"{col_name}_flag"
                        if flag_col_name not in df_out.columns:
                            df_out[flag_col_name] = 0
                        
                        df_out.loc[final_mask, flag_col_name] = rule['flag_value']
            except Exception as e:
                logging.warning(f"Błąd reguły flagowania dla '{group_id}' (kolumna: {col_to_flag}): {e}")
                
    return df_out

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

def correct_and_report_chronology(df: pd.DataFrame, context_name: str) -> pd.DataFrame:
    """
    Koryguje chronologię, zakładając stały interwał, ale potencjalny dryf zegara.
    Działa na większym zbiorze danych (np. partii plików).
    """
    if df.empty or 'TIMESTAMP' not in df.columns:
        return df

    df_sorted = df.sort_values(by='TIMESTAMP', kind='mergesort').reset_index(drop=True)
    time_diffs = df_sorted['TIMESTAMP'].diff()
    
    # Znajdź "skoki" czasu do tyłu
    backwards_jumps = time_diffs[time_diffs < pd.Timedelta(0)]
    
    if not backwards_jumps.empty:
        logging.warning(f"Wykryto {len(backwards_jumps)} skoków czasu do tyłu w partii '{context_name}'. Rozpoczynanie korekty.")
        
        # Wykrywamy modalny (najczęstszy) interwał pomiarowy
        median_interval = time_diffs.median()
        if pd.isna(median_interval) or median_interval <= pd.Timedelta(0):
            logging.error(f"Nie można wiarygodnie określić interwału dla partii '{context_name}'. Przerywanie korekty.")
            return df_sorted # Zwróć posortowane, ale bez korekty

        logging.info(f"Wykryty interwał dla korekty: {median_interval}")

        # Iteracyjna korekta skoków
        new_timestamps = df_sorted['TIMESTAMP'].copy()
        for i, jump in backwards_jumps.items():
            # Przesuwamy wszystkie kolejne znaczniki czasu o wielkość "cofnięcia"
            offset = new_timestamps[i-1] + median_interval - new_timestamps[i]
            if offset > pd.Timedelta(0):
                new_timestamps.iloc[i:] += offset
        
        df_sorted['TIMESTAMP'] = new_timestamps
        logging.info(f"Zakończono korektę chronologii dla partii '{context_name}'.")

    return df_sorted

def clean_and_validate_record_column(df: pd.DataFrame, file_path: str, timestamp_col='TIMESTAMP'):
    """
    Sprawdza spójność kolumny RECORD i filtruje wiersze, które nie pasują
    do chronologicznej sekwencji, zachowując te, które wracają do normy.
    UWAGA: W tej wersji skryptu funkcja nie jest aktywowana. Wymaga dopracowania.
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

def apply_column_mapping(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Zmienia nazwy kolumn w ramce danych na podstawie zdefiniowanych reguł mapowania,
    specyficznych dla danej stacji/grupy.
    """
    file_id = config.get('file_id')
    if not file_id:
        return df

    # Znajdź nazwę zestawu reguł dla danego file_id
    ruleset_name = STATION_MAPPING_FOR_COLUMNS.get(file_id)
    if not ruleset_name:
        return df # Brak zdefiniowanego mapowania dla tej grupy

    # Pobierz właściwy słownik mapujący
    mapping_dict = COLUMN_MAPPING_RULES.get(ruleset_name)
    if not mapping_dict:
        logging.warning(f"Nie znaleziono definicji reguł '{ruleset_name}' w COLUMN_MAPPING_RULES.")
        return df

    # Zmień nazwy kolumn
    df.rename(columns=mapping_dict, inplace=True)
    logging.debug(f"Zastosowano mapowanie kolumn '{ruleset_name}' dla grupy '{file_id}'.")
    
    return df
def apply_all_corrections(df: pd.DataFrame, config: dict, context_name: str) -> pd.DataFrame:
    """
    Uruchamia pełną, ujednoliconą sekwencję korekt na danej ramce danych.
    
    Args:
        df: Ramka danych do przetworzenia (z jednego pliku binarnego lub z partii CSV).
        config: Słownik konfiguracyjny dla bieżącej grupy.
        context_name: Nazwa kontekstu (nazwa pliku lub partii) do celów logowania.
    
    Returns:
        Przetworzona ramka danych.
    """
    if df.empty:
        return df
    
    # 1. Standaryzacja nazw kolumn
    df = apply_column_mapping(df, config)
    
    # 2. Specjalna korekta chronologii (działa tylko dla partii CSV)
    if "Partia" in context_name:
         df = correct_and_report_chronology(df, context_name)

    # 3. Pozostałe, standardowe korekty w ustalonej kolejności
    df['TIMESTAMP'] = apply_timezone_correction(df['TIMESTAMP'], config['file_id'])
    df.dropna(subset=['TIMESTAMP'], inplace=True)
    if df.empty: return df

    df = apply_manual_time_shifts(df, config['file_id'])
    
    # === POCZĄTEK POPRAWKI ===
    # Przekazujemy konkretną wartość 'file_id', a nie cały słownik
    df = apply_calibration(df, config['file_id'])
    # === KONIEC POPRAWKI ===
    
    df = apply_value_range_flags(df)
    df = apply_quality_flags(df, config)
    df, _ = align_timestamp(df, force_interval=config.get('interval'))
    
    return df
    
# ======================================================================
# === KONIEC BLOKU FUNKCJI POMOCNICZYCH ===
# ======================================================================

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
    
def identify_file_type(file_path: Path) -> str:
    """Szybko identyfikuje typ pliku na podstawie pierwszej linii."""
    try:
        with open(file_path, 'r', encoding='latin-1') as f:
            first_line = f.readline().strip()
        if first_line.startswith('"TOB1"'): return 'TOB1'
        if first_line.startswith('"TOA5"'): return 'TOA5'
        if first_line.startswith('"Timestamp"'): return 'CSV'
        return 'UNKNOWN'
    except Exception:
        return 'UNKNOWN'

def process_binary_file(args: tuple) -> Optional[pd.DataFrame]:
    """Potok przetwarzania dla pojedynczego pliku binarnego (TOB1/TOA5)."""
    file_path, config = args
    active_file_id = config['file_id']
    force_interval = config['interval']
    
    try:
        df = pd.DataFrame()
        file_type = identify_file_type(file_path)
        
        if file_type == 'TOB1':
            metadata = get_tob1_metadata(file_path)
            if metadata: df = read_tob1_data(file_path, metadata)
        elif file_type == 'TOA5':
            metadata = get_toa5_metadata(file_path)
            if metadata: df = read_toa5_data(file_path, metadata)
        else:
            return None # Ta funkcja nie obsługuje innych typów

        if df.empty: return None
        df = apply_column_mapping(df, config) # zastosuj słownik nazw zmiennych

        # Kompletny potok korekt dla pliku binarnego
        df = filter_by_realistic_date_range(df, file_path)
        if df.empty: return None

        df = apply_all_corrections(df, config, context_name=str(file_path.name))

        return df
    except Exception as e:
        logging.error(f"Krytyczny błąd (plik binarny) {file_path.name}: {e}", exc_info=True)
        return None
        
def worker_task_binary(args: tuple):
    """Zadanie dla procesu roboczego: przetwarza plik binarny i zapisuje do bazy."""
    file_path, config, lock = args
    processed_df = process_binary_file((file_path, config))
    if processed_df is not None and not processed_df.empty:
        save_dataframe_to_sqlite(processed_df, config, lock)
        return True
    return False

def main():
    """Główna funkcja orkiestrująca w architekturze z bazą danych SQLite."""
    parser = argparse.ArgumentParser(description="Przetwarza i scala pliki danych do bazy SQLite.")
    parser.add_argument("-i", "--input_dir", required=True, nargs='+', help="Katalogi wejściowe.")
    parser.add_argument("-o", "--output_dir", required=True, help="Katalog wyjściowy (dla logów i cache).")
    parser.add_argument("--db-path", required=True, help="Ścieżka do pliku bazy danych SQLite.")
    parser.add_argument("-fid", "--file_id", required=True, help="Identyfikator grupy.")
    parser.add_argument("-j", "--jobs", type=int, default=os.cpu_count(), help="Liczba procesów.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Poziom logowania.")
    parser.add_argument("--no-cache", action='store_true', help="Wyłącza cache.")
    
    args = parser.parse_args()
    
    setup_logging(args.log_level)
    
    # Inicjalizacja bazy danych na starcie
    initialize_database(Path(args.db_path))

    logging.info(f"{'='*20} Rozpoczęto przetwarzanie dla grupy: '{args.file_id}' {'='*20}")
    
    group_config = FILE_ID_MERGE_GROUPS.get(args.file_id, {})
    group_config['file_id'] = args.file_id
    group_config['db_path'] = args.db_path # Przekazujemy ścieżkę do bazy

    # Skanowanie i cache
    processed_files_cache = load_cache() if not args.no_cache else {}
    all_files = scan_for_files(args.input_dir, group_config.get('source_ids', []))
    files_to_process = [p for p in all_files if not is_file_in_cache(p, processed_files_cache)]
    
    if not files_to_process:
        logging.info("Brak nowych lub zmodyfikowanych plików do przetworzenia.")
        return
        
    logging.info(f"Znaleziono {len(files_to_process)} nowych/zmienionych plików.")

    binary_files, csv_files = [], []
    for path in files_to_process:
        file_type = identify_file_type(path)
        if file_type in ['TOB1', 'TOA5']: binary_files.append(path)
        elif file_type == 'CSV': csv_files.append(path)

    with multiprocessing.Manager() as manager:
        lock = manager.Lock()
        
        if binary_files:
            logging.info(f"Przetwarzanie {len(binary_files)} plików binarnych równolegle...")
            binary_args = [(p, group_config, lock) for p in binary_files]
            with multiprocessing.Pool(processes=args.jobs) as pool:
                list(tqdm(pool.imap_unordered(worker_task_binary, binary_args), total=len(binary_files), desc="Pliki binarne"))
        
        if csv_files:
            logging.info(f"Przetwarzanie {len(csv_files)} plików CSV w partiach (w procesie głównym)...")
            csv_files.sort()
            batch_size = 30
            
            batch_indices = range(0, len(csv_files), batch_size)
            for i in tqdm(batch_indices, desc="Partie plików CSV"):
                batch_paths = csv_files[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                # Wczytaj i połącz pliki z partii
                batch_dfs = [read_simple_csv_data(p) for p in batch_paths]
                batch_df = pd.concat([df for df in batch_dfs if not df.empty], ignore_index=True)
                if batch_df.empty: continue
                
                # === ZASTOSOWANIE NOWEJ, WSPÓLNEJ FUNKCJI ===
                batch_df = apply_all_corrections(batch_df, group_config, context_name=f"Partia {batch_num}")
                
                # Zapisz wynik przetworzonej partii natychmiast
                if not batch_df.empty:
                    save_dataframe_to_sqlite(batch_df, group_config, lock)

    if not args.no_cache:
        update_cache(files_to_process, processed_files_cache)
        save_cache(processed_files_cache)
        logging.info("Cache został zaktualizowany.")

    logging.info(f"{'='*20} Zakończono przetwarzanie dla grupy: '{args.file_id}' {'='*20}\n")
    
if __name__ == '__main__':
    # Ustawienie metody startowej jest ważne dla Windows/macOS
    if os.name in ['nt', 'posix']:
        multiprocessing.set_start_method('spawn', force=True)
    main()