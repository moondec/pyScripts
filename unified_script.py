# -*- coding: utf-8 -*-

"""
================================================================================
  Uniwersalny Skrypt do Agregacji Danych (Wersja 7.0 - Zunifikowana)
================================================================================
Opis:
    Wersja 7.0 stanowi kompletną unifikację i refaktoryzację poprzednich
    skryptów (split.py i splitSQ.py). Łączy w sobie najlepsze cechy obu
    wersji, tworząc jedno, spójne narzędzie do przetwarzania danych pomiarowych.
    Skrypt oferuje elastyczny wybór formatu wyjściowego (SQLite, CSV lub oba)
    i implementuje zaawansowaną logikę hierarchii źródeł danych.

Architektura:
    - Centralizacja konfiguracji: Wszystkie reguły (mapowanie, kalibracja,
      korekty czasu, flagi jakości) są zarządzane z poziomu globalnych
      słowników.
    - Hierarchia źródeł danych: Skrypt w pierwszej kolejności przetwarza
      surowe dane z loggerów (TOA5, TOB1, CSV). Następnie, ewentualne luki
      w danych (wartości NaN) są uzupełniane danymi z plików .MAT.
    - Dwa potoki przetwarzania:
        1. Pliki binarne (TOA5/TOB1) są przetwarzane indywidualnie, z użyciem
           filtra `filter_by_realistic_date_range`.
        2. Pliki CSV są przetwarzane w partiach, z użyciem funkcji korekty
           chronologii `correct_and_report_chronology`.
    - Elastyczny zapis wyników: Możliwość zapisu do bazy SQLite, plików CSV
      lub obu formatów jednocześnie, kontrolowana argumentem `--output-format`.
    - Zaawansowane logowanie i cache: Mechanizm logowania z kontrolą poziomu
      oraz cache oparty na sumie kontrolnej i dacie modyfikacji pliku w celu
      unikania ponownego przetwarzania.

Główne funkcjonalności:
    - Obsługa wielu formatów wejściowych: TOB1, TOA5, CSV i .MAT.
    - Dekodowanie formatów binarnych (w tym FP2).
    - Zaawansowane zarządzanie strefami czasowymi i korekty manualne.
    - Kalibracja danych na podstawie reguł dla poszczególnych sensorów.
    - Automatyczne flagowanie jakości danych (reguły czasowe i zakresowe).
    - Równoległe przetwarzanie plików w celu maksymalizacji wydajności.
    - Wbudowany tryb testowy (`--run-tests`) do weryfikacji spójności wyników.

Wymagania:
    - Python 3.10+
    - Biblioteki: pandas, pytest, scipy, tqdm
      pip install pandas pytest scipy tqdm

Uruchamianie:
    Skrypt należy uruchamiać z wiersza poleceń.

    Składnia podstawowa:
    python unified_script.py -i <katalog_wejsciowy> -o <katalog_wyjsciowy> -fid <id_grupy> --db-path <sciezka_do_bazy> [opcje]

    Nowe i zaktualizowane argumenty:
      -i, --input_dir       (Wymagany) Ścieżka do katalogu z danymi wejściowymi.
      -o, --output_dir      (Wymagany) Ścieżka do katalogu wyjściowego (dla CSV, logów, cache).
      -fid, --file_id       (Wymagany) Identyfikator grupy do przetworzenia.
      --db-path             (Opcjonalny) Ścieżka do pliku bazy SQLite. Wymagany dla formatu 'sqlite' lub 'both'.
      -j, --jobs            (Opcjonalny) Liczba równoległych procesów. Domyślnie: liczba rdzeni CPU.
      --log-level           (Opcjonalny) Poziom logowania: DEBUG, INFO, WARNING, ERROR. Domyślnie: INFO.
      --output-format       (Opcjonalny) Format wyjściowy: 'sqlite', 'csv', 'both'. Domyślnie: 'sqlite'.
      --no-cache            (Opcjonalny) Wyłącza użycie cache.
      --run-tests           (Opcjonalny) Uruchamia tryb testowy do weryfikacji spójności wyników.
      --overwrite           (Opcjonalny) Uruchamia tryb nadpisywania
    
    Przykłady użycia:
      # Przetwarzanie i zapis do bazy SQLite (domyślnie)
      python unified_script.py -i /dane/tuczno -o /wyniki -fid TU_MET_30min --db-path /wyniki/tuczno.db

      # Przetwarzanie i zapis tylko do plików CSV
      python unified_script.py -i /dane/tuczno -o /wyniki/tuczno_csv -fid TU_MET_30min --output-format csv

      # Przetwarzanie i zapis do obu formatów
      python unified_script.py -i /dane/tuczno -o /wyniki/tuczno_csv -fid TU_MET_30min --db-path /wyniki/tuczno.db --output-format both

--------------------------------------------------------------------------------
    Autor: Marek Urbaniak
    Wersja: 7.0 - Zunifikowana
    Data ostatniej modyfikacji: 30.07.2025
--------------------------------------------------------------------------------
"""

import argparse
import json
import logging
import math
import multiprocessing
import os
import re
import sqlite3
import sqlalchemy
import struct
import tempfile
from collections import defaultdict
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional
import numpy as np
import pandas as pd
import pytest
from scipy.io import loadmat
from tqdm import tqdm


# --- Globalne definicje ---
CAMPBELL_EPOCH = pd.Timestamp('1990-01-01 00:00:00')
STRUCT_FORMAT_MAP = {'ULONG':'L', 'IEEE4':'f', 'IEEE8':'d', 'LONG':'l', 'BOOL':'?', 'SHORT':'h', 'USHORT':'H', 'BYTE':'b'}
BASE_DIR = Path(__file__).parent
CACHE_FILE_PATH = BASE_DIR / ".cache_split.json"
LOG_FILE_PATH = BASE_DIR / "log_split.txt"
CHRONOLOGY_LOG_FILENAME = BASE_DIR / "log_chronology_correction.txt"
chronology_logger = None

# --- POCZĄTEK SEKCJI KONFIGURACJI ---

# 1. LISTA KOLUMN DO POMINIĘCIA Z PLIKÓW CSV
COLUMNS_TO_EXCLUDE_FROM_CSV = [
    'record_no',
    '2SERIAL (State)',
    '2SERIAL (State).1',
    '2SERIAL (State).2',
    '2SERIAL (State).3',
    'TZ',
    'Temp (degC)',
    'Humidity (%)',
    'RTD_1 (degC)', 
    'RTD_1_AV (degC)', 
    'RTD_2 (degC)', 
    'RTD_2_AV (degC)',
    'RTD_3 (degC)',
    'RTD_3_AV (degC)',
    'Prom_1 (mV)',
    'Prom_1_AV (mV)', 
    'Prom_1_SD (mV)',
    'Prom_2 (mV)',
    'Prom_2_AV (mV)',
    'Prom_2_SD (mV)',
    'RTD_4 (degC)',
    'RTD_4_AV (degC)',
    'RTD_5 (degC)',
    'RTD_5_AV (degC)',
    'RTD_6 (degC)',
    'RTD_6_AV (degC)',
    'RTD_7 (degC)',
    'RTD_7_AV (degC)',
    'RTD_8 (degC)',
    'RTD_8_AV (degC)',
    'TA_1_1_1 (degC)',
    'RH_1_1_1 (%)',
    'PPFD_BC_IN_2_1_1 (mV)',
    'PPFD_BC_IN_2_1_1_SD (mV)',
    'PPFD_BC_IN_2_2_1 (mV)',
    'PPFD_BC_IN_2_2_1_SD (mV)',
    'PPFD_BC_IN_1_1_1_SD',
    'PPFD_BC_IN_1_1_2_SD',
    'TS_6_1_1 (degC)',
    'TS_6_2_1 (degC)',
    'TS_7_1_1 (degC)',
    'TS_7_2_1 (degC)',
    'TS_8_1_1 (degC)',
    'TS_8_2_1 (degC)',
    'TS_9_1_1 (degC)',
    'TS_9_2_1 (degC)',
    'TS_10_1_1 (degC)',
    'TS_10_2_1 (degC)',
    'TA_1_2_1.1',
    'SKR_UP_1 (mV)',
    'SKR_UP_2 (mV)',
    'SKR_UP_3 (mV)',
    'SKR_UP_4 (mV)',
    'SKR_DOWN_1 (mV)',
    'SKR_DOWN_2 (mV)',
    'SKR_DOWN_3 (mV)',
    'SKR_DOWN_4 (mV)',
    'LWin_1_2_V',
    'CNR4_PT100_AVdegC',
    'G_1_1_1mV',
    'G_2_1_1mV',
    'G_3_1_1mV',
    'G_4_1_1mV',
    'G_5_1_1mV',
    'G_6_1_1mV',
    'G_7_1_1mV',
    'G_8_1_1mV',
    'G_9_1_1mV',
    'G_10_1_1mV',
    'SDI12_33',
    'SDI12_52',
    'SDI12_32',
    'SDI12_23',
    'SDI12_31',
    'SDI12_24',
    'SDI12_13',
    'SDI12_43',
    'SDI12_41',
    'U_plytka1mV',
    'SDI12_21',
    'SDI12_53',
    'SDI12_14',
    'SDI12_54',
    'SDI12_22',
    'SDI12_44',
    'SDI12_51',
    'SDI12_11',
    'SDI12_42',
    'SDI12_12',
    'SDI12_34',
    'PPFDd_1_1_1_AV',
    'PPFD_1_2_1_SD',
    'PPFDd_1_1_1_SD',
    'BF5Sunshine_CH2mV',
    'BF5Sunshine_CH1mV',
    'Thermocouple_21 (degC)',
    'Thermocouple_21_SD (degC)',
    'RTD_5_SD (degC)',
    'Voltage_10 (mV)',
    'Voltage_5 (mV)',
    'RTD_9 (degC)',
    'TA_2_1_1 (degC)',
    'RH_2_1_1 (%)',
    'PPFD_BC_IN_1_1_1 (mV)',
    'PPFD_BC_IN_1_1_1_AV (mV)',
    'PPFD_BC_IN_1_1_1_SD (mV)',
    'PPFD_BC_IN_1_2_1 (mV)',
    'PPFD_BC_IN_1_2_1_AV (mV)',
    'PPFD_BC_IN_1_2_1_SD (mV)',
    'CNR4_PT100 (degC)',
    'CNR4_PT100_AV (degC)',
    'CNR4_PT100_SD (degC)',
    'BF5Sunshine_CH1 (mV)',
    'BF5Sunshine_CH1_AV (mV)',
    'BF5Sunshine_CH1_SD (mV)',
    'BF5Sunshine_CH2 (mV)',
    'BF5Sunshine_CH2_AV (mV)',
    'BF5Sunshine_CH2_SD (mV)',
    'PPFD_1_2_1_AV',
    'PPFDr_1_2_1_AV',
    'PPFDr_1_2_1_SD',
    'PPFD_1_1_1_AV',
    'PPFD_1_1_1_SD',
    'Temp_NUM',
    'Humidity_NUM',
    'Prom_2_NUM',
    'RTD_4_NUM',
    'RTD_2_NUM',
    'RTD_5_NUM',
    'Prom_1_NUM',
    'RTD_3_NUM',
    'RTD_7_NUM',
    'LowSpeedCounter_11',
    'RTD_1_NUM',
    'RTD_8_NUM',
    'RTD_6_NUM',
    '1038CV',
    '1160CV',
    '1161CV'
    # Dodaj tutaj kolejne nazwy kolumn, które chcesz pominąć
]

# 2. Lista grup, gdzie dane są usupełniane z plików .MAT (Surowe dane niedostępne)
GROUP_IDS_FOR_MATLAB_FILL = ['TL1_MET_30', 'TL1_RAD_30', 'TL1_SOIL_30', 'TL1_RAD_1', 'TL2_MET_1m', 'TL2_MET_30m', 'RZ_CSI_30', 'RZ_WET_30m']

# 3. Koordynaty geograficzne stacji
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
    'TL1a_Rain_down': {'lat': 53.634, 'lon': 18.2561},    # Tlen1a
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

# 4. SŁOWNIK GRUPUJĄCY PLIKI ŹRÓDŁOWE
FILE_ID_MERGE_GROUPS = {
    # Rzecin
    # 'RZ_MET_30min': { 'source_ids': ['Meteo_32', 'meteo_Meteo_32', 'Meteo_30', 'CR1000_Meteo_30', '_LiCor'], 'interval': '30min' },
    # 'RZ_MET_1min': { 'source_ids': ['sky_SKYE_1min_', 'CR1000_2_meteo_SKYE_1min', 'CR1000_sky_SKYE_1min', '3_SKYE', 'sky_SKYE', 'CR1000_2_meteo_Meteo_1', 'CR1000_Meteo_1', 'CR1000_2_meteo_Meteo_2', 'CR1000_2_Meteo_2', 'CR1000_Methan_1', 'CR1000_Methan_2', 'Parsival', 'NetRadiometers', 'SWD'], 'interval': '1min' },
    # 'RZ_MET_30sec': { 'source_ids': ['CR1000_wet_RadTaRH', 'CR1000_wet_TEMP_PRT', 'CR1000_wet_Ts_P'], 'interval': '30s' }, # Zmieniono '30sec' na '30s' dla spójności
    'RZ_CSI_30': {"source_ids": ["Meteo_30", "CR3000_Barometr", "LiCor"], "interval": "30m"},
    'RZ_CSI_MET2_30': {"source_ids": ["Meteo_32"], "interval": "30min"},
    'RZ_SKYE_30': {"source_ids": ["sky_SKYE"], "interval": "30min"},
    'RZ_PROF_2m': {"source_ids": ["LI840_stor"], "interval": "2min"},
    'RZ_METHAN_1': {"source_ids": ["Methan_1", "Methan_2"], "interval": "1min"},
    'RZ_Parsival': {"source_ids": ["sky_Parsival_data"], "interval": "1min"},
    'RZ_NR_lite': {"source_ids": ["NetRadiometers"], "interval": "1min"},
    'RZ_SWD': {"source_ids": ["SWD"], "interval": "1min"},
    'RZ_WET_30s_RTRH': {"source_ids": ["wet_RadTaRH"], "interval": "30s"},
    'RZ_WET_30s': {"source_ids": ["wet_TEMP_PRT"], "interval": "30s"},
    'RZ_WET_30m': {"source_ids": ["wet_Ts_P"], "interval": "30min"},
    # Tuczno
    'TU_PROF_1s': {"source_ids": ["profil_LI840", 'CR1000_LI840', 'CR1000_soil2_LI840'], "interval": "1s"},
    'TU_SOIL_5s': {"source_ids": ["soil_Temperatury_5$", "soil_PPFD_under_tree"], "interval": "5s"},
    'TU_MET_5s': {"source_ids": ["meteo_GlobalRad"], "interval": "5s"},
    'TU_SOIL2_30s': {"source_ids": ["soil2_SoTemS13"], "interval": "30s"},
    'TU_EC_30m': {"source_ids": ["CR5000_flux", "CR5000_Rad_tu"], "interval": "30min"},
    'TU_MET_30m': {"source_ids": ["meteo_EneBal", "meteo_Prec_Top", "meteo_Rad_tu"], "interval": "30min"},
    'TU_WXT_30m': {"source_ids": ["_meteo_WXTmet", "CR200Series_Table1"], "interval": "30min"},
    'TU_Bole_30m': {"source_ids": ["soil2_Bole_temp"], "interval": "30min"},
    'TU_PROF_30m': {"source_ids": ["profil_Results"], "interval": "30min"},
    'TU_SOIL_30m': {"source_ids": ["soil_Temperatury$", "soil_Soil_HFP01"], "interval": "30min"},
    'TU_GARDEN_30m': {"source_ids": ["garden_RainGauge", "garden_T107_gar"], "interval": "30min"},
    'TU_GARDEN_10m': {"source_ids": ["garden_CS_616"], "interval": "10min"},
    'TU_STUDNIA_1_10m': {"source_ids": ["studnia_1_CS616S1"], "interval": "10min"},
    'TU_PROF_2m': {"source_ids": ["LI840_stor"], "interval": "2min"},
    'TU_RAD_1': {"source_ids": ["meteo_Rad_1min", "CR5000_Rad_1min"], "interval": "1min"},
    # 'TU_MET_30min': { 'source_ids': ['CR5000_flux', 'Rad_tu', '_Bole_temp', '_meteo_Spec_idx', '_meteo_EneBal', '_meteo_Prec_Top', 'CR200Series_Table1', '_meteo_WXTmet', '_Results', '_profil_comp_integ', '_soil_Temperatury$', '_soil_Soil_HFP01', '_garden_RainGauge', '_garden_T107_gar'], 'interval': '30min' },
    # 'TU_MET_10min': { 'source_ids': ['_garden_CS_616', '_studnia_1_CS616S1'], 'interval': '10min' },
    # 'TU_MET_2min': { 'source_ids': ['CR1000_LI840_stor'], 'interval': '2min' },
    # 'TU_MET_1min': { 'source_ids': ['_Rad_1min'], 'interval': '1min' },
    # 'TU_MET_30sec': { 'source_ids': ['CR1000_soil2_SoTemS13'], 'interval': '30s' }, # Zmieniono '30sec' na '30S'
    # 'TU_MET_5sec': { 'source_ids': ['_soil_PPFD_under_tree', 'CR1000_meteo_GlobalRad', '_soil_Temperatury_5$', 'CR1000_meteo_GlobalRad'], 'interval': '5s' }, # Zmieniono '5sec' na '5S'
    # 'TU_MET_1sec': { 'source_ids': ['CR1000_profil_LI840', 'CR1000_LI840', 'CR1000_soil2_LI840'], 'interval': '1s' }, # Zmieniono '1sec' na '1S'
    # Brody
    'BR_CSI_30': {"source_ids": ["CR3000_Barometr", "CR3000_CR3000", "CR3000_Multiplekser_dat", "CR3000_Rain"], "interval": "30min"},
    'BR_SpectralData_1m': {"source_ids": ["CR3000_SpectralData"], "interval": "1min"},
    'BR_Spec_Veg_Ind_30m': {"source_ids": ["CR3000_Spec_Veg_Ind"], "interval": "30min"},
    # 'BR_MET_30min': { 'source_ids': ['CR3000_Barometr', 'CR3000_CR3000', 'CR3000_Multiplekser_dat', 'CR3000_Rain', 'CR3000_Spec_Veg_Ind'], 'interval': '30min' },
    # 'BR_MET_1min': { 'source_ids': ['CR3000_SpectralData'], 'interval': '1min' },
    # Tlen1
    'TL1_MET_30': {"source_ids": ["Meteo_TR_30min"], "interval": "30min"},
    'TL1_RAD_30': {"source_ids": ["Rad_TR_30min"], "interval": "30min"},
    'TL1_SOIL_30': {"source_ids": ["Soil_TR_30min"], "interval": "30min"},
    'TL1_RAD_1': {"source_ids": ["Rad_TR_1min"], "interval": "1min"},
    # 'TL1_RAD_30min': { 'source_ids': [ 'TR_30min' ], 'interval': '30min' },
    # 'TL1_RAD_1min': { 'source_ids': [ 'TR_1min' ], 'interval': '1min' },
    # Tlen1a
    'TL1a_MET_30_dT': {"source_ids": ["pom30m_"], "interval": "30min"},
    'TL1a_Rain_down_dT': {"source_ids": ["deszcz_d_"], "interval": "30min"},
    'TL1a_MET_1_dT': {"source_ids": ["pom1m_"], "interval": "1min"},
    'TL1a_CalPlates_dT': {"source_ids": ["plytki_calib_"], "interval": "1min"},
    # 'TL1a_MET_30_dT': { 'source_ids': [ 'pom30m_'], 'interval': '30min' },
    # 'TL1a_Rain_down_30min': { 'source_ids': [ 'deszcz_d_'], 'interval': '30min' },
    # 'TL1a_MET_1_dT': { 'source_ids': [ 'pom1m_' ], 'interval': '1min' },
    # 'TL1a_CalPlates_1min': { 'source_ids': [ 'plytki_calib_' ], 'interval': '1min' },
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
    'SA_MET_30min': { 'source_ids': [ 'SA_biomet_Biomet', '_cnf4_data', 'SA_soil'], 'interval': '30min' },
    'SA_MET_1min': { 'source_ids': [ 'SA_biomet_Meteo_1min'], 'interval': '1min' }
}

# 5. SŁOWNIK KOREKTY STREF CZASOWYCH
TIMEZONE_CORRECTIONS = {
    # 5.1.1. Definicja "konfiguracji-matki" dla stacji TU
    'TU_TZSHIFT': {
        'source_tz': 'Europe/Warsaw', # Nazwa strefy czasowej, w której rejestrator zapisywał dane przed datą poprawki (np. 'Europe/Warsaw'). Używanie nazw z bazy IANA (jak Europe/Warsaw) jest kluczowe, ponieważ automatycznie obsługują one zarówno czas zimowy (CET, UTC+1), jak i letni (CEST, UTC+2).
        'correction_end_date': '2011-05-27 09:00:00', # Data i godzina, po której dane są już zapisywane poprawnie. Skrypt zastosuje specjalną korektę tylko do danych z timestampami wcześniejszymi lub równymi tej dacie.
        'post_correction_tz': 'Etc/GMT-1', # Strefa czasowa "poprawnych" danych (tych po correction_end_date).
        'target_tz': 'Etc/GMT-1' # Docelowa, jednolita strefa czasowa dla wszystkich danych w plikach wynikowych. ("UTC/GMT +1 godzina"), użyjemy 'Etc/GMT-1'. (Uwaga: notacja Etc/GMT ma odwrócony znak, więc Etc/GMT-1 oznacza UTC+1).
    },

   # 5.1.2. Poszczególne file_id, które wskazują na wspólną konfigurację
    'TU_PROF_1s': 'TU_TZSHIFT',
    'TU_SOIL_5s': 'TU_TZSHIFT',
    'TU_MET_5s': 'TU_TZSHIFT',
    'TU_SOIL2_30s': 'TU_TZSHIFT',
    'TU_EC_30m': 'TU_TZSHIFT',
    'TU_MET_30m': 'TU_TZSHIFT',
    'TU_WXT_30m': 'TU_TZSHIFT',
    'TU_Bole_30m': 'TU_TZSHIFT',
    'TU_PROF_30m': 'TU_TZSHIFT',
    'TU_SOIL_30m': 'TU_TZSHIFT',
    'TU_GARDEN_30m': 'TU_TZSHIFT',
    'TU_GARDEN_10m': 'TU_TZSHIFT',
    'TU_STUDNIA_1_10m': 'TU_TZSHIFT',
    'TU_PROF_2m': 'TU_TZSHIFT',
    'TU_RAD_1': 'TU_TZSHIFT',

    # 5.2.1. Definicja "konfiguracji-matki" dla stacji TL1
    'TL1_TZSHIFT': {
        'source_tz': 'Etc/GMT-1',
        'correction_end_date': '2012-01-02 00:00:00', #'2016-01-02 00:00:00',
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },

    'TL1_MET_30' : 'TL1_TZSHIFT',
    'TL1_RAD_30' : 'TL1_TZSHIFT',
    'TL1_SOIL_30' : 'TL1_TZSHIFT',
    'TL1_RAD_1' : 'TL1_TZSHIFT',

    # 5.3.1. Definicja "konfiguracji-matki" dla stacji TL1a
    'TL1a_TZSHIFT': {
        'source_tz': 'Etc/GMT-2',
        'correction_end_date': '2050-05-10 12:00:00',
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    # 5.3.2. Definicja "konfiguracji-matki" dla stacji TL1a
    'TL1a_MET_30_dT' : 'TL1a_TZSHIFT',
    'TL1a_Rain_down_dT' : 'TL1a_TZSHIFT',
    'TL1a_MET_1_dT' : 'TL1a_TZSHIFT',
    'TL1a_CalPlates_dT' : 'TL1a_TZSHIFT',

    'TL1a_NEW_TZSHIFT': {
        'source_tz': 'Etc/GMT-1',
        'correction_end_date': '2050-05-10 12:00:00', # znajdź datę po której jest CET
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    # 5.3.3. Definicja "konfiguracji-matki" dla stacji TL1a (oryginalne dane są w czasie zimowym górowanie słońca o ok. 12:00, brak konieczności systemowej zmiany czasu zima/lato)
    'TL1a_MET_30_csi' : 'TL1a_NEW_TZSHIFT',
    'TL1a_MET_1_csi' : 'TL1a_NEW_TZSHIFT',

    # 5.4.1. Definicja "konfiguracji-matki" dla stacji TL2
    'TL2_TZSHIFT': {
        'source_tz': 'Etc/GMT-2',
        'correction_end_date': '2051-05-1 12:00:00',
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    # 5.4.2. Definicja "konfiguracji-matki" dla stacji TL2
    'TL2_CalPlates_dT' : 'TL2_TZSHIFT',
    'TL2_MET_1_dT' : 'TL2_TZSHIFT',
    'TL2_MET_30_dT' : 'TL2_TZSHIFT',

    'TL2_TZSHIFT2': {
        'source_tz': 'Europe/Warsaw',
        'correction_end_date': '2014-10-26 1:00:00',
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    # 5.4.3. Definicja "konfiguracji-matki" dla stacji TL2
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

    # 5.5.1 Definicja "konfiguracji-matki" dla stacji ME
    'ME_TZSHIFT': {
        'source_tz': 'Etc/GMT-1', # GMT
        'correction_end_date': '2050-05-10 12:00:00',
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    # 5.5.2. Definicja "konfiguracji-matki" dla stacji ME
     'ME_TOP_MET_30min' : 'ME_TZSHIFT',
     'ME_DOWN_MET_30min' : 'ME_TZSHIFT',
     'ME_Rain_down' : 'ME_TZSHIFT',
     'ME_DOWN_MET_1min' : 'ME_TZSHIFT',
     'ME_DOWN_MET_30min' : 'ME_TZSHIFT',
     'ME_Rain_top' : 'ME_TZSHIFT',
     'ME_CalPlates' : 'ME_TZSHIFT',
     'ME_MET_10m' : 'ME_TZSHIFT',
     'ME_TOP_MET_1min' : 'ME_TZSHIFT',

     # 5.6.1. Definicja "konfiguracji-matki" dla stacji SA
    'SA_TZSHIFT': {
        'source_tz': 'Europe/Warsaw',
        'correction_end_date': '2022-01-14 11:00:00',
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    # 5.6.2. Definicja "konfiguracji-matki" dla stacji SA
     'SA_MET_30min' : 'SA_TZSHIFT',
     'SA_MET_1min' : 'SA_TZSHIFT'
}

# 6. SŁOWNIK RĘCZNYCH PRZESUNIĘĆ CZASOWYCH
MANUAL_TIME_SHIFTS = {
    'TU_MTSHIFT': [
        {'start': '2008-01-01 00:00:00', 'end': '2008-03-31 00:00:00', 'offset_hours': 1 },
        {'start': '2011-03-27 00:00:00', 'end': '2011-05-27 09:00:00', 'offset_hours': 1 },
    ],

    'TU_PROF_1s': 'TU_MTSHIFT', 'TU_SOIL_5s': 'TU_MTSHIFT', 'TU_MET_5s': 'TU_MTSHIFT', 'TU_SOIL2_30s': 'TU_MTSHIFT', 'TU_EC_30m': 'TU_MTSHIFT',
    'TU_MET_30m': 'TU_MTSHIFT', 'TU_WXT_30m': 'TU_TZSHIFT', 'TU_Bole_30m': 'TU_MTSHIFT', 'TU_PROF_30m': 'TU_MTSHIFT', 'TU_SOIL_30m': 'TU_MTSHIFT',
    'TU_GARDEN_30m': 'TU_MTSHIFT', 'TU_GARDEN_10m': 'TU_MTSHIFT', 'TU_STUDNIA_1_10m': 'TU_MTSHIFT', 'TU_PROF_2m': 'TU_MTSHIFT',
    'TU_RAD_1': 'TU_MTSHIFT',

    'ME_MTSHIFT': [
        {"start": "2018-01-11 08:07:00", "end": "2018-01-15 05:19:00", "offset_hours": 4.204861 },
        # {"start": "2018-05-30 16:03:00", "end": "2018-06-02 21:40:00", "offset_hours": 59 },
        # {"start": "2018-09-03 16:03:00", "end": "2018-09-05 01:00:00", "offset_hours": -5 },
        # {"start": "2019-07-04 23:46:00", "end": "2019-07-08 20:21:00", "offset_hours": 18.43 },
        # {"start": "2019-11-22 16:04:00", "end": "2019-11-28 06:59:00", "offset_hours": 5.7 },
        # {"start": "2019-12-09 11:51:00", "end": "2019-12-31 23:59:00", "offset_hours": 2.62 },
        # {"start": "2020-01-01 00:00:00", "end": "2020-01-06 11:59:00", "offset_hours": 2.62 },
        # {"start": "2020-01-11 11:14:00", "end": "2020-01-15 15:19:00", "offset_hours": 100.91666 }, #100.95 109.8167
        # # {"start": "2020-01-15 01:19:00", "end": "2020-01-15 05:19:00", "offset_hours": 8.866667 },
        # {"start": "2021-06-29 03:17:00", "end": "2021-06-30 08:17:00", "offset_hours": 557.76 },
        # {"start": "2021-08-01 04:12:00", "end": "2021-08-04 08:59:00", "offset_hours": 37 },
        # {"start": "2021-08-07 19:30:00", "end": "2021-08-08 19:42:00", "offset_hours": 111.45 },
        # {"start": "2021-11-10 10:58:00", "end": "2021-11-12 00:42:00", "offset_hours": 14 },
        
        # {"start": "2021-11-10 10:58:00", "end": "2021-11-12 01:13:00", "offset_hours": 14.5 },
        # {"start": "2021-11-24 11:23:00", "end": "2021-11-25 05:49:00", "offset_hours": 28.52 }, #
        # {"start": "2021-12-06 12:16:00", "end": "2021-12-08 07:55:00", "offset_hours": 25.63 },
        # {"start": "2022-06-13 15:28:00", "end": "2022-07-12 23:46:00", "offset_hours": -1.73 },
        # {"start": "2023-02-12 10:13:00", "end": "2023-02-12 11:12:00", "offset_hours": 6.4666667 },
        # {'start': '2024-09-10 08:36:00', 'end': '2024-09-10 12:20:00', "offset_hours": 333.85 },
        # {'start': '2024-11-10 03:19:00', 'end': '2024-11-10 03:27:00', "offset_hours": 52.35 },
        # {'start': '2024-11-19 13:41:00', 'end': '2024-11-20 17:01:00', "offset_hours": 50.73 },
        # {'start': '2024-12-10 04:08:00', 'end': '2024-12-10 05:08:00', "offset_hours": 9.42 },
        # {'start': '2025-01-08 02:46:00', 'end': '2025-01-08 16:08:00', "offset_hours": 41.12 },
        # {'start': '2025-05-15 05:26:00', 'end': '2025-05-20 08:53:00', "offset_hours": 28.4 },
    ],
    'ME_DOWN_MET_30min' : 'ME_MTSHIFT', 'ME_DOWN_MET_1min' : 'ME_MTSHIFT',
    'ME_Rain_down' : 'ME_MTSHIFT', 'ME_CalPlates' : 'ME_MTSHIFT',
    # 'ME_TOP_MET_30min' : 'ME_MTSHIFT', 'ME_DOWN_MET_30min' : 'ME_MTSHIFT', 'ME_Rain_down' : 'ME_MTSHIFT',
    # 'ME_DOWN_MET_1min' : 'ME_MTSHIFT', 'ME_DOWN_MET_30min' : 'ME_MTSHIFT', 'ME_Rain_top' : 'ME_MTSHIFT',
    # 'ME_CalPlates' : 'ME_MTSHIFT', 'ME_MET_10m' : 'ME_MTSHIFT',

    'TL2_MTSHIFT': [
        { "start": "2014-10-26 02:00:00", "end": "2015-03-19 11:49", "offset_hours": -2},
        { "start": "2015-03-19 11:50:00", "end": "2051-09-16", "offset_hours": -1},
    ],
    'TL2_MET_30m' : 'TL2_MTSHIFT', 'TL2_MET_1m' : 'TL2_MTSHIFT',

    'TL1_MTSHIFT': [
        # { "start": "2013-01-01 00:00:00", "end": "2055-08-12 14:00", "offset_hours": -2},
        { "start": "2015-08-12 12:05:00", "end": "2016-01-12 01:00:00", "offset_hours": -0.2},
        # { "start": "2016-01-02 01:00:00", "end": "2055-03-19 11:49", "offset_hours": -2},
    ],
    'TL1_RAD_30min' : 'TL1_MTSHIFT', 'TL1_RAD_1min' : 'TL1_MTSHIFT',

    'TL1_dT_MTSHIFT': [
        # { "start": "2021-11-03 01:00:00", "end": "2055-01-26 21:00", "offset_hours": 0},
        # { "start": "2021-01-01 00:00:00", "end": "2021-02-20 00:00", "offset_hours": -1},
        # { "start": "2021-08-05 00:00:00", "end": "2051-09-20 20:00", "offset_hours": 0.5},
        { "start": "2018-09-18 00:00:00", "end": "2018-11-18 00:00", "offset_hours": 0.5},
        { "start": "2021-10-25 00:00:00", "end": "2055-01-26 21:00", "offset_hours": 1},
        # { "start": "2021-09-19 01:00:00", "end": "2021-09-23 11:49", "offset_hours": -1}, # godzina bez - znaczenia braki w danych
    ],
    'TL1a_MET_30_dT' : 'TL1_dT_MTSHIFT', 'TL1a_MET_1_dT' : 'TL1_dT_MTSHIFT', 'TL1a_Rain_down_dT' : 'TL1_dT_MTSHIFT', 'TL1a_CalPlates_1min' : 'TL1_dT_MTSHIFT',

    'SA_MTSHIFT': [
        { "start": "2021-10-31 03:00:00", "end": "2022-01-10 00:00", "offset_hours": -1},
    ],
    'SA_MET_30min' : 'SA_MTSHIFT', 'SA_MET_1min' : 'SA_MTSHIFT',
}

# 7. SŁOWNIK KALIBRACJI
CALIBRATION_RULES_BY_STATION = {
    'TUCZNO_CAL': {
        # Wszystkie reguły dla JEDNEJ kolumny muszą być w JEDNEJ liście
        'PPFD_IN_1_1_1': [
            {'start': '2008-01-01 00:00:00', 'end': '2011-01-19 16:00:00', 'multiplier': 0.6040033, 'addend': 0, 'reason': 'SKP215, (data in umol/m2/s1)'},
            {'start': '2011-03-08 07:30:00', 'end': '2011-04-20 17:00:00', 'multiplier': 0.6040033, 'addend': 0, 'reason': 'SKP215, (data in umol/m2/s1)'},
            # {'start': '2011-03-08 07:30:00', 'end': '2011-03-08 17:30:00', 'multiplier': 0.6040033, 'addend': 0, 'reason': 'SKP215, (data in umol/m2/s1)'},
            {'start': '2011-02-12 05:30:00', 'end': '2011-02-15 13:00:00', 'multiplier': 0.6040033, 'addend': 0, 'reason': 'SKP215 - korekta'},
        ],
        'PPFD_BC_IN_1_1_2': [
            {'start': '2018-08-08 13:30:00', 'end': '2018-11-17 12:00:00', 'multiplier': 1, 'addend': -650, 'reason': 'LQA3028 - stara korekta'},
            {'start': '2018-11-13 23:00:00', 'end': '2058-11-13 23:00:00', 'multiplier': 3288.716, 'addend': 0, 'reason': 'LQA3028, (data in umol/m2/s1)'},
            {'start': '2019-09-01 19:30:00', 'end': '2019-09-13 06:00:00', 'multiplier': 1, 'addend': -230, 'reason': 'LQA3028 - korekta'},
        ]
    },
    'MEZYK_DOWN_CAL': {
        # Wszystkie reguły dla JEDNEJ kolumny muszą być w JEDNEJ liście
        'PPFD_BC_IN_1_1_1': [
            {'start': '2018-11-13 23:00:00', 'end': '2058-11-13 23:00:00', 'multiplier': 3397.547, 'addend': 0, 'reason': 'LQA3027, (data in umol/m2/s1)'},
            {'start': '2019-09-01 19:30:00', 'end': '2019-09-13 06:00:00', 'multiplier': 1, 'addend': -230, 'reason': 'LQA3028 - korekta'},
        ],
        'PPFD_BC_IN_1_1_2': [
            {'start': '2018-08-08 13:30:00', 'end': '2018-11-17 12:00:00', 'multiplier': 1, 'addend': -650, 'reason': 'LQA3028 - stara korekta'},
            {'start': '2018-11-13 23:00:00', 'end': '2058-11-13 23:00:00', 'multiplier': 3288.716, 'addend': 0, 'reason': 'LQA3028, (data in umol/m2/s1)'},
            {'start': '2019-09-01 19:30:00', 'end': '2019-09-13 06:00:00', 'multiplier': 1, 'addend': -230, 'reason': 'LQA3028 - korekta'},
        ],
        'G_1_1_1': [
            {'start': '2018-06-16 14:00:00', 'end': '2018-11-14', 'multiplier': 15.865, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
            {'start': '2018-06-16 14:00:00', 'end': '2018-07-16 19:00:00', 'multiplier': 0.1, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
        ],
        'G_2_1_1': [
            {'start': '2018-06-16 14:00:00', 'end': '2018-11-14', 'multiplier': 15.830, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
            {'start': '2018-06-16 14:00:00', 'end': '2018-07-16 19:00:00', 'multiplier': 0.1, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
        ],
        'G_3_1_1': [
            {'start': '2018-06-16 14:00:00', 'end': '2018-11-14', 'multiplier': 15.110, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
            {'start': '2018-06-16 14:00:00', 'end': '2018-07-16 19:00:00', 'multiplier': 0.1, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
        ],
        'G_4_1_1': [
              {'start': '2018-06-16 14:00:00', 'end': '2018-11-14', 'multiplier': 16.168, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
              {'start': '2018-06-16 14:00:00', 'end': '2018-07-16 19:00:00', 'multiplier': 0.1, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
        ],
        'G_5_1_1': [
            {'start': '2018-06-16 14:00:00', 'end': '2018-11-14', 'multiplier': 15.681, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
            {'start': '2018-06-16 14:00:00', 'end': '2018-07-16 19:00:00', 'multiplier': 0.1, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
        ],
        'G_6_1_1': [
             {'start': '2018-06-16 14:00:00', 'end': '2018-11-14', 'multiplier': 15.530, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
             {'start': '2018-06-16 14:00:00', 'end': '2018-07-16 19:00:00', 'multiplier': 0.1, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
        ],
        'G_7_1_1': [
            {'start': '2018-06-16 14:00:00', 'end': '2018-11-14', 'multiplier': 15.681, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
            {'start': '2018-06-16 14:00:00', 'end': '2018-07-16 19:00:00', 'multiplier': 0.1, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
        ],
        'G_8_1_1': [
            {'start': '2018-06-16 14:00:00', 'end': '2018-11-14', 'multiplier': 15.929, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
            {'start': '2018-06-16 14:00:00', 'end': '2018-07-16 19:00:00', 'multiplier': 0.1, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
        ],
        'G_9_1_1': [
             {'start': '2018-06-16 14:00:00', 'end': '2018-11-14', 'multiplier': 15.743, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
             {'start': '2018-06-16 14:00:00', 'end': '2018-07-16 19:00:00', 'multiplier': 0.1, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
        ],
        'G_10_1_1': [
             {'start': '2018-06-16 14:00:00', 'end': '2018-11-14', 'multiplier': 15.718, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
             {'start': '2018-06-16 14:00:00', 'end': '2018-07-16 19:00:00', 'multiplier': 0.1, 'addend': 0, 'reason': 'Hukseflux - korekta mV->w/m2'},
        ],
    },
    'MEZYK_TOP_CAL': {
        'PPFD_1_2_1': [
            {'start': '2018-01-01 00:00:00', 'end': '2018-11-15 12:00:00', 'multiplier': 199.601, 'addend': 0, 'reason': 'CNR4 (data in W/m2)'},
        ],
        'PPFDr_1_2_1': [
            {'start': '2018-01-01 00:00:00', 'end': '2018-11-15 12:00:00', 'multiplier': 201.613, 'addend': 0, 'reason': 'CNR4 (data in W/m2)'},
        ],
        'SWin_1_2_1': [
            {'start': '2018-08-09 04:30:00', 'end': '2018-11-15 12:00:00', 'multiplier': 74.85, 'addend': 0, 'reason': 'CNR4 (data in W/m2)'},
        ],
        'SWout_1_2_1': [
            {'start': '2018-08-09 04:30:00', 'end': '2018-11-15 12:00:00', 'multiplier': 81.5, 'addend': 0, 'reason': 'CNR4 (data in W/m2)'},
        ]
    },
    ## Tlen1 old site added by Klaudia- 19.07.2025
    'TL1_CAL': {
	#Radiation fluxes- NR01
        'SW_IN_1_1_1' : [
            {'start':  '2013-04-25 17:30:00', 'end' : '2014-07-09 17:30:00', 'multiplier': 1.1658, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000- NR01 poprawne współczynniki i lokalizacja kanałów pomiarowych'},
        ],
        'SW_OUT_1_1_1' : [
            {'start':  '2013-04-25 17:30:00', 'end' : '2014-07-09 17:30:00', 'multiplier': 0.6935, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000- NR01 poprawne współczynniki i lokalizacja kanałów pomiarowych'},
        ],
    	'LW_IN_1_1_1' : [
            {'start':  '2013-04-25 17:30:00', 'end' : '2014-07-09 17:30:00', 'multiplier': 1.2976, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000- NR01 poprawne współczynniki i lokalizacja kanałów pomiarowych'},
        ],
        'LW_OUT_1_1_1' : [
            {'start':  '2013-04-25 17:30:00', 'end' : '2014-07-09 17:30:00', 'multiplier': 0.9750, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000- NR01 poprawne współczynniki i lokalizacja kanałów pomiarowych'},
        ],
	#Soil heat flux measurements
        'G_1_1_1': [
            {'start': '2013-04-25 17:30:00', 'end': '2014-06-17 08:00:00', 'multiplier': 16.07, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'},
            {'start': '2014-06-24 02:30:00', 'end': '2014-07-09 22:00:00', 'multiplier': 16.07, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'}
        ],
         'G_1_2_1': [
            {'start': '2013-04-25 17:30:00', 'end': '2014-06-17 08:00:00', 'multiplier': 16.06, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'},
            {'start': '2014-06-24 02:30:00', 'end': '2014-07-09 22:00:00', 'multiplier': 16.06, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'}
        ],
        'G_1_3_1': [
            {'start': '2013-04-25 17:30:00', 'end': '2014-06-17 08:00:00', 'multiplier': 16.29, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'},
            {'start': '2014-06-24 02:30:00', 'end': '2014-07-09 22:00:00', 'multiplier': 16.29, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'}
        ],
        'G_1_4_1': [
            {'start': '2013-04-25 17:30:00', 'end': '2014-06-17 08:00:00', 'multiplier': 16.29, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'},
            {'start': '2014-06-24 02:30:00', 'end': '2014-07-09 22:00:00', 'multiplier': 16.99, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'}
        ],
    },
    ## Tlen1a site - dataTacker measurements (03.08.2018- 12.06.2024)
    'TL1adT_CAL': {
        'SWin_1_1_1' : [
            {'start':  '2014-07-08 09:30:00', 'end' : '2018-11-13 09:30:00', 'multiplier': 77.101, 'addend': 0, 'reason': 'NR01 (data in W/m2)'},
        ],
        'PPFD_1_1_1' : [
            {'start':  '2014-07-08 09:30:00', 'end' : '2018-11-13 09:30:00', 'multiplier': 210.97, 'addend': 0, 'reason': 'PQ1 (data in umol/m2/s)'},
        ]
    },
    ## Tlen2 "old" tower added by Klaudia- 19.07.2025
     'TL2_CAL': {
        'G_1_1_1': [
            {'start': '2014-07-10 09:30:00', 'end': '2014-07-18 01:30:00', 'multiplier': 1, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'}
        ],
         'G_1_2_1': [
            {'start': '2014-07-10 09:30:00', 'end': '2014-07-18 01:30:00', 'multiplier':  0.0625, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'}
        ],
        'G_1_3_1': [
            {'start': '2014-07-10 09:30:00', 'end': '2014-07-18 01:30:00', 'multiplier': 16.008, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'}
        ],
        'G_1_4_1': [
            {'start': '2014-07-10 09:30:00', 'end': '2014-07-18 01:30:00', 'multiplier': 1, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'}
        ],
     },
    'TL2dT_CAL': {
        'SWin_1_2_1' : [
            {'start':  '2014-07-08 09:30:00', 'end' : '2018-11-16 12:30:00', 'multiplier': 69.638, 'addend': 0, 'reason': 'NR01 (data in W/m2)'},
        ],
        'PPFD_1_2_1' : [
            {'start':  '2014-07-08 09:30:00', 'end' : '2018-11-16 12:30:00', 'multiplier': 213.675, 'addend': 0, 'reason': 'PQ1 (data in umol/m2/s)'},
        ]
    }
}
# 7.2. Mapowanie grup kalibracyjnych 
STATION_MAPPING_FOR_CALIBRATION = {
    # Mezyk (top)
    'ME_TOP_MET_30min': 'MEZYK_TOP_CAL',
    'ME_TOP_MET_1min' : 'MEZYK_TOP_CAL',
    'ME_Rain_top': 'MEZYK_TOP_CAL',
    # Mezyk (down)
    'ME_DOWN_MET_30min' : 'MEZYK_DOWN_CAL',
    'ME_DOWN_MET_1min' : 'MEZYK_DOWN_CAL',
    'ME_Rain_down' : 'MEZYK_DOWN_CAL',
    'ME_CalPlates' : 'MEZYK_DOWN_CAL',
    # Tlen1 old site added by Klaudia- 19.07.2025
    'TL1_RAD_30min' : 'TL1_CAL',
    'TL1_RAD_1min' : 'TL1_CAL',
    'TL1_MET_30': 'TL1_CAL',
    'TL1_SOIL_30': 'TL1_CAL',
    # Tlen1a site - dataTacker measurements (03.08.2018- 12.06.2024)
    'TL1a_MET_30_dT' : 'TL1adT_CAL',
    'TL1a_MET_1_dT' : 'TL1adT_CAL',
    # Tlen2 "old" tower added by Klaudia- 19.07.2025
    'TL2_MET_1m' : 'TL2_CAL',
    'TL2_MET_30m': 'TL2_CAL',
    # Tlen2 site - "new" tower - dataTacker measurements (01.08.2018- 26.06.2024)
    'TL2_MET_1_dT' : 'TL2dT_CAL',
    'TL2_MET_30_dT' : 'TL2dT_CAL',
}

# 8. SŁOWNIK FLAG JAKOŚCI
QUALITY_FLAGS = {
    # Kluczem jest grupa zdefiniowana w słowniku FILE_ID_MERGE_GROUPS
    'ME_DOWN_QF': {
        # Kluczem jest nazwa kolumny (parametru)
        '*': [
            # {'start': '2018-06-01 10:19:00', 'end': '2018-06-03 14:04:00', 'flag_value': 2, 'reason': 'Awaria rejestratora1'},
            {'start': '2018-06-07 12:03:00', 'end': '2018-06-07 14:23:00', 'flag_value': 2, 'reason': 'Awaria rejestratora1'},
            # {'start': '2018-09-03 05:00:00', 'end': '2018-09-05 00:00:00', 'flag_value': 2, 'reason': 'Awaria rejestratora1'},
            {'start': '2018-09-21 00:06:00', 'end': '2018-09-21 23:30:00', 'flag_value': 2, 'reason': 'Błąd synchronizacji'},
            {'start': '2018-11-13 22:30:00', 'end': '2018-11-14 23:30:00', 'flag_value': 2, 'reason': 'Awaria rejestratora1'},
            {'start': '2020-01-10 06:57:00', 'end': '2020-01-11 16:14:00', 'flag_value': 2, 'reason': 'Awaria rejestratora2'},
            {'start': '2021-07-31 20:48:00', 'end': '2021-08-01 04:11:00', 'flag_value': 2, 'reason': 'Awaria rejestratora3'},
            {'start': '2021-10-03 20:03:00', 'end': '2021-10-03 22:04:00', 'flag_value': 2, 'reason': 'Awaria rejestratora4'},
            {'start': '2021-11-24 11:30:00', 'end': '2021-11-24 13:30:00', 'flag_value': 2, 'reason': 'Awaria rejestratora5'},
            {'start': '2020-01-10 06:57:00', 'end': '2020-01-11 16:14:00', 'flag_value': 2, 'reason': 'Awaria rejestratora2'},
        ],
        'G_1_1_1': [
            {'start': '2019-09-01 16:00:00', 'end': '2019-09-13 00:00:00', 'flag_value': 2, 'reason': 'Nierealne wartości strumienia G'},
        ],
        'G_2_1_1': [
            {'start': '2019-09-01 16:00:00', 'end': '2019-09-13 00:00:00', 'flag_value': 2, 'reason': 'Nierealne wartości strumienia G'},
        ],
        'G_3_1_1': [
            {'start': '2019-09-01 16:00:00', 'end': '2019-09-13 00:00:00', 'flag_value': 2, 'reason': 'Nierealne wartości strumienia G'},
        ],
        'G_5_1_1': [
            {'start': '2019-09-01 16:00:00', 'end': '2019-09-13 00:00:00', 'flag_value': 2, 'reason': 'Nierealne wartości strumienia G'},
        ],
        'G_6_1_1': [
            {'start': '2019-09-01 16:00:00', 'end': '2019-09-13 00:00:00', 'flag_value': 2, 'reason': 'Nierealne wartości strumienia G'},
        ],
        'G_7_1_1': [
            {'start': '2019-09-01 16:00:00', 'end': '2019-09-13 00:00:00', 'flag_value': 2, 'reason': 'Nierealne wartości strumienia G'},
        ],
        'PPFD_BC_IN_1_1_1': [
            {'start': '2018-01-01 00:00:00', 'end': '2018-05-14 10:30:00', 'flag_value': 3, 'reason': 'brak sensora'},
            # {'start': '2018-06-07 12:03:00', 'end': '2018-06-07 14:23:00', 'flag_value': 3, 'reason': 'zerowe w dzien'},
            {'start': '2018-11-17 14:21:00', 'end': '2018-11-17 14:24:00', 'flag_value': 3, 'reason': 'spike'},
            {'start': '2018-10-06 20:20:00', 'end': '2018-10-06 20:20:00', 'flag_value': 3, 'reason': 'spike'},
            
        ],
        'PPFD_BC_IN_1_1_2': [
            {'start': '2018-01-01 00:00:00', 'end': '2018-05-14 10:30:00', 'flag_value': 3, 'reason': 'brak sensora'},
            # {'start': '2018-06-07 12:03:00', 'end': '2018-06-07 14:23:00', 'flag_value': 3, 'reason': 'zerowe w dzien'},
            {'start': '2018-11-17 14:21:00', 'end': '2018-11-17 14:24:00', 'flag_value': 3, 'reason': 'spike'},
        ],
        'PPFD_IN_1_2_1': [
            {'start': '2018-01-01 00:00:00', 'end': '2018-05-14 10:30:00', 'flag_value': 3, 'reason': 'brak sensora'},
        ],
        'PPFD_OUT_1_2_1': [
            {'start': '2018-01-01 00:00:00', 'end': '2018-05-14 10:30:00', 'flag_value': 3, 'reason': 'brak sensora'},
            {'start': '2018-11-17 14:21:00', 'end': '2018-11-17 14:24:00', 'flag_value': 3, 'reason': 'spike'},
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
            {'start': '2019-10-15', 'end': '2019-10-18 03:30:00', 'flag_value': 3, 'reason': 'brak sensora'},
        ],
        'PPFD_DIF_1_1_1':[
            {'start': '2019-10-15', 'end': '2019-10-18 03:30:00', 'flag_value': 3, 'reason': 'brak sensora'},
        ],
        'SW_IN_1_2_1':[
            {'start': '2017-10-15', 'end': '2018-08-09 04:30:00', 'flag_value': 3, 'reason': 'brak sensora - zanim instalcja'},
        ],
        'SW_OUT_1_2_1':[
            {'start': '2017-10-15', 'end': '2018-08-09 04:30:00', 'flag_value': 3, 'reason': 'brak sensora - zanim instalcja'},
        ],
        'LW_IN_1_2_1':[
            {'start': '2017-10-15', 'end': '2018-08-09 04:30:00', 'flag_value': 3, 'reason': 'brak sensora - zanim instalcja'},
        ],
        'LW_OUT_1_2_1':[
            {'start': '2017-10-15', 'end': '2018-08-09 04:30:00', 'flag_value': 3, 'reason': 'brak sensora - zanim instalcja'},
        ]
    },
    'TU_QF': {
        'PPFD_IN_1_1_1': [
            {'start': '2008-01-01', 'end': '2008-03-30 09:30:00', 'flag_value': 3, 'reason': 'Brak czujnika SKP215'},
        ],
        'PPFD_IN_1_1_2': [
            {'start': '2017-03-05', 'end': '2017-11-21 13:30:00', 'flag_value': 2, 'reason': 'Awaria czujnika BF3H/BF5'},
        ],
        'PPFD_DIF_Avg': [
            {'start': '2017-03-05', 'end': '2017-11-21 13:30:00', 'flag_value': 2, 'reason': 'Awaria czujnika BF3H/BF5'},
        ],
        'SunStat_Tot': [
            {'start': '2017-03-05', 'end': '2017-11-21 13:30:00', 'flag_value': 2, 'reason': 'Awaria czujnika BF3H/BF5'},
        ],
        'TS_4_3_1': [
            {'start': '2012-12-15 21:00:00', 'end': '2012-12-15 22:00:00', 'flag_value': 2, 'reason': 'Awaria czujnika t107'},
        ],
        'TS_4_1_1': [
            {'start': '2012-01-23 23:30:00', 'end': '2012-12-15 22:00:00', 'flag_value': 2, 'reason': 'Awaria czujnika t107'},
        ],
        # Wildcard '*' TYLKO dla plików zawierających w nazwie "_meteo_WXTmet" i "_meteo_Spec_idx"
        '*': [
            {'start': '2011-01-24 14:00:00', 'end': '2011-01-24 22:00:00', 'flag_value': 2, 'reason': 'Awaria czujnika promieniowania'},
            {'start': '2011-11-15 13:29:00', 'end': '2011-11-15 13:31:00', 'flag_value': 2, 'reason': 'Awaria tylko czujnika WXT', 'filename_contains': '_meteo_WXTmet'},
            {'start': '2018-11-18 12:30:00', 'end': '2019-03-04 10:30:00', 'flag_value': 3, 'reason': 'Zdemontowany WXT do upgrade', 'filename_contains': '_meteo_WXTmet'},
            {'start': '2014-02-05 15:30:00', 'end': '2014-08-12 15:30:00', 'flag_value': 2, 'reason': 'Awaria tylko czujnika Multispc', 'filename_contains': '_meteo_Spec_idx'},
            {'start': '2014-08-12 13:30:00', 'end': '2015-08-27 13:30:00', 'flag_value': 3, 'reason': 'Multispc w kalibracji', 'filename_contains': '_meteo_Spec_idx'},
            {'start': '2018-06-18 15:10:00', 'end': '2050-06-18 15:10:00', 'flag_value': 3, 'reason': 'Multispc zdemontowane', 'filename_contains': '_meteo_Spec_idx'},
        ]
    },
    'TL1_QF': {
        '*': [
            {'start': '2013-03-29 17:30:00', 'end': '2013-04-25 17:30:00', 'flag_value': 3, 'reason': 'Delay in meteo sensors intalation in relation to flux data'},
            {'start': '2021-07-14 23:00:00', 'end': '2021-07-20 09:30:00', 'flag_value': 3, 'reason': 'Storm damage'},
        ],
	# Radiation fluxes
        'PPFD_IN_1_1_1':[
            {'start': '2014-06-16 00:00:00', 'end': '2014-08-26 12:00:00', 'flag_value': 3, 'reason': 'condensation inside te sensor dome BF3/BF5H- deinstalled'},
            {'start': '2015-12-08 12:00:00', 'end': '2016-01-13 12:00:00', 'flag_value': 3, 'reason': 'condensation inside te sensor dome BF3/BF5H- deinstalled 2'},
            {'start': '2016-12-02 10:30:00', 'end': '2017-02-13 12:30:00', 'flag_value': 3, 'reason': 'condensation inside te sensor dome BF3/BF5H- deinstalled 3'},
            {'start': '2017-10-18 10:00:00', 'end': '2018-04-10 16:00:00', 'flag_value': 3, 'reason': 'condensation inside te sensor dome BF3/BF5H- deinstalled 4'},
			{'start': '2019-08-12 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'BF5 sensor deinstalled for good'},
        ],
        'PPFD_DIF_1_1_1':[
            {'start': '2014-06-16 00:00:00', 'end': '2014-08-26 12:00:00', 'flag_value': 3, 'reason': 'condensation inside te sensor dome BF3/BF5H- deinstalled'},
            {'start': '2015-12-08 12:00:00', 'end': '2016-01-13 12:00:00', 'flag_value': 3, 'reason': 'condensation inside te sensor dome BF3/BF5H- deinstalled 2'},
            {'start': '2016-12-02 10:30:00', 'end': '2017-02-13 12:30:00', 'flag_value': 3, 'reason': 'condensation inside te sensor dome BF3/BF5H- deinstalled 3'},
            {'start': '2017-10-18 10:00:00', 'end': '2018-04-10 16:00:00', 'flag_value': 3, 'reason': 'condensation inside te sensor dome BF3/BF5H- deinstalled 4'},
			{'start': '2019-08-12 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'BF5 sensor deinstalled for good'},
        ],
        'PPFD_IN_1_1_2':[
            {'start': '2013-04-25 17:30:00', 'end': '2014-01-13 10:30:00', 'flag_value': 2, 'reason': 'incorrect SKP215 readings?'},
            {'start': '2022-11-14 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'PPFD_OUT_1_1_1':[
            {'start': '2013-04-25 17:30:00', 'end': '2014-01-13 10:30:00', 'flag_value': 2, 'reason': 'incorrect SKP215 readings?'},
            {'start': '2022-11-14 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'SW_IN_1_1_1':[
            {'start': '2022-11-14 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'SW_OUT_1_1_1':[
            {'start': '2022-11-14 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'LW_IN_1_1_1':[
            {'start': '2022-11-14 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'LW_OUT_1_1_1':[
            {'start': '2022-11-14 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
	#Air temperature and humidity
        'TA_1_1_1':[
            {'start': '2013-04-25 17:30:00', 'end': '2013-05-17 12:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '2022-11-14 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'RH_1_1_1':[
            {'start': '2013-04-25 17:30:00', 'end': '2013-05-17 12:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '2022-11-14 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'TA_1_2_1':[
            {'start': '2013-04-25 17:30:00', 'end': '2014-04-04 11:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '2022-11-14 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'RH_1_2_1':[
            {'start': '2013-04-25 17:30:00', 'end': '2014-04-04 11:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '2022-11-14 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ]
    },
    # Tlen2 "old" tower added by Klaudia- 19.07.2025
    'TL2_QF': {
     '*': [
            {'start': '2018-04-13 21:30:00', 'end': '2018-05-10 16:30:00', 'flag_value': 3, 'reason': 'datalogger power source mulfuntion'},
            {'start': '2018-07-19 21:00:00', 'end': '2018-07-27 19:30:00', 'flag_value': 3, 'reason': 'power cut- damage'},
            {'start': '2020-12-30 01:30:00', 'end': '2021-04-28 13:30:00', 'flag_value': 3, 'reason': 'datalogger mulfuntion'},
        ],
    # Soil heat flux plates
        'G_1_1_1': [
            {'start': '2016-10-25 16:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 2, 'reason': 'Sensor damaged'}
        ],
       'G_1_4_1': [
            {'start': '2016-12-14 06:30:00', 'end': '2099-01-01 00:00:00', 'flag_value': 2, 'reason': 'Sensor damaged'}
        ],
    # Soil temp.
        'TS_2_2_1': [
            {'start': '2016-12-14 06:30:00', 'end': '2099-01-01 00:00:00', 'flag_value': 2, 'reason': 'Sensor damaged'}
        ],
	# Radiation fluxes
        'PPFD_IN_1_1_1':[
            {'start': '2014-07-10 09:30:00', 'end': '2014-08-29 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '2018-07-25 10:00:00', 'end': '2018-09-04 15:30:00', 'flag_value': 3, 'reason': 'gap in data'},
			{'start': '2018-09-05 05:30:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'SKL sensors deinstalled for good'},
			{'start': '2019-07-29 19:30:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'SKL sensors deinstalled for good???'},
        ],
        'PPFD_OUT_1_1_1':[
            {'start': '2014-07-10 09:30:00', 'end': '2014-08-29 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '2018-07-25 10:00:00', 'end': '2018-09-04 15:30:00', 'flag_value': 3, 'reason': 'gap in data'},
			{'start': '2018-09-05 05:30:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'SKL sensors deinstalled for good'},
			{'start': '2019-07-29 19:30:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'SKL sensors deinstalled for good???'},
        ],
        'SW_IN_1_1_1':[
            {'start': '2014-07-10 09:30:00', 'end': '2014-08-29 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet?? corrected after that?'},
            {'start': '2015-03-18 09:30:00', 'end': '2015-03-19 00:30:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'},
            {'start': '2018-07-24 09:00:00', 'end': '2018-09-09 15:30:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'},
            {'start': '2015-05-13 03:30:00', 'end': '2015-05-15 21:00:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'},
            {'start': '2019-05-13 04:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'SW_OUT_1_1_1':[
            {'start': '2014-07-10 09:30:00', 'end': '2014-08-29 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet?? corrected after that?'},
            {'start': '2015-03-18 09:30:00', 'end': '2015-03-19 00:30:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'},
            {'start': '2018-07-24 09:00:00', 'end': '2018-09-09 15:30:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'},
            {'start': '2015-05-13 03:30:00', 'end': '2015-05-15 21:00:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'},
            {'start': '2019-05-13 04:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'LW_IN_1_1_1':[
            {'start': '2014-07-10 09:30:00', 'end': '2014-08-29 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet?? corrected after that?'},
            {'start': '2015-03-18 09:30:00', 'end': '2015-03-19 00:30:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'},
            {'start': '2018-07-24 09:00:00', 'end': '2018-09-09 15:30:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'},
            {'start': '2015-05-13 03:30:00', 'end': '2015-05-15 21:00:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'},
            {'start': '2019-05-13 04:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'LW_OUT_1_1_1':[
            {'start': '2014-07-10 09:30:00', 'end': '2014-08-29 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet?? corrected after that?'},
            {'start': '2015-03-18 09:30:00', 'end': '2015-03-19 00:30:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'},
            {'start': '2018-07-24 09:00:00', 'end': '2018-09-09 15:30:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'},
            {'start': '2015-05-13 03:30:00', 'end': '2015-05-15 21:00:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'},
            {'start': '2019-05-13 04:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
	#Air temperature and humidity
        'TA_1_1_1':[
            {'start': '2014-07-10 09:30:00', 'end': '2014-08-29 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '2018-07-18 13:30:00', 'end': '2018-09-04 16:30:00', 'flag_value': 3, 'reason': 'Sensor mulfuntion'},
        ],
        'RH_1_1_1':[
            {'start': '2014-07-10 09:30:00', 'end': '2014-08-29 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '2018-07-18 13:30:00', 'end': '2018-09-04 16:30:00', 'flag_value': 3, 'reason': 'Sensor mulfuntion'},
        ],
        'TA_1_2_1':[
            {'start': '2014-07-10 09:30:00', 'end': '2014-08-29 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '2018-07-18 13:30:00', 'end': '2018-09-04 16:30:00', 'flag_value': 3, 'reason': 'Sensor mulfuntion'},
        ],
        'RH_1_2_1':[
            {'start': '2014-07-10 09:30:00', 'end': '2014-08-29 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '2018-07-18 13:30:00', 'end': '2018-09-04 16:30:00', 'flag_value': 3, 'reason': 'Sensor mulfuntion'},
        ],
    }
}
# 8.1 Mapowanie grup flag jakości danych
STATION_MAPPING_FOR_QC = {
    # Tuczno
    'TU_PROF_1s': 'TU_QF',
    'TU_SOIL_5s': 'TU_QF',
    'TU_MET_5s': 'TU_QF',
    'TU_SOIL2_30s': 'TU_QF',
    'TU_EC_30m': 'TU_QF',
    'TU_MET_30m': 'TU_QF',
    'TU_WXT_30m': 'TU_QF',
    'TU_Bole_30m': 'TU_QF',
    'TU_PROF_30m': 'TU_QF',
    'TU_SOIL_30m': 'TU_QF',
    'TU_GARDEN_30m': 'TU_QF',
    'TU_GARDEN_10m': 'TU_QF',
    'TU_STUDNIA_1_10m': 'TU_QF',
    'TU_PROF_2m': 'TU_QF',
    'TU_RAD_1': 'TU_QF',
    # Mezyk
    'ME_DOWN_MET_30min': 'ME_DOWN_QF',
    'ME_DOWN_MET_1min': 'ME_DOWN_QF',
    'ME_Rain_down': 'ME_DOWN_QF',
    'ME_CalPlates': 'ME_DOWN_QF',
    'ME_TOP_MET_30min': 'ME_TOP_QF',
    'ME_TOP_MET_1min': 'ME_TOP_QF',
    'ME_Rain_top': 'ME_TOP_QF',
    # Tlen1
    'TL1_MET_30': 'TL1_QF',
    'TL1_RAD_30': 'TL1_QF',
    'TL1_SOIL_30': 'TL1_QF',
    'TL1_RAD_1': 'TL1_QF',
    # Tlen2
    'TL2_MET_30m': 'TL2_QF',
    'TL2_MET_1m': 'TL2_QF',
}
# 9. Mapowanie nazw zmiennych. Zmienia nazwy zmiennych z oryginalnych w loggerze na FLUXNET
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
        'PPFD_191down_Avg': 'PPFD_BC_OUT_1_1_1', # nowe LI191 2 projekt
        'PPFD_191up_Avg': 'PPFD_BC_IN_1_1_2', # nowe LI191 2 projekt
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
        'T107_C2': 'TS_6_1_1',
        'T107_C3': 'TS_7_1_1',
        'T107_C4': 'TS_8_1_1',
        'T107_C5': 'TS_9_1_1',
        'T107_C6': 'TS_10_1_1',
        'T107_C7': 'TS_11_1_1',
        'T107_C8': 'TS_12_1_1',
        'T107_C9': 'TS_13_1_1',
        #
        'T107_C(1)': 'TS_5_1_1',
        'T107_C(2)': 'TS_6_1_1',
        'T107_C(3)': 'TS_7_1_1',
        'T107_C(4)': 'TS_8_1_1',
        'T107_C(5)': 'TS_9_1_1',
        'T107_C(6)': 'TS_10_1_1',
        'T107_C(7)': 'TS_11_1_1',
        'T107_C(8)': 'TS_12_1_1',
        'T107_C(9)': 'TS_13_1_1',
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
        'LWOUT_1_2_1': 'LW_OUT_1_1_1', # CNR4
        'Rn_1_2_1': 'RN_1_1_1', # CNR4
        'PPFD_BC_IN_1_1_1': 'PPFD_BC_IN_1_1_1', #
        'PPFD_BC_IN_1_1_2': 'PPFD_BC_IN_1_1_2', #
        'TA_2_2_1': 'TA_1_1_2', # CNR4
        'P_1_1_1': 'P_1_2_1', # Dolny
        'P_2_1_1': 'P_1_1_1', # Górny
        # 'TS_6_1_1 (degC)'
        # SWC, G i TS zdefiniowane poprawnie w loggerze - bez mapowania nazw
    },
    'SARBIA_MAP': {
        'PPFDR_1_1_1': 'PPFD_OUT_1_1_1', # APOGE
        'PPFD_1_1_1': 'PPFD_IN_1_1_1', # APOGE
        'SWIN_1_1_1': 'SW_IN_1_1_1', # CNR4
        'RG_1_1_1': 'SW_IN_1_1_2', # zika
        'SWOUT_1_1_1': 'SW_OUT_1_1_1', # CNR4
        'LWin_1_1_1': 'LW_IN_1_1_1', # CNR4
        'LWIN_1_1_1': 'LW_IN_1_1_1', # CNR4
        'LWOUT_1_1_1': 'LW_OUT_1_1_1', # CNR4
        'Rn_Avg': 'RN_1_1_1', # CNR4
        'PPFDBC_7_24_1_2_1': 'PPFD_BC_IN_1_1_1', #
        'PPFDBC_7_24_1_2_2': 'PPFD_BC_IN_1_1_2', #
        # 'TA_1_1_1': 'TA_1_1_1', #
        'TA_2_1_1_2_1': 'TA_1_2_1', # dolny
        'RH_19_3_1_2_1': 'RH_1_2_1',
        'P_8_18_2_2_1': 'P_2_2_1', # Przy solarach
        'P_8_18_2_2_2': 'P_2_2_1', # Przy solarach
        'SWC_11_36_1_1_1': 'SWC_1_1_1',
        'SWC_11_36_2_1_1': 'SWC_2_1_1',
        'SWC_11_36_3_1_1': 'SWC_3_1_1',
        'SWC_11_36_4_1_1': 'SWC_4_1_1',
        'SWC_11_36_5_1_1': 'SWC_5_1_1',
        'TS_2_38_1_1_1': 'TS_1_1_1',
        'TS_2_38_1_2_1': 'TS_1_2_1',
        'TS_2_38_2_1_1': 'TS_2_1_1',
        'TS_2_38_2_2_1': 'TS_2_2_1',
        'TS_2_38_3_1_1': 'TS_3_1_1',
        'TS_2_38_3_2_1': 'TS_3_2_1',
        'TS_2_38_4_1_1': 'TS_4_1_1',
        'TS_2_38_4_2_1': 'TS_4_2_1',
        'TS_2_38_5_1_1': 'TS_5_1_1',
        'TS_2_38_5_2_1': 'TS_5_2_1',

    },
    'RZECIN_MAP': {
        'T_1_1_Avg': 'TA_1_1_1'
    },
    # Tlen1 old site added by Klaudia- 19.07.2025
    'TLEN1_MAP': {
	    #Radiation – BF3H /BF5sensors (PPFD) operated on and off from 25-Apr-2013 17:30:00 until 12-Aug-2019 and
        #SKP (PPFD_2_1_1_Avg) operated from 13-Jan-2014 10:30:00 until the end of the meteo instruments operation- demounted on 14-Nov-2022.
        'PPFD_1_1_1_Avg':'PPFD_IN_1_1_1',  # BF3/BF5 incoming direct PPDF
        'PPFDd_1_1_1_Avg': 'PPFD_DIF_1_1_1', # BF3/BF5 incoming diffused PPDF
        'PPFD_2_1_1_Avg': 'PPFD_IN_1_1_2',  #SKP215 incoming direct PPDF
        'PPFDr_1_1_1_Avg': 'PPFD_OUT_1_1_1',  #SKP215 reflected PPDF
        #NR01 4-component net radiometer measurements
        'SWin_1_1_1_Avg': 'SW_IN_1_1_1', # NR01
        'SWout_1_1_1_Avg': 'SW_OUT_1_1_1', # NR01
        'LWin_1_1_1_Avg': 'LW_IN_1_1_1', # NR01
        'LWout_1_1_1_Avg': 'LW_OUT_1_1_1', # NR01
        # Soil heat plates Hukseflux HFP 01
        'G_1_1_1_Avg': 'G_1_1_1',
        'G_2_1_1_Avg': 'G_2_1_1',
        'G_3_1_1_Avg': 'G_3_1_1',
        'G_4_1_1_Avg': 'G_4_1_1',
        # Soil moisture, 1 profile (10, 30, 50cm)- CS616
        'VW_1': 'SWC_1_1_1', # CS616 10cm
        'VW_2': 'SWC_1_2_1',  # CS616 30cm
        'VW_3': 'SWC_1_3_1', # CS616 50cm
        # Soil Temperature- T107 soil termometer
        # profile 1
        'T107_1_1_1_Avg': 'TS_1_1_1', # soil profile 1 – 2cm depth
        'T107_1_2_1_Avg': 'TS_1_2_1',  # soil profile 1 – 5cm depth
        'T107_1_3_1_Avg': 'TS_1_3_1',   # soil profile 1 – 10cm depth
        'T107_1_4_1_Avg': 'TS_1_4_1',  # soil profile 1 – 30cm depth
        'T107_1_5_1_Avg': 'TS_1_5_1',  # soil profile 1 – 50cm depth
        # profile 2
        'T107_2_1_1_Avg': 'TS_2_1_1',  # soil profile 2 – 2cm depth
        'T107_2_2_1_Avg': 'TS_2_2_1',  # soil profile 2 – 5cm depth
        'T107_2_3_1_Avg': 'TS_2_3_1',  # soil profile 2 – 10cm depth
        'T107_2_4_1_Avg': 'TS_2_4_1',  # soil profile 2 – 30cm depth
        'T107_2_5_1_Avg': 'TS_2_5_1',  # soil profile 2 – 500cm depth
        # Precipitation measurements- forest floor – Tipping Rain gauges  A-ster TPG
        'P_rain_1_1_1_Tot': 'P_1_1_1', #Rain gauge 1
        'P_rain_1_2_1_Tot':'P_1_1_2', #Rain gauge 2
        # Air temperature and humidity- HMP 155, Vaisala and NR01
        'Ta_1_1_1_Avg': 'TA_1_1_1',  # HMP 155 air temp. at 2m above ground
        'RH_1_1_1_Avg': 'RH_1_1_1',  # HMP 155 air temp. at 2m above ground
        'Ta_1_2_1_Avg': 'TA_1_2_1',  # HMP 155 air temp. at 30cm above ground
        'RH_1_2_1_Avg': 'RH_1_2_1',  # HMP 155 air temp. at 30cm above ground
        },
        ## Tlen2 "old" tower added by Klaudia- 19.07.2025
    'TLEN2_MAP': {
        #PPFD Radiation – SKL 2620 operated from 2014,7,8,16,30,0 until the end of the meteo instruments operation- demounted on 05-Sep-2018 05:30:00.
        'PPFD_1_1_1_Avg':'PPFD_IN_1_1_1',  # SKL 2620 incoming PPDF
        'PPFDr_1_1_1_Avg': 'PPFD_OUT_1_1_1',  #SKL 2620 reflected PPDF
        #NR01 4-component net radiometer measurements
        'SWin_1_1_1_Avg': 'SW_IN_1_1_1', # NR01
        'SWout_1_1_1_Avg': 'SW_OUT_1_1_1', # NR01
        'LWin_1_1_1_Avg': 'LW_IN_1_1_1', # NR01
        'LWout_1_1_1_Avg': 'LW_OUT_1_1_1', # NR01
        # Soil heat plates Hukseflux HFP 01
        'G_1_1_1_Avg': 'G_1_1_1',
        'G_2_1_1_Avg': 'G_2_1_1',
        'G_3_1_1_Avg': 'G_3_1_1',
        'G_4_1_1_Avg': 'G_4_1_1',
        # Soil moisture, 1 profile (10, 30, 50cm)- CS616
        'VW_1': 'SWC_1_1_1', # CS616 10cm
        'VW_2': 'SWC_1_2_1',  # CS616 30cm
        'VW_3': 'SWC_1_3_1', # CS616 50cm
        # Soil Temperature- T107 soil termometer
        # profile 1
        'Ts_1_1_1_Avg': 'TS_1_1_1', # soil profile 1 – 2cm depth
        'Ts_1_2_1_Avg': 'TS_1_2_1',  # soil profile 1 – 5cm depth
        'Ts_1_3_1_Avg': 'TS_1_3_1',   # soil profile 1 – 10cm depth
        'Ts_2_4_1_Avg': 'TS_1_4_1',  # soil profile 1 – 30cm depth
        'Ts_2_5_1_Avg': 'TS_1_5_1',  # soil profile 1 – 50cm depth
        # profile 2
        'Ts_2_1_1_Avg': 'TS_2_1_1',  # soil profile 2 – 2cm depth
        'Ts_2_2_1_Avg': 'TS_2_2_1',  # soil profile 2 – 5cm depth
        'Ts_2_3_1_Avg': 'TS_2_3_1',  # soil profile 2 – 10cm depth
        'Ts_2_4_1_Avg': 'TS_2_4_1',  # soil profile 2 – 30cm depth
        'Ts_2_5_1_Avg': 'TS_2_5_1',  # soil profile 2 – 500cm depth
        # Precipitation measurements- forest floor – Tipping Rain gauges  A-ster TPG
        'P_rain_1_1_1_Tot': 'P_1_1_1', #Rain gauge 1
        'P_rain_1_2_1_Tot':'P_1_1_2', #Rain gauge 2
        # Air temperature and humidity- HMP 155, Vaisala and NR01
        'Ta_1_1_1_Avg': 'TA_1_1_1',  # HMP 155 air temp. at 2m above ground
        'RH_1_1_1_Avg': 'RH_1_1_1',  # HMP 155 air temp. at 2m above ground
        'Ta_1_2_1_Avg': 'TA_1_2_1',  # HMP 155 air temp. at 30cm above ground
        'RH_1_2_1_Avg': 'RH_1_2_1',  # HMP 155 air temp. at 30cm above ground
        }
}
# 9.1 Mapowanie nazw kolumn w grupach
STATION_MAPPING_FOR_COLUMNS = {
    # ----- TUCZNO -----
    'TU_PROF_1s': 'TUCZNO_MAP',
    'TU_SOIL_5s': 'TUCZNO_MAP',
    'TU_MET_5s': 'TUCZNO_MAP',
    'TU_SOIL2_30s': 'TUCZNO_MAP',
    'TU_EC_30m': 'TUCZNO_MAP',
    'TU_MET_30m': 'TUCZNO_MAP',
    'TU_WXT_30m': 'TUCZNO_MAP',
    'TU_Bole_30m': 'TUCZNO_MAP',
    'TU_PROF_30m': 'TUCZNO_MAP',
    'TU_SOIL_30m': 'TUCZNO_MAP',
    'TU_GARDEN_30m': 'TUCZNO_MAP',
    'TU_GARDEN_10m': 'TUCZNO_MAP',
    'TU_STUDNIA_1_10m': 'TUCZNO_MAP',
    'TU_PROF_2m': 'TUCZNO_MAP',
    'TU_RAD_1': 'TUCZNO_MAP',
    # ----- MEZYK -----
    'ME_TOP_MET_30min': 'MEZYK_MAP',
    'ME_TOP_MET_1min': 'MEZYK_MAP',
    'ME_DOWN_MET_30min': 'MEZYK_MAP',
    'ME_DOWN_MET_1min': 'MEZYK_MAP',
    'ME_Rain_down': 'MEZYK_MAP',
    'ME_Rain_top': 'MEZYK_MAP',
    # ----- SARBIA -----
    'SA_MET_30min': 'SARBIA_MAP',
    'SA_MET_1min': 'SARBIA_MAP',
    # ----- TLEN1 -----
    'TL1_MET_30' : 'TLEN1_MAP',
    'TL1_SOIL_30' : 'TLEN1_MAP',
    'TL1_RAD_30' : 'TLEN1_MAP',
    'TL1_RAD_1' : 'TLEN1_MAP',
    # ----- TLEN2 -----
    'TL2_MET_1m' : 'TLEN2_MAP',
    'TL2_MET_30m': 'TLEN2_MAP',
}
# 11. Automatyczne przypisanie flag jakości na podstawie zakresów (działa po kalibracji i zmianie nazw)
VALUE_RANGE_FLAGS = {
    # Ta reguła zostanie zastosowana do wszystkich kolumn zaczynających się na 'TA'
    # np. TA_1_1_1, TA_1_1_2, TA_2_1_1 itd.
    'TA_': {'min': -40, 'max': 45},
    'air_temperature': {'min': 233, 'max': 313},
    'RH_': {'min': 10, 'max': 105},
    'TS_': {'min': -30, 'max': 60},
    'T107_': {'min': -30, 'max': 60},
    'SW_IN_': {'min': -2, 'max': 1000},
    'SW_OUT_': {'min': -2, 'max': 250},
    'LW_IN_': {'min': 150, 'max': 600},
    'LW_OUT_': {'min': 150, 'max': 600},
    'RN_': {'min': -200, 'max': 900},
    'PPFD_IN_': {'min': -2, 'max': 3000},
    'PPFD_BC_IN_': {'min': -2, 'max': 2000},
    'PPFDBC_IN_': {'min': -2, 'max': 2000},
    'PPFDd': {'min': -2, 'max': 2000},
    'PPFDd_': {'min': -2, 'max': 2000},
    'PPFD_DIF': {'min': -2, 'max': 2000},
    'PPFD_OUT_': {'min': -2, 'max': 500},
    'PPFDr_': {'min': -2, 'max': 500},
    'G_': {'min': -50, 'max': 200},
    'SHF_': {'min': -50, 'max': 200},
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

# 12. SŁOWNIK RĘCZNEGO NADPISYWANIA WARTOŚCI
MANUAL_VALUE_OVERRIDES = {
    'TUCZNO_OVR': {
        # Kluczem jest nazwa kolumny (po mapowaniu)
        'P_1_1_1': [
            # Reguła 1: Zerowanie opadu w danym okresie
            {'start': '2040-07-15 12:00:00', 'end': '2040-07-16 18:00:00', 'new_value': 0.0, 'reason': 'Wyczyszczenie błędnego piku opadu.'},
        ],
        'TA_1_1_1': [
            # Reguła 2: Wstawienie stałej wartości temperatury
            {'start': '2041-01-20 08:00:00', 'end': '2041-01-20 10:00:00', 'new_value': -5.5, 'reason': 'Korekta wartości po awarii czujnika.'},
        ]
    },
    # Można dodać kolejne zestawy reguł dla innych stacji
    'MEZYK_OVR': {
        'G_1_1_1': [
             {'start': '2049-08-01 00:00:00', 'end': '2049-08-05 23:59:59', 'new_value': -9999.0, 'reason': 'Oznaczenie danych jako brakujące.'},
        ]
    }
}
# 12.1 Mapowanie po grupach dla nadpisywania ręcznego
STATION_MAPPING_FOR_OVERRIDES = {
    # Tuczno
    'TU_MET_30m': 'TUCZNO_OVR',
    # Mezyk
    'ME_DOWN_MET_30min': 'MEZYK_OVR'
}
# --- KONIEC SEKCJI KONFIGURACJI ---


# --- MODUŁY POMOCNICZE I LOGOWANIA ---
def setup_logging(level: str = 'INFO'):
    """Konfiguruje system logowania."""
    # check if handlers are already attached
    # this prevents duplicate logs in interactive environments
    if logging.getLogger().handlers:
        return

    log_level = getattr(logging, level.upper(), logging.INFO)
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    file_handler = RotatingFileHandler(LOG_FILE_PATH, maxBytes=5*1024*1024, backupCount=2, encoding='utf-8')
    file_handler.setFormatter(log_formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)

    root_logger = logging.getLogger()
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

def setup_chronology_logger():
    """Konfiguruje dedykowany logger do zapisywania informacji o korektach chronologii."""
    global chronology_logger
    chronology_logger = logging.getLogger('chronology')
    chronology_logger.propagate = False
    chronology_logger.setLevel(logging.INFO)
    
    if not chronology_logger.handlers:
        handler = logging.FileHandler(CHRONOLOGY_LOG_FILENAME, mode='a', encoding='utf-8')
        # Używamy prostego formatowania, aby plik był w formacie CSV
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        chronology_logger.addHandler(handler)
        
        # Jeśli plik jest nowy, dodaj nagłówek (nowy, rozszerzony format)
        if handler.stream.tell() == 0:
            chronology_logger.info("LogDate;SourceFilePth;BlockStartIndex;BlockEndIndex;OriginalStartTS;OriginalEndTS;CorrectedStartTS;CorrectedEndTS")
            
def parse_header_line(line):
    # function to parse a header line from a TOA5/TOB1 file
    return [field.strip() for field in line.strip('"').split('","')]

# --- FUNKCJE OBSŁUGI DANYCH WYJŚCIOWYCH ---

def initialize_database(db_path: Path):
    """Tworzy strukturę relacyjnej bazy danych z tabelami metadanych."""
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Enable foreign key support
        cursor.execute("PRAGMA foreign_keys = ON;")

        # Tabela stacji (unikalne lokalizacje)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stations (
                station_id TEXT PRIMARY KEY,
                name TEXT,
                latitude REAL,
                longitude REAL
            )
        """)

        # Tabela grup pomiarowych
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                group_id TEXT PRIMARY KEY,
                station_id TEXT NOT NULL,
                interval TEXT,
                FOREIGN KEY (station_id) REFERENCES stations (station_id)
            )
        """)
        
        conn.commit()
        conn.close()
        logging.info(f"Relacyjna baza danych zainicjalizowana w: {db_path}")
    except Exception as e:
        logging.error(f"Nie można zainicjalizować bazy danych: {e}")
        raise

def add_missing_columns(df: pd.DataFrame, conn, table_name: str):
    """
    Dynamicznie dodaje brakujące kolumny do określonej tabeli, używając SQLAlchemy.
    Wersja 7.24: Usunięto zagnieżdżoną transakcję, aby uniknąć błędu InvalidRequestError.
    """
    try:
        inspector = sqlalchemy.inspect(conn)
        existing_cols_info = inspector.get_columns(table_name)
        existing_cols = {col['name'] for col in existing_cols_info}
        
        missing_cols = set(df.columns) - existing_cols

        if not missing_cols:
            return

        # Usunięto blok 'with conn.begin()'. Operacje wykonają się w transakcji nadrzędnej.
        for col in missing_cols:
            if col.endswith('_flag'):
                sql_type = "INTEGER"
            elif pd.api.types.is_integer_dtype(df[col]):
                sql_type = "INTEGER"
            elif pd.api.types.is_numeric_dtype(df[col]):
                sql_type = "REAL"
            else:
                sql_type = "TEXT"
            
            alter_sql = sqlalchemy.text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {sql_type}')
            conn.execute(alter_sql)
            logging.debug(f"Dodano nową kolumnę '{col}' do tabeli '{table_name}'")

    except Exception as e:
        logging.error(f"Nie udało się dodać kolumn do tabeli '{table_name}': {e}")
        raise

def save_dataframe_to_sqlite(df: pd.DataFrame, config: dict, lock: multiprocessing.Lock):
    """
    Zapisuje dane do bazy SQLite, zapewniając poprawny format TIMESTAMP dla zewnętrznych narzędzi.
    Wersja 7.43 FINAL: Poprawiona kolejność operacji dla prawidłowego formatowania daty.
    """
    if df.empty:
        return

    group_id = config['file_id']
    db_path = Path(config['db_path'])
    overwrite_mode = config.get('overwrite', False)
    coords = STATION_COORDINATES.get(group_id, {'lat': None, 'lon': None})
    station_id = group_id.split('_')[0]
    table_name = f"data_{group_id}"
    temp_table_name = f"temp_{group_id}"

    with lock:
        try:
            engine = sqlalchemy.create_engine(f"sqlite:///{db_path}")
            with engine.connect() as conn:
                
                # Krok 1: Wstępne oczyszczenie i przygotowanie nowych danych
                df_clean = _enforce_numeric_types(df.copy())

                # Krok 2: Utworzenie tabeli docelowej, jeśli nie istnieje
                if not conn.dialect.has_table(conn, table_name):
                    # Zdefiniuj schemat na podstawie CZYSTYCH danych wejściowych
                    cols_with_types = [f'"{col}" {("INTEGER" if col.endswith("_flag") or pd.api.types.is_integer_dtype(dtype) else "REAL" if pd.api.types.is_numeric_dtype(dtype) else "TEXT")}' 
                                       for col, dtype in df_clean.dtypes.items() if col != 'TIMESTAMP']
                    create_sql = f'CREATE TABLE "{table_name}" (TIMESTAMP TEXT PRIMARY KEY, {", ".join(cols_with_types)})'
                    conn.execute(sqlalchemy.text(create_sql))
                    conn.commit()
                    logging.info(f"Utworzono nową tabelę danych: {table_name}")
                
                # Krok 3: Odczytaj istniejące dane z bazy
                existing_df = pd.DataFrame()
                timestamps_to_check = df_clean['TIMESTAMP'].dt.strftime('%Y-%m-%dT%H:%M:%S').tolist()
                if timestamps_to_check:
                    chunk_size = 900
                    all_existing_dfs = []
                    for i in range(0, len(timestamps_to_check), chunk_size):
                        chunk_ts = timestamps_to_check[i:i + chunk_size]
                        placeholders = ", ".join([f"'{ts}'" for ts in chunk_ts])
                        query = f'SELECT * FROM "{table_name}" WHERE TIMESTAMP IN ({placeholders})'
                        try:
                           chunk = pd.read_sql_query(query, conn)
                           chunk['TIMESTAMP'] = pd.to_datetime(chunk['TIMESTAMP'])
                           all_existing_dfs.append(chunk)
                        except Exception: # Błąd jeśli tabela jest pusta lub odpytanie nie zadziała
                            pass 
                    if all_existing_dfs:
                        existing_df = pd.concat(all_existing_dfs, ignore_index=True)

                # Krok 4: Połącz dane zgodnie z trybem i ponownie oczyść typy
                if not existing_df.empty:
                    df_indexed = df_clean.set_index('TIMESTAMP')
                    existing_df_indexed = existing_df.set_index('TIMESTAMP')
                    if overwrite_mode:
                        merged_df = df_indexed.combine_first(existing_df_indexed)
                    else:
                        merged_df = existing_df_indexed.combine_first(df_indexed)
                    df_to_save = merged_df.reset_index()
                else:
                    df_to_save = df_clean.copy()
                
                df_to_save = _enforce_numeric_types(df_to_save)
                add_missing_columns(df_to_save, conn, table_name)

                # Krok 5: Zapisz dane do tabeli tymczasowej
                dtype_map = {col: sqlalchemy.types.Integer for col in df_to_save.columns if col.endswith('_flag')}
                df_to_save.to_sql(temp_table_name, conn, if_exists='replace', index=False, dtype=dtype_map, chunksize=2000)

                # --- POCZĄTEK KLUCZOWEJ ZMIANY ---
                # Krok 6: Sformatuj TIMESTAMP na TEXT Zgodny z ISO 8601 w tabeli tymczasowej
                format_sql = f'UPDATE "{temp_table_name}" SET TIMESTAMP = strftime("%Y-%m-%dT%H:%M:%S", TIMESTAMP);'
                conn.execute(sqlalchemy.text(format_sql))
                # --- KONIEC KLUCZOWEJ ZMIANY ---

                # Krok 7: Wykonaj operację INSERT OR REPLACE
                columns_str = ", ".join([f'"{c}"' for c in df_to_save.columns])
                upsert_sql = f'INSERT OR REPLACE INTO "{table_name}" ({columns_str}) SELECT {columns_str} FROM "{temp_table_name}"'
                conn.execute(sqlalchemy.text(upsert_sql))

                # Krok 8: Usuń tabelę tymczasową
                conn.execute(sqlalchemy.text(f'DROP TABLE "{temp_table_name}"'))
                conn.commit()

                # Krok 9: Zaktualizuj tabele metadanych
                with conn.begin():
                    conn.execute(sqlalchemy.text("INSERT OR IGNORE INTO stations (station_id, name, latitude, longitude) VALUES (:sid, :name, :lat, :lon)"), 
                                   {"sid": station_id, "name": station_id, "lat": coords['lat'], "lon": coords['lon']})
                    conn.execute(sqlalchemy.text("INSERT OR IGNORE INTO groups (group_id, station_id, interval) VALUES (:gid, :sid, :intv)"), 
                                   {"gid": group_id, "sid": station_id, "intv": config.get('interval', 'N/A')})
                
                logging.info(f"Zapisano/zaktualizowano {len(df_to_save)} wierszy w tabeli '{table_name}'.")
        except Exception as e:
            logging.error(f"Krytyczny błąd zapisu do bazy danych dla grupy '{group_id}': {e}", exc_info=True)
            
def save_dataframe_to_csv(final_df: pd.DataFrame, year: int, config: dict, lock: multiprocessing.Lock):
    """
    Zapisuje ramkę danych do pliku CSV z logiką 'uzupełnij' lub 'nadpisz'.
    Wersja 7.10: Usunięto modyfikacje inplace, aby uniknąć efektów ubocznych.
    """
    if final_df.empty:
        return

    group_id = config['file_id']
    output_dir = Path(config['output_dir'])
    output_filepath = output_dir / str(year) / f"{group_id}.csv"
    overwrite_mode = config.get('overwrite', False)

    with lock:
        try:
            output_filepath.parent.mkdir(parents=True, exist_ok=True)
            
            existing_df = pd.DataFrame()
            if output_filepath.exists():
                try:
                    temp_df = pd.read_csv(output_filepath, parse_dates=['TIMESTAMP'], low_memory=False)
                    if 'TIMESTAMP' in temp_df.columns:
                        existing_df = temp_df
                except Exception:
                    logging.warning(f"Błąd odczytu {output_filepath.name}. Plik zostanie nadpisany.")
            
            if not existing_df.empty:
                # Użyj kopii, aby nie modyfikować oryginalnej ramki danych
                df_indexed = final_df.set_index('TIMESTAMP')
                existing_df_indexed = existing_df.set_index('TIMESTAMP')

                if overwrite_mode:
                    merged_df = df_indexed.combine_first(existing_df_indexed)
                else:
                    merged_df = existing_df_indexed.combine_first(df_indexed)
                
                df_to_save = merged_df.reset_index()
            else:
                df_to_save = final_df.copy()

            all_columns = sorted([col for col in df_to_save.columns if col != 'TIMESTAMP'])
            all_columns.insert(0, 'TIMESTAMP')
            
            df_to_save = df_to_save[all_columns]
            df_to_save.sort_values(by='TIMESTAMP', inplace=True)

            logging.info(f"Zapisywanie {len(df_to_save)} wierszy do pliku CSV: {output_filepath}")
            df_to_save.to_csv(output_filepath, index=False, date_format='%Y-%m-%d %H:%M:%S')

        except Exception as e:
            logging.error(f"Błąd podczas zapisu do pliku CSV {output_filepath}: {e}", exc_info=True)


# --- FUNKCJE PRZETWARZANIA DANYCH (WSPÓLNE) ---
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
    (Wersja 2.1) Zawiera poprawkę na błąd typu danych przekazywanych
    do dekodera FP2, co zapewnia stabilność przy przetwarzaniu binarnym.
    """
    col_names, struct_pattern, num_header_lines, fp2_cols = metadata
    all_chunks = []
    try:
        record_size = struct.calcsize(struct_pattern)
        if record_size == 0: return pd.DataFrame()

        with open(file_path, 'rb') as f:
            for _ in range(num_header_lines): f.readline()
            
            chunk_size_rows = 100_000
            while True:
                records_in_chunk = []
                for _ in range(chunk_size_rows):
                    chunk_bytes = f.read(record_size)
                    if not chunk_bytes or len(chunk_bytes) < record_size:
                        break
                    records_in_chunk.append(struct.unpack(struct_pattern, chunk_bytes))

                if not records_in_chunk: break

                chunk_df = pd.DataFrame(records_in_chunk, columns=col_names)
                
                if not chunk_df.empty and fp2_cols:
                    for fp2_col_name in fp2_cols:
                        if fp2_col_name in chunk_df.columns:
                            # === POCZĄTEK POPRAWKI ===
                            # Krok 1: Konwertuj na typ numeryczny, zamieniając błędy na NaN
                            numeric_series = pd.to_numeric(chunk_df[fp2_col_name], errors='coerce')
                            # Krok 2: Wypełnij ewentualne NaN zerem i rzutuj na typ integer
                            integer_series = numeric_series.fillna(0).astype(int)
                            # Krok 3: Dopiero teraz zastosuj funkcję dekodującą
                            chunk_df[fp2_col_name] = integer_series.apply(decode_csi_fs2_float)
                            # === KONIEC POPRAWKI ===
                
                if 'SECONDS' in chunk_df.columns and 'NANOSECONDS' in chunk_df.columns:
                    secs = pd.to_numeric(chunk_df['SECONDS'], errors='coerce')
                    nanos = pd.to_numeric(chunk_df['NANOSECONDS'], errors='coerce')
                    chunk_df['TIMESTAMP'] = CAMPBELL_EPOCH + pd.to_timedelta(secs, unit='s') + pd.to_timedelta(nanos, unit='ns')
                
                all_chunks.append(chunk_df)

                if len(records_in_chunk) < chunk_size_rows: break

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
        final_df = clean_column_names(final_df)
        return final_df
    except Exception as e:
        logging.error(f"Błąd odczytu danych TOA5 z {file_path.name}: {e}")
        return pd.DataFrame()

def read_simple_csv_data(file_path: Path) -> pd.DataFrame:
    """
    Wczytuje dane CSV, pomijając zdefiniowane kolumny i obsługując niestandardowe wartości NaN.
    """
    all_chunks = []
    try:
        # Zdefiniuj listę wartości, które mają być traktowane jako NaN (brak danych)
        custom_nan_values = ["OverRange", "UnderRange", "NAN", "INF", "-INF", ""]

        chunk_iterator = pd.read_csv(
            file_path,
            header=0,
            low_memory=False, 
            encoding='latin-1',
            on_bad_lines='warn',
            chunksize=100_000,
            na_values=custom_nan_values, # <-- KLUCZOWA ZMIANA
            usecols=lambda col_name: col_name not in COLUMNS_TO_EXCLUDE_FROM_CSV
        )
        
        for chunk_df in chunk_iterator:
            if 'Timestamp' not in chunk_df.columns and 'TIMESTAMP' not in chunk_df.columns:
                continue 
            
            if 'Timestamp' in chunk_df.columns:
                chunk_df.rename(columns={'Timestamp': 'TIMESTAMP'}, inplace=True)

            chunk_df['TIMESTAMP'] = pd.to_datetime(chunk_df['TIMESTAMP'], errors='coerce')
            all_chunks.append(chunk_df)

        if not all_chunks:
            return pd.DataFrame()
            
        final_df = pd.concat(all_chunks, ignore_index=True)
        final_df.dropna(subset=['TIMESTAMP'], inplace=True)
        
        final_df['source_filename'] = file_path.name
        final_df['original_row_index'] = final_df.index
        final_df['source_filepath'] = str(file_path.resolve())
        
        final_df = clean_column_names(final_df)
        return final_df
    except Exception as e:
        logging.error(f"Krytyczny błąd odczytu SimpleCSV z {file_path.name}: {e}")
        return pd.DataFrame()

# (usunięto) _csv_has_backward_time – logika przeniesiona do correct_and_report_chronology
        
def matlab_to_datetime(matlab_datenum: float) -> datetime:
    """Konwertuje numer seryjny daty z MATLABa na obiekt datetime Pythona."""
    return datetime.fromordinal(int(matlab_datenum)) + timedelta(days=matlab_datenum % 1) - timedelta(days=366)

def load_matlab_data(year: int, config: dict) -> pd.DataFrame:
    """
    (Wersja 3.2) Zawiera poprawki na warunek interwału (<= 5s) oraz
    inteligentne odczytywanie nazwy zmiennej z plików .mat.
    """
    group_id = config['file_id']
    main_input_path_str = config.get('main_input_path')
    
    if not main_input_path_str:
        return pd.DataFrame()

    try:
        parts = group_id.split('_', 1)
        if len(parts) < 2: return pd.DataFrame()
        station_code, matlab_folder_name = parts
        base_project_path = Path(main_input_path_str).parent.parent
        matlab_base_path = base_project_path / f"met-data_{station_code.upper()}"
        data_path = matlab_base_path / str(year) / matlab_folder_name / "zero_level"
    except Exception:
        return pd.DataFrame()

    if not data_path.exists():
        return pd.DataFrame()

    interval = config.get('interval', '')
    # === POCZĄTEK POPRAWKI #1: Poprawny warunek interwału ===
    is_monthly = ('sec' in interval or 's' in interval) and int(re.sub(r'\D', '', interval)) <= 5
    # === KONIEC POPRAWKI #1 ===
    
    monthly_dfs = []
    months_to_check = range(1, 13) if is_monthly else [None]

    for month in months_to_check:
        try:
            time_file_name = f"tv_{month:02d}.mat" if month else "tv.mat"
            time_file_path = data_path / time_file_name
            if not time_file_path.exists(): continue

            # === POCZĄTEK POPRAWKI #2: Inteligentne odczytywanie klucza ===
            mat_contents = loadmat(time_file_path)
            time_key = next((k for k in mat_contents if not k.startswith('__')), None)
            if not time_key:
                logging.warning(f"Nie znaleziono zmiennej z danymi w pliku czasu: {time_file_path.name}")
                continue
            time_vector_raw = mat_contents[time_key].flatten()
            # === KONIEC POPRAWKI #2 ===
            
            timestamps = [matlab_to_datetime(t) for t in time_vector_raw]
            
            var_files = [f for f in data_path.glob('*.mat') if not f.name.startswith('tv')]
            matlab_data = {'TIMESTAMP': timestamps}
            
            for var_file in var_files:
                var_name = var_file.stem
                if month and not var_name.endswith(f"_{month:02d}"): continue
                
                clean_var_name = var_name.rsplit('_', 1)[0] if month else var_name
                mat_contents = loadmat(var_file)
                data_key = next((k for k in mat_contents if not k.startswith('__')), None)
                if data_key:
                    data = mat_contents[data_key].flatten()
                    matlab_data[clean_var_name] = data
            
            monthly_dfs.append(pd.DataFrame(matlab_data))

        except KeyError as e:
             logging.warning(f"Błąd klucza podczas wczytywania danych MATLABa dla {year}/{month or ''}: {e}")
             continue
        except Exception as e:
            logging.warning(f"Inny błąd podczas wczytywania danych MATLABa dla {year}/{month or ''}: {e}")
            continue

    if not monthly_dfs:
        return pd.DataFrame()

    final_df = pd.concat(monthly_dfs, ignore_index=True)
    final_df = clean_column_names(final_df)
    return final_df
    
def apply_timezone_correction(ts_series_naive: pd.Series, file_id: str) -> pd.Series:
    """
    Stosuje korekty stref czasowych i zawsze zwraca serię w formacie "naiwnym".
    Wersja 7.11: Poprawia błąd, który błędnie nadawał strefę GMT, gdy brakowało reguły.
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
        # --- POCZĄTEK POPRAWKI ---
        # Jeśli nie ma reguły, po prostu upewnij się, że dane są w formacie datetime i zwróć je jako naiwne.
        return pd.to_datetime(ts_series_naive)
        # --- KONIEC POPRAWKI ---

    source_tz = final_config['source_tz']
    post_correction_tz = final_config['post_correction_tz']
    target_tz = final_config['target_tz']
    correction_end_date = pd.to_datetime(final_config['correction_end_date'])

    pre_mask = ts_series_naive <= correction_end_date
    post_mask = ~pre_mask

    pre_series = ts_series_naive[pre_mask].dt.tz_localize(source_tz, ambiguous='NaT', nonexistent='NaT')
    post_series = ts_series_naive[post_mask].dt.tz_localize(post_correction_tz, ambiguous='NaT', nonexistent='NaT')

    series_to_concat = [s for s in [pre_series, post_series] if not s.empty]
    
    if not series_to_concat:
        return pd.Series(dtype='datetime64[ns]')

    combined_series = pd.to_datetime(pd.concat(series_to_concat).sort_index(), errors='coerce', utc=True)

    if combined_series.empty:
        return combined_series

    # Zawsze zwracaj dane bez strefy czasowej (naiwne)
    return combined_series.dt.tz_convert(target_tz).dt.tz_localize(None)

def apply_manual_time_shifts(df: pd.DataFrame, file_id: str) -> pd.DataFrame:
    """(Wersja 2.0) Poprawiona, aby działać na naiwnych znacznikach czasu."""
    config_entry = MANUAL_TIME_SHIFTS.get(file_id)
    rules = None
    if isinstance(config_entry, str):
        rules = MANUAL_TIME_SHIFTS.get(config_entry)
    elif isinstance(config_entry, list):
        rules = config_entry

    if not rules or df.empty:
        return df
    
    df_out = df.copy()
    for rule in rules:
        try:
            # Tworzymy naiwne daty do porównania
            start_ts = pd.to_datetime(rule['start'])
            end_ts = pd.to_datetime(rule['end'])
            offset = pd.Timedelta(hours=rule['offset_hours'])
            mask = (df_out['TIMESTAMP'] >= start_ts) & (df_out['TIMESTAMP'] <= end_ts)
            if mask.any():
                df_out.loc[mask, 'TIMESTAMP'] += offset
        except Exception as e:
            logging.warning(f"Błąd reguły manualnej dla '{file_id}': {e}.")
    return df_out

def apply_calibration(df: pd.DataFrame, file_id: str) -> pd.DataFrame:
    """(Wersja 2.0) Poprawiona, aby działać na naiwnych znacznikach czasu."""
    station_name = STATION_MAPPING_FOR_CALIBRATION.get(file_id)
    if not station_name or station_name not in CALIBRATION_RULES_BY_STATION:
        return df
    
    column_rules = CALIBRATION_RULES_BY_STATION[station_name]
    df_calibrated = df.copy()
    for col_name, rules_list in column_rules.items():
        if col_name not in df_calibrated.columns:
            continue
        df_calibrated[col_name] = pd.to_numeric(df_calibrated[col_name], errors='coerce')
        for rule in rules_list:
            try:
                # Tworzymy naiwne daty do porównania
                start_ts = pd.to_datetime(rule['start'])
                end_ts = pd.to_datetime(rule['end'])
                multiplier = float(rule.get('multiplier', 1.0))
                addend = float(rule.get('addend', 0.0))
                mask = (df_calibrated['TIMESTAMP'] >= start_ts) & (df_calibrated['TIMESTAMP'] <= end_ts)
                if mask.any():
                    df_calibrated.loc[mask, col_name] = (df_calibrated.loc[mask, col_name] * multiplier) + addend
            except Exception as e:
                logging.warning(f"Błąd reguły kalibracji dla '{col_name}': {e}.")
    return df_calibrated

def apply_value_range_flags(df: pd.DataFrame) -> pd.DataFrame:
    # apply quality flags for values outside of defined ranges
    if df.empty or not VALUE_RANGE_FLAGS: return df
    df_out = df.copy()
    for prefix, range_dict in VALUE_RANGE_FLAGS.items():
        target_cols = [col for col in df_out.columns if str(col).startswith(prefix)]
        for col_name in target_cols:
            numeric_col = pd.to_numeric(df_out[col_name], errors='coerce')
            min_val, max_val = range_dict.get('min', -float('inf')), range_dict.get('max', float('inf'))
            out_of_range_mask = (numeric_col < min_val) | (numeric_col > max_val)
            if out_of_range_mask.any():
                flag_col_name = f"{col_name}_flag"
                if flag_col_name not in df_out.columns: df_out[flag_col_name] = 0
                df_out[flag_col_name] = pd.to_numeric(df_out[flag_col_name], errors='coerce').fillna(0).astype(int)
                df_out.loc[out_of_range_mask, flag_col_name] = 4
    return df_out

def apply_quality_flags(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Dodaje flagi jakości, używając dwuetapowego systemu słowników.
    Wersja 7.9: Poprawia błąd, wskazując na właściwy słownik reguł (QUALITY_FLAGS).
    """
    group_id = config.get('file_id')
    if not group_id or df.empty:
        return df

    # Krok 1: Znajdź nazwę zestawu reguł dla danej grupy
    ruleset_name = STATION_MAPPING_FOR_QC.get(group_id)
    if not ruleset_name:
        return df  # Celowy brak reguł dla tej grupy

    # --- POCZĄTEK POPRAWKI ---
    # Krok 2: Pobierz właściwy słownik z regułami z QUALITY_FLAGS (a nie STATION_MAPPING_FOR_QC)
    station_rules = QUALITY_FLAGS.get(ruleset_name)
    # --- KONIEC POPRAWKI ---
    if not station_rules:
        logging.warning(f"Nie znaleziono definicji reguł '{ruleset_name}' w QUALITY_FLAGS dla grupy '{group_id}'.")
        return df

    df_out = df.copy()
    # ... (reszta funkcji pozostaje bez zmian) ...
    for col_to_flag, rules_list in station_rules.items():
        if col_to_flag == '*':
            target_cols = [c for c in df_out.select_dtypes(include='number').columns if not c.endswith('_flag')]
        elif col_to_flag in df_out.columns:
            target_cols = [col_to_flag]
        else:
            continue

        for rule in rules_list:
            try:
                start_ts = pd.to_datetime(rule['start'])
                end_ts = pd.to_datetime(rule['end'])
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

def align_timestamp(df: pd.DataFrame, force_interval: str) -> pd.DataFrame:
    """Rounds timestamps to a specified frequency."""
    if df.empty or not force_interval: return df
    try:
        df.loc[:, 'TIMESTAMP'] = df['TIMESTAMP'].dt.round(freq=force_interval)
    except Exception as e:
        logging.error(f"Błąd podczas wyrównywania czasu do interwału '{force_interval}': {e}")
    return df

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
    
# def correct_and_report_chronology(df: pd.DataFrame, context_name: str, known_interval: str, timestamp_col: str = 'TIMESTAMP') -> pd.DataFrame:
    # """
    # Koryguje chronologię, używając logiki wejścia/wyjścia z trybu korekty opartej na
    # wykrywaniu cofnięć i dużych skoków czasu w przód, z dodatkowym logowaniem błędów wewnętrznych.
    # Wersja 7.72 FINAL: Ostateczna logika z pełnym logowaniem wewnętrznych anomalii.
    # """
    # if df.empty or len(df) < 2 or timestamp_col not in df.columns:
        # return df

    # try:
        # interval_td = pd.to_timedelta(known_interval)
    # except ValueError:
        # logging.error(f"Nieprawidłowy format interwału '{known_interval}'.")
        # return df

    # df_corrected = df.copy()
    # df_corrected.reset_index(drop=True, inplace=True)

    # original_timestamps = df_corrected[timestamp_col].to_numpy()
    # corrected_timestamps = original_timestamps.copy()
    
    # is_in_block = False
    # time_jump_found = False
    # block_start_index = -1
    
    # if chronology_logger and chronology_logger.handlers[0].stream.tell() == 0:
        # chronology_logger.info("LogDate;EventType;SourceFilePath;OriginalIndex;OriginalTimestamp;CorrectedTimestamp;ShiftHours")

    # for i in range(1, len(corrected_timestamps)):
        # last_original_ts = pd.to_datetime(original_timestamps[i-1])
        # current_original_ts = pd.to_datetime(original_timestamps[i])
        # diff = current_original_ts - last_original_ts

        # last_corrected_ts = pd.to_datetime(corrected_timestamps[i-1])

        # row_info = df_corrected.loc[i]
        # log_time = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        # shift_hours = -diff.total_seconds() / 3600

        # if not is_in_block:
            # # Warunek wejścia w tryb korekty
            # if diff < pd.Timedelta(0):
                # is_in_block = True
                # time_jump_found = True
                # block_start_index = i
                
                # log_entry = (f"{log_time};POCZATEK_BLOKU;"
                             # f"{row_info.get('source_filepath', 'N/A')};"
                             # f"{row_info.get('original_row_index', 'N/A')};"
                             # f"{current_original_ts.strftime('%Y-%m-%dT%H:%M:%S')};;"
                             # f"{shift_hours:.2f}")
                # if chronology_logger: chronology_logger.info(log_entry)
                
                # corrected_timestamps[i] = last_corrected_ts + interval_td
        # else:
            # # Jesteśmy wewnątrz bloku - szukamy końca lub wewnętrznych błędów
            
            # # Warunek wyjścia z trybu korekty
            # if diff > pd.Timedelta(hours=12):
                # is_in_block = False
                
                # end_info = df_corrected.loc[i-1]
                # log_entry = (f"{log_time};KONIEC_BLOKU;"
                             # f"{row_info.get('source_filepath', 'N/A')};"
                             # f"{end_info.get('original_row_index', 'N/A')};"
                             # f"{current_original_ts.strftime('%Y-%m-%dT%H:%M:%S')};;"
                             # f"{shift_hours:.2f}")
                # if chronology_logger: chronology_logger.info(log_entry)
                
                # corrected_timestamps[i] = original_timestamps[i]
            # else:
                # # --- POCZĄTEK NOWEJ LOGIKI ---
                # # Logowanie wewnętrznych anomalii, zgodnie z prośbą
                # if diff < pd.Timedelta(0):
                    # log_entry = (f"{log_time};BLAD_WEWNETRZNY;"
                                 # f"{row_info.get('source_filepath', 'N/A')};"
                                 # f"{row_info.get('original_row_index', 'N/A')};"
                                 # f"{current_original_ts.strftime('%Y-%m-%dT%H:%M:%S')};;"
                                 # f"Wewnętrzne cofnięcie czasu wewnątrz bloku ({shift_hours:.2f}h)")
                    # if chronology_logger: chronology_logger.info(log_entry)
                # elif diff > (interval_td * 1.5) and diff <= pd.Timedelta(hours=12):
                     # log_entry = (f"{log_time};BLAD_WEWNETRZNY;"
                                  # f"{row_info.get('source_filepath', 'N/A')};"
                                  # f"{row_info.get('original_row_index', 'N/A')};"
                                  # f"{current_original_ts.strftime('%Y-%m-%dT%H:%M:%S')};;"
                                  # f"Krótka przerwa lub nieregularność ({shift_hours:.2f}h) wewnątrz bloku")
                     # if chronology_logger: chronology_logger.info(log_entry)
                # # --- KONIEC NOWEJ LOGIKI ---

                # # Niezależnie od wewnętrznych błędów, kontynuuj generowanie czasu
                # corrected_timestamps[i] = last_corrected_ts + interval_td
    
    # if time_jump_found:
        # df_corrected[timestamp_col] = corrected_timestamps
        # logging.info(f"Zakończono pomyślnie korektę chronologii dla '{context_name}'.")

    # return df_corrected

# def correct_and_report_chronology(df: pd.DataFrame, context_name: str, known_interval: str, timestamp_col: str = 'TIMESTAMP') -> pd.DataFrame:
    # """
    # Koryguje chronologię, łącząc inteligentną obsługę przerw z udoskonaloną,
    # dwuwarunkową logiką wyjścia z trybu korekty.
    # Wersja 7.74 FINAL: Zaimplementowano dodatkowy warunek wyjścia z korekty.
    # """
    # if df.empty or len(df) < 2 or timestamp_col not in df.columns:
        # return df

    # try:
        # interval_td = pd.to_timedelta(known_interval)
    # except ValueError:
        # logging.error(f"Nieprawidłowy format interwału '{known_interval}'.")
        # return df

    # df_corrected = df.copy()
    # df_corrected.reset_index(drop=True, inplace=True)

    # original_timestamps = df_corrected[timestamp_col].to_numpy()
    # corrected_timestamps = original_timestamps.copy()
    
    # is_in_block = False
    # time_jump_found = False
    # block_start_index = -1
    
    # # Upewnij się, że nagłówek logu jest poprawny
    # if chronology_logger and chronology_logger.handlers[0].stream.tell() == 0:
        # chronology_logger.info("LogDate;EventType;SourceFilePath;OriginalIndex;OriginalTimestamp;CorrectedTimestamp;ShiftHours;Details")

    # for i in range(1, len(corrected_timestamps)):
        # last_original_ts = pd.to_datetime(original_timestamps[i-1])
        # current_original_ts = pd.to_datetime(original_timestamps[i])
        # diff = current_original_ts - last_original_ts

        # # Zmienna przechowująca ostatni poprawny (już skorygowany) znacznik czasu
        # last_corrected_ts = pd.to_datetime(corrected_timestamps[i-1])

        # row_info = df_corrected.loc[i]
        # log_time = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        # shift_hours = -diff.total_seconds() / 3600

        # if not is_in_block:
            # # Warunek wejścia w tryb korekty
            # if diff < pd.Timedelta(0):
                # is_in_block = True
                # time_jump_found = True
                # block_start_index = i
                
                # log_entry = (f"{log_time};POCZATEK_BLOKU;"
                             # f"{row_info.get('source_filepath', 'N/A')};"
                             # f"{row_info.get('original_row_index', 'N/A')};"
                             # f"{current_original_ts.strftime('%Y-%m-%dT%H:%M:%S')};;"
                             # f"{shift_hours:.2f};"
                             # f"Wykryto cofnięcie czasu z {last_original_ts.strftime('%Y-%m-%dT%H:%M:%S')}")
                # if chronology_logger: chronology_logger.info(log_entry)
                
                # corrected_timestamps[i] = last_corrected_ts + interval_td
        # else:
            # # Jesteśmy wewnątrz bloku - sprawdzamy warunki wyjścia
            
            # # --- POCZĄTEK NOWEJ LOGIKI WYJŚCIA ---
            # # Warunek Wyjścia 1: Wykryto duży skok w przód w oryginalnych danych
            # exit_on_large_gap = diff > pd.Timedelta(hours=2.5)

            # # Warunek Wyjścia 2: Skorygowany czas "dogonił" oryginalny z zapasem jednego interwału
            # exit_on_resync = last_corrected_ts >= (current_original_ts - interval_td * 2)

            # if exit_on_large_gap or exit_on_resync:
                # is_in_block = False
                
                # reason = "duży skok w przód" if exit_on_large_gap else "resynchronizacja"
                # end_info = df_corrected.loc[i-1]
                # log_entry = (f"{log_time};KONIEC_BLOKU;"
                             # f"{row_info.get('source_filepath', 'N/A')};"
                             # f"{end_info.get('original_row_index', 'N/A')};"
                             # f"{current_original_ts.strftime('%Y-%m-%dT%H:%M:%S')};;"
                             # f"{shift_hours:.2f};"
                             # f"Wykryto {reason}, kończący blok")
                # if chronology_logger: chronology_logger.info(log_entry)
                
                # # Ufamy już oryginalnemu znacznikowi czasu
                # corrected_timestamps[i] = original_timestamps[i]
            # # --- KONIEC NOWEJ LOGIKI WYJŚCIA ---
            # else:
                # # Twoja logika "mikropoprawek" pozostaje bez zmian
                # if diff > (interval_td * 1.5):
                    # corrected_timestamps[i] = last_corrected_ts + diff
                    
                    # log_entry = (f"{log_time};PRZERWA_W_BLOKU;"
                                 # f"{row_info.get('source_filepath', 'N/A')};"
                                 # f"{row_info.get('original_row_index', 'N/A')};"
                                 # f"{current_original_ts.strftime('%Y-%m-%dT%H:%M:%S')};"
                                 # f"{pd.to_datetime(corrected_timestamps[i]).strftime('%Y-%m-%dT%H:%M:%S')};"
                                 # f"{shift_hours:.2f};"
                                 # f"Zachowano przerwę {diff} wewnątrz bloku")
                    # if chronology_logger: chronology_logger.info(log_entry)
                # else:
                    # corrected_timestamps[i] = last_corrected_ts + interval_td
    
    # if time_jump_found:
        # df_corrected[timestamp_col] = corrected_timestamps
        # logging.info(f"Zakończono pomyślnie korektę chronologii dla '{context_name}'.")

    # return df_corrected

def correct_and_report_chronology(
    df: pd.DataFrame,
    context_name: str,
    known_interval: str,
    timestamp_col: str = 'TIMESTAMP',
    big_forward_hours: float = 3.0,
) -> pd.DataFrame:
    """
    Koryguje chronologię zgodnie ze specyfikacją:
    1) Skan od drugiego wiersza; na cofnięciu czasu wchodzi w tryb korekty i nadpisuje TIMESTAMP wg interwału z ciągłością.
    2) Uwzględnia mikroskoki czasu w przód do 180 minut wewnątrz bloku.
    3) Kończy korektę, gdy skorygowany czas i następna linia różnią się o 1/2 interwał.
    4) Kończy korektę, gdy następny oryginalny znacznik czasu jest nowszy od ostatniego skorygowanego.
    5) Generuje jeden wpis logu per blok z polami: LogDate;SourceFilePth;BlockStartIndex;BlockEndIndex;OriginalStartTS;OriginalEndTS;CorrectedStartTS;CorrectedEndTS.
    """
    if df.empty or len(df) < 2 or timestamp_col not in df.columns:
        return df

    try:
        interval_td = pd.to_timedelta(known_interval)
    except ValueError:
        logging.error(f"Nieprawidłowy format interwału '{known_interval}'.")
        return df

    df_corrected = df.copy()
    df_corrected.reset_index(drop=True, inplace=True)

    original = pd.to_datetime(df_corrected[timestamp_col]).to_numpy()
    corrected = original.copy()
    # chronology_tag: 0 = bez korekty (good), 1 = skorygowane (bad sector)
    chronology_tag = np.zeros(len(df_corrected), dtype=int)

    in_block = False
    any_fix = False
    block_start_index = None
    block_original_start_ts = None
    block_corrected_start_ts = None
    source_path_for_block = None
    original_start_row_index = None

    micro_jump_limit = pd.Timedelta(minutes=180)
    big_forward_threshold = pd.to_timedelta(f"{big_forward_hours}h")

    def finalize_block(end_index: int):
        nonlocal in_block, block_start_index, block_original_start_ts, block_corrected_start_ts, source_path_for_block, original_start_row_index
        if block_start_index is None:
            return
        log_time = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        start_row = df_corrected.loc[block_start_index]
        end_row = df_corrected.loc[end_index]
        src = source_path_for_block or start_row.get('source_filepath', start_row.get('source_file', start_row.get('source_filename', 'N/A')))
        start_idx_str = start_row.get('original_row_index', 'N/A')
        end_idx_str = end_row.get('original_row_index', 'N/A')
        original_end_ts = pd.to_datetime(original[end_index]).strftime('%Y-%m-%dT%H:%M:%S')
        corrected_end_ts = pd.to_datetime(corrected[end_index]).strftime('%Y-%m-%dT%H:%M:%S')
        original_start_ts_str = pd.to_datetime(block_original_start_ts).strftime('%Y-%m-%dT%H:%M:%S')
        corrected_start_ts_str = pd.to_datetime(block_corrected_start_ts).strftime('%Y-%m-%dT%H:%M:%S')
        log_line = (
            f"{log_time};{src};{start_idx_str};{end_idx_str};"
            f"{original_start_ts_str};{original_end_ts};{corrected_start_ts_str};{corrected_end_ts}"
        )
        if chronology_logger:
            chronology_logger.info(log_line)
        # reset block state
        in_block = False
        block_start_index = None
        block_original_start_ts = None
        block_corrected_start_ts = None
        source_path_for_block = None
        original_start_row_index = None
        anchor = None

    i = 1
    while i < len(corrected):
        prev_orig = pd.to_datetime(original[i-1])
        curr_orig = pd.to_datetime(original[i])
        prev_corr = pd.to_datetime(corrected[i-1])

        if not in_block:
            # Wejście w tryb korekty, gdy wykryto cofnięcie czasu (ściśle <) od drugiego wiersza
            if curr_orig < prev_orig:
                in_block = True
                any_fix = True
                block_start_index = i
                block_original_start_ts = curr_orig
                block_corrected_start_ts = prev_corr + interval_td
                corrected[i] = block_corrected_start_ts
                chronology_tag[i] = 1
                anchor = prev_orig
                # zapamiętaj ścieżkę i indeksy do logu
                row = df_corrected.loc[i]
                source_path_for_block = row.get('source_filepath', row.get('source_file', row.get('source_filename', 'N/A')))
                original_start_row_index = row.get('original_row_index', 'N/A')
            # w przeciwnym razie nic nie robimy w trybie normalnym
        else:
            # Najpierw oceń warunek zakończenia bloku wg dużego skoku do przodu
            diff_from_prev_orig = curr_orig - prev_orig
            if diff_from_prev_orig >= big_forward_threshold and (anchor is None or curr_orig > anchor):
                # Koniec złego sektora nastąpił w poprzednim wierszu
                finalize_block(i-1)
                # Obecny wiersz traktujemy jako powrót do dobrego sektora
                corrected[i] = curr_orig
                chronology_tag[i] = 0
            else:
                # Mikroskoki w przód do 180 min: zachowaj tę przerwę
                if diff_from_prev_orig > interval_td and diff_from_prev_orig <= micro_jump_limit:
                    corrected[i] = prev_corr + diff_from_prev_orig
                else:
                    # Standardowa korekta o 1 interwał
                    corrected[i] = prev_corr + interval_td
                chronology_tag[i] = 1

                # Dodatkowy warunek zakończenia gdy następny oryginalny jest nowszy od ostatnio skorygowanego
                if i + 1 < len(corrected):
                    next_orig = pd.to_datetime(original[i+1])
                    if next_orig > pd.to_datetime(corrected[i]) - interval_td / 2:
                        finalize_block(i)
        i += 1

    # Jeżeli dotarliśmy do końca w trybie korekty, zamknij blok na ostatnim wierszu
    if in_block:
        finalize_block(len(corrected) - 1)

    if any_fix:
        df_corrected[timestamp_col] = corrected
        # Uzupełnij chronology_tag: wszystko poza skorygowanymi zostaje 0
        if 'chronology_tag' in df_corrected.columns:
            try:
                base_tag = pd.to_numeric(df_corrected['chronology_tag'], errors='coerce').fillna(0).astype(int).to_numpy()
                chronology_tag = np.maximum(chronology_tag, base_tag)
            except Exception:
                pass
        df_corrected['chronology_tag'] = chronology_tag.astype(int)
        logging.info(f"Zakończono pomyślnie korektę chronologii dla '{context_name}'.")

    return df_corrected

# def correct_and_report_chronology(df: pd.DataFrame, context_name: str, known_interval: str, timestamp_col: str = 'TIMESTAMP') -> pd.DataFrame:
    # """
    # Koryguje bloki danych, które nie zostały oznaczone jako poprawne (chronology_flag != 0),
    # chroniąc zweryfikowane dane. Wersja z poprawką na FutureWarning.
    # """
    # if df.empty or 'chronology_flag' not in df.columns:
        # return df

    # df_corrected = df.copy()
    # df_corrected.reset_index(drop=True, inplace=True)
    
    # try:
        # interval_td = pd.to_timedelta(known_interval)
    # except ValueError:
        # logging.error(f"Nieprawidłowy format interwału '{known_interval}'.")
        # return df_corrected

    # # --- POCZĄTEK POPRAWKI ---
    # # Znajdź indeksy, gdzie zaczynają się bloki do korekty (logika zgodna z nowym standardem pandas)
    # is_bad_block = df_corrected['chronology_flag'].isnull()
    # # Stwórz przesuniętą serię, wypełnij NaN i jawnie zmień jej typ na boolean
    # is_not_prev_bad_block = ~is_bad_block.shift(1).fillna(False).astype(bool)
    # block_starts = df_corrected.index[is_bad_block & is_not_prev_bad_block]
    # # --- KONIEC POPRAWKI ---

    # if len(block_starts) == 0:
        # logging.info("Brak bloków do korekty chronologii.")
        # return df_corrected

    # logging.warning(f"Znaleziono {len(block_starts)} bloków do korekty chronologii w '{context_name}'.")

    # for start_index in block_starts:
        # if start_index == 0:
            # logging.error(f"Nie można skorygować bloku zaczynającego się od pierwszego wiersza (indeks 0). Ten blok zostanie pominięty.")
            # continue
            
        # end_index_arr = df_corrected.index[~is_bad_block & (df_corrected.index > start_index)]
        # end_index = end_index_arr[0] if len(end_index_arr) > 0 else len(df_corrected)

        # last_good_ts = df_corrected.loc[start_index - 1, timestamp_col]
        # num_points_to_correct = end_index - start_index
        
        # logging.info(f"  -> Korygowanie bloku od wiersza {start_index} do {end_index - 1} ({num_points_to_correct} rekordów).")
        
        # new_timestamps = pd.date_range(
            # start=last_good_ts + interval_td,
            # periods=num_points_to_correct,
            # freq=interval_td
        # )
        
        # df_corrected.loc[start_index : end_index - 1, timestamp_col] = new_timestamps
        # df_corrected.loc[start_index : end_index - 1, 'chronology_flag'] = 10

    # corrected_ts = set(df_corrected[df_corrected['chronology_flag'] == 10][timestamp_col])
    # good_ts = set(df[df['chronology_flag'] == 0][timestamp_col])
    # conflicts = corrected_ts.intersection(good_ts)

    # if conflicts:
        # logging.error(f"KRYTYCZNY BŁĄD LOGICZNY: Wykryto {len(conflicts)} konfliktów, gdzie dane skorygowane nadpisały dane poprawne!")

    # return df_corrected

# def correct_and_report_chronology(df: pd.DataFrame, context_name: str, known_interval: str, timestamp_col: str = 'TIMESTAMP') -> pd.DataFrame:
    # """
    # Ostateczna wersja korygująca chronologię, która ignoruje pierwszy wiersz
    # i generuje udoskonalony, zwięzły log.
    # Wersja 7.77 FINAL: Ostateczna, niezawodna logika.
    # """
    # if df.empty or len(df) < 2 or timestamp_col not in df.columns:
        # return df

    # try:
        # interval_td = pd.to_timedelta(known_interval)
    # except ValueError:
        # logging.error(f"Nieprawidłowy format interwału '{known_interval}'.")
        # return df

    # df_corrected = df.copy()
    # df_corrected.reset_index(drop=True, inplace=True)

    # timestamps = df_corrected[timestamp_col].to_numpy()
    # original_timestamps = timestamps.copy() 
    # time_jump_found = False
    
    # # Upewnij się, że nagłówek logu ma nowy format
    # if chronology_logger and chronology_logger.handlers[0].stream.tell() == 0:
        # chronology_logger.info("LogDate;SourceFile;BlockStartIndex;BlockEndIndex;OriginalStartTS;OriginalEndTS;CorrectedStartTS;CorrectedEndTS")

    # i = 1
    # while i < len(timestamps):
        # # Porównanie zaczyna się od drugiego wiersza (i=1)
        # if timestamps[i] <= timestamps[i-1]:
            # if not time_jump_found:
                # time_jump_found = True
                # logging.warning(f"Wykryto błąd chronologii w '{context_name}'. Rozpoczynanie korekty blokowej.")

            # start_index = i
            # last_good_ts = timestamps[i-1]
            
            # logging.info(f"  -> Błędny blok danych rozpoczyna się przy wierszu {start_index}.")

            # # Skanuj w przód, aby znaleźć koniec błędnego bloku
            # end_index = -1
            # for j in range(start_index, len(original_timestamps)):
                # if original_timestamps[j] > last_good_ts:
                    # end_index = j
                    # break
            
            # if end_index == -1:
                # end_index = len(timestamps) # Korekta do końca paczki

            # num_points_to_correct = end_index - start_index
            # new_timestamps = pd.date_range(
                # start=last_good_ts + interval_td,
                # periods=num_points_to_correct,
                # freq=interval_td
            # )
            
            # # --- Udoskonalone logowanie ---
            # start_info = df_corrected.loc[start_index]
            # end_info = df_corrected.loc[end_index - 1]
            # log_entry = (f"{pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')};"
                         # f"{start_info.get('source_filename', 'N/A')};"
                         # f"{start_info.get('original_row_index', 'N/A')};{end_info.get('original_row_index', 'N/A')};"
                         # f"{pd.to_datetime(original_timestamps[start_index]).strftime('%Y-%m-%dT%H:%M:%S')};"
                         # f"{pd.to_datetime(original_timestamps[end_index-1]).strftime('%Y-%m-%dT%H:%M:%S')};"
                         # f"{pd.to_datetime(new_timestamps[0]).strftime('%Y-%m-%dT%H:%M:%S')};"
                         # f"{pd.to_datetime(new_timestamps[-1]).strftime('%Y-%m-%dT%H:%M:%S')}")
            # if chronology_logger: chronology_logger.info(log_entry)
            # # --- Koniec logowania ---

            # timestamps[start_index:end_index] = new_timestamps
            # i = end_index 
        # else:
            # i += 1 
    
    # if time_jump_found:
        # df_corrected[timestamp_col] = timestamps
        # logging.info(f"Zakończono pomyślnie korektę chronologii dla '{context_name}'.")

    # return df_corrected

def diagnose_chronology_blocks(df: pd.DataFrame, context_name: str, known_interval: str, timestamp_col: str = 'TIMESTAMP') -> pd.DataFrame:
    """
    WERSJA DIAGNOSTYCZNA: Nie modyfikuje danych. Skanuje i loguje bloki z błędną chronologią
    zgodnie z zaawansowaną specyfikacją użytkownika.
    """
    logging.info(f"--- URUCHAMIANIE ZAAWANSOWANEJ DIAGNOZY CHRONOLOGII DLA: {context_name} ---")
    if df.empty or len(df) < 2 or timestamp_col not in df.columns:
        return df

    interval_td = pd.to_timedelta(known_interval)
    df_diag = df.copy()
    df_diag.reset_index(drop=True, inplace=True)
    
    timestamps = df_diag[timestamp_col].to_numpy()
    
    is_in_block = False
    
    for i in range(1, len(timestamps)):
        last_ts = pd.to_datetime(timestamps[i-1])
        current_ts = pd.to_datetime(timestamps[i])
        diff = current_ts - last_ts

        row_info = df_diag.loc[i]
        log_time = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

        if not is_in_block:
            # Punkt 1: Znajdź "prawdziwy początek" nowego bloku
            if diff < pd.Timedelta(0):
                is_in_block = True
                log_entry = (f"{log_time};POCZATEK_BLOKU;"
                             f"{row_info.get('source_filename', 'N/A')};"
                             f"{row_info.get('original_row_index', 'N/A')};"
                             f"{current_ts.strftime('%Y-%m-%dT%H:%M:%S')};"
                             f"Wykryto pierwsze cofnięcie czasu z {last_ts.strftime('%Y-%m-%dT%H:%M:%S')}")
                if chronology_logger: chronology_logger.info(log_entry)
        else:
            # Jesteśmy wewnątrz bloku - szukamy błędów wewnętrznych lub końca
            
            # Punkt 3: Znajdź "właściwy koniec" bloku (duży skok w przód)
            if diff > pd.Timedelta(hours=12):
                is_in_block = False
                end_row_info = df_diag.loc[i-1] # Blok zakończył się na poprzednim wierszu
                log_entry = (f"{log_time};KONIEC_BLOKU;"
                             f"{row_info.get('source_filename', 'N/A')};"
                             f"{end_row_info.get('original_row_index', 'N/A')};"
                             f"{current_ts.strftime('%Y-%m-%dT%H:%M:%S')};"
                             f"Wykryto duży skok w przód o {diff}, kończący blok.")
                if chronology_logger: chronology_logger.info(log_entry)

            # Punkt 2: Znajdź "błędy wewnętrzne"
            elif diff < pd.Timedelta(0):
                log_entry = (f"{log_time};BLAD_WEWNETRZNY;"
                             f"{row_info.get('source_filename', 'N/A')};"
                             f"{row_info.get('original_row_index', 'N/A')};"
                             f"{current_ts.strftime('%Y-%m-%dT%H:%M:%S')};"
                             f"Wewnętrzne cofnięcie czasu wewnątrz bloku.")
                if chronology_logger: chronology_logger.info(log_entry)
            elif diff > (interval_td * 1.5) and diff <= pd.Timedelta(hours=12):
                 log_entry = (f"{log_time};BLAD_WEWNETRZNY;"
                             f"{row_info.get('source_filename', 'N/A')};"
                             f"{row_info.get('original_row_index', 'N/A')};"
                             f"{current_ts.strftime('%Y-%m-%dT%H:%M:%S')};"
                             f"Krótka przerwa lub nieregularność ({diff}) wewnątrz bloku.")
                 if chronology_logger: chronology_logger.info(log_entry)

    logging.info(f"Zakończono diagnozę chronologii. Sprawdź plik {CHRONOLOGY_LOG_FILENAME.name}.")
    
    # Zwróć oryginalną, niezmodyfikowaną ramkę danych
    return df
    
def diagnose_chronology_scan(df: pd.DataFrame, context_name: str, known_interval: str, timestamp_col: str = 'TIMESTAMP') -> pd.DataFrame:
    """
    WERSJA DIAGNOSTYCZNA: Nie modyfikuje danych. Generuje szczegółowy log
    do analizy działania algorytmu "skanowania w przód" na problematycznym zbiorze.
    """
    logging.info(f"--- URUCHAMIANIE TRYBU DIAGNOSTYCZNEGO DLA CHRONOLOGII (SKANOWANIE) ---")
    if df.empty or timestamp_col not in df.columns:
        return df

    df_corrected = df.copy()
    df_corrected.reset_index(drop=True, inplace=True)
    timestamps = df_corrected[timestamp_col].to_numpy()
    
    i = 1
    while i < len(timestamps):
        if timestamps[i] <= timestamps[i-1]:
            start_index = i
            last_good_ts = timestamps[i-1]
            
            logging.debug(f"\n{'='*20} ROZPOCZĘCIE DIAGNOZY BLOKU BŁĘDÓW {'='*20}")
            logging.debug(f"Wykryto błąd w wierszu: {start_index}")
            logging.debug(f"Ostatni poprawny znacznik czasu (last_good_ts): {pd.to_datetime(last_good_ts)}")
            logging.debug("Rozpoczynam skanowanie w przód w poszukiwaniu końca bloku...")
            logging.debug("-" * 80)
            logging.debug(f"{'Indeks (j)':<12} | {'Oryginalny Czas [j]':<28} | Warunek: Oryginalny > Ostatni Poprawny? | Wynik")
            logging.debug("-" * 80)

            end_index = -1
            for j in range(start_index, len(timestamps)):
                current_original_ts = timestamps[j]
                condition_met = current_original_ts > last_good_ts
                
                # Zaloguj tylko kluczowe momenty, aby nie zalać logu:
                # - Pierwsze 10 iteracji w bloku
                # - Co 500-ny wiersz
                # - Ostatnie 10 wierszy w paczce
                # - Wiersz, w którym warunek został spełniony
                if (j < start_index + 10) or (j % 500 == 0) or (j > len(timestamps) - 10) or condition_met:
                    logging.debug(f"{j:<12d} | {str(pd.to_datetime(current_original_ts)):<28} | {str(pd.to_datetime(current_original_ts))} > {str(pd.to_datetime(last_good_ts))} | {condition_met}")

                if condition_met:
                    end_index = j
                    logging.debug("-" * 80)
                    logging.debug(f"Zakończono skanowanie. Koniec bloku znaleziony przy indeksie: {end_index}")
                    logging.debug(f"{'='*20} ZAKOŃCZENIE DIAGNOZY BLOKU BŁĘDÓW {'='*20}\n")
                    break
            
            if end_index == -1:
                logging.debug("Skanowanie zakończone. Nie znaleziono końca bloku w tej paczce.")
            
            # Zakończ diagnostykę po pierwszym znalezionym bloku, aby log nie był zbyt duży
            logging.info(f"--- ZAKOŃCZONO TRYB DIAGNOSTYCZNY ---")
            return df 
        else:
            i += 1
            
    logging.debug("Nie znaleziono żadnych błędów chronologii w tej paczce.")
    logging.info(f"--- ZAKOŃCZONO TRYB DIAGNOSTYCZNY ---")
    return df

def diagnose_chronology(df: pd.DataFrame, context_name: str, known_interval: str, timestamp_col: str = 'TIMESTAMP') -> pd.DataFrame:
    """
    WERSJA DIAGNOSTYCZNA: Nie modyfikuje danych. Generuje szczegółowy log
    do analizy działania algorytmu korekty na problematycznym zbiorze danych.
    """
    logging.info(f"--- URUCHAMIANIE TRYBU DIAGNOSTYCZNEGO DLA CHRONOLOGII: {context_name} ---")
    if df.empty or timestamp_col not in df.columns:
        return df

    try:
        interval_td = pd.to_timedelta(known_interval)
    except ValueError:
        logging.error(f"Nieprawidłowy format interwału '{known_interval}'.")
        return df

    df_corrected = df.copy()
    df_corrected.reset_index(drop=True, inplace=True)

    original_timestamps = df_corrected[timestamp_col].to_numpy()
    # Tablica 'corrected' będzie symulować, jak wyglądałaby korekta
    corrected_timestamps = original_timestamps.copy()
    
    is_in_correction_mode = False
    
    for i in range(1, len(corrected_timestamps)):
        last_corrected_ts = pd.to_datetime(corrected_timestamps[i-1])
        current_original_ts = pd.to_datetime(original_timestamps[i])
        previous_original_ts = pd.to_datetime(original_timestamps[i-1])

        log_prefix = f"Wiersz {i:05d}:"
        
        if is_in_correction_mode:
            expected_next_ts = last_corrected_ts + interval_td
            
            # Sprawdzanie warunków wyjścia
            is_resynced = abs((current_original_ts - last_corrected_ts) - interval_td) < pd.Timedelta('1s')
            is_sequence_broken = abs((current_original_ts - previous_original_ts) - interval_td) > pd.Timedelta('1s')
            
            decision = ""
            if is_resynced or is_sequence_broken:
                is_in_correction_mode = False
                corrected_timestamps[i] = current_original_ts
                reason = "resynchronizacja" if is_resynced else "przerwana sekwencja"
                decision = f"DECYZJA: WYJŚCIE Z KOREKTY (powód: {reason}). Używam oryginału: {current_original_ts}"
            else:
                corrected_timestamps[i] = expected_next_ts
                decision = f"DECYZJA: KONTYNUACJA KOREKTY. Nowy czas: {pd.to_datetime(corrected_timestamps[i])}"

            logging.debug(f"{log_prefix} W TRYBIE KOREKTY. "
                          f"Poprzedni (skorygowany): {last_corrected_ts}, "
                          f"Oryginalny: {current_original_ts}. "
                          f"Warunki wyjścia -> Resync: {is_resynced}, PrzerwanaSekwencja: {is_sequence_broken}. {decision}")

        elif current_original_ts <= last_corrected_ts:
            is_in_correction_mode = True
            corrected_timestamps[i] = last_corrected_ts + interval_td
            logging.debug(f"{log_prefix} NORMALNY TRYB. "
                          f"Poprzedni: {last_corrected_ts}, "
                          f"Oryginalny: {current_original_ts}. "
                          f"DECYZJA: WEJŚCIE W TRYB KOREKTY. Nowy czas: {pd.to_datetime(corrected_timestamps[i])}")
        else:
            # W tym wierszu wszystko jest w porządku
            pass
            
    logging.info(f"--- ZAKOŃCZONO TRYB DIAGNOSTYCZNY DLA CHRONOLOGII ---")
    # Zwróć oryginalną, niezmodyfikowaną ramkę danych
    return df

# detect_good_chronology_mask: usunięta – logika przeniesiona do correct_and_report_chronology
    
def apply_column_mapping(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    # rename columns based on mapping rules
    file_id = config.get('file_id')
    if not file_id: return df
    ruleset_name = STATION_MAPPING_FOR_COLUMNS.get(file_id)
    if not ruleset_name: return df
    mapping_dict = COLUMN_MAPPING_RULES.get(ruleset_name)
    if not mapping_dict: return df
    df.rename(columns=mapping_dict, inplace=True)
    return df

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Removes sync-conflict suffixes from column names."""
    conflict_pattern = re.compile(r'\.sync-conflict-.*$')
    rename_map = {col: conflict_pattern.sub('', col) for col in df.columns if isinstance(col, str) and conflict_pattern.search(col)}
    if rename_map: df.rename(columns=rename_map, inplace=True)
    return df

def _sanitize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sanityzuje nazwy kolumn. Usuwa znaki specjalne. Jeśli w wyniku sanityzacji
    powstanie zduplikowana nazwa, do nowej nazwy dodawany jest sufiks '_rm'.
    """
    final_rename_map = {}
    # Zbiór do śledzenia już przetworzonych/stworzonych nazw, aby wykrywać kolizje
    seen_new_names = set()

    for col in df.columns:
        if col == 'TIMESTAMP':
            seen_new_names.add(col)
            continue
        
        # Usuń każdy znak, który nie jest literą, cyfrą ani podkreślnikiem
        new_col = re.sub(r'[^a-zA-Z0-9_]', '', col)
        
        # Sprawdź, czy oczyszczona nazwa już istnieje
        if new_col in seen_new_names:
            unique_col = f"{new_col}_rm"
            # W mało prawdopodobnym przypadku, gdy 'kolumna_rm' też już istnieje, dodaj kolejny sufiks
            while unique_col in seen_new_names:
                unique_col += "_rm"
            
            final_rename_map[col] = unique_col
            seen_new_names.add(unique_col)
        else:
            # Jeśli nazwa nie jest duplikatem, użyj jej
            if new_col != col:
                final_rename_map[col] = new_col
            seen_new_names.add(new_col)
            
    if final_rename_map:
        logging.debug(f"Sanityzacja nazw kolumn z obsługą duplikatów: {final_rename_map}")
        df.rename(columns=final_rename_map, inplace=True)
        
    return df
    
def _enforce_numeric_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Wymusza konwersję kolumn na typ numeryczny, ze specjalnym traktowaniem kolumn flag.
    Wersja 7.13: Dodano agresywną konwersję kolumn '_flag' na typ integer.
    """
    cols_to_skip = ['TIMESTAMP', 'group_id', 'source_file', 'interval', 'TZ', '5M METAR Tab.4678', '1M METAR Tab.4678', '5MMETARTab4678', '1MMETARTab4678', 'source_filename', 'source_filepath']

    for col in df.columns:
        if col in cols_to_skip:
            continue

        # --- POCZĄTEK NOWEJ LOGIKI ---
        # Specjalne, agresywne traktowanie kolumn z flagami
        if col.endswith('_flag'):
            # Krok 1: Konwertuj na liczbę (błędy zamień na NaN).
            # Krok 2: Wypełnij ewentualne braki (NaN) wartością 0 (dane dobre).
            # Krok 3: Rzutuj na typ integer.
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            continue  # Przejdź do następnej kolumny
        # --- KONIEC NOWEJ LOGIKI ---

        # Standardowa obsługa pozostałych kolumn z danymi
        if not pd.api.types.is_numeric_dtype(df[col]):
            original_dtype = df[col].dtype
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            if df[col].dtype != original_dtype:
                logging.debug(f"Konwersja kolumny '{col}' z typu {original_dtype} na {df[col].dtype}.")
    
    return df
    
def apply_manual_overrides(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Overwrites values in specified columns and time ranges based on config.
    Operates on naive timestamps.
    """
    group_id = config.get('file_id')
    if not group_id:
        return df

    # Find the correct ruleset for the given group_id
    ruleset_name = STATION_MAPPING_FOR_OVERRIDES.get(group_id)
    if not ruleset_name:
        return df

    station_rules = MANUAL_VALUE_OVERRIDES.get(ruleset_name)
    if not station_rules:
        return df

    df_out = df.copy()

    for col_name, rules_list in station_rules.items():
        if col_name not in df_out.columns:
            logging.warning(f"Nadpisywanie wartości: Kolumna '{col_name}' nie istnieje w danych dla grupy '{group_id}'.")
            continue

        for rule in rules_list:
            try:
                start_ts = pd.to_datetime(rule['start'])
                end_ts = pd.to_datetime(rule['end'])
                new_value = rule['new_value']
                reason = rule.get('reason', 'Brak powodu.')

                mask = (df_out['TIMESTAMP'] >= start_ts) & (df_out['TIMESTAMP'] <= end_ts)

                if mask.any():
                    df_out.loc[mask, col_name] = new_value
                    logging.info(f"Nadpisano {mask.sum()} wartości w kolumnie '{col_name}' na '{new_value}'. Powód: {reason}")

            except Exception as e:
                logging.error(f"Błąd podczas stosowania reguły nadpisywania dla '{col_name}': {e}")

    return df_out

def _ensure_flag_columns_exist(df: pd.DataFrame) -> pd.DataFrame:
    """
    Zapewnia, że dla każdej numerycznej kolumny danych istnieje odpowiednia kolumna '_flag'.
    Wersja 7.76 FINAL: Implementuje nową logikę flag domyślnych (99 dla NaN, 0 dla wartości).
    """
    df = df.copy() # Defragmentacja na wejściu

    cols_to_skip = [
        'TIMESTAMP', 'group_id', 'source_file', 'interval', 
        'latitude', 'longitude', 'source_filepath', 
        'source_filename', 'original_row_index', 'RECORD'
    ]
    
    # Słownik do przechowania danych dla nowych kolumn flag
    new_cols_data = {}
    
    # Iteruj po wszystkich kolumnach, aby znaleźć te, które potrzebują flag
    for col_name in df.columns:
        # Pomiń metadane i kolumny, które już są flagami
        if col_name in cols_to_skip or col_name.endswith('_flag'):
            continue
        
        # Sprawdź, czy to kolumna numeryczna (tylko takie flagujemy)
        if not pd.api.types.is_numeric_dtype(df[col_name]):
            continue

        flag_name = f"{col_name}_flag"
        # Stwórz nową flagę tylko, jeśli jeszcze nie istnieje
        if flag_name not in df.columns:
            # Użyj np.where do wektorowego stworzenia nowej kolumny:
            # Gdzie wartość w col_name istnieje -> flaga = 0
            # Gdzie wartość w col_name jest NaN -> flaga = 99
            new_cols_data[flag_name] = np.where(df[col_name].notna(), 0, 99)

    # Jeśli znaleziono brakujące kolumny, dodaj je wszystkie naraz
    if new_cols_data:
        logging.debug(f"Tworzenie {len(new_cols_data)} brakujących kolumn flag z logiką 99/0.")
        
        # Stwórz nową ramkę danych z nowymi kolumnami i poprawnym typem
        new_flags_df = pd.DataFrame(new_cols_data, index=df.index).astype('int8')
        
        # Połącz z oryginalną ramką
        df = pd.concat([df, new_flags_df], axis=1)
            
    return df
    
def _filter_future_timestamps(df: pd.DataFrame, timestamp_col: str = 'TIMESTAMP') -> pd.DataFrame:
    """
    Usuwa wiersze, których znacznik czasu jest w przyszłości.
    """
    if df.empty or timestamp_col not in df.columns:
        return df

    # Pobierz aktualny czas
    now = pd.Timestamp.now()
    
    # Stwórz maskę logiczną dla wierszy z przyszłą datą
    future_mask = df[timestamp_col] > now

    # Jeśli znaleziono jakiekolwiek przyszłe daty, odfiltruj je i zaraportuj
    if future_mask.any():
        num_removed = future_mask.sum()
        logging.warning(
            f"Usunięto {num_removed} rekordów ze znacznikiem czasu z przyszłości (późniejszym niż {now.strftime('%Y-%m-%d %H:%M:%S')})."
        )
        return df[~future_mask]
    
    return df
    
# def _validate_csv_file(df: pd.DataFrame, file_path: Path, config: dict) -> bool:
    # """
    # Sprawdza, czy pojedynczy plik CSV jest spójny.
    # Wersja 7.78: Zastępuje walidację mediany interwału ścisłą kontrolą monotoniczności.
    # """
    # if df.empty or len(df) < 2:
        # return False

    # # 1. Walidacja nazwy pliku względem ostatniego znacznika czasu (bez zmian)
    # try:
        # # Wyodrębnij datę z nazwy pliku, np. 'pom1m_20250524T234500' -> '20250524'
        # filename_date_str = re.search(r'(\d{8}T\d{6})', file_path.stem).group(1)[:8]
        # filename_date = pd.to_datetime(filename_date_str, format='%Y%m%d').date()
        
        # last_timestamp_date = df['TIMESTAMP'].iloc[-1].date()

        # if filename_date != last_timestamp_date:
            # logging.debug(f"Plik '{file_path.name}' nie przeszedł walidacji: data z nazwy ({filename_date}) "
                          # f"nie zgadza się z datą ostatniego rekordu ({last_timestamp_date}).")
            # return False
    # except (AttributeError, IndexError, TypeError):
        # # TypeError dodany na wypadek problemów z .date() przy błędnych danych
        # logging.debug(f"Nie można było zweryfikować daty w nazwie pliku '{file_path.name}'.")
        # return False

    # # --- POCZĄTEK NOWEJ LOGIKI ---
    # # 2. Walidacja monotoniczności (sprawdzenie, czy czas się nie cofa)
    # # Atrybut .is_monotonic_increasing jest wysoce zoptymalizowany do tego zadania.
    # if not df['TIMESTAMP'].is_monotonic_increasing:
        # logging.debug(f"Plik '{file_path.name}' nie przeszedł walidacji: wykryto cofnięcie czasu wewnątrz pliku.")
        # return False
    # # --- KONIEC NOWEJ LOGIKI ---

    # logging.info(f"Plik '{file_path.name}' pomyślnie przeszedł walidację.")
    # return True
    
def _validate_csv_file(df: pd.DataFrame, file_path: Path, config: dict) -> bool:
    """
    Sprawdza, czy pojedynczy plik CSV jest spójny.
    Wersja 7.79: Walidacja oparta na dacie modyfikacji pliku, a nie jego nazwie.
    """
    if df.empty or len(df) < 2:
        return False

    # --- POCZĄTEK ZMIANY ---
    # 1. Walidacja daty ostatniej modyfikacji pliku względem ostatniego znacznika czasu
    try:
        # Pobierz datę ostatniej modyfikacji pliku z systemu plików
        mod_time_unix = file_path.stat().st_mtime
        modification_date = pd.to_datetime(mod_time_unix, unit='s').date()
        
        # Pobierz datę ostatniego rekordu w pliku
        last_timestamp_date = df['TIMESTAMP'].iloc[-1].date()

        # Porównaj obie daty
        if modification_date != last_timestamp_date:
            logging.debug(f"Plik '{file_path.name}' nie przeszedł walidacji: data modyfikacji ({modification_date}) "
                          f"nie zgadza się z datą ostatniego rekordu ({last_timestamp_date}).")
            return False
    except (IndexError, TypeError):
        logging.debug(f"Nie można było zweryfikować daty ostatniej modyfikacji dla pliku '{file_path.name}'.")
        return False
    # --- KONIEC ZMIANY ---

    # 2. Walidacja monotoniczności (bez zmian)
    if not df['TIMESTAMP'].is_monotonic_increasing:
        logging.debug(f"Plik '{file_path.name}' nie przeszedł walidacji: wykryto cofnięcie czasu wewnątrz pliku.")
        return False

    logging.info(f"Plik '{file_path.name}' pomyślnie przeszedł walidację.")
    return True    
    
# --- GŁÓWNE FUNKCJE PRZETWARZANIA ---

def scan_for_files(input_dirs: List[str], source_ids: List[str]) -> List[Path]:
    """Scans directories for matching files, ignoring sync-conflict files."""
    all_file_paths = []
    for input_dir in input_dirs:
        p_input = Path(input_dir)
        if not p_input.is_dir(): continue
        for p_file in p_input.rglob('*'):
            if "sync-conflict" in p_file.name: continue
            if "CONFIG" in p_file.name: continue
            if "tmp" in p_file.name: continue
            if "checkpoint" in p_file.name: continue
            if "pom1m_20210629T234501" in p_file.name: continue
            if "pom1m_20230614T234500" in p_file.name: continue
            if "pom1m_20210813T234500" in p_file.name: continue
            if "pom1m_20210822T234501" in p_file.name: continue
            if p_file.is_file():
                if any((sid.endswith('$') and p_file.stem.endswith(sid.rstrip('$'))) or (sid in p_file.name) for sid in source_ids):
                    all_file_paths.append(p_file.resolve())
    return sorted(list(set(all_file_paths)))

def identify_file_type(file_path: Path) -> str:
    """Quickly identifies file type from its first line."""
    try:
        with open(file_path, 'r', encoding='latin-1') as f:
            first_line = f.readline().strip()
        if first_line.startswith('"TOB1"'): return 'TOB1'
        if first_line.startswith('"TOA5"'): return 'TOA5'
        if '"Timestamp"' in first_line: return 'CSV' # More robust check for CSV
        return 'UNKNOWN'
    except Exception:
        return 'UNKNOWN'

def process_binary_file(args: tuple) -> Optional[pd.DataFrame]:
    """Processing pipeline for a single binary file (TOB1/TOA5)."""
    file_path, config = args
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
            return None

        if df.empty: return None

        # Apply only the specified filter for this pipeline
        df = filter_by_realistic_date_range(df, file_path)
        return df
    except Exception as e:
        logging.error(f"Krytyczny błąd (plik binarny) {file_path.name}: {e}", exc_info=True)
        return None

def process_and_save_data(raw_dfs: List[pd.DataFrame], config: dict, lock: multiprocessing.Lock):
    """
    Final, unified processing pipeline.
    Wersja 7.25: Optymalizuje moment defragmentacji, aby wyciszyć ostrzeżenia o wydajności.
    """
    if not raw_dfs:
        logging.info("Brak danych do przetworzenia.")
        return
    
    group_id = config['file_id']
    data_by_year = defaultdict(list)
    for df in raw_dfs:
        if not df.empty and 'TIMESTAMP' in df.columns:
            df.dropna(subset=['TIMESTAMP'], inplace=True)
            for year, year_group in df.groupby(df['TIMESTAMP'].dt.year):
                data_by_year[year].append(year_group)

    logging.info(f"Rozpoczynanie finalnego przetwarzania i zapisu dla {len(data_by_year)} lat...")

    for year, dfs_for_year in sorted(data_by_year.items()):
        try:
            logging.info(f"--- Przetwarzanie roku: {int(year)} | Grupa: {group_id} ---")

            logger_data_raw = pd.concat(dfs_for_year, ignore_index=True)
            if logger_data_raw.empty:
                continue
            
            # Priorytetyzacja: 0 (GOOD_ORIGINAL), 1 (CORRECTED), 2 (brak tagu)
            try:
                tmp_df = logger_data_raw.copy()
                if 'chronology_tag' in tmp_df.columns:
                    prio = pd.to_numeric(tmp_df['chronology_tag'], errors='coerce')
                    tmp_df['_prio'] = prio.where(prio.isin([0, 1]), other=2).fillna(2).astype(int)
                else:
                    tmp_df['_prio'] = 2
                tmp_df = tmp_df.sort_values(['TIMESTAMP', '_prio'], kind='stable')
                logger_data_df = tmp_df.drop_duplicates(subset=['TIMESTAMP'], keep='first').drop(columns=['_prio'], errors='ignore')
            except Exception:
                # Fallback na poprzednie zachowanie
                logger_data_df = logger_data_raw.groupby('TIMESTAMP').first()

            if group_id in GROUP_IDS_FOR_MATLAB_FILL:
                matlab_df = load_matlab_data(int(year), config)
                if not matlab_df.empty:
                    matlab_df.set_index('TIMESTAMP', inplace=True)
                    combined_df = logger_data_df.combine_first(matlab_df).reset_index()
                else:
                    combined_df = logger_data_df.reset_index()
            else:
                combined_df = logger_data_df.reset_index()

            mapped_df = apply_column_mapping(combined_df, config)

            if mapped_df.columns.duplicated().any():
                mapped_df = mapped_df.T.groupby(level=0).first().T
                mapped_df['TIMESTAMP'] = pd.to_datetime(mapped_df['TIMESTAMP'], errors='coerce')
                mapped_df = _enforce_numeric_types(mapped_df)
                mapped_df.dropna(subset=['TIMESTAMP'], inplace=True)

            mapped_df = _sanitize_column_names(mapped_df)
            if mapped_df.empty: continue

            # --- POCZĄTEK KLUCZOWEJ ZMIANY ---
            # Defragmentacja ramki danych zaraz po jej finalnym ustrukturyzowaniu.
            # To zapobiega ostrzeżeniom w kolejnych funkcjach i poprawia ich wydajność.
            corrected_df = mapped_df.copy()
            # --- KONIEC KLUCZOWEJ ZMIANY ---

            corrected_df['TIMESTAMP'] = apply_timezone_correction(corrected_df['TIMESTAMP'], config['file_id'])
            corrected_df.dropna(subset=['TIMESTAMP'], inplace=True)
            if corrected_df.empty: continue

            corrected_df = apply_manual_time_shifts(corrected_df, config['file_id'])
            corrected_df = apply_calibration(corrected_df, config['file_id'])
            corrected_df = apply_value_range_flags(corrected_df)
            corrected_df = apply_quality_flags(corrected_df, config)
            corrected_df = apply_manual_overrides(corrected_df, config)
            corrected_df = align_timestamp(corrected_df, config.get('interval'))
            corrected_df = _ensure_flag_columns_exist(corrected_df)
            # Po zapewnieniu kolumn flag: ustaw 5 dla wszystkich *_flag dla wierszy skorygowanych czasowo
            if 'chronology_tag' in corrected_df.columns:
                try:
                    # chronology_tag: 0 = bez korekty, 1 = skorygowane
                    mask_corr = pd.to_numeric(corrected_df['chronology_tag'], errors='coerce').fillna(0).astype(int) == 1
                    if mask_corr.any():
                        flag_cols = [c for c in corrected_df.columns if c.endswith('_flag')]
                        if flag_cols:
                            corrected_df.loc[mask_corr, flag_cols] = 5
                            logging.info(f"[CHRONOLOGY] Ustawiono flagę=5 dla {int(mask_corr.sum())} wierszy skorygowanych.")
                except Exception as e:
                    logging.warning(f"[CHRONOLOGY] Nie udało się oznaczyć flagą=5 wierszy skorygowanych: {e}")
            corrected_df = _enforce_numeric_types(corrected_df)
            corrected_df = corrected_df.copy()
            corrected_df = _filter_future_timestamps(corrected_df)
            
            output_format = config.get('output_format', 'sqlite')
            if output_format in ['sqlite', 'both']:
                save_dataframe_to_sqlite(corrected_df, config, lock)
            if output_format in ['csv', 'both']:
                save_dataframe_to_csv(corrected_df, int(year), config, lock)

        except Exception as e:
            logging.error(f"Krytyczny błąd podczas finalnego przetwarzania roku {int(year)}: {e}", exc_info=True)
            
# --- FUNKCJA TESTUJĄCA ---
def _run_tests(config: dict):
    """
    Uruchamia zautomatyzowany test, używając logiki przygotowania danych
    identycznej jak w funkcji main() i poprawnie zamykając połączenia z bazą.
    Wersja 7.82 FINAL: Pełna spójność logiki i poprawne zarządzanie zasobami.
    """
    logging.info("="*20 + " URUCHAMIANIE TRYBU TESTOWEGO " + "="*20)
    with tempfile.TemporaryDirectory() as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        test_db_path = temp_dir / "test_db.sqlite"
        test_csv_dir = temp_dir / "test_csv_output"
        test_csv_dir.mkdir()

        test_config = config.copy()
        test_config['db_path'] = str(test_db_path)
        test_config['output_dir'] = str(test_csv_dir)
        test_config['output_format'] = 'both'

        # --- Uruchomienie głównej logiki skryptu ---
        all_files = scan_for_files(test_config['input_dir'], test_config.get('source_ids', []))
        if not all_files:
            logging.warning("Test pominięty: nie znaleziono plików.")
            return
        
        binary_files = [p for p in all_files if identify_file_type(p) in ['TOB1', 'TOA5']]
        csv_files = [p for p in all_files if identify_file_type(p) == 'CSV']
        
        all_raw_results = []
        
        if binary_files:
            # Ta część pozostaje uproszczona, zakładając, że główny problem leży w CSV
            binary_args = [(p, test_config) for p in binary_files]
            with multiprocessing.Pool(processes=test_config['jobs']) as pool:
                binary_results = list(pool.imap_unordered(process_binary_file, binary_args))
            all_raw_results.extend([df for df in binary_results if df is not None and not df.empty])
        
        if csv_files:
            # Użyj DOKŁADNIE tej samej logiki deduplikacji, co w main()
            csv_files.sort(key=lambda p: p.stat().st_mtime)
            all_csv_dfs = [read_simple_csv_data(p) for p in csv_files]
            non_empty_dfs = [df for df in all_csv_dfs if df is not None and not df.empty]
            if non_empty_dfs:
                batch_df = pd.concat(non_empty_dfs, ignore_index=True)
                
                if 'TIMESTAMP' in batch_df.columns:
                    initial_rows = len(batch_df)
                    metadata_cols = ['TIMESTAMP', 'source_filename', 'original_row_index', 'source_filepath']
                    cols_to_check = [col for col in batch_df.columns if col not in metadata_cols]
                    
                    if cols_to_check:
                        df_for_dedup = batch_df.copy()
                        numeric_cols_to_round = [col for col in df_for_dedup.select_dtypes(include=np.number).columns if col not in metadata_cols]
                        df_for_dedup[numeric_cols_to_round] = df_for_dedup[numeric_cols_to_round].round(4)
                        indices_to_keep = df_for_dedup.drop_duplicates(subset=cols_to_check, keep='first').index
                        batch_df = batch_df.loc[indices_to_keep]
                    
                    rows_removed = initial_rows - len(batch_df)
                    if rows_removed > 0:
                        logging.info(f"[Test] Usunięto {rows_removed} zduplikowanych wierszy.")

                known_interval = test_config.get('interval')
                corrected_batch = correct_and_report_chronology(batch_df, f"Testowe pliki CSV", known_interval)
                all_raw_results.append(corrected_batch)

        if not all_raw_results:
            logging.error("[Test] Nie udało się przetworzyć żadnych danych.")
            return

        lock = multiprocessing.Manager().Lock()
        initialize_database(Path(test_config['db_path']))
        process_and_save_data(all_raw_results, test_config, lock)

        logging.info("[Test] Porównywanie wyników z bazy SQLite i plików CSV...")

        engine = sqlalchemy.create_engine(f"sqlite:///{test_db_path}")
        table_name = f"data_{test_config['file_id']}"
        db_df = pd.DataFrame() # Zapewnij istnienie zmiennej
        try:
            db_df = pd.read_sql_table(table_name, engine, parse_dates=['TIMESTAMP'])
        except Exception as e:
            logging.error(f"[Test FAILED] Nie można odczytać danych z bazy: {e}")
        finally:
            # --- KLUCZOWA ZMIANA: Zawsze zamykaj połączenie z bazą ---
            engine.dispose()

        if db_df.empty:
            logging.error("[Test FAILED] Ramka danych z bazy jest pusta.")
            return

        csv_dfs = [pd.read_csv(p, parse_dates=['TIMESTAMP'], low_memory=False) for p in test_csv_dir.glob("**/*.csv")]
        if not csv_dfs:
            logging.error("[Test FAILED] Nie wygenerowano żadnych plików CSV.")
            return
        csv_df = pd.concat(csv_dfs, ignore_index=True)

        cols_to_drop_from_csv = ['group_id', 'interval', 'latitude', 'longitude', 'source_filepath', 'source_filename', 'original_row_index']
        csv_df.drop(columns=[c for c in cols_to_drop_from_csv if c in csv_df.columns], inplace=True, errors='ignore')
        
        shared_cols = sorted(list(set(db_df.columns) & set(csv_df.columns)))
        if not shared_cols or 'TIMESTAMP' not in shared_cols:
             logging.error("[Test FAILED] Brak wspólnych kolumn do porównania.")
             return

        db_df = db_df[shared_cols].sort_values(by='TIMESTAMP').reset_index(drop=True)
        csv_df = csv_df[shared_cols].sort_values(by='TIMESTAMP').reset_index(drop=True)

        for col in db_df.columns:
            if col != 'TIMESTAMP':
                try:
                    db_df[col] = pd.to_numeric(db_df[col])
                    csv_df[col] = pd.to_numeric(csv_df[col])
                except (ValueError, TypeError):
                    pass 

        try:
            pd.testing.assert_frame_equal(db_df, csv_df, check_dtype=False, atol=1e-5)
            logging.info("✅ [Test PASSED] Wyniki z bazy SQLite i plików CSV są identyczne.")
        except AssertionError as e:
            logging.error("❌ [Test FAILED] Wykryto rozbieżności między wynikami z bazy SQLite i plików CSV.")
            logging.error(f"Szczegóły błędu:\n{e}")

    logging.info("="*20 + " ZAKOŃCZONO TRYB TESTOWY " + "="*20)

def main():
    """Główna funkcja orkiestrująca."""
    parser = argparse.ArgumentParser(description="Uniwersalny skrypt do przetwarzania danych pomiarowych.")
    parser.add_argument("-i", "--input_dir", required=True, nargs='+', help="Katalogi wejściowe.")
    parser.add_argument("-o", "--output_dir", required=True, help="Katalog wyjściowy (dla CSV, logów, cache).")
    parser.add_argument("-fid", "--file_id", required=True, help="Identyfikator grupy do przetworzenia.")
    parser.add_argument("--db-path", help="Ścieżka do pliku bazy danych SQLite.")
    parser.add_argument("-j", "--jobs", type=int, default=os.cpu_count() or 1, help="Liczba procesów.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Poziom logowania.")
    parser.add_argument("--output-format", default="sqlite", choices=["sqlite", "csv", "both"], help="Format wyjściowy.")
    parser.add_argument("--no-cache", action='store_true', help="Wyłącza użycie cache.")
    parser.add_argument("--run-tests", action='store_true', help="Uruchamia tryb testowy.")
    parser.add_argument("--overwrite", action='store_true', help="Wymusza ponowne przetworzenie i nadpisanie istniejących danych w plikach CSV.")
    parser.add_argument("--csv-debug-good-only", action='store_true', help="W trybie CSV: zapisuj tylko dobre (monotoniczne) sektory, bez łączenia ze skorygowanymi.")
    
    args = parser.parse_args()
    setup_logging(args.log_level)
    setup_chronology_logger()
    
    # --- Configuration validation ---
    if args.output_format in ['sqlite', 'both'] and not args.db_path:
        parser.error("--db-path jest wymagany dla formatu 'sqlite' lub 'both'.")
    if args.run_tests and not args.db_path:
         parser.error("--db-path jest wymagany do uruchomienia testów.")

    group_config = FILE_ID_MERGE_GROUPS.get(args.file_id, {})
    if not group_config:
        logging.error(f"Nie znaleziono konfiguracji dla grupy '{args.file_id}'. Przerwanie pracy.")
        return

    group_config.update({
        'file_id': args.file_id,
        'output_dir': args.output_dir,
        'db_path': args.db_path,
        'input_dir': args.input_dir,
        'main_input_path': args.input_dir[0] if args.input_dir else None,
        'output_format': args.output_format,
        'jobs': args.jobs,
        'overwrite': args.overwrite
    })

    # Overwrite implies no-cache
    use_cache = not (args.no_cache or args.overwrite)
    processed_files_cache = load_cache() if use_cache else {}
    all_files = scan_for_files(args.input_dir, group_config.get('source_ids', []))
    files_to_process = [p for p in all_files if not is_file_in_cache(p, processed_files_cache)]
    
    if args.run_tests:
        _run_tests(group_config)
        return

    logging.info(f"{'='*20} Rozpoczęto przetwarzanie dla grupy: '{args.file_id}' {'='*20}")
    
    # Initialize DB if needed
    if args.output_format in ['sqlite', 'both']:
        initialize_database(Path(args.db_path))

    processed_files_cache = load_cache() if not args.no_cache else {}
    all_files = scan_for_files(args.input_dir, group_config.get('source_ids', []))
    files_to_process = [p for p in all_files if not is_file_in_cache(p, processed_files_cache)]

    if not files_to_process:
        logging.info("Brak nowych lub zmodyfikowanych plików do przetworzenia.")
        return
    logging.info(f"Znaleziono {len(files_to_process)} nowych/zmienionych plików.")

    binary_files = [p for p in files_to_process if identify_file_type(p) in ['TOB1', 'TOA5']]
    csv_files = [p for p in files_to_process if identify_file_type(p) == 'CSV']
    
    all_raw_results = []
    
    # Pipeline 1: Process binary files in parallel
    if binary_files:
        logging.info(f"Przetwarzanie {len(binary_files)} plików binarnych (TOB1/TOA5)...")
        binary_args = [(p, group_config) for p in binary_files]
        with multiprocessing.Pool(processes=args.jobs) as pool:
            binary_results = list(tqdm(pool.imap_unordered(process_binary_file, binary_args), total=len(binary_files), desc="Pliki binarne"))
        all_raw_results.extend([df for df in binary_results if df is not None and not df.empty])

    # # Pipeline 2: Process CSV files using an overlapping (sliding) window
    # if csv_files:
        # batch_size = args.csv_batch_size
        # overlap_size = args.csv_overlap_size

        # if overlap_size >= batch_size:
            # logging.error("Rozmiar zakładki musi być mniejszy niż rozmiar paczki. Przerwanie pracy.")
            # return

        # step_size = batch_size - overlap_size
        # csv_files.sort()
        
        # last_timestamp_kept = None
        
        # logging.info(f"Przetwarzanie {len(csv_files)} plików CSV w ruchomym oknie (rozmiar: {batch_size}, zakładka: {overlap_size})...")

        # for i in tqdm(range(0, len(csv_files), step_size), desc="Paczki plików CSV (ruchome okno)"):
            # batch_paths = csv_files[i : i + batch_size]
            # if not batch_paths:
                # continue

            # batch_dfs = [read_simple_csv_data(p) for p in batch_paths]
            # non_empty_dfs = [df for df in batch_dfs if df is not None and not df.empty]
            
            # if not non_empty_dfs:
                # continue

            # batch_df = pd.concat(non_empty_dfs, ignore_index=True)
            # if batch_df.empty:
                # continue

            # # KROK KRYTYCZNY: Usuń duplikaty oparte na całym wierszu PRZED korektą chronologii
            # initial_rows = len(batch_df)
            # batch_df.drop_duplicates(inplace=True)
            # rows_removed = initial_rows - len(batch_df)
            # if rows_removed > 0:
                # logging.info(f"Usunięto {rows_removed} zduplikowanych wierszy w paczce CSV od pliku {i+1}.")

            # # Zastosuj korektę chronologii na oczyszczonej paczce danych
            # known_interval = group_config.get('interval')
            # corrected_batch = correct_and_report_chronology(batch_df, f"Partia CSV od pliku {i+1}", known_interval)

            # if not corrected_batch.empty:
                # if last_timestamp_kept is not None:
                    # data_to_keep = corrected_batch[corrected_batch['TIMESTAMP'] > last_timestamp_kept]
                # else:
                    # data_to_keep = corrected_batch

                # if not data_to_keep.empty:
                    # last_timestamp_kept = data_to_keep['TIMESTAMP'].iloc[-1]
                    # all_raw_results.append(data_to_keep)
    
    # # Pipeline 2: Process ALL CSV files at once to ensure full context for chronology correction
    # if csv_files:
        # logging.info(f"Wczytywanie wszystkich {len(csv_files)} plików CSV do pamięci...")
        
        # all_csv_dfs = [read_simple_csv_data(p) for p in tqdm(csv_files, desc="Wczytywanie plików CSV")]
        # non_empty_dfs = [df for df in all_csv_dfs if df is not None and not df.empty]
        
        # if non_empty_dfs:
            # batch_df = pd.concat(non_empty_dfs, ignore_index=True)
            
            # # KROK KRYTYCZNY: Poprawiona logika usuwania duplikatów
            # if 'TIMESTAMP' in batch_df.columns:
                # initial_rows = len(batch_df)
                # # Usuń duplikaty na podstawie wszystkich kolumn POZA TIMESTAMP
                # columns_to_check = [col for col in batch_df.columns if col != 'TIMESTAMP']
                # if columns_to_check: # Upewnij się, że są inne kolumny do sprawdzenia
                    # batch_df.drop_duplicates(subset=columns_to_check, keep='first', inplace=True)
                
                # rows_removed = initial_rows - len(batch_df)
                # if rows_removed > 0:
                    # logging.info(f"Usunięto {rows_removed} zduplikowanych wierszy (na podstawie wartości pomiarowych).")

            # # Uruchom korektę chronologii na kompletnym i ODCZYSZCZONYM zbiorze danych
            # known_interval = group_config.get('interval')
            # corrected_batch = correct_and_report_chronology(batch_df, f"Wszystkie pliki CSV", known_interval)
            # all_raw_results.append(corrected_batch)
    
    # Pipeline 2: Process ALL CSV files at once, sorted by modification time
    if csv_files:
        # --- DEBUG: Zapisz listę plików PRZED sortowaniem ---
        try:
            debug_before_path = BASE_DIR / "debug_files_before_sort.txt"
            logging.info(f"DEBUG: Zapisywanie listy plików PRZED sortowaniem do: {debug_before_path.name}")
            with open(debug_before_path, 'w', encoding='utf-8') as f:
                for p in csv_files:
                    f.write(f"{p.name}\n")
        except Exception as e:
            logging.error(f"DEBUG: Nie udało się zapisać pliku listy przed sortowaniem: {e}")
        # --- KONIEC DEBUG ---
        unique_files = []
        seen_names = set() # A set for fast lookups of filenames we've already added.

        for p in csv_files:
            # Check if a file with this name has already been seen.
            if p.name not in seen_names:
                # If not, add the full path object to our unique list.
                unique_files.append(p)
                # And record the name so we can ignore its future duplicates.
                seen_names.add(p.name)
        # KROK KRYTYCZNY: Sortuj pliki CSV według czasu ich modyfikacji
        # logging.info(f"Sortowanie {len(csv_files)} plików CSV według czasu modyfikacji...")
        
        # csv_files.sort(key=lambda p: p.stat().st_mtime)
        # Wersja nowa (sortowanie alfabetyczne po nazwie pliku)
        logging.info(f"Sortowanie {len(unique_files)} plików CSV alfabetycznie (po samej nazwie, bez ścieżki, case-insensitive)...")
        unique_files.sort(key=lambda p: int(re.sub(r'[^0-9]', '', p.name)))
        
        # --- DEBUG: Zapisz listę plików PO sortowaniu ---
        try:
            debug_after_path = BASE_DIR / "debug_files_after_sort.txt"
            logging.info(f"DEBUG: Zapisywanie listy plików PO sortowaniu do: {debug_after_path.name}")
            with open(debug_after_path, 'w', encoding='utf-8') as f:
                f.write("filename;fullpath;modified_utc;size_mb\n")
                for p in unique_files:
                    try:
                        st = p.stat()
                        modified_utc = datetime.utcfromtimestamp(st.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                        size_mb = round(st.st_size / (1024*1024), 3)
                    except Exception:
                        modified_utc = "N/A"
                        size_mb = "N/A"
                    f.write(f"{p.name};{str(p.resolve())};{modified_utc};{size_mb}\n")
        except Exception as e:
            logging.error(f"DEBUG: Nie udało się zapisać pliku listy po sortowaniu: {e}")
        # --- KONIEC DEBUG ---
        
        logging.info(f"Wczytywanie wszystkich {len(unique_files)} posortowanych plików CSV do pamięci...")
        
        # Serial loading of all CSV files
        per_file_dfs = []
        for p in tqdm(unique_files, desc="Wczytywanie plików CSV"):
            df = read_simple_csv_data(p)
            if df is None or df.empty:
                continue
            per_file_dfs.append(df)
        
        # # Parallel loading of all CSV files
        # logging.info(f"Wczytywanie wszystkich {len(csv_files)} plików CSV do pamięci (równolegle)...")
        # all_csv_dfs = []
        # with multiprocessing.Pool(processes=args.jobs) as pool:
            # # Użyj imap_unordered dla wydajności i owiń w tqdm dla paska postępu
            # results_iterator = pool.imap_unordered(read_simple_csv_data, csv_files)
            
            # for df in tqdm(results_iterator, total=len(csv_files), desc="Wczytywanie plików CSV"):
                # all_csv_dfs.append(df)

        # non_empty_dfs = [df for df in all_csv_dfs if df is not None and not df.empty]
        
        if per_file_dfs:
            batch_df = pd.concat(per_file_dfs, ignore_index=True)
            
            if 'TIMESTAMP' in batch_df.columns:
                # Sortuj po czasie, aby 'keep="first"' zachował najwcześniejszy rekord w ramach grupy duplikatów
                batch_df = batch_df.sort_values('TIMESTAMP', kind='stable').reset_index(drop=True)
                initial_rows = len(batch_df)
                # Ważne: wyłączamy TIMESTAMP, metadane, RECORD oraz wszystkie kolumny *_flag z deduplikacji
                metadata_cols = ['source_filename', 'original_row_index', 'source_filepath'] # 'TIMESTAMP', 
                if 'RECORD' in batch_df.columns:
                    metadata_cols.append('RECORD')
                metadata_cols.extend([c for c in batch_df.columns if str(c).endswith('_flag')])
                cols_to_check = [col for col in batch_df.columns if col not in metadata_cols]
                
                if cols_to_check:
                    df_for_dedup = batch_df.copy()
                    # Lekka normalizacja wartości tekstowych, aby wyeliminować różnice białych znaków
                    object_cols = [c for c in df_for_dedup[cols_to_check].select_dtypes(include='object').columns]
                    if object_cols:
                        df_for_dedup[object_cols] = df_for_dedup[object_cols].apply(lambda s: s.astype(str).str.strip())
                    numeric_cols_to_round = [col for col in df_for_dedup.select_dtypes(include=np.number).columns if col not in metadata_cols]
                    df_for_dedup[numeric_cols_to_round] = df_for_dedup[numeric_cols_to_round].round(4)
                    # Diagnostyka: policz potencjalne duplikaty wg podzbioru kolumn
                    try:
                        potential_dups = int(df_for_dedup.duplicated(subset=cols_to_check, keep='first').sum())
                        logging.info(f"[DEDUP] Wykryte potencjalne duplikaty (wg kolumn pomiarowych): {potential_dups}")
                    except Exception:
                        pass
                    indices_to_keep = df_for_dedup.drop_duplicates(subset=cols_to_check, keep='first').index
                    batch_df = batch_df.loc[indices_to_keep]
                
                rows_removed = initial_rows - len(batch_df)
                if rows_removed > 0:
                    logging.info(f"Usunięto {rows_removed} zduplikowanych wierszy (na podstawie wartości pomiarowych, z uwzględnieniem precyzji).")
                    
                # --- DEBUG: Zapisz ramkę danych po deduplikacji ---
                try:
                    debug_path_after_dedup = Path(group_config.get('output_dir')) / f"debug_after_deduplication_{group_config['file_id']}.csv"
                    debug_path_after_dedup.parent.mkdir(parents=True, exist_ok=True)
                    logging.info(f"DEBUG: Zapisywanie stanu danych po deduplikacji do: {debug_path_after_dedup.name}")
                    batch_df.to_csv(debug_path_after_dedup, index=False)
                except Exception as e:
                    logging.error(f"DEBUG: Nie udało się zapisać pliku po deduplikacji: {e}")
                # --- KONIEC DEBUG ---

            # 1) Uruchom korektę na całym zbiorze – funkcja ustawia chronology_tag (0/1)
            known_interval = group_config.get('interval')
            corrected_batch = correct_and_report_chronology(batch_df, f"Wszystkie pliki CSV", known_interval, big_forward_hours=3.0)

            # 2) Podziel na dobre i skorygowane wg chronology_tag
            if 'chronology_tag' in corrected_batch.columns:
                tag_series = pd.to_numeric(corrected_batch['chronology_tag'], errors='coerce').fillna(0).astype(int)
            else:
                tag_series = pd.Series(0, index=corrected_batch.index, dtype=int)
            good_original = corrected_batch.loc[tag_series == 0].copy()
            corrected_only = corrected_batch.loc[tag_series == 1].copy()
            if not corrected_only.empty:
                # Ustaw flagi = 5 na wszystkich *_flag
                flag_cols = [c for c in corrected_only.columns if c.endswith('_flag')]
                if flag_cols:
                    corrected_only.loc[:, flag_cols] = 5

            # 3) Złóż wynik: w trybie debug zwróć tylko dobre; inaczej dobre + skorygowane (z priorytetem)
            combined = []
            if not good_original.empty:
                combined.append(good_original)
            if not args.csv_debug_good_only and not corrected_only.empty:
                combined.append(corrected_only)
            # Gdy debug-good-only i brak dobrych: zwróć pusty; inaczej fallback do corrected_batch
            final_csv_df = (pd.concat(combined, ignore_index=True)
                            if combined else (pd.DataFrame() if args.csv_debug_good_only else corrected_batch))
            if 'TIMESTAMP' in final_csv_df.columns and 'chronology_tag' in final_csv_df.columns and not final_csv_df.empty:
                try:
                    # priority: 0 (good) first, 1 (corrected) later
                    final_csv_df['_priority'] = pd.to_numeric(final_csv_df['chronology_tag'], errors='coerce').fillna(1).astype(int)
                    final_csv_df = final_csv_df.sort_values(['TIMESTAMP','_priority'], kind='stable')
                    # keep the first occurrence per TIMESTAMP (good wins)
                    final_csv_df = final_csv_df.drop_duplicates(subset=['TIMESTAMP'], keep='first')
                    final_csv_df.drop(columns=['_priority'], inplace=True, errors='ignore')
                except Exception as e:
                    logging.warning(f"[CHRONOLOGY] Priorytetyzacja sektorów nie powiodła się: {e}")

            # 5) Porządkowanie kolumn pomocniczych przed dalszym potokiem
            for helper_col in ['chronology_needs_correction']:
                if helper_col in final_csv_df.columns:
                    final_csv_df.drop(columns=[helper_col], inplace=True, errors='ignore')

            all_raw_results.append(final_csv_df)
    
    # # Pipeline 2: Process ALL CSV files using the new two-stage logic
    # if csv_files:
        # logging.info(f"Sortowanie {len(csv_files)} plików CSV według czasu modyfikacji...")
        # csv_files.sort(key=lambda p: p.stat().st_mtime)
        
        # # ETAP 1: Walidacja plików i wstępne flagowanie
        # logging.info("ETAP 1: Walidacja poszczególnych plików CSV...")
        # all_csv_dfs = []
        # for file_path in tqdm(csv_files, desc="Walidacja plików CSV"):
            # df = read_simple_csv_data(file_path)
            # if df.empty:
                # continue
            
            # # Sprawdź, czy plik jest spójny
            # if _validate_csv_file(df, file_path, group_config):
                # df['chronology_flag'] = 0 # Oznacz jako poprawne
            # else:
                # df['chronology_flag'] = np.nan # Oznacz jako do sprawdzenia/korekty
            
            # all_csv_dfs.append(df)
        
        # # ETAP 2: Łączenie, deduplikacja i korekta
        # logging.info("ETAP 2: Łączenie, deduplikacja i korekta chronologii...")
        # if all_csv_dfs:
            # batch_df = pd.concat(all_csv_dfs, ignore_index=True)
            
            # # Deduplikacja na całości (zgodnie z poprzednimi ustaleniami)
            # if 'TIMESTAMP' in batch_df.columns:
                # initial_rows = len(batch_df)
                # metadata_cols = ['TIMESTAMP', 'source_filename', 'original_row_index', 'source_filepath']
                # cols_to_check = [col for col in batch_df.columns if col not in metadata_cols]
                # if cols_to_check:
                    # batch_df.drop_duplicates(subset=cols_to_check, keep='first', inplace=True)
                # rows_removed = initial_rows - len(batch_df)
                # if rows_removed > 0:
                    # logging.info(f"Usunięto {rows_removed} zduplikowanych wierszy.")

            # # Uruchom korektę chronologii na kompletnym zbiorze danych
            # known_interval = group_config.get('interval')
            # corrected_batch = correct_and_report_chronology(batch_df, f"Wszystkie pliki CSV", known_interval)
            # all_raw_results.append(corrected_batch)
            # all_raw_results.append(batch_df)
    # Final processing and saving
    if all_raw_results:
        lock = multiprocessing.Manager().Lock()
        process_and_save_data(all_raw_results, group_config, lock)

    if use_cache:
        update_cache(files_to_process, processed_files_cache)
        save_cache(processed_files_cache)
        logging.info("Cache został zaktualizowany.")

    logging.info(f"{'='*20} Zakończono przetwarzanie dla grupy: '{args.file_id}' {'='*20}\n")


if __name__ == '__main__':
    if os.name in ['nt', 'posix']:
        multiprocessing.set_start_method('spawn', force=True)
    main()