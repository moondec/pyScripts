# -*- coding: utf-8 -*-

"""
================================================================================
            Centralny Plik Konfiguracyjny
================================================================================

Opis:
    Ten plik zawiera wszystkie słowniki konfiguracyjne używane przez skrypt
    do przetwarzania danych. Centralizacja konfiguracji ułatwia zarządzanie
    regułami i zapewnia spójność między różnymi trybami działania.

Struktura:
    1. STATION_COORDINATES: Współrzędne geograficzne stacji.
    2. FILE_ID_MERGE_GROUPS: Grupowanie plików źródłowych.
    3. TIMEZONE_CORRECTIONS: Reguły korekty stref czasowych.
    4. MANUAL_TIME_SHIFTS: Ręczne przesunięcia czasowe.
    5. CALIBRATION_RULES_BY_STATION: Reguły kalibracji sensorów.
    6. STATION_MAPPING_FOR_CALIBRATION: Mapowanie grup na reguły kalibracji.
    7. QUALITY_FLAGS: Reguły flagowania danych.
    8. STATION_MAPPING_FOR_QC: Mapowanie grup na reguły flagowania.
    9. COLUMN_MAPPING_RULES: Reguły zmiany nazw kolumn.
    10. STATION_MAPPING_FOR_COLUMNS: Mapowanie grup na reguły zmiany nazw.
    11. VALUE_RANGE_FLAGS: Dopuszczalne zakresy wartości dla zmiennych.
    12. MANUAL_VALUE_OVERRIDES: Ręczne nadpisywanie wartości.
    13. STATION_MAPPING_FOR_OVERRIDES: Mapowanie grup na reguły nadpisów.
--------------------------------------------------------------------------------
"""
import json  # noqa: F401
from datetime import datetime, timedelta
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
    '1161CV',
    'C038CV',
    'C160CV',
    'CSERIAL_State_1',
    'CSERIAL_State_2',
    'CSERIAL_State_3',
    'CSERIAL_State_4',
    'Volt_12_V',
    'Voltage_13_mV',
    'LWin_1_2_W',
    'LWout_1_2_W',
    'Res_100_Ohms',
    'Resistance_12_Ohms'
    # Dodaj tutaj kolejne nazwy kolumn, które chcesz pominąć
]

# 2. Lista grup, gdzie dane są usupełniane z plików .MAT (Surowe dane niedostępne)
GROUP_IDS_FOR_MATLAB_FILL = ['TL1_MET_30', 'TL1_RAD_30', 'TL1_SOIL_30', 'TL1_RAD_1', 'TL2_MET_1m', 'TL2_MET_30m', 'RZ_CSI_30', 'RZ_WET_30m']
# GROUP_IDS_FOR_MATLAB_FILL = ['RZ_CSI_30', 'RZ_WET_30m']

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
    'TL1_MET_30': {'lat': 53.634836, 'lon': 18.257957},      # Tlen1
    'TL1_MET_1': {'lat': 53.634836, 'lon': 18.257957},       # Tlen1
    'TL1_RAD_30': {'lat': 53.634836, 'lon': 18.257957},      # Tlen1
    'TL1_RAD_1': {'lat': 53.634836, 'lon': 18.257957},       # Tlen1
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
    # 'TL1_RAD_30': { 'source_ids': [ 'TR_30min' ], 'interval': '30min' },
    # 'TL1_RAD_1': { 'source_ids': [ 'TR_1min' ], 'interval': '1min' },
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
    # 'TL2_MET_1_csi': { 'source_ids': [ 'Tlen_2_Soil_moist_1min', 'Tlen2_biomet_Meteo_1min' ], 'interval': '1min' },
    'TL2_MET_1_dT': { 'source_ids': [ 'pom1m_'], 'interval': '1min' },
    # 'TL2_MET_1m': { 'source_ids': [ 'Tlen_2_Soil_moist_1min'], 'interval': '1min' },
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
    # 'TL2_MET_1m' : 'TL2_TZSHIFT2',

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
        {"start": "2018-06-05 09:29:00", "end": "2018-06-07 13:02:00", "offset_hours": -1.18 },
        # {"start": "2018-10-03 00:00:00", "end": "2018-12-22 00:00:00", "offset_hours": 0.5 },
        {"start": "2019-07-05 06:01:00", "end": "2019-07-10 12:32:00", "offset_hours": 2.25 },
        {"start": "2019-09-01 14:45:00", "end": "2019-09-01 18:12:00", "offset_hours": 1.28  },
        {"start": "2020-01-07 23:02:00", "end": "2020-01-14 12:10:00", "offset_hours": -1.03  },
        {"start": "2020-01-14 12:11:00", "end": "2020-01-18 04:40:00", "offset_hours": 29.58 },
        {"start": "2021-06-29 03:17:00", "end": "2021-06-30 08:54:00", "offset_hours": 556.77 },
        {"start": "2021-07-23 14:41:00", "end": "2021-07-25 06:58:00", "offset_hours": -1 },
        {"start": "2021-08-01 04:12:00", "end": "2021-08-04 16:23:00", "offset_hours": 6 },
        {"start": "2021-08-07 19:30:00", "end": "2021-08-08 21:04:00", "offset_hours":  110.43 },
        {"start": "2021-09-11 05:38:00", "end": "2021-09-11 15:51:00", "offset_hours": 246.12 },
        {"start": "2021-09-21 22:59:00", "end": "2021-09-24 19:41:00", "offset_hours": -1 },
        {"start": "2021-10-03 20:03:00", "end": "2021-10-03 22:04:00", "offset_hours": 39.23 }, #
        {"start": "2021-11-12 01:11:00", "end": "2021-11-12 01:41:00", "offset_hours": 104.62 },
        {"start": "2021-11-10 10:58:00", "end": "2021-11-12 01:10:00", "offset_hours": -10.0 },
        {"start": "2021-11-24 11:23:00", "end": "2021-11-25 05:49:00", "offset_hours":  28.52  },
        {"start": "2021-12-06 12:16:00", "end": "2021-12-08 07:56:00", "offset_hours":  25.63  },
        {"start": "2022-04-05 16:24:00", "end": "2022-06-03 10:34:00", "offset_hours":  -1  },
        {"start": "2022-06-03 10:35:00", "end": "2022-06-12 13:09:00", "offset_hours":  20.27  },
        {"start": "2022-06-13 10:25:00", "end": "2022-10-06 09:00:00", "offset_hours":  -1  },
        {"start": "2023-02-12 10:13:00", "end": "2023-02-12 11:12:00", "offset_hours":  6.47  },
        # {'start': '2024-09-10T08:36:00', 'end': '2024-09-10T12:20:00', "offset_hours": 333.85 },
        # {'start': '2024-11-10 03:20:00', 'end': '2024-11-10 03:27:00', "offset_hours": 1507.45 },
        # {'start': '2024-11-19 13:41:00', 'end': '2024-11-20 17:01:00', "offset_hours": 50.72 },
        # {'start': '2024-12-10T04:09:00', 'end': '2024-12-10T05:08:00', "offset_hours": 9.42 },
        {'start': '2025-01-08 02:47:00', 'end': '2025-01-08 16:08:00', "offset_hours": 41.10 },
        # {'start': '2025-05-15 05:26:00', 'end': '2025-05-20 08:53:00', "offset_hours": 28.38 },
    ],
    'ME_DOWN_MET_30min' : 'ME_MTSHIFT', 'ME_DOWN_MET_1min' : 'ME_MTSHIFT',
    'ME_Rain_down' : 'ME_MTSHIFT', 'ME_CalPlates' : 'ME_MTSHIFT',
    # 'ME_TOP_MET_30min' : 'ME_MTSHIFT', 'ME_DOWN_MET_30min' : 'ME_MTSHIFT', 'ME_Rain_down' : 'ME_MTSHIFT',
    # 'ME_DOWN_MET_1min' : 'ME_MTSHIFT', 'ME_DOWN_MET_30min' : 'ME_MTSHIFT', 'ME_Rain_top' : 'ME_MTSHIFT',
    # 'ME_CalPlates' : 'ME_MTSHIFT', 'ME_MET_10m' : 'ME_MTSHIFT',

    'TL2_MTSHIFT': [
        { "start": "2014-10-26 02:00:00", "end": "2015-03-19 11:49", "offset_hours": -1},
        # { "start": "2015-03-19 11:50:00", "end": "2051-09-16", "offset_hours": 0},
    ],
    'TL2_MET_30m' : 'TL2_MTSHIFT', # 'TL2_MET_1m' : 'TL2_MTSHIFT',
    
    'TL2dt_MTSHIFT': [
        # { "start": "2021-01-04 22:46:00", "end": "2021-04-28 13:44:00", "offset_hours": 1189.03},
    ],
    'TL2_CalPlates_dT' : 'TL2dt_MTSHIFT', 'TL2_MET_1_dT' : 'TL2dt_MTSHIFT', 'TL2_MET_30_dT' : 'TL2dt_MTSHIFT',
    
    'TL1_MTSHIFT': [
        # { "start": "2013-01-01 00:00:00", "end": "2055-08-12 14:00", "offset_hours": -2},
        { "start": "2015-08-12 12:05:00", "end": "2016-01-11 14:18:00", "offset_hours": 1},
        # { "start": "2016-01-02 01:00:00", "end": "2055-03-19 11:49", "offset_hours": -2},
    ],
    'TL1_RAD_30' : 'TL1_MTSHIFT', 'TL1_RAD_1' : 'TL1_MTSHIFT',

    'TL1_dT_MTSHIFT': [
        # { "start": "2021-11-03 01:00:00", "end": "2055-01-26 21:00", "offset_hours": 0},
        # { "start": "2021-01-01 00:00:00", "end": "2021-02-20 00:00", "offset_hours": -1},
        # { "start": "2018-08-01 22:46:00", "end": "2018-08-22 13:44:00", "offset_hours": -14.8166666667},
        # { "start": "2018-09-18 00:00:00", "end": "2018-11-18 00:00", "offset_hours": 0.5},
        # { "start": "2021-10-25 00:00:00", "end": "2022-04-08 00:00", "offset_hours": 1},
        # { "start": "2022-07-02 12:59:00", "end": "2051-09-23 11:49", "offset_hours": -1}, # godzina bez - znaczenia braki w danych
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
        ],
    },
    'MEZYK_DOWN_CAL': {
        # Wszystkie reguły dla JEDNEJ kolumny muszą być w JEDNEJ liście
        'PPFD_BC_IN_1_1_1': [
            {'start': '2018-11-16 19:09:00', 'end': '2058-11-13 23:00:00', 'multiplier': 3397.547, 'addend': 0, 'reason': 'LQA3027, (data in umol/m2/s1)'}, # '2018-11-13 23:00:00' -> '2018-11-16 19:09:00'
            {'start': '2019-09-01 19:30:00', 'end': '2019-09-13 06:00:00', 'multiplier': 1, 'addend': -230, 'reason': 'LQA3028 - korekta'},
        ],
        'PPFD_BC_IN_1_1_2': [
            {'start': '2018-08-08 13:30:00', 'end': '2018-11-17 12:00:00', 'multiplier': 1, 'addend': -650, 'reason': 'LQA3028 - stara korekta'},
            {'start': '2018-11-16 19:09:00', 'end': '2058-11-13 23:00:00', 'multiplier': 3288.716, 'addend': 0, 'reason': 'LQA3028, (data in umol/m2/s1)'},
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
        'PPFD_IN_1_1_1': [
            {'start': '2018-01-01 00:00:00', 'end': '2018-11-15 12:00:00', 'multiplier': 199.601, 'addend': 0, 'reason': 'CNR4 (data in W/m2)'},
        ],
        'PPFD_OUT_1_1_1': [
            {'start': '2018-01-01 00:00:00', 'end': '2018-11-15 12:00:00', 'multiplier': 201.613, 'addend': 0, 'reason': 'CNR4 (data in W/m2)'},
        ],
        'SW_IN_1_1_1': [
            {'start': '2018-08-09 04:30:00', 'end': '2018-11-15 12:00:00', 'multiplier': 74.85, 'addend': 0, 'reason': 'CNR4 (data in W/m2)'},
        ],
        'SW_OUT_1_1_1': [
            {'start': '2018-08-09 04:30:00', 'end': '2018-11-15 12:00:00', 'multiplier': 81.5, 'addend': 0, 'reason': 'CNR4 (data in W/m2)'},
        ],
        'LW_IN_1_1_1': [
        {'start': '2018-08-09 04:30:00', 'end': '2018-11-15 12:00:00', 'type': 'formula', 'expression': 'LW_IN_1_1_1 * @multiplier + 5.67e-8 * ((TA_1_1_2 + 273.15)**4)','constants': {'multiplier': 86.505}}
        ],
        'LW_OUT_1_1_1': [
        {'start': '2018-08-09 04:30:00', 'end': '2018-11-15 12:00:00', 'type': 'formula', 'expression': 'LW_OUT_1_1_1 * @multiplier + 5.67e-8 * ((TA_1_1_2 + 273.15)**4)','constants': {'multiplier': 88.810}}
        ],
    },
    ## Tlen1 old site added by Klaudia- 19.07.2025
    'TL1_CAL': {
	#Radiation fluxes- NR01 2014-06-17 08:00:00
      # RAD_30$SWin_1_1_1_Avg[ir] <- (Rs_old[ir] / 74.074) * 86.356
      # RAD_30$LWin_1_1_1_Avg[ir] <- (-SWout_old[ir] / 69.93) * 90.744
      # RAD_30$LWout_1_1_1_Avg[ir] <- (LWout_old[ir] / 91.743) * 89.445
      # RAD_30$SWout_1_1_1_Avg[ir] <- (LWin_old[ir] / 95.238) * 66.05
        '_SWAP_RADIATION': [
            {
                'start': '2000-01-01 00:00:00', 
                'end': '2014-07-09 14:30:00',
                'type': 'formula_swap',
                # Definicja zamiany kanałów: nowa_nazwa: stara_nazwa_lub_formuła
                'swaps': {
                    'SW_IN_1_1_1_new': '(SW_IN_1_1_1 / 74.074) * 86.356',
                    # 'LW_IN_1_1_1_new': '(-SW_OUT_1_1_1 / 69.93) * 90.744 )',
                    # 'LW_OUT_1_1_1_new': '(LW_OUT_1_1_1/ 91.743) * 89.445 )',
                    'LW_IN_1_1_1_new': '(-SW_OUT_1_1_1 / 69.93) * 90.744 + (5.67e-8 * ((TA_1_1_2 + 273.15)**4))',
                    'LW_OUT_1_1_1_new': '(LW_OUT_1_1_1/ 91.743) * 89.445 + (5.67e-8 * ((TA_1_1_2 + 273.15)**4))',
                    'SW_OUT_1_1_1_new': '(LW_IN_1_1_1 / 95.238) * 66.05'
                },
                # Definicja ostatecznego mapowania nazw
                'final_mapping': {
                    'SW_IN_1_1_1': 'SW_IN_1_1_1_new',
                    'LW_IN_1_1_1': 'LW_IN_1_1_1_new',
                    'LW_OUT_1_1_1': 'LW_OUT_1_1_1_new',
                    'SW_OUT_1_1_1': 'SW_OUT_1_1_1_new'
                }
            }
        ],
        'LW_IN_1_1_1': [
        {'start': '2014-07-09 14:31:00', 'end': '2118-11-13 09:30:00', 'type': 'formula', 'expression': 'LW_IN_1_1_1 * @multiplier + 5.67e-8 * ((TA_1_1_2 + 273.15)**4)','constants': {'multiplier': 1}, 'reason': 'NR01 (data in W/m2)'}
        ],
        'LW_OUT_1_1_1': [
        {'start': '2014-07-09 14:31:00', 'end': '2118-11-13 09:30:00', 'type': 'formula', 'expression': 'LW_OUT_1_1_1 * @multiplier + 5.67e-8 * ((TA_1_1_2 + 273.15)**4)','constants': {'multiplier': 1}, 'reason': 'NR01 (data in W/m2)'}
        ],
    },
    'TL1_SOIL_CAL': {
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
	# Radiation measurements
    'TL1adT_CAL': {
        'SW_IN_2_1_1' : [
            {'start':  '2018-08-02 15:30:00', 'end': '2018-11-13 09:30:00', 'multiplier': 77.101, 'addend': 8, 'reason': 'CNR4 (data in W/m2)'},
            {'start':  '2018-11-13 09:31:00', 'end': '2048-11-13 09:30:00', 'multiplier': 1, 'addend': 8, 'reason': 'CNR4 (data in W/m2)'},
            # {'start':  '2018-11-13 09:31:00', 'end': '2018-11-13 09:30:00', 'multiplier': 1, 'addend': 8, 'reason': 'CNR4 (data in W/m2)'},
        ],
        'SW_OUT_2_1_1' : [
            {'start':  '2018-08-02 15:30:00', 'end': '2018-11-13 09:30:00', 'multiplier': 70.423, 'addend': 8, 'reason': 'CNR4 (data in W/m2)'},
            {'start':  '2018-11-13 09:31:00', 'end': '2048-11-13 09:30:00', 'multiplier': 1, 'addend': 8, 'reason': 'CNR4 (data in W/m2)'},
        ],
        'LW_IN_2_1_1': [
        {'start': '2018-08-02 15:30:00', 'end': '2018-11-13 09:30:00', 'type': 'formula', 'expression': 'LW_IN_2_1_1 * @multiplier + 5.67e-8 * ((TA_2_1_2 + 273.15)**4)', 'constants': {'multiplier': 70.522}, 'reason': 'NR01 (data in W/m2)'}
        ],
        'LW_OUT_2_1_1': [
        {'start': '2018-08-02 15:30:00', 'end': '2018-11-13 09:30:00', 'type': 'formula', 'expression': 'LW_OUT_2_1_1 * @multiplier + 5.67e-8 * ((TA_2_1_2 + 273.15)**4)', 'constants': {'multiplier': 68.446}, 'reason': 'NR01 (data in W/m2)'}
        ],
        'PPFD_IN_2_1_1' : [
        #    {'start':  '2018-08-02 15:30:00', 'end': '2019-08-06 09:30:00', 'multiplier': 210.970, 'addend': 0, 'reason': 'PQS1 (data in umol/m2/s)'},
             {'start': '2018-08-02 15:30:00', 'end': '2018-11-13 09:30:00', 'multiplier': 210.970, 'addend': 0, 'reason': 'PQS1 (data in umol/m2/s)'},
		],
        'PPFD_OUT_2_1_1' : [
        #    {'start':  '2018-08-02 15:30:00', 'end': '2019-08-06 09:30:00', 'multiplier': 200.4008, 'addend': 0, 'reason': 'PQ1 (data in umol/m2/s)'},
            {'start':  '2018-08-02 15:30:00', 'end': '2018-11-13 09:30:00', 'multiplier': 200.4008, 'addend': 0, 'reason': 'PQ1 (data in umol/m2/s)'},
		],
        'PPFD_BC_IN_2_1_1': [
            #{'start': '2014-07-08 09:30:00', 'end': '2019-08-06 09:30:00', 'multiplier': 2685, 'addend': 0, 'reason': 'LQA3016, (data in umol/m2/s1)'},
            {'start': '2018-08-02 15:30:00', 'end': '2018-11-13 09:30:00', 'multiplier': 2763.4, 'addend': 0, 'reason': 'LQA3016, (data in umol/m2/s1)'},
		],
        'PPFD_BC_IN_2_1_2': [
        # {'start': '2014-07-08 09:30:00', 'end': '2019-08-06 09:30:00', 'multiplier': 3618.75, 'addend': 0, 'reason': 'LQA3013, (data in umol/m2/s1)'},
        {'start': '2018-11-13 09:30:00', 'end': '2019-08-14 12:30:00', 'multiplier': 3724.4, 'addend': 0, 'reason': 'LQA3016, (data in umol/m2/s1)'},
		],
        'PPFD_BC_IN_2_1_2': [
            #{'start': '2014-07-08 09:30:00', 'end': '2019-08-06 09:30:00', 'multiplier': 3618.75, 'addend': 0, 'reason': 'LQA3013, (data in umol/m2/s1)'},
        {'start': '2018-11-3 09:30:00', 'end': '2019-08-14 12:30:00', 'multiplier': 3724.4, 'addend': 0, 'reason': 'LQA3016, (data in umol/m2/s1)'},
		],
		    },
# Soil Heat flux plates
# Soil temp. and moisture
# Air temp. and moisture

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
        'SW_IN_1_1_2' : [
            {'start':  '2014-07-08 09:30:00', 'end': '2018-11-16 12:30:00', 'multiplier': 69.638, 'addend': 0, 'reason': 'NR01 (data in W/m2)'},
        ],
        'SW_OUT_1_1_2' : [
            {'start':  '2014-07-08 09:30:00', 'end': '2018-11-16 12:30:00', 'multiplier': 76.394, 'addend': 0, 'reason': 'NR01 (data in W/m2)'},
        ],
        'LW_IN_1_1_2': [
        {'start': '2014-07-08 09:30:00', 'end': '2018-11-16 12:30:00', 'type': 'formula', 'expression': 'LW_IN_1_1_2 * @multiplier + 5.67e-8 * ((TA_2_1_2 + 273.15)**4)','constants': {'multiplier': 70.671}, 'reason': 'NR01 (data in W/m2)'}
        ],
        'LW_OUT_1_1_2': [
        {'start': '2014-07-08 09:30:00', 'end': '2018-11-16 12:30:00', 'type': 'formula', 'expression': 'LW_OUT_1_1_2 * @multiplier + 5.67e-8 * ((TA_2_1_2 + 273.15)**4)','constants': {'multiplier': 72.886}, 'reason': 'NR01 (data in W/m2)'}
        ],
        'PPFD_IN_1_1_2' : [
            {'start':  '2014-07-08 09:30:00', 'end': '2018-11-16 12:30:00', 'multiplier': 213.675, 'addend': 0, 'reason': 'PQ1 (data in umol/m2/s)'},
        ],
        'PPFD_OUT_1_1_2' : [
            {'start':  '2014-07-08 09:30:00', 'end': '2018-11-16 12:30:00', 'multiplier': 208.768, 'addend': 0, 'reason': 'PQ1 (data in umol/m2/s)'},
        ],
        'PPFD_BC_IN_1_1_1': [
            {'start': '2018-08-01 13:09:00', 'end': '2019-08-14 10:00:00', 'multiplier': 3101.25, 'addend': 0, 'reason': 'LQA3038, (data in umol/m2/s1)'},
        ],
        'PPFD_BC_IN_1_1_2': [
            {'start': '2018-08-01 13:09:00', 'end': '2019-08-14 10:00:00', 'multiplier': 3491.25, 'addend': 0, 'reason': 'LQA3014, (data in umol/m2/s1)'},
        ],
    },
    'SA_CAL': {
        'SWC_1_1_1' : [
            {'start':  '2014-07-08 09:30:00', 'end': '2118-11-16 12:30:00', 'multiplier': 100, 'addend': 0, 'reason': 'SWC (data in %)'},
        ],
        'SWC_2_1_1' : [
            {'start':  '2014-07-08 09:30:00', 'end': '2118-11-16 12:30:00', 'multiplier': 100, 'addend': 0, 'reason': 'SWC (data in %)'},
        ],
        'SWC_3_1_1' : [
            {'start':  '2014-07-08 09:30:00', 'end': '2118-11-16 12:30:00', 'multiplier': 100, 'addend': 0, 'reason': 'SWC (data in %)'},
        ],
        'SWC_4_1_1' : [
            {'start':  '2014-07-08 09:30:00', 'end': '2118-11-16 12:30:00', 'multiplier': 100, 'addend': 0, 'reason': 'SWC (data in %)'},
        ],
        'SWC_5_1_1' : [
            {'start':  '2014-07-08 09:30:00', 'end': '2118-11-16 12:30:00', 'multiplier': 100, 'addend': 0, 'reason': 'SWC (data in %)'},
        ],
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
    'TL1_RAD_30' : 'TL1_CAL',
    'TL1_RAD_1' : 'TL1_CAL',
    # 'TL1_MET_30': 'TL1_CAL',
    'TL1_SOIL_30': 'TL1_SOIL_CAL',
    # Tlen1a site - dataTacker measurements (03.08.2018- 12.06.2024)
    'TL1a_MET_30_dT' : 'TL1adT_CAL',
    'TL1a_MET_1_dT' : 'TL1adT_CAL',
    # Tlen2 "old" tower added by Klaudia- 19.07.2025
    'TL2_MET_1m' : 'TL2_CAL',
    'TL2_MET_30m': 'TL2_CAL',
    # Tlen2 site - "new" tower - dataTacker measurements (01.08.2018- 26.06.2024)
    'TL2_MET_1_dT' : 'TL2dT_CAL',
    'TL2_MET_30_dT' : 'TL2dT_CAL',
    'SA_MET_1min': 'SA_CAL',
    'SA_MET_30min': 'SA_CAL',
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
            {'start': '2025-07-13 01:20:00', 'end': '2025-07-13T01:24:00', 'flag_value': 2, 'reason': 'Awaria rejestratora2'},
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
            # {'start': '2021-01-18 00:44:00', 'end': '2021-01-18 00:44:00', 'flag_value': 3, 'reason': 'spike'},
        ],
        'PPFD_IN_1_1_1':[
            {'start': '2019-10-15', 'end': '2019-10-18 03:30:00', 'flag_value': 3, 'reason': 'brak sensora'},
            # {'start': '2024-08-20 13:28:00', 'end': '2024-08-20 13:28:00', 'flag_value': 3, 'reason': 'awaria sensora'},
            # Tu wprowadzam generator json dla pomiarów które w chaotyczny sposób okazały się błędne 
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
            {'start': '2013-03-29 17:30:00', 'end': '2013-04-25 17:30:00', 'flag_value': 3, 'reason': 'Delay in meteo sensors installation in relation to flux data'},
            {'start': '2021-07-14 23:00:00', 'end': '2021-07-20 09:30:00', 'flag_value': 3, 'reason': 'Storm damage'},
        ],
	# Radiation fluxes
        'PPFD_IN_1_1_1':[
            {'start': '2014-06-16 00:00:00', 'end': '2014-08-26 12:00:00', 'flag_value': 3, 'reason': 'condensation inside the sensor dome BF3/BF5H- deinstalled'},
            {'start': '2015-12-08 12:00:00', 'end': '2016-01-13 12:00:00', 'flag_value': 3, 'reason': 'condensation inside the sensor dome BF3/BF5H- deinstalled 2'},
            {'start': '2016-12-02 10:30:00', 'end': '2017-02-13 12:30:00', 'flag_value': 3, 'reason': 'condensation inside the sensor dome BF3/BF5H- deinstalled 3'},
            {'start': '2017-10-18 10:00:00', 'end': '2018-04-10 16:00:00', 'flag_value': 3, 'reason': 'condensation inside the sensor dome BF3/BF5H- deinstalled 4'},
			{'start': '2019-08-12 00:00:00', 'end': '2020-01-13 10:30:00', 'flag_value': 3, 'reason': 'condensation inside the sensor dome BF3/BF5H- deinstalled 5'},
			{'start': '2020-12-02 14:30:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'BF5 sensor deinstalled for good'},	
        ],
        'PPFD_DIF_1_1_1':[
            {'start': '2014-06-16 00:00:00', 'end': '2014-08-26 12:00:00', 'flag_value': 3, 'reason': 'condensation inside the sensor dome BF3/BF5H- deinstalled'},
            {'start': '2015-12-08 12:00:00', 'end': '2016-01-13 12:00:00', 'flag_value': 3, 'reason': 'condensation inside the sensor dome BF3/BF5H- deinstalled 2'},
            {'start': '2016-12-02 10:30:00', 'end': '2017-02-13 12:30:00', 'flag_value': 3, 'reason': 'condensation inside the sensor dome BF3/BF5H- deinstalled 3'},
            {'start': '2017-10-18 10:00:00', 'end': '2018-04-10 16:00:00', 'flag_value': 3, 'reason': 'condensation inside the sensor dome BF3/BF5H- deinstalled 4'},
            {'start': '2019-08-12 00:00:00', 'end': '2020-01-13 10:30:00', 'flag_value': 3, 'reason': 'condensation inside the sensor dome BF3/BF5H- deinstalled 5'},
			{'start': '2020-12-02 14:30:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'BF5 sensor deinstalled for good'},
        ],
        'PPFD_IN_1_1_2':[
            {'start': '2013-04-25 17:30:00', 'end': '2014-01-13 10:30:00', 'flag_value': 2, 'reason': 'incorrect SKP215 readings?'},
            {'start': '2022-11-14 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurement terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'PPFD_OUT_1_1_1':[
            {'start': '2013-04-25 17:30:00', 'end': '2014-01-13 10:30:00', 'flag_value': 2, 'reason': 'incorrect SKP215 readings?'},
            {'start': '2022-11-14 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurement terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'SW_IN_1_1_1':[
            {'start': '2022-11-14 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurement terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'SW_OUT_1_1_1':[
            {'start': '2022-11-14 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurement terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'LW_IN_1_1_1':[
            {'start': '2022-11-14 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurement terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'LW_OUT_1_1_1':[
            {'start': '2022-11-14 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurement terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
	#Air temperature and humidity
        'TA_1_1_1':[
			{'start': '2013-01-00 00:00:00', 'end': '2013-04-25 17:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '2013-04-25 17:30:00', 'end': '2013-05-17 12:00:00', 'flag_value': 2, 'reason': 'sensor malfunction- not reasonable data'},
			{'start': '2013-12-06 4:30:00', 'end': '2014-01-13 13:00:00', 'flag_value': 2, 'reason': 'sensor malfunction- not reasonable data'},
			{'start': '2014-02-25 14:30:00', 'end': '2014-04-04 11:00:00', 'flag_value': 2, 'reason': 'sensor malfunction- not reasonable data'},
			{'start': '2014-06-24 03:00:00', 'end': '2014-06-24 03:30:00', 'flag_value': 2, 'reason': 'sensor malfunction- spike'},
			{'start': '2014-11-18 13:30:00', 'end': '2015-01-04 16:30:00', 'flag_value': 2, 'reason': 'sensor malfunction- not reasonable data'},
			{'start': '2015-01-10 22:00:00', 'end': '2015-03-18 12:30:00', 'flag_value': 2, 'reason': 'sensor malfunction- not reasonable data'},
			{'start': '2015-05-17 21:00:00', 'end': '2015-05-19 10:30:00', 'flag_value': 2, 'reason': 'sensor malfunction- not reasonable data'},
			{'start': '2021-02-24T14:00:00', 'end': '2021-04-28 00:00:00', 'flag_value': 2, 'reason': 'sensor malfunction- not reasonable data'},
			{'start': '2021-04-28 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'sensor demounted'},
            #{'start': '2022-11-14 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurement terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'RH_1_1_1':[
			{'start': '2013-01-00 00:00:00', 'end': '2013-04-25 17:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
           # {'start': '2013-04-25 17:30:00', 'end': '2013-05-17 12:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
			{'start': '2013-12-06 4:30:00', 'end': '2014-01-13 13:00:00', 'flag_value': 2, 'reason': 'sensor malfunction- not reasonable data'},
			{'start': '2014-02-25 14:30:00', 'end': '2014-04-04 11:00:00', 'flag_value': 2, 'reason': 'sensor malfunction- not reasonable data'},
			{'start': '2014-06-24 03:00:00', 'end': '2014-06-24 03:30:00', 'flag_value': 2, 'reason': 'sensor malfunction- spike'},
			{'start': '2014-11-18 13:30:00', 'end': '2015-01-04 16:30:00', 'flag_value': 2, 'reason': 'sensor malfunction- not reasonable data'},
			{'start': '2015-01-10 22:00:00', 'end': '2015-03-18 12:30:00', 'flag_value': 2, 'reason': 'sensor malfunction- not reasonable data'},
			{'start': '2015-05-17 21:00:00', 'end': '2015-05-19 10:30:00', 'flag_value': 2, 'reason': 'sensor malfunction- not reasonable data'},
			{'start': '2021-02-24T14:00:00', 'end': '2021-04-28 00:00:00', 'flag_value': 2, 'reason': 'sensor malfunction- not reasonable data'},
			{'start': '2021-04-28 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'sensor demounted'},
            #{'start': '2022-11-14 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurement terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'TA_1_2_1':[
			{'start': '2013-01-00 00:00:00', 'end': '2013-04-25 17:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '2013-04-25 17:30:00', 'end': '2014-01-13 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
			{'start': '2014-01-23 02:30:00', 'end': '2014-04-04 11:00:00', 'flag_value': 2, 'reason': 'sensor malfunction- not reasonable data'},
			{'start': '2014-11-18 13:30:00', 'end': '2015-03-18 12:30:00', 'flag_value': 2, 'reason': 'sensor malfunction- not reasonable data'},
			{'start': '2015-05-17 21:00:00', 'end': '2015-05-19 10:30:00', 'flag_value': 2, 'reason': 'sensor malfunction- spike'},
			{'start': '2021-02-23 14:30:00', 'end': '2021-04-28 00:00:00', 'flag_value': 2, 'reason': 'sensor malfunction- not reasonable data'},
			{'start': '2021-04-28 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'sensor demounted'},
            #{'start': '2022-11-14 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurement terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'RH_1_2_1':[
			{'start': '2013-01-00 00:00:00', 'end': '2013-04-25 17:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '2013-04-26T13:00:00', 'end': '2013-04-29 11:30:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
			{'start': '2013-12-06 4:30:00', 'end': '2014-01-13 13:00:00', 'flag_value': 2, 'reason': 'sensor malfunction- not reasonable data'},
			{'start': '2014-01-23 02:30:00', 'end': '2014-04-04 11:00:00', 'flag_value': 2, 'reason': 'sensor malfunction- not reasonable data'},
			{'start': '2014-11-18 13:30:00', 'end': '2015-03-18 12:30:00', 'flag_value': 2, 'reason': 'sensor malfunction- not reasonable data'},
			{'start': '2015-05-17 21:00:00', 'end': '2015-05-19 10:30:00', 'flag_value': 2, 'reason': 'sensor malfunction- spike'},
			{'start': '2021-02-23 14:30:00', 'end': '2021-04-28 00:00:00', 'flag_value': 2, 'reason': 'sensor malfunction- not reasonable data'},
			{'start': '2021-04-28 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'sensor demounted'},
            #{'start': '2022-11-14 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurement terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
    },  

#Tlen1a – DataTacker datalogger
    'TL1adT_QF': {
        '*': [
            {'start': '2018-07-01 00:00:00', 'end': '2018-07-31 16:00:00', 'flag_value': 3, 'reason': 'DataTacker not operating yet'},
            {'start': '2024-06-12 14:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'DataTacker demounted'},
        ],
            #Radiation fluxes
        'SW_IN_2_1_1':[
            {'start': '2018-07-31 00:00:00', 'end': '2018-08-02 23:59:00', 'flag_value': 2, 'reason': 'sensor malfunction'},
            {'start': '2018-08-21 22:54:00', 'end': '2018-08-22 13:45:00', 'flag_value': 2, 'reason': 'sensor malfunction'},
            {'start': '2019-01-04 16:00:00', 'end': '2019-01-19 14:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2019-06-17 00:25:00', 'end': '2019-06-27 13:58:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2019-07-11 02:30:00', 'end': '2019-07-24 08:30:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2019-08-06 00:00:00', 'end': '2019-08-28 16:30:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2019-10-15 00:00:00', 'end': '2019-11-03 12:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-02-05 19:00:00', 'end': '2020-03-09 10:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-04-29 18:00:00', 'end': '2020-05-05 18:30:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-05-27 18:00:00', 'end': '2020-06-18 14:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-08-07 23:46:00', 'end': '2020-08-31 18:30:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-09-13 23:06:00', 'end': '2020-10-01 14:17:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-10-30 20:27:00', 'end': '2020-12-21 12:35:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-12-22 06:16:00', 'end': '2021-01-26 21:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-02-19 05:40:00', 'end': '2021-04-28 15:03:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-05-06 12:28:00', 'end': '2021-07-12 16:29:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-07-13 23:59:00', 'end': '2021-08-05 13:46:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-08-26 22:20:00', 'end': '2021-08-28 11:50:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-09-19 00:00:00', 'end': '2021-09-29 10:46:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-10-01 01:42:00', 'end': '2021-10-17 16:07:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-10-25 00:23:00', 'end': '2021-10-27 11:43:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-10-28 04:36:00', 'end': '2021-11-04 09:12:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-11-15 08:27:00', 'end': '2021-12-07 14:34:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2022-09-26 05:00:00', 'end': '2022-10-23 23:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},
            {'start': '2022-10-23 23:46:00', 'end': '2023-04-25 00:56:00', 'flag_value': 3, 'reason': 'no raw data'},
            #{'start':  '2024-05-20 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 2, 'reason': 'sensor malfunction - cable damage by rodents?'},
            {'start':  '2024-06-08 17:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'sensor demounted'},#Check the exact date!
        ],

        'PPFD_IN_2_1_1':[
            #{'start': '2020-05-27 17:00:00', 'end': '2021-08-21 13:30:00', 'flag_value': 2, 'reason': 'sensor malfunction'},#CHECK AGAIN!
            {'start': '2018-07-01 00:00:00', 'end': '2018-08-03 00:30:00', 'flag_value': 2, 'reason': 'sensor malfunction'},#CHECK AGAIN!
            {'start': '2019-01-04 16:00:00', 'end': '2019-01-19 14:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2019-06-17 00:25:00', 'end': '2019-06-27 13:58:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2019-07-11 02:30:00', 'end': '2019-07-24 08:30:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2019-08-06 00:00:00', 'end': '2019-08-28 16:30:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2019-10-15 00:00:00', 'end': '2019-11-03 12:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-02-05 19:00:00', 'end': '2020-03-09 10:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-04-29 18:00:00', 'end': '2020-05-05 18:30:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-05-27 18:00:00', 'end': '2020-06-18 14:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-08-07 23:46:00', 'end': '2020-08-31 18:30:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-09-13 23:06:00', 'end': '2020-10-01 14:17:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-10-30 20:27:00', 'end': '2020-12-21 12:35:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-12-22 06:16:00', 'end': '2021-01-26 21:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-02-19 05:40:00', 'end': '2021-04-28 15:03:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-05-06 12:28:00', 'end': '2021-07-12 16:29:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-07-13 13:23:00', 'end': '2021-08-05 13:46:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-08-26 22:20:00', 'end': '2021-08-28 11:50:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-09-09 23:46:00', 'end': '2021-09-29 10:46:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-10-01 01:42:00', 'end': '2021-10-17 16:07:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-10-25 00:23:00', 'end': '2021-10-27 11:43:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-10-28 04:36:00', 'end': '2021-11-04 09:12:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-11-15 08:27:00', 'end': '2021-12-07 14:34:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2022-10-23 23:46:00', 'end': '2023-04-25 00:56:00', 'flag_value': 3, 'reason': 'no raw data'},
            #{'start':  '2024-05-20 00:00:00', 'end': '2099-01-01 00:00:00','flag_value': 2, 'reason': 'sensor malfunction'},
            {'start':  '2024-06-08 17:00:00', 'end': '2099-01-01 00:00:00','flag_value': 2, 'reason': 'sensor demounted'},#Check the exact date!
        ],
            #Temperature data
        'TA_2_1_1':[
            {'start': '2018-07-31 00:00:00', 'end': '2018-08-15 20:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},#CHECK AGAIN!},
            {'start': '2019-06-17 00:25:00', 'end': '2019-06-27 13:58:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2019-07-11 02:30:00', 'end': '2019-07-24 08:30:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2019-08-06 00:00:00', 'end': '2019-08-28 16:30:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2019-10-15 00:00:00', 'end': '2019-11-03 12:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-02-05 19:00:00', 'end': '2020-03-09 10:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-04-29 18:00:00', 'end': '2020-05-05 18:30:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-05-27 18:00:00', 'end': '2020-06-18 14:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-08-07 23:46:00', 'end': '2020-08-31 18:30:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-09-13 23:06:00', 'end': '2020-10-01 14:17:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-10-30 20:27:00', 'end': '2020-12-21 12:35:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-12-22 06:16:00', 'end': '2021-01-26 21:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-02-19 05:40:00', 'end': '2021-04-28 15:03:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-05-06 12:28:00', 'end': '2021-07-12 16:29:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-07-13 13:23:00', 'end': '2021-08-05 13:46:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-08-26 22:20:00', 'end': '2021-08-28 11:50:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-09-09 23:46:00', 'end': '2021-09-29 10:46:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-10-01 01:42:00', 'end': '2021-10-17 16:07:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-10-25 00:23:00', 'end': '2021-10-27 11:43:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-10-28 04:36:00', 'end': '2021-11-04 09:12:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-11-15 08:27:00', 'end': '2021-12-07 14:34:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2022-10-23 23:46:00', 'end': '2023-04-25 00:56:00', 'flag_value': 3, 'reason': 'no raw data'},
        ],
        'TA_2_1_2':[
            {'start': '2018-07-31 00:00:00', 'end': '2018-08-28 13:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},#CHECK AGAIN!},
            {'start': '2019-06-17 00:25:00', 'end': '2019-06-27 13:58:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2019-07-11 02:30:00', 'end': '2019-07-24 08:30:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2019-08-06 00:00:00', 'end': '2019-08-28 16:30:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2019-10-15 00:00:00', 'end': '2019-11-03 12:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-02-05 19:00:00', 'end': '2020-03-09 10:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-04-29 18:00:00', 'end': '2020-05-05 18:30:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-05-27 18:00:00', 'end': '2020-06-18 14:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-08-07 23:46:00', 'end': '2020-08-31 18:30:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-09-13 23:06:00', 'end': '2020-10-01 14:17:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-10-30 20:27:00', 'end': '2020-12-21 12:35:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-12-22 06:16:00', 'end': '2021-01-26 21:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-02-19 05:40:00', 'end': '2021-04-28 15:03:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-05-06 12:28:00', 'end': '2021-07-12 16:29:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-07-13 13:23:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'sensor malfunction'},
        ],
        'TA_2_2_1':[
            {'start': '2018-07-31 00:00:00', 'end': '2018-08-15 19:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},
            {'start': '2018-08-21 22:54:00', 'end': '2018-08-22 13:45:00', 'flag_value': 2, 'reason': 'sensor malfunction'},
            {'start': '2019-01-04 16:00:00', 'end': '2019-01-19 14:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2019-06-17 00:25:00', 'end': '2019-06-27 13:58:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2019-07-11 02:30:00', 'end': '2019-07-24 08:30:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2019-08-06 00:00:00', 'end': '2019-08-28 16:30:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2019-10-15 00:00:00', 'end': '2019-11-03 12:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-02-05 19:00:00', 'end': '2020-03-09 10:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-04-29 18:00:00', 'end': '2020-05-05 18:30:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-05-27 18:00:00', 'end': '2020-06-18 14:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-08-07 23:46:00', 'end': '2020-08-31 18:30:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-09-13 23:06:00', 'end': '2020-10-01 14:17:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-10-30 20:27:00', 'end': '2020-12-21 12:35:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-12-22 06:16:00', 'end': '2021-01-26 21:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-02-19 05:40:00', 'end': '2021-04-28 15:03:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-05-06 12:28:00', 'end': '2021-07-12 16:29:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-07-13 13:23:00', 'end': '2021-08-05 13:46:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-08-26 22:20:00', 'end': '2021-08-28 11:50:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-09-09 23:46:00', 'end': '2021-09-29 10:46:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-10-01 01:42:00', 'end': '2021-10-17 16:07:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-10-25 00:23:00', 'end': '2021-10-27 11:43:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-10-28 04:36:00', 'end': '2021-11-04 09:12:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2021-11-15 08:27:00', 'end': '2021-12-07 14:34:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2022-09-26 05:00:00', 'end': '2022-10-23 23:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},
            {'start': '2022-10-23 23:46:00', 'end': '2023-04-25 00:56:00', 'flag_value': 3, 'reason': 'no raw data'},
            #{'start':  '2024-05-20 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 2, 'reason': 'sensor malfunction - cable damage by rodents?'},
            {'start':  '2024-06-08 17:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'sensor demounted'},#Check the exact date!
        ],
    },

 # # Tlen2 "old" tower added by Klaudia- 19.07.2025
    'TL2_QF': {
     '*': [
            {'start': '2018-04-13 21:30:00', 'end': '2018-05-10 16:30:00', 'flag_value': 3, 'reason': 'datalogger power source malfunction'},
            {'start': '2018-07-19 21:00:00', 'end': '2018-07-27 19:30:00', 'flag_value': 3, 'reason': 'power cut- damage'},
            {'start': '2020-06-19 13:02:00', 'end': '2020-10-01 11:49:00', 'flag_value': 3, 'reason': 'datalogger malfunction'},
            # {'start': '2020-12-30 01:30:00', 'end': '2021-04-28 13:30:00', 'flag_value': 3, 'reason': 'datalogger malfunction'},
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
        'PPFD_BC_IN_1_1_2':[
            {'start': '2019-12-03 02:01:00', 'end': '2019-12-23 11:51:00', 'flag_value': 3, 'reason': 'sensor malfunction'},
        ],
        'PPFD_IN_1_1_1':[
            {'start': '2014-07-10 09:30:00', 'end': '2014-08-29 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '2018-07-27 10:01:00', 'end': '2018-09-04 15:00:00', 'flag_value': 3, 'reason': 'gap in data'},
			{'start': '2019-07-30 10:30:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'SKL sensors deinstalled for good'},
			# {'start': '2018-07-29 19:30:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'SKL sensors deinstalled for good???'},
        ],
        'PPFD_OUT_1_1_1':[
            {'start': '2014-07-10 09:30:00', 'end': '2014-08-29 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '2018-07-27 10:00:00', 'end': '2018-09-04 15:30:00', 'flag_value': 3, 'reason': 'gap in data'},
			{'start': '2019-07-30 10:30:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'SKL sensors deinstalled for good'},
			# {'start': '2019-07-29 19:30:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'SKL sensors deinstalled for good???'},
        ],
        'SW_IN_1_1_1':[
            {'start': '2014-07-10 09:30:00', 'end': '2014-08-29 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet?? corrected after that?'},
            {'start': '2015-03-18 09:30:00', 'end': '2015-03-19 00:30:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'},
            {'start': '2018-07-24 09:00:00', 'end': '2018-09-09 15:30:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'},
            {'start': '2015-05-13 03:30:00', 'end': '2015-05-15 21:00:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'},
            {'start': '2019-05-13 12:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
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
    },
    'TL2dt_QF': {
        '*': [
            {'start': '2018-07-01 00:00:00', 'end': '2018-08-01 14:00:00', 'flag_value': 3, 'reason': 'datalogger not installed yet'},
            {'start': '2024-07-24 00:00:00', 'end': '2041-04-28 13:30:00', 'flag_value': 3, 'reason': 'datalogger demounted'},
           # {'start': '2021-05-16 00:00:00', 'end': '2041-04-28 13:30:00', 'flag_value': 3, 'reason': 'datalogger mulfuntion'},
        ],
         #Radiation fluxes
        'SW_IN_1_1_2':[
            {'start': '2018-07-31 00:00:00', 'end': '2018-08-01 14:30:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '2018-11-13 00:00:00', 'end': '2018-11-16 13:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},
            {'start': '2019-02-10 00:00:00', 'end': '2019-02-20 00:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-04-20 12:30:00', 'end': '2020-05-05 17:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-06-19 13:30:00', 'end': '2020-10-01 12:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},
            {'start': '2021-01-01 00:00:00', 'end': '2021-04-28 14:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},
            {'start': '2021-05-02 08:00:00', 'end': '2021-05-10 10:00:00', 'flag_value': 2, 'reason': 'no raw data'},
            {'start': '2021-05-15 10:00:00', 'end': '2021-07-12 00:00:00', 'flag_value': 2, 'reason': 'no raw data'},
            {'start': '2021-07-12 00:00:00', 'end': '2022-01-02 00:00:00', 'flag_value': 2, 'reason': 'sensor malfunction??'},
            {'start': '2022-10-23 00:00:00', 'end': '2023-04-16 00:00:00', 'flag_value': 2, 'reason': 'no raw data?'},
            {'start': '2023-06-16 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 2, 'reason': 'sensor malfunction??'},
        ],
        'PPFD_IN_1_1_2':[
            {'start': '2018-07-31 00:00:00', 'end': '2018-08-01 14:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},
            {'start': '2018-11-13 00:00:00', 'end': '2018-11-16 13:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},
            {'start': '2019-02-10 00:00:00', 'end': '2019-02-20 00:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-04-20 12:30:00', 'end': '2020-05-05 17:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-06-19 13:30:00', 'end': '2020-10-01 12:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},
            {'start': '2021-01-01 00:00:00', 'end': '2021-04-28 14:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},
            {'start': '2021-05-02 08:00:00', 'end': '2021-05-10 10:00:00', 'flag_value': 2, 'reason': 'no raw data'},
            {'start': '2021-05-15 10:00:00', 'end': '2021-07-12 00:00:00', 'flag_value': 2, 'reason': 'no raw data'},
            {'start': '2021-07-12 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 2, 'reason': 'sensor malfunction??'},
        ],
        #Temperature data
        'TA_1_1_2':[
            {'start': '2018-07-31 00:00:00', 'end': '2018-08-28 14:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},#CHECK AGAIN!},
            {'start': '2018-11-13 00:00:00', 'end': '2018-11-16 13:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},
            {'start': '2019-02-10 00:00:00', 'end': '2019-02-20 00:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2019-11-15 00:00:00', 'end': '2019-12-04 12:00:00', 'flag_value': 2, 'reason': 'sensor malfunction?'},
            {'start': '2020-04-20 12:30:00', 'end': '2020-05-05 17:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-06-19 12:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},

        ],
        'TA_1_2_2':[
            {'start': '2018-07-25 14:30:00', 'end': '2018-08-28 14:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},#CHECK AGAIN!},
            {'start': '2018-11-13 00:00:00', 'end': '2018-11-16 13:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},
            {'start': '2019-02-10 00:00:00', 'end': '2019-02-20 00:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2019-11-15 00:00:00', 'end': '2019-12-04 12:00:00', 'flag_value': 2, 'reason': 'sensor malfunction?'},
            {'start': '2020-04-20 12:30:00', 'end': '2020-05-05 17:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-06-19 12:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},
            ],
        'TA_2_1_2':[
            {'start': '2018-07-25 14:30:00', 'end': '2018-11-13 14:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},#CHECK AGAIN!},
            {'start': '2018-11-13 00:00:00', 'end': '2018-11-16 13:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},
            {'start': '2019-02-10 00:00:00', 'end': '2019-02-20 00:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-04-20 12:30:00', 'end': '2020-05-05 17:00:00', 'flag_value': 3, 'reason': 'no raw data'},
            {'start': '2020-06-19 13:30:00', 'end': '2020-10-01 12:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},
            {'start': '2021-01-01 00:00:00', 'end': '2021-04-28 14:00:00', 'flag_value': 2, 'reason': 'sensor malfunction'},
            {'start': '2021-05-02 08:00:00', 'end': '2021-05-10 10:00:00', 'flag_value': 2, 'reason': 'no raw data'},
            {'start': '2021-05-15 10:00:00', 'end': '2021-07-12 00:00:00', 'flag_value': 2, 'reason': 'no raw data'},
            {'start': '2021-07-12 00:00:00', 'end': '2099-01-01 00:00:00', 'flag_value': 2, 'reason': 'sensor malfunction??'},
        ],
    },
    'TL2csi_QF': {
     '*': [
            {'start': '2024-07-01 00:00:00', 'end': '2024-07-25 14:30:00', 'flag_value': 3, 'reason': 'datalogger not installed yet'},
        ],

	# Radiation fluxes
        'PPFD_BC_IN_1_1_1':[
            {'start': '2024-07-25 00:00:00', 'end': '2039-12-23 11:51:00', 'flag_value': 3, 'reason': 'sensor malfunction'},
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
    # nowa lub zmieniona linia
    # Tlen1
    'TL1_MET_30': 'TL1_QF',
    'TL1_RAD_30': 'TL1_QF',
    'TL1_SOIL_30': 'TL1_QF',
    'TL1_RAD_1': 'TL1_QF',
    'TL1a_MET_30_dT': 'TL1adT_QF',
    'TL1a_Rain_down_dT': 'TL1adT_QF',
    'TL1a_MET_1_dT': 'TL1adT_QF',
    'TL1a_CalPlates_dT': 'TL1adT_QF',
    # # Tlen2
    'TL2_MET_30m': 'TL2_QF',
    'TL2_CalPlates_dT': 'TL2dt_QF',
    'TL2_MET_1_dT': 'TL2dt_QF',
    'TL2_MET_30_dT': 'TL2dt_QF',    
}

# 8.2 Skrypt tworzący JSONa z pojedynczymi punktami do oflagowania
# 'date_list_ME_TOP' zawiera listę dat w których występują złe pomiary
date_list_ME_TOP = [
            '2024-08-20 13:28:00', '2024-08-20 13:33:00', '2024-08-20 13:36:00', '2024-08-20 13:48:00', '2024-08-20 13:57:00', '2024-08-20 14:03:00', '2024-08-20 14:11:00', '2024-08-20 14:16:00', '2024-08-20 14:17:00', '2024-08-20 14:18:00', '2024-08-20 14:20:00', '2024-08-20 14:24:00', '2024-08-20 14:26:00', '2024-08-20 14:30:06', '2024-08-20 14:33:00', '2024-08-20 14:36:00', '2024-08-20 14:38:00', '2024-08-20 14:39:00', '2024-08-20 14:41:00', '2024-08-20 14:42:00', '2024-08-20 14:49:00', '2024-08-20 14:52:00', '2024-08-20 14:55:00', '2024-08-20 14:57:00', '2024-08-20 14:58:00', '2024-08-20 15:02:00', '2024-08-20 15:04:00', '2024-08-20 15:05:00', '2024-08-20 15:06:00', '2024-08-20 15:08:00', '2024-08-20 15:10:00', '2024-08-20 15:11:00', '2024-08-20 15:12:00', '2024-08-20 15:15:00', '2024-08-20 15:17:00', '2024-08-20 15:18:00', '2024-08-20 15:20:00', '2024-08-20 15:21:00', '2024-08-20 15:24:00', '2024-08-20 15:27:00', '2024-08-20 15:28:00', '2024-08-20 15:30:06', '2024-08-20 15:32:00', '2024-08-20 15:34:00', '2024-08-20 15:35:00', '2024-08-20 15:36:00', '2024-08-20 15:38:00', '2024-08-20 15:39:00', '2024-08-20 15:40:00', '2024-08-20 15:42:00', '2024-08-20 15:43:00', '2024-08-20 15:44:00', '2024-08-20 15:47:00', '2024-08-20 15:51:00', '2024-08-20 15:54:00', '2024-08-20 15:55:00', '2024-08-20 15:56:00', '2024-08-20 16:01:00', '2024-08-20 16:02:00', '2024-08-20 16:06:00', '2024-08-20 16:07:00', '2024-08-20 16:09:00', '2024-08-20 16:11:00', '2024-08-20 16:12:00', '2024-08-20 16:15:00', '2024-08-20 16:19:00', '2024-08-20 16:22:00', '2024-08-20 16:23:00', '2024-08-20 16:25:00', '2024-08-20 16:27:00', '2024-08-20 16:28:00', '2024-08-20 16:29:00', '2024-08-20 16:35:00', '2024-08-20 16:38:00', '2024-08-20 16:41:00', '2024-08-20 16:44:00', '2024-08-20 16:49:00', '2024-08-20 16:51:00', '2024-08-20 16:53:00', '2024-08-20 16:55:00', '2024-08-20 16:56:00', '2024-08-20 16:59:00', '2024-08-20 17:02:00', '2024-08-20 17:03:00', '2024-08-20 17:10:00', '2024-08-20 17:12:00', '2024-08-20 17:15:00', '2024-08-20 17:17:00', '2024-08-20 17:23:00', '2024-08-20 17:30:06', '2024-08-20 17:31:00', '2024-08-20 17:37:00', '2024-08-20 17:43:00', '2024-09-02 14:56:00', '2024-09-02 14:58:00', '2024-09-02 15:00:06', '2024-09-02 15:03:00', '2024-09-02 15:08:00', '2024-09-02 15:11:00', '2024-09-02 15:13:00', '2024-09-02 15:15:00', '2024-09-02 15:18:00', '2024-09-02 15:19:00', '2024-09-02 15:28:00', '2024-09-02 15:34:00', '2024-09-02 15:36:00', '2024-09-02 15:37:00', '2024-09-02 15:38:00', '2024-09-02 15:40:00', '2024-09-02 15:44:00', '2024-09-02 15:47:00', '2024-09-02 15:48:00', '2024-09-02 15:50:00', '2024-09-02 15:52:00', '2024-09-02 15:54:00', '2024-09-02 15:57:00', '2024-09-02 15:58:00', '2024-09-02 16:00:06', '2024-09-02 16:01:00', '2024-09-02 16:02:00', '2024-09-02 16:04:00', '2024-09-02 16:06:00', '2024-09-02 16:08:00', '2024-09-02 16:09:00', '2024-09-02 16:11:00', '2024-09-02 16:14:00', '2024-09-02 16:15:00', '2024-09-02 16:18:00', '2024-09-02 16:19:00', '2024-09-02 16:20:00', '2024-09-02 16:22:00', '2024-09-02 16:23:00', '2024-09-02 16:24:00', '2024-09-02 16:27:00', '2024-09-02 16:31:00', '2024-09-02 16:33:00', '2024-09-02 16:36:00', '2024-09-02 16:37:00', '2024-09-02 16:38:00', '2024-09-02 16:41:00', '2024-09-02 16:42:00', '2024-09-02 16:43:00', '2024-09-02 16:45:00', '2024-09-02 16:46:00', '2024-09-02 16:48:00', '2024-09-02 16:49:00', '2024-09-02 16:50:00', '2024-09-02 16:51:00', '2024-09-02 16:52:00', '2024-09-02 16:53:00', '2024-09-02 16:54:00', '2024-09-02 16:55:00', '2024-09-02 16:56:00', '2024-09-02 17:03:00', '2024-09-02 17:04:00', '2024-09-02 17:07:00', '2024-09-02 17:10:00', '2024-09-02 17:11:00', '2024-09-02 17:16:00', '2024-09-02 17:19:00', '2024-09-02 17:20:00', '2024-09-02 17:22:00', '2024-09-02 17:23:00', '2024-09-02 17:24:00', '2024-09-02 17:29:00', '2024-09-03 11:58:00', '2024-09-03 12:16:00', '2024-09-03 12:18:00', '2024-09-03 12:32:00', '2024-09-03 12:43:00', '2024-09-03 12:44:00', '2024-09-03 12:45:00', '2024-09-03 12:47:00', '2024-09-03 12:48:00', '2024-09-03 12:50:00', '2024-09-03 12:52:00', '2024-09-03 12:55:00', '2024-09-03 12:57:00', '2024-09-03 12:58:00', '2024-09-03 13:01:00', '2024-09-03 13:08:00', '2024-09-03 13:10:00', '2024-09-03 13:12:00', '2024-09-03 13:13:00', '2024-09-03 13:14:00', '2024-09-03 13:17:00', '2024-09-03 13:20:00', '2024-09-03 13:22:00', '2024-09-03 13:23:00', '2024-09-03 13:24:00', '2024-09-03 13:26:00', '2024-09-03 13:28:00', '2024-09-03 13:29:00', '2024-09-03 13:30:06', '2024-09-03 13:31:00', '2024-09-03 13:32:00', '2024-09-03 13:33:00', '2024-09-03 13:36:00', '2024-09-03 13:38:00', '2024-09-03 13:39:00', '2024-09-03 13:41:00', '2024-09-03 13:43:00', '2024-09-03 13:44:00', '2024-09-03 13:46:00', '2024-09-03 13:48:00', '2024-09-03 13:50:00', '2024-09-03 13:54:00', '2024-09-03 13:56:00', '2024-09-03 13:57:00', '2024-09-03 14:03:00', '2024-09-03 14:04:00', '2024-09-03 14:06:00', '2024-09-03 14:08:00', '2024-09-03 14:09:00', '2024-09-03 14:10:00', '2024-09-03 14:11:00', '2024-09-03 14:12:00', '2024-09-03 14:14:00', '2024-09-03 14:15:00', '2024-09-03 14:19:00', '2024-09-03 14:20:00', '2024-09-03 14:21:00', '2024-09-03 14:23:00', '2024-09-03 14:26:00', '2024-09-03 14:29:00', '2024-09-03 14:30:06', '2024-09-03 14:34:00', '2024-09-03 14:36:00', '2024-09-03 14:39:00', '2024-09-03 14:40:00', '2024-09-03 14:42:00', '2024-09-03 14:43:00', '2024-09-03 14:46:00', '2024-09-03 14:50:00', '2024-09-03 14:51:00', '2024-09-03 14:54:00', '2024-09-03 14:55:00', '2024-09-03 14:56:00', '2024-09-03 14:57:00', '2024-09-03 14:58:00', '2024-09-03 14:59:00', '2024-09-03 15:00:06', '2024-09-03 15:01:00', '2024-09-03 15:03:00', '2024-09-03 15:04:00', '2024-09-03 15:06:00', '2024-09-03 15:07:00', '2024-09-03 15:10:00', '2024-09-03 15:12:00', '2024-09-03 15:13:00', '2024-09-03 15:14:00', '2024-09-03 15:15:00', '2024-09-03 15:16:00', '2024-09-03 15:19:00', '2024-09-03 15:21:00', '2024-09-03 15:22:00', '2024-09-03 15:25:00', '2024-09-03 15:29:00', '2024-09-03 15:31:00', '2024-09-03 15:32:00', '2024-09-03 15:33:00', '2024-09-03 15:35:00', '2024-09-03 15:37:00', '2024-09-03 15:38:00', '2024-09-03 15:40:00', '2024-09-03 15:43:00', '2024-09-03 15:45:00', '2024-09-03 15:46:00', '2024-09-03 15:47:00', '2024-09-03 15:49:00', '2024-09-03 15:52:00', '2024-09-03 15:54:00', '2024-09-03 15:55:00', '2024-09-03 15:58:00', '2024-09-03 16:00:07', '2024-09-03 16:05:00', '2024-09-03 16:06:00', '2024-09-03 16:08:00', '2024-09-03 16:09:00', '2024-09-03 16:10:00', '2024-09-03 16:12:00', '2024-09-03 16:15:00', '2024-09-03 16:17:00', '2024-09-03 16:19:00', '2024-09-03 16:20:00', '2024-09-03 16:21:00', '2024-09-03 16:22:00', '2024-09-03 16:23:00', '2024-09-03 16:25:00', '2024-09-03 16:26:00', '2024-09-03 16:27:00', '2024-09-03 16:28:00', '2024-09-03 16:31:00', '2024-09-03 16:33:00', '2024-09-03 16:34:00', '2024-09-03 16:35:00', '2024-09-03 16:36:00', '2024-09-03 16:37:00', '2024-09-03 16:38:00', '2024-09-03 16:42:00', '2024-09-03 16:44:00', '2024-09-03 16:45:00', '2024-09-03 16:47:00', '2024-09-03 16:48:00', '2024-09-03 16:49:00', '2024-09-03 16:51:00', '2024-09-03 16:53:00', '2024-09-03 16:54:00', '2024-09-03 16:55:00', '2024-09-03 16:56:00', '2024-09-03 16:58:00', '2024-09-03 16:59:00', '2024-09-03 17:02:00', '2024-09-03 17:03:00', '2024-09-03 17:06:00', '2024-09-03 17:07:00', '2024-09-03 17:09:00', '2024-09-03 17:11:00', '2024-09-03 17:12:00', '2024-09-03 17:13:00', '2024-09-03 17:15:00', '2024-09-03 17:18:00', '2024-09-03 17:19:00', '2024-09-03 17:20:00', '2024-09-03 17:22:00', '2024-09-03 17:24:00', '2024-09-03 17:25:00', '2024-09-03 17:28:00', '2024-09-03 17:29:00', '2024-09-03 17:33:00', '2024-09-03 17:34:00', '2024-09-03 17:36:00', '2024-09-03 17:38:00', '2024-09-03 17:41:00', '2024-09-03 17:43:00', '2024-09-03 17:45:00', '2024-09-03 17:48:00', '2024-09-03 17:49:00', '2024-09-03 17:52:00', '2024-09-03 17:55:00', '2024-09-03 17:56:00', '2024-09-03 17:59:00', '2024-09-03 18:00:06', '2024-09-03 18:05:00', '2024-09-03 18:07:00', '2024-09-03 18:08:00', '2024-09-03 18:11:00', '2024-09-03 18:12:00', '2024-09-03 18:17:00', '2024-09-03 18:19:00', '2024-09-03 18:20:00', '2024-09-03 18:22:00', '2024-09-03 18:24:00', '2024-09-03 18:31:00', '2024-09-03 18:32:00', '2024-09-03 18:33:00', '2024-09-03 18:36:00', '2024-09-02 14:56:00', '2024-09-02 14:58:00', '2024-09-02 15:00:06', '2024-09-02 15:03:00', '2024-09-02 15:08:00', '2024-09-02 15:11:00', '2024-09-02 15:13:00', '2024-09-02 15:15:00', '2024-09-02 15:18:00', '2024-09-02 15:19:00', '2024-09-02 15:28:00', '2024-09-02 15:34:00', '2024-09-02 15:36:00', '2024-09-02 15:37:00', '2024-09-02 15:38:00', '2024-09-02 15:40:00', '2024-09-02 15:44:00', '2024-09-02 15:47:00', '2024-09-02 15:48:00', '2024-09-02 15:50:00', '2024-09-02 15:52:00', '2024-09-02 15:54:00', '2024-09-02 15:57:00', '2024-09-02 15:58:00', '2024-09-02 16:00:06', '2024-09-02 16:01:00', '2024-09-02 16:02:00', '2024-09-02 16:04:00', '2024-09-02 16:06:00', '2024-09-02 16:08:00', '2024-09-02 16:09:00', '2024-09-02 16:11:00', '2024-09-02 16:14:00', '2024-09-02 16:15:00', '2024-09-02 16:18:00', '2024-09-02 16:19:00', '2024-09-02 16:20:00', '2024-09-02 16:22:00', '2024-09-02 16:23:00', '2024-09-02 16:24:00', '2024-09-02 16:27:00', '2024-09-02 16:31:00', '2024-09-02 16:33:00', '2024-09-02 16:36:00', '2024-09-02 16:37:00', '2024-09-02 16:38:00', '2024-09-02 16:41:00', '2024-09-02 16:42:00', '2024-09-02 16:43:00', '2024-09-02 16:45:00', '2024-09-02 16:46:00', '2024-09-02 16:48:00', '2024-09-02 16:49:00', '2024-09-02 16:50:00', '2024-09-02 16:51:00', '2024-09-02 16:52:00', '2024-09-02 16:53:00', '2024-09-02 16:54:00', '2024-09-02 16:55:00', '2024-09-02 16:56:00', '2024-09-02 17:03:00', '2024-09-02 17:04:00', '2024-09-02 17:07:00', '2024-09-02 17:10:00', '2024-09-02 17:11:00', '2024-09-02 17:16:00', '2024-09-02 17:19:00', '2024-09-02 17:20:00', '2024-09-02 17:22:00', '2024-09-02 17:23:00', '2024-09-02 17:24:00', '2024-09-02 17:29:00', '2024-09-03 11:58:00', '2024-09-03 12:16:00', '2024-09-03 12:18:00', '2024-09-03 12:32:00', '2024-09-03 12:43:00', '2024-09-03 12:44:00', '2024-09-03 12:45:00', '2024-09-03 12:47:00', '2024-09-03 12:48:00', '2024-09-03 12:50:00', '2024-09-03 12:52:00', '2024-09-03 12:55:00', '2024-09-03 12:57:00', '2024-09-03 12:58:00', '2024-09-03 13:01:00', '2024-09-03 13:08:00', '2024-09-03 13:10:00', '2024-09-03 13:12:00', '2024-09-03 13:13:00', '2024-09-03 13:14:00', '2024-09-03 13:17:00', '2024-09-03 13:20:00', '2024-09-03 13:22:00', '2024-09-03 13:23:00', '2024-09-03 13:24:00', '2024-09-03 13:26:00', '2024-09-03 13:28:00', '2024-09-03 13:29:00', '2024-09-03 13:30:06', '2024-09-03 13:31:00', '2024-09-03 13:32:00', '2024-09-03 13:33:00', '2024-09-03 13:36:00', '2024-09-03 13:38:00', '2024-09-03 13:39:00', '2024-09-03 13:41:00', '2024-09-03 13:43:00', '2024-09-03 13:44:00', '2024-09-03 13:46:00', '2024-09-03 13:48:00', '2024-09-03 13:50:00', '2024-09-03 13:54:00', '2024-09-03 13:56:00', '2024-09-03 13:57:00', '2024-09-03 14:03:00', '2024-09-03 14:04:00', '2024-09-03 14:06:00', '2024-09-03 14:08:00', '2024-09-03 14:09:00', '2024-09-03 14:10:00', '2024-09-03 14:11:00', '2024-09-03 14:12:00', '2024-09-03 14:14:00', '2024-09-03 14:15:00', '2024-09-03 14:19:00', '2024-09-03 14:20:00', '2024-09-03 14:21:00', '2024-09-03 14:23:00', '2024-09-03 14:26:00', '2024-09-03 14:29:00', '2024-09-03 14:30:06', '2024-09-03 14:34:00', '2024-09-03 14:36:00', '2024-09-03 14:39:00', '2024-09-03 14:40:00', '2024-09-03 14:42:00', '2024-09-03 14:43:00', '2024-09-03 14:46:00', '2024-09-03 14:50:00', '2024-09-03 14:51:00', '2024-09-03 14:54:00', '2024-09-03 14:55:00', '2024-09-03 14:56:00', '2024-09-03 14:57:00', '2024-09-03 14:58:00', '2024-09-03 14:59:00', '2024-09-03 15:00:06', '2024-09-03 15:01:00', '2024-09-03 15:03:00', '2024-09-03 15:04:00', '2024-09-03 15:06:00', '2024-09-03 15:07:00', '2024-09-03 15:10:00', '2024-09-03 15:12:00', '2024-09-03 15:13:00', '2024-09-03 15:14:00', '2024-09-03 15:15:00', '2024-09-03 15:16:00', '2024-09-03 15:19:00', '2024-09-03 15:21:00', '2024-09-03 15:22:00', '2024-09-03 15:25:00', '2024-09-03 15:29:00', '2024-09-03 15:31:00', '2024-09-03 15:32:00', '2024-09-03 15:33:00', '2024-09-03 15:35:00', '2024-09-03 15:37:00', '2024-09-03 15:38:00', '2024-09-03 15:40:00', '2024-09-03 15:43:00', '2024-09-03 15:45:00', '2024-09-03 15:46:00', '2024-09-03 15:47:00', '2024-09-03 15:49:00', '2024-09-03 15:52:00', '2024-09-03 15:54:00', '2024-09-03 15:55:00', '2024-09-03 15:58:00', '2024-09-03 16:00:07', '2024-09-03 16:05:00', '2024-09-03 16:06:00', '2024-09-03 16:08:00', '2024-09-03 16:09:00', '2024-09-03 16:10:00', '2024-09-03 16:12:00', '2024-09-03 16:15:00', '2024-09-03 16:17:00', '2024-09-03 16:19:00', '2024-09-03 16:20:00', '2024-09-03 16:21:00', '2024-09-03 16:22:00', '2024-09-03 16:23:00', '2024-09-03 16:25:00', '2024-09-03 16:26:00', '2024-09-03 16:27:00', '2024-09-03 16:28:00', '2024-09-03 16:31:00', '2024-09-03 16:33:00', '2024-09-03 16:34:00', '2024-09-03 16:35:00', '2024-09-03 16:36:00', '2024-09-03 16:37:00', '2024-09-03 16:38:00', '2024-09-03 16:42:00', '2024-09-03 16:44:00', '2024-09-03 16:45:00', '2024-09-03 16:47:00', '2024-09-03 16:48:00', '2024-09-03 16:49:00', '2024-09-03 16:51:00', '2024-09-03 16:53:00', '2024-09-03 16:54:00', '2024-09-03 16:55:00', '2024-09-03 16:56:00', '2024-09-03 16:58:00', '2024-09-03 16:59:00', '2024-09-03 17:02:00', '2024-09-03 17:03:00', '2024-09-03 17:06:00', '2024-09-03 17:07:00', '2024-09-03 17:09:00', '2024-09-03 17:11:00', '2024-09-03 17:12:00', '2024-09-03 17:13:00', '2024-09-03 17:15:00', '2024-09-03 17:18:00', '2024-09-03 17:19:00', '2024-09-03 17:20:00', '2024-09-03 17:22:00', '2024-09-03 17:24:00', '2024-09-03 17:25:00', '2024-09-03 17:28:00', '2024-09-03 17:29:00', '2024-09-03 17:33:00', '2024-09-03 17:34:00', '2024-09-03 17:36:00', '2024-09-03 17:38:00', '2024-09-03 17:41:00', '2024-09-03 17:43:00', '2024-09-03 17:45:00', '2024-09-03 17:48:00', '2024-09-03 17:49:00', '2024-09-03 17:52:00', '2024-09-03 17:55:00', '2024-09-03 17:56:00', '2024-09-03 17:59:00', '2024-09-03 18:00:06', '2024-09-03 18:05:00', '2024-09-03 18:07:00', '2024-09-03 18:08:00', '2024-09-03 18:11:00', '2024-09-03 18:12:00', '2024-09-03 18:17:00', '2024-09-03 18:19:00', '2024-09-03 18:20:00', '2024-09-03 18:22:00', '2024-09-03 18:24:00', '2024-09-03 18:31:00', '2024-09-03 18:32:00', '2024-09-03 18:33:00', '2024-09-03 18:36:00', '2024-09-04 12:24:00', '2024-09-04 12:26:00', '2024-09-04 12:27:00', '2024-09-04 12:30:06', '2024-09-04 12:31:00', '2024-09-04 12:33:00', '2024-09-04 12:38:00', '2024-09-04 12:41:00', '2024-09-04 12:46:00', '2024-09-04 12:49:00', '2024-09-04 12:51:00', '2024-09-04 12:52:00', '2024-09-04 12:54:00', '2024-09-04 12:56:00', '2024-09-04 12:58:00', '2024-09-04 12:59:00', '2024-09-04 13:00:06', '2024-09-04 13:02:00', '2024-09-04 13:03:00', '2024-09-04 13:05:00', '2024-09-04 13:07:00', '2024-09-04 13:09:00', '2024-09-04 13:12:00', '2024-09-04 13:13:00', '2024-09-04 13:14:00', '2024-09-04 13:15:00', '2024-09-04 13:17:00', '2024-09-04 13:19:00', '2024-09-04 13:21:00', '2024-09-04 13:22:00', '2024-09-04 13:24:00', '2024-09-04 13:25:00', '2024-09-04 13:27:00', '2024-09-04 13:29:00', '2024-09-04 13:30:07', '2024-09-04 13:31:00', '2024-09-04 13:32:00', '2024-09-04 13:36:00', '2024-09-04 13:37:00', '2024-09-04 13:40:00', '2024-09-04 13:48:00', '2024-09-04 13:53:00', '2024-09-04 13:57:00', '2024-09-04 13:58:00', '2024-09-04 13:59:00', '2024-09-04 14:00:06', '2024-09-04 14:02:00', '2024-09-04 14:03:00', '2024-09-04 14:04:00', '2024-09-04 14:05:00', '2024-09-04 14:08:00', '2024-09-04 14:10:00', '2024-09-04 14:12:00', '2024-09-04 14:15:00', '2024-09-04 14:17:00', '2024-09-04 14:18:00', '2024-09-04 14:19:00', '2024-09-04 14:22:00', '2024-09-04 14:23:00', '2024-09-04 14:24:00', '2024-09-04 14:25:00', '2024-09-04 14:26:00', '2024-09-04 14:27:00', '2024-09-04 14:29:00', '2024-09-04 14:30:06', '2024-09-04 14:32:00', '2024-09-04 14:33:00', '2024-09-04 14:35:00', '2024-09-04 14:38:00', '2024-09-04 14:39:00', '2024-09-04 14:42:00', '2024-09-04 14:45:00', '2024-09-04 14:47:00', '2024-09-04 14:49:00', '2024-09-04 14:50:00', '2024-09-04 14:51:00', '2024-09-04 14:52:00', '2024-09-04 14:54:00', '2024-09-04 14:56:00', '2024-09-04 14:58:00', '2024-09-04 15:00:07', '2024-09-04 15:02:00', '2024-09-04 15:04:00', '2024-09-04 15:05:00', '2024-09-04 15:06:00', '2024-09-04 15:07:00', '2024-09-04 15:10:00', '2024-09-04 15:11:00', '2024-09-04 15:12:00', '2024-09-04 15:13:00', '2024-09-04 15:15:00', '2024-09-04 15:16:00', '2024-09-04 15:17:00', '2024-09-04 15:19:00', '2024-09-04 15:21:00', '2024-09-04 15:22:00', '2024-09-04 15:24:00', '2024-09-04 15:25:00', '2024-09-04 15:27:00', '2024-09-04 15:28:00', '2024-09-04 15:30:06', '2024-09-04 15:31:00', '2024-09-04 15:32:00', '2024-09-04 15:33:00', '2024-09-04 15:35:00', '2024-09-04 15:37:00', '2024-09-04 15:38:00', '2024-09-04 15:39:00', '2024-09-04 15:41:00', '2024-09-04 15:42:00', '2024-09-04 15:43:00', '2024-09-04 15:44:00', '2024-09-04 15:47:00', '2024-09-04 15:49:00', '2024-09-04 15:51:00', '2024-09-04 15:53:00', '2024-09-04 15:54:00', '2024-09-04 15:55:00', '2024-09-04 15:58:00', '2024-09-04 16:02:00', '2024-09-04 16:04:00', '2024-09-04 16:06:00', '2024-09-04 16:07:00', '2024-09-04 16:10:00', '2024-09-04 16:11:00', '2024-09-04 16:12:00', '2024-09-04 16:13:00', '2024-09-04 16:15:00', '2024-09-04 16:16:00', '2024-09-04 16:17:00', '2024-09-04 16:18:00', '2024-09-04 16:19:00', '2024-09-04 16:20:00', '2024-09-04 16:23:00', '2024-09-04 16:24:00', '2024-09-04 16:26:00', '2024-09-04 16:27:00', '2024-09-04 16:28:00', '2024-09-04 16:29:00', '2024-09-04 16:32:00', '2024-09-04 16:33:00', '2024-09-04 16:36:00', '2024-09-04 16:38:00', '2024-09-04 16:39:00', '2024-09-04 16:43:00', '2024-09-04 16:44:00', '2024-09-04 16:46:00', '2024-09-04 16:48:00', '2024-09-04 16:49:00', '2024-09-04 16:51:00', '2024-09-04 16:52:00', '2024-09-04 16:53:00', '2024-09-04 16:56:00', '2024-09-04 16:59:00', '2024-09-04 17:01:00', '2024-09-04 17:02:00', '2024-09-04 17:04:00', '2024-09-04 17:08:00', '2024-09-04 17:09:00', '2024-09-04 17:10:00', '2024-09-04 17:12:00', '2024-09-04 17:13:00', '2024-09-04 17:15:00', '2024-09-04 17:17:00', '2024-09-04 17:22:00', '2024-09-04 17:23:00', '2024-09-04 17:24:00', '2024-09-04 17:26:00', '2024-09-04 17:28:00', '2024-09-04 17:29:00', '2024-09-04 17:30:06', '2024-09-04 17:33:00', '2024-09-04 17:34:00', '2024-09-04 17:36:00', '2024-09-04 17:37:00', '2024-09-04 17:38:00', '2024-09-04 17:39:00', '2024-09-04 17:40:00', '2024-09-04 17:44:00', '2024-09-04 17:45:00', '2024-09-04 17:48:00', '2024-09-04 17:51:00', '2024-09-04 17:52:00', '2024-09-04 17:54:00', '2024-09-04 17:57:00', '2024-09-04 17:58:00', '2024-09-04 18:00:06', '2024-09-04 18:01:00', '2024-09-04 18:02:00', '2024-09-04 18:03:00', '2024-09-04 18:04:00', '2024-09-04 18:05:00', '2024-09-04 18:06:00', '2024-09-04 18:07:00', '2024-09-04 18:08:00', '2024-09-04 18:09:00', '2024-09-04 18:10:00', '2024-09-04 18:13:00', '2024-09-04 18:14:00', '2024-09-04 18:17:00', '2024-09-04 18:18:00', '2024-09-04 18:20:00', '2024-09-04 18:21:00', '2024-09-04 18:24:00', '2024-09-04 18:30:06', '2024-09-04 18:43:00','2024-08-21 04:16:00'
            ]

source_flag_value = 3
source_reason = 'sensor malfungtion'
# Tworzenie nowych rekordów z dat
# z kluczami kolumn, np. 'PPFD_IN_1_1_1'

new_records_ME_TOP = [
    {
        'start': (datetime.strptime(dt, '%Y-%m-%d %H:%M:%S') - timedelta(seconds=15)).strftime('%Y-%m-%d %H:%M:%S'),
        'end': (datetime.strptime(dt, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=15)).strftime('%Y-%m-%d %H:%M:%S'),
        'flag_value': source_flag_value,
        'reason': source_reason
    }
    for dt in date_list_ME_TOP
]

# Dodanie nowych rekordów do listy jakościowej dla PPFD_IN_1_1_1
QUALITY_FLAGS['ME_TOP_QF']['PPFD_IN_1_1_1'].extend(new_records_ME_TOP)

# Wynikowy JSON
# print(json.dumps(QUALITY_FLAGS['ME_TOP_QF'], ensure_ascii=False, indent=2))

# 9. Mapowanie nazw zmiennych. Zmienia nazwy zmiennych z oryginalnych w loggerze na FLUXNET _H_V_R
# https://fluxnet.org/data/aboutdata/data-variables/
# Horizontal position (H): same value identifies the same position in the horizontal plane. 
# For example all the variables associated to sensors in a vertical profile would have the same H qualifier.

# Vertical position (V): indexes must be in order, starting from the highest (for example V=1 
# for the highest temperature sensor of a profile or for the higher, i.e. more superficial, 
# soil temperature sensor in a profile). The indexes are assigned on the basis of the relative 
# position for each vertical profile separately.
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
        'LWout_1_2_1': 'LW_OUT_1_1_1', # CNR4
        'Rn_1_2_1': 'RN_1_1_1', # CNR4
        # 'PPFD_BC_IN_1_1_1': 'PPFD_BC_IN_1_1_1', #
        # 'PPFD_BC_IN_1_1_2': 'PPFD_BC_IN_1_1_2', #
        'TA_2_2_1': 'TA_1_1_2', # CNR4
        'TA_1_2_1': 'TA_1_1_1', # rotronic
        'RH_1_2_1': 'RH_1_1_1', # rotronic
        'TA_1_1_1': 'TA_1_2_1', # rotronic
        'RH_1_1_1': 'RH_1_2_1', # rotronic
        # deszczomierze korytkowe
        'P_1_2_1': 'P_1_2_1', #down
        'P_1_1_2': 'P_1_2_2',
        'P_1_1_3': 'P_1_2_3',
        'P_1_1_4': 'P_1_2_4',
        'P_1_1_1': 'P_1_1_1', #top
        'P_2_1_2': 'P_1_1_2',
        'TS_10_1_1':'TS_1_1_1',
        'TS_10_2_1':'TS_1_2_1',
        'TS_6_1_1':'TS_1_1_2',
        'TS_6_2_1':'TS_1_2_2',
        'TS_7_1_1':'TS_1_1_3',
        'TS_7_2_1':'TS_1_2_3',
        'TS_8_1_1':'TS_1_1_4',
        'TS_8_2_1':'TS_1_2_4',
        'TS_9_1_1':'TS_1_1_5',
        'TS_9_2_1':'TS_1_2_5', 
        # 'RTD_1_AV (degC)': 'TS_6_1_1',
        # 'RTD_2_AV (degC)': 'TS_6_2_1',
        # 'RTD_3_AV (degC)': 'TS_7_1_1',
        # 'RTD_4_AV (degC)': 'TS_7_2_1',
        # 'RTD_5_AV (degC)': 'TS_8_1_1',
        # 'RTD_6_AV (degC)': 'TS_8_2_1',
        # 'RTD_7_AV (degC)': 'TS_9_1_1',
        # 'RTD_8_AV (degC)': 'TS_9_2_1',
        # 'RTD_9_AV (degC)': 'TS_10_1_1',
        # 'RTD_10_AV (degC)': 'TS_10_2_1',
        # SWC, G i TS zdefiniowane poprawnie w loggerze - bez mapowania nazw
    },
    'MEZYK_Prec_down_MAP': {
        'Precipitationamountmm': 'P_1_2_1', # Dolny
        'Precipitation amount [mm': 'P_1_2_1', # Dolny
    },
    'MEZYK_Prec_top_MAP': {
        'Precipitationamountmm': 'P_1_1_1', # Górny
        'Precipitation amount [mm': 'P_1_1_1', # Górny
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
        'T_1_1_1_Avg': 'TA_1_1_1'
    },
    # Tlen1 old site added by Klaudia- 19.07.2025
    'TLEN1_MAP': {
        # Soil heat plates Hukseflux HFP 01
        'G_1_1_1_Avg': 'G_1_1_1',
        'G_2_1_1_Avg': 'G_1_1_2',
        'G_3_1_1_Avg': 'G_1_1_3',
        'G_4_1_1_Avg': 'G_1_1_4',
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
    'TLEN1_RAD_MAP': {
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
        'NR01TC_Avg': 'TA_1_1_2', # NR01
        # SKR1850A/SKYE (po naprawie i kalibracji: SKR1860 ND/A)
        'MSR_Avg' : 'MSR_1_1_1',
        'NDVI_Avg' : 'NDVI_1_1_1',
        'PRI_Avg': 'PRI_1_1_1',
        'SAVI_Avg': 'SAVI_1_1_1',
        'SR_Avg': 'SR_1_1_1',
        },
    'TLEN1a_MAP': {
        'PPFDr_1_2_1': 'PPFD_OUT_2_1_1', # PQS1
        'PPFD_1_2_1': 'PPFD_IN_2_1_1', # PQS1
        'SWin_1_2_1': 'SW_IN_2_1_1', # CNR4
        'SWout_1_2_1': 'SW_OUT_2_1_1', # CNR4
        'PPFD_1_1_1': 'PPFD_IN_2_1_1', # PQS1
        'SWIN_1_2_1': 'SW_IN_2_1_1', # CNR4
        'SWOUT_1_2_1': 'SW_OUT_2_1_1', # CNR4
        'LWin_1_2_1': 'LW_IN_2_1_1', # CNR4
        'LWout_1_2_1': 'LW_OUT_2_1_1', # CNR4
        'Rn_1_2_1': 'RN_2_1_1', # CNR4
        'PPFD_BC_IN_1_1_1': 'PPFD_BC_IN_2_1_1', ##LI191
        'PPFD_BC_IN_1_1_2': 'PPFD_BC_IN_2_1_2', ##LI191
        'TA_2_2_1': 'TA_2_1_2', # CNR4
        'P_1_1_1': 'P_2_1_1', # deszczomierz laserowy LPM, Thies
        'P_1_2_1': 'P_2_1_2', # deszczomierz korytkowy 1  RM Young (52202H)
        'P_1_3_1': 'P_2_1_3', # deszczomierz korytkowy 2  RM Young (52202H)
        'TA_1_1_1': 'TA_2_1_1', #Termohigrometry HYGROvue5/ Rotronic? ??? air temp. at 2m above ground 
        'TA_1_2_1': 'TA_2_2_1', #Termohigrometry HYGROvue5/ Rotronic? ??? air temp. at 30cm above ground
        'RH_1_1_1': 'RH_2_1_1', #Termohigrometry HYGROvue5/ Rotronic? ??? air humidity at 2m above ground
		'RH_1_2_1': 'RH_2_2_1',#Termohigrometry HYGROvue5/ Rotronic? ???  air humidity at 30cm above ground
        'TA_2_2_1_Avg': 'TA_2_2_1',#CNR4 Temp.
		# płytki glebowe Hukseflux model: HFP01SC-20 at 5cm depth
        'G_1_1_1': 'G_2_1_1',
        'G_2_1_1': 'G_2_1_2',
        'G_3_1_1': 'G_2_1_3',
        'G_4_1_1': 'G_2_1_4',
        'G_5_1_1': 'G_2_1_5',
        'G_6_1_1': 'G_2_1_6',
        'G_7_1_1': 'G_2_1_7',
        'G_8_1_1': 'G_2_1_8',
        'G_9_1_1': 'G_2_1_9',
        'G_10_1_1': 'G_2_1_10',
        'G_1_1_1_Avg': 'G_2_1_1',
        'G_2_1_1_Avg': 'G_2_1_2',
        'G_3_1_1_Avg': 'G_2_1_3',
        'G_4_1_1_Avg': 'G_2_1_4',
        'G_5_1_1_Avg': 'G_2_1_5',
        'G_6_1_1_Avg': 'G_2_1_6',
        'G_7_1_1_Avg': 'G_2_1_7',
        'G_8_1_1_Avg': 'G_2_1_8',
        'G_9_1_1_Avg': 'G_2_1_9',
        'G_10_1_1_Avg': 'G_2_1_10',
		# Temperatura i wilgotność gleby
        'SWC_1_1_1_Avg': 'SWC_2_1_1',# wilgotność gleby na 5 cm Czujniki wilgotności i temperstury gleby Acclima (ACC-SEN-SDI) - lokalizacja BRUZDA (w wartstwie mineralnej gleby otoczonej organiczną)
        'SWC_2_1_1_Avg': 'SWC_2_1_2',# wilgotność gleby na 5 cm Czujniki wilgotności i temperstury gleby Acclima (ACC-SEN-SDI) - lokalizacja BRUZDA (w wartstwie mineralnej gleby otoczonej organiczną)
        'SWC_3_1_1_Avg': 'SWC_2_1_3',# wilgotność gleby na 5 cm Czujniki wilgotności i temperstury gleby Acclima (ACC-SEN-SDI) - gleba mineralna
        'SWC_4_1_1_Avg': 'SWC_2_1_4',# wilgotność gleby na 5 cm Czujniki wilgotności i temperstury gleby Acclima (ACC-SEN-SDI) - gleba mineralna
        'SWC_5_1_1_Avg': 'SWC_2_1_5',# wilgotność gleby na 5 cm Czujniki wilgotności i temperstury gleby Acclima (ACC-SEN-SDI) - gleba mineralna
        'SWC_1_1_1': 'SWC_2_1_1',
        'SWC_2_1_1': 'SWC_2_1_2',
        'SWC_3_1_1': 'SWC_2_1_3',
        'SWC_4_1_1': 'SWC_2_1_4',
        'SWC_5_1_1': 'SWC_2_1_5',
        'TS_1_1_1': 'TS_2_1_1',# temperatura gleby na 5 cm Czujniki wilgotności i temperstury gleby Acclima (ACC-SEN-SDI) - lokalizacja BRUZDA (w wartstwie mineralnej gleby otoczonej organiczną)
        'TS_2_1_1': 'TS_2_1_2',# temperatura gleby na 5 cm Czujniki wilgotności i temperstury gleby Acclima (ACC-SEN-SDI) - lokalizacja BRUZDA (w wartstwie mineralnej gleby otoczonej organiczną)
        'TS_3_1_1': 'TS_2_1_3',# temperatura gleby na 5 cm Czujniki wilgotności i temperstury gleby Acclima (ACC-SEN-SDI) - gleba mineralna
        'TS_4_1_1': 'TS_2_1_4',# temperatura gleby na 5 cm Czujniki wilgotności i temperstury gleby Acclima (ACC-SEN-SDI) - gleba mineralna
        'TS_5_1_1': 'TS_2_1_5',# temperatura gleby na 5 cm Czujniki wilgotności i temperstury gleby Acclima (ACC-SEN-SDI) - gleba mineralna
        'TS_1_1_1_Avg': 'TS_2_1_1',
        'TS_2_1_1_Avg': 'TS_2_1_2',
        'TS_3_1_1_Avg': 'TS_2_1_3',
        'TS_4_1_1_Avg': 'TS_2_1_4',
        'TS_5_1_1_Avg': 'TS_2_1_5',
		# Opad
        'Precipitationamountmm': 'P_2_1_1',# deszczomierz laserowy LPM, Thies
        'Precipitation amount [mm': 'P_2_1_1',# deszczomierz laserowy LPM, Thies
		# Dodatkowe czujniki temp. i wilgotności powietrza
        'Ta_1ROT_Avg': 'TA_2_1_2', #Termohigrometry Alfa Tech/Rotronic, HYGRO CLIP HC2-S3 air temp. at ca. 1,3 m above ground *considered as 2m measurements repetition?
        'Ta_2ROT_Avg': 'TA_2_1_3', #Termohigrometry Alfa Tech/Rotronic, HYGRO CLIP HC2-S3 air temp. at ca. 1,3 m a above ground *considered as 2m measurements repetition?
        'RH_1ROT_Avg': 'RH_2_1_2', #Termohigrometry Alfa Tech/Rotronic, HYGRO CLIP HC2-S3 air humidity at ca. 1,3 m a above ground *considered as 2m measurements repetition?
        'RH_2ROT_Avg': 'RH_2_1_3', #Termohigrometry Alfa Tech/Rotronic, HYGRO CLIP HC2-S3 air humidity at ca. 1,3 m a above ground *considered as 2m measurements repetition?
        },
        # Tlen2 "old" tower added by Klaudia- 19.07.2025 and "NEW" tower adjustment (added by Klaudia- 08.10.2025)
    'TLEN2_MAP': {
        'PPFD_1_1_1': 'PPFD_IN_1_1_1', # SKL 2620 incoming PPDF
        #PPFD Radiation – SKL 2620 operated on the "OLD tower" CR1000 Campbell datalogger from 2014-07-08 16:30:0 until the end of the meteo instruments operation- demounted on 2018-09-05 05:30:00.
        'PPFD_1_1_1_Avg':'PPFD_IN_1_1_1',  # SKL 2620 incoming PPDF
        'PPFDr_1_1_1_Avg': 'PPFD_OUT_1_1_1',  #SKL 2620 reflected PPDF
    	#PPFD Radiation – PQS1 operated on the "new tower" - DataTacker datalogger from 2018-08-01 16:30:00 until 2024-07-2024 14:00:00 (datalogger replaced by the CR1000X datalogger). Assumed as the SAME location (REPETITION)
        'PPFD_1_2_1': 'PPFD_IN_1_1_2', # PQS1 
        'PPFDr_1_2_1': 'PPFD_OUT_1_1_2', # PQS1
        #NR01 4-component net radiometer measurements operated on the "OLD tower" CR1000 Campbell datalogger from 2014-07-08 16:30:0 until the end of the meteo instruments operation- demounted on 2018-09-05 05:30:00.
        'SWin_1_1_1_Avg': 'SW_IN_1_1_1', # NR01
        'SWout_1_1_1_Avg': 'SW_OUT_1_1_1', # NR01
        'LWin_1_1_1_Avg': 'LW_IN_1_1_1', # NR01
        'LWout_1_1_1_Avg': 'LW_OUT_1_1_1', # NR01
        'NR01TC_Avg': 'TA_1_1_2', # NR01
        #CNR4 4-component net radiometer measurements operated on the "NEW tower" - DataTacker datalogger from 2018-08-01 16:30:00 until 2024-07-2024 14:00:00 (datalogger replaced by the CR1000X datalogger) and since then still working
        'SWin_1_2_1': 'SW_IN_1_1_2', # CNR4
        'SWout_1_2_1': 'SW_OUT_1_1_2', # CNR4
        'LWin_1_2_1': 'LW_IN_1_1_2', # CNR4
        'LWout_1_2_1': 'LW_OUT_1_1_2', # CNR4
        'Rn_1_2_1': 'RN_1_1_2', # CNR4
        'Ta_2_2_1_Avg': 'TA_2_1_2', # CNR4
        'TA_2_2_1': 'TA_2_1_2', # CNR4
        # Below canopy PPFD radiation - NEW tower
        'PPFD_BC_IN_1_1_1': 'PPFD_BC_IN_1_1_1', #LI191 -NEED CHANGE!!! (_1_1_1)?
        'PPFD_BC_IN_1_1_2': 'PPFD_BC_IN_1_1_2', #LI191  -NEED CHANGE!!! (_1_1_2)?
        # Soil heat plates Hukseflux HFP 01- all in ca. 5 cm depth?- OLD tower
        'G_1_1_1_Avg': 'G_1_1_1',
        'G_2_1_1_Avg': 'G_1_1_2',
        'G_3_1_1_Avg': 'G_1_1_3',
        'G_4_1_1_Avg': 'G_1_1_4',
        # Soil heat plates Hukseflux HFP01SC-20 at 5cm depth - all in ca. 5 cm depth?- NEW tower - INSTALLED AT THE SAME SITE (LOCATION) AS Hukseflux HFP 01- all in ca. 5 cm depth (treated ad REPETITIONS?)
        'G_1_1_1': 'G_1_1_5', 
        'G_2_1_1': 'G_1_1_6', 
        'G_3_1_1': 'G_1_1_7',
        'G_4_1_1': 'G_1_1_8',
        'G_5_1_1': 'G_1_1_9', 
        'G_6_1_1': 'G_1_1_10', 
        'G_7_1_1': 'G_1_1_11', 
        'G_8_1_1': 'G_1_1_12', 
        'G_9_1_1': 'G_1_1_13',  
        'G_10_1_1': 'G_1_1_14', 
        'G_1_1_1_Avg': 'G_1_1_5',# noqa: F601
        'G_2_1_1_Avg': 'G_1_1_6', # noqa: F601
        'G_3_1_1_Avg': 'G_1_1_7', # noqa: F601
        'G_4_1_1_Avg': 'G_1_1_8', # noqa: F601
        'G_5_1_1_Avg': 'G_1_1_9', 
        'G_6_1_1_Avg': 'G_1_1_10', 
        'G_7_1_1_Avg': 'G_1_1_11',
        'G_8_1_1_Avg': 'G_1_1_12', 
        'G_9_1_1_Avg': 'G_1_1_13',  
        'G_10_1_1_Avg': 'G_1_1_14',
        # Soil moisture, 1 profile (10, 30, 50cm)- CS616- OLD tower
        'VW_1': 'SWC_1_2_1', # CS616 -10cm 
        'VW_2': 'SWC_1_3_1',  # CS616 -30cm 
        'VW_3': 'SWC_1_4_1', # CS616 -50cm
        # Soil Temperature- T107 soil thermometer - OLD tower
        # profile 1- OLD tower
        'Ts_1_1_1_Avg': 'TS_1_1_1', # soil profile 1 – 2cm depth
        'Ts_1_2_1_Avg': 'TS_1_2_1',  # soil profile 1 – 5cm depth
        'Ts_1_3_1_Avg': 'TS_1_3_1',   # soil profile 1 – 10cm depth
        'Ts_1_4_1_Avg': 'TS_1_4_1',  # soil profile 1 – 30cm depth
        'Ts_1_5_1_Avg': 'TS_1_5_1',  # soil profile 1 – 50cm depth
        # profile 2- OLD tower
        'Ts_2_1_1_Avg': 'TS_2_1_1',  # soil profile 2 – 2cm depth
        'Ts_2_2_1_Avg': 'TS_2_2_1',  # soil profile 2 – 5cm depth
        'Ts_2_3_1_Avg': 'TS_2_3_1',  # soil profile 2 – 10cm depth
        'Ts_2_4_1_Avg': 'TS_2_4_1',  # soil profile 2 – 30cm depth
        'Ts_2_5_1_Avg': 'TS_2_5_1',  # soil profile 2 – 500cm depth
        # Soil moisture and temperature at 5cm depth- Acclima (ACC-SEN-SDI) sensor- NEW tower-INSTALLED AT THE SAME SITLE (LOCATION) AS CS616 and T107 soil thermometer at ca. 5 cm depth (REPETITIONS?)
        'SWC_1_1_1': 'SWC_1_1_1',  
        'SWC_2_1_1': 'SWC_1_1_2',  
        'SWC_3_1_1': 'SWC_1_1_3', 
        'SWC_4_1_1': 'SWC_1_1_4',
        'SWC_5_1_1': 'SWC_1_1_5', 
        'TS_1_1_1': 'TS_3_2_1',  # na 5 cm = poziom głębokości "2"
        'TS_2_1_1': 'TS_3_2_2', 
        'TS_3_1_1': 'TS_3_2_3', 
        'TS_4_1_1': 'TS_3_2_4',
        'TS_5_1_1': 'TS_3_2_5',
        'SWC_1_1_1_Avg': 'SWC_1_1_1', 
        'SWC_2_1_1_Avg': 'SWC_1_1_2',  
        'SWC_3_1_1_Avg': 'SWC_1_1_3', 
        'SWC_4_1_1_Avg': 'SWC_1_1_4', 
        'SWC_5_1_1_Avg': 'SWC_1_1_5', 
        'TS_1_1_1_Avg': 'TS_3_2_1', 
        'TS_2_1_1_Avg': 'TS_3_2_2', 
        'TS_3_1_1_Avg': 'TS_3_2_3', 
        'TS_4_1_1_Avg': 'TS_3_2_4',
        'TS_5_1_1_Avg': 'TS_3_2_5',
        # Precipitation measurements- forest floor – Tipping Rain gauges  A-ster TPG- OLD tower
        'P_rain_1_1_1_Tot': 'P_1_1_1', #Rain gauge 1
        'P_rain_1_2_1_Tot':'P_1_1_2', #Rain gauge 2
    	# Precipitation measurements- forest floor – Tipping Rain gauges RM Young (52202H)- NEW tower (located near Aster rain gauges)
        'P_1_2_1': 'P_1_1_3', #Rain gauge 1 
        'P_1_3_1': 'P_1_1_4', #Rain gauge 2
        # Air temperature and humidity- HMP 155, Vaisala- OLD tower
        'Ta_1_1_1_Avg': 'TA_1_1_1',  # HMP 155 air temp. at 2m above ground
        'RH_1_1_1_Avg': 'RH_1_1_1',  # HMP 155 air temp. at 2m above ground
        'Ta_1_2_1_Avg': 'TA_1_2_1',  # HMP 155 air temp. at 30cm above ground
        'RH_1_2_1_Avg': 'RH_1_2_1',  # HMP 155 air temp. at 30cm above ground
        # Air temperature and humidity- Alfa Tech/Rotronic, HYGRO CLIP HC2-S3 - NEW tower
        'TA_1_1_1': 'TA_1_1_2', #air temp. at 2m above ground
        'TA_1_2_1': 'TA_1_2_2', #air temp. at 30cm above ground
        'RH_1_1_1': 'RH_1_1_2', #air humidity at 2m above ground
        'RH_1_2_1': 'RH_1_2_2', #air humidity at 30cm above ground
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
    'ME_Rain_down': 'MEZYK_Prec_down_MAP',
    'ME_Rain_top': 'MEZYK_Prec_top_MAP',
    # ----- SARBIA -----
    'SA_MET_30min': 'SARBIA_MAP',
    'SA_MET_1min': 'SARBIA_MAP',
    # ----- TLEN1 -----
    'TL1_MET_30' : 'TLEN1_MAP',
    'TL1_SOIL_30' : 'TLEN1_MAP',
    'TL1_RAD_30' : 'TLEN1_RAD_MAP',
    'TL1_RAD_1' : 'TLEN1_RAD_MAP',
    # ----- TLEN1a -----
    'TL1a_MET_30_dT': 'TLEN1a_MAP',
    'TL1a_Rain_down_dT': 'TLEN1a_MAP',
    'TL1a_MET_1_dT': 'TLEN1a_MAP',
    'TL1a_CalPlates_dT': 'TLEN1a_MAP',
    'TL1a_MET_30_csi': 'TLEN1a_MAP',
    'TL1a_MET_1_csi': 'TLEN1a_MAP',
    # ----- TLEN2 -----
    # 'TL2_MET_1m' : 'TLEN2_MAP',
    'TL2_MET_30m': 'TLEN2_MAP',
    'TL2_MET_1_dT': 'TLEN2_MAP',
    'TL2_MET_30_dT': 'TLEN2_MAP',
    'TL2_MET_30_csi': 'TLEN2_MAP',
    'TL2_MET_1_csi': 'TLEN2_MAP',
}
# 11. Automatyczne przypisanie flag jakości na podstawie zakresów (działa po kalibracji i zmianie nazw)
VALUE_RANGE_FLAGS = {
    # Ta reguła zostanie zastosowana do wszystkich kolumn zaczynających się na 'TA'
    # np. TA_1_1_1, TA_1_1_2, TA_2_1_1 itd.
    'TA_': {'min': -39, 'max': 45},
    'air_temperature': {'min': 234, 'max': 313},
    'RH_': {'min': 10, 'max': 105},
    'TS_': {'min': -30, 'max': 60},
    'T107_': {'min': -30, 'max': 60},
    'SW_IN_': {'min': -10, 'max': 1000},
    'SW_OUT_': {'min': -10, 'max': 250},
    'LW_IN_': {'min': 150, 'max': 600},
    'LW_OUT_': {'min': 150, 'max': 600},
    'RN_': {'min': -200, 'max': 900},
    'PPFD_IN_': {'min': -20, 'max': 3000},
    'PPFD_BC_IN_': {'min': -20, 'max': 2000},
    'PPFDBC_IN_': {'min': -20, 'max': 2000},
    'PPFDd': {'min': -20, 'max': 2000},
    'PPFDd_': {'min': -20, 'max': 2000},
    'PPFD_DIF': {'min': -20, 'max': 2000},
    'PPFD_OUT_': {'min': -20, 'max': 500},
    'PPFDr_': {'min': -20, 'max': 500},
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















