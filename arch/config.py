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
--------------------------------------------------------------------------------
"""

# 1. SŁOWNIK WSPÓŁRZĘDNYCH STACJI
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

# 2. SŁOWNIK GRUPUJĄCY PLIKI ŹRÓDŁOWE
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
    'TL1_MET_30': {"source_ids": ["Meteo_TR_30min"], "interval": "30m"},
    'TL1_RAD_30': {"source_ids": ["Rad_TR_30min"], "interval": "30m"},
    'TL1_SOIL_30': {"source_ids": ["Soil_TR_30min"], "interval": "30m"},
    'TL1_RAD_1': {"source_ids": ["Rad_TR_1min"], "interval": "1m"},
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
    'SA_MET_30min': { 'source_ids': [ 'SA_biomet_Biomet', '_cnf4_data'], 'interval': '30min' },
    'SA_MET_1min': { 'source_ids': [ 'SA_biomet_Meteo_1min'], 'interval': '1min' }
}

# 3. SŁOWNIK KOREKTY STREF CZASOWYCH
TIMEZONE_CORRECTIONS = {
    # 1.1. Definicja "konfiguracji-matki" dla stacji TU
    'TU_TZSHIFT': { 
        'source_tz': 'Europe/Warsaw', # Nazwa strefy czasowej, w której rejestrator zapisywał dane przed datą poprawki (np. 'Europe/Warsaw'). Używanie nazw z bazy IANA (jak Europe/Warsaw) jest kluczowe, ponieważ automatycznie obsługują one zarówno czas zimowy (CET, UTC+1), jak i letni (CEST, UTC+2).
        'correction_end_date': '2011-05-27 09:00:00', # Data i godzina, po której dane są już zapisywane poprawnie. Skrypt zastosuje specjalną korektę tylko do danych z timestampami wcześniejszymi lub równymi tej dacie.
        'post_correction_tz': 'Etc/GMT-1', # Strefa czasowa "poprawnych" danych (tych po correction_end_date). 
        'target_tz': 'Etc/GMT-1' # Docelowa, jednolita strefa czasowa dla wszystkich danych w plikach wynikowych. ("UTC/GMT +1 godzina"), użyjemy 'Etc/GMT-1'. (Uwaga: notacja Etc/GMT ma odwrócony znak, więc Etc/GMT-1 oznacza UTC+1).
    },
    
   # 1.2. Poszczególne file_id, które wskazują na wspólną konfigurację
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

    # 2.1. Definicja "konfiguracji-matki" dla stacji TL1
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
    
    # 3.1. Definicja "konfiguracji-matki" dla stacji TL1a 
    'TL1a_TZSHIFT': {
        'source_tz': 'Etc/GMT-2',
        'correction_end_date': '2050-05-10 12:00:00', 
        'post_correction_tz': 'Etc/GMT-1',
        'target_tz': 'Etc/GMT-1'
    },
    # 3.2. Definicja "konfiguracji-matki" dla stacji TL1a 
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
     'ME_MET_10m' : 'ME_TZSHIFT',
    
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

# 4. SŁOWNIK RĘCZNYCH PRZESUNIĘĆ CZASOWYCH
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
    # 'ME_DOWN_MET_30min' : 'ME_MTSHIFT', 'ME_DOWN_MET_1min' : 'ME_MTSHIFT',
    # 'ME_Rain_down' : 'ME_MTSHIFT', 'ME_CalPlates' : 'ME_MTSHIFT',
    'ME_TOP_MET_30min' : 'ME_MTSHIFT', 'ME_DOWN_MET_30min' : 'ME_MTSHIFT', 'ME_Rain_down' : 'ME_MTSHIFT',
    'ME_DOWN_MET_1min' : 'ME_MTSHIFT', 'ME_DOWN_MET_30min' : 'ME_MTSHIFT', 'ME_Rain_top' : 'ME_MTSHIFT',
    'ME_CalPlates' : 'ME_MTSHIFT', 'ME_MET_10m' : 'ME_MTSHIFT',
    
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

# 5. SŁOWNIK KALIBRACJI
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
        'SW_IN_1_1_1'' : [
            {'start':  '25-Apr-2013 17:30:00', 'end' : '9-Jul-2014 17:30:00', 'multiplier': 1.1658, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000- NR01 poprawne współczynniki i lokalizacja kanałów pomiarowych'},
        ],
        'SW_OUT_1_1_1'' : [
            {'start':  '25-Apr-2013 17:30:00', 'end' : '9-Jul-2014 17:30:00', 'multiplier': 0.6935, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000- NR01 poprawne współczynniki i lokalizacja kanałów pomiarowych'},
        ],
    	'LW_IN_1_1_1'' : [
            {'start':  '25-Apr-2013 17:30:00', 'end' : '9-Jul-2014 17:30:00', 'multiplier': 1.2976, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000- NR01 poprawne współczynniki i lokalizacja kanałów pomiarowych'},
        ],
        'LW_OUT_1_1_1'' : [
            {'start':  '25-Apr-2013 17:30:00', 'end' : '9-Jul-2014 17:30:00', 'multiplier': 0.9750, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000- NR01 poprawne współczynniki i lokalizacja kanałów pomiarowych'},
        ],
	#Soil heat flux measurements
        'G_1_1_1': [
            {'start': '25-Apr-2013 17:30:00', 'end': '17-Jun-2014 08:00:00', 'multiplier': 16.07, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'},
            {'start': '24-Jun-2014 02:30:00', 'end': '9-Jul-2014 22:00:00', 'multiplier': 16.07, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'}
        ],
         'G_1_2_1': [
            {'start': '25-Apr-2013 17:30:00', 'end': '17-Jun-2014 08:00:00', 'multiplier': 16.06, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'},
            {'start': '24-Jun-2014 02:30:00', 'end': '9-Jul-2014 22:00:00', 'multiplier': 16.06, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'}
        ],
        'G_1_3_1': [
            {'start': '25-Apr-2013 17:30:00', 'end': '17-Jun-2014 08:00:00', 'multiplier': 16.29, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'},
            {'start': '24-Jun-2014 02:30:00', 'end': '9-Jul-2014 22:00:00', 'multiplier': 16.29, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'}
        ],
        'G_1_4_1': [
            {'start': '25-Apr-2013 17:30:00', 'end': '17-Jun-2014 08:00:00', 'multiplier': 16.29, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'},
            {'start': '24-Jun-2014 02:30:00', 'end': '9-Jul-2014 22:00:00', 'multiplier': 16.99, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'}
        ],
    ## Tlen1a site - dataTacker measurements (03.08.2018- 12.06.2024) - TO BE COMPLETED (what are those??? )
    'TL1dT_CAL': {
        'SWin_1_1_1' : [
            {'start':  '2014-07-08 09:30:00', 'end' : '2018-11-13 9:30:00', 'multiplier': 77.101, 'addend': 0, 'reason': 'NR01 (data in W/m2)'},
        ],
        'PPFD_1_1_1' : [
            {'start':  '2014-07-08 09:30:00', 'end' : '2018-11-13 9:30:00', 'multiplier': 210.97, 'addend': 0, 'reason': 'PQ1 (data in umol/m2/s)'},
        ]
    },
    ## Tlen2 "old" tower added by Klaudia- 19.07.2025  
     'TL2_CAL': {
        'G_1_1_1': [
            {'start': '10-Jul-2014 9:30:00', 'end': '18-Jul-2014 1:30:00', 'multiplier': 1, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'}
        ],
         'G_1_2_1': [
            {'start': '10-Jul-2014 9:30:00', 'end': '18-Jul-2014 1:30:00', 'multiplier':  0.0625, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'}
        ],
        'G_1_3_1': [
            {'start': '10-Jul-2014 9:30:00', 'end': '18-Jul-2014 1:30:00', 'multiplier': 16.008, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'}
        ],
        'G_1_4_1': [
            {'start': '10-Jul-2014 9:30:00', 'end': '18-Jul-2014 1:30:00', 'multiplier': 1, 'addend': 0, 'reason': '%zmiana programu Campbel CR1000 -współczynniki do płytek glebowych'}
        ],
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
            {'start':  '2014-07-08 09:30:00', 'end' : '2018-11-16 12:30:00', 'multiplier': 213.675, 'addend': 0, 'reason': 'PQ1 (data in umol/m2/s)'},
        ]
    }
}

# 6. SŁOWNIK MAPOWANIA GRUP NA REGUŁY KALIBRACJI
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
    # Tlen1a site - dataTacker measurements (03.08.2018- 12.06.2024)   
    'TL1a_MET_30_dT' : 'TL1adT_CAL',
    'TL1a_MET_1_dT' : 'TL1adT_CAL',
    # 'TL1a_Rain_down_dT' : 'TL1adT_CAL', # not need calibration
    # Tlen2 "old" tower added by Klaudia- 19.07.2025 
    'TL2_MET_1m' : 'TL2_CAL',
    'TL2_MET_30m': 'TL2_CAL',
    # Tlen2 site - "new" tower - dataTacker measurements (01.08.2018- 26.06.2024)
    'TL2_MET_1_dT' : 'TL2dT_CAL',
    'TL2_MET_30_dT' : 'TL2dT_CAL',
}

# 7. SŁOWNIK FLAG JAKOŚCI
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
        ],
        'PPFD_BC_IN_1_1_2': [
            {'start': '2018-01-01 00:00:00', 'end': '2018-05-14 10:30:00', 'flag_value': 3, 'reason': 'brak sensora'},
        ],
        'PPFD_IN_1_2_1': [
            {'start': '2018-01-01 00:00:00', 'end': '2018-05-14 10:30:00', 'flag_value': 3, 'reason': 'brak sensora'},
        ],
        'PPFD_OUT_1_2_1': [
            {'start': '2018-01-01 00:00:00', 'end': '2018-05-14 10:30:00', 'flag_value': 3, 'reason': 'brak sensora'},
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
            {'start': '29-Mar-2013 17:30:00', 'end': '25-Apr-2013 17:30:00', 'flag_value': 3, 'reason': 'Delay in meteo sensors intalation in relation to flux data'},
            {'start': '14-Jul-2021 23:00:00', 'end': '20-Jul-2021 09:30:00', 'flag_value': 3, 'reason': 'Storm damage'},
        ],
	# Radiation fluxes  
        'PPFD_IN_1_1_1':[
            {'start': '16-Jun-2014 00:00:00', 'end': '26-Aug-2014 12:00:00', 'flag_value': 3, 'reason': 'condensation inside te sensor dome BF3/BF5H- deinstalled'},
            {'start': '08-Dec-2015 12:00:00', 'end': '13-Jan-2016 12:00:00', 'flag_value': 3, 'reason': 'condensation inside te sensor dome BF3/BF5H- deinstalled 2'},
            {'start': '02-Dec-2016 10:30:00', 'end': '13-Feb-2017 12:30:00', 'flag_value': 3, 'reason': 'condensation inside te sensor dome BF3/BF5H- deinstalled 3'},
            {'start': '18-Oct-2017 10:00:00', 'end': '10-Apr-2018 16:00:00', 'flag_value': 3, 'reason': 'condensation inside te sensor dome BF3/BF5H- deinstalled 4'},
			{'start': '12-Aug-2019', 'end': 'now', 'flag_value': 3, 'reason': 'BF5 sensor deinstalled for good'},		
        ],
        'PPFD_DIF_1_1_1':[
            {'start': '16-Jun-2014 00:00:00', 'end': '26-Aug-2014 12:00:00', 'flag_value': 3, 'reason': 'condensation inside te sensor dome BF3/BF5H- deinstalled'},
            {'start': '08-Dec-2015 12:00:00', 'end': '13-Jan-2016 12:00:00', 'flag_value': 3, 'reason': 'condensation inside te sensor dome BF3/BF5H- deinstalled 2'},
            {'start': '02-Dec-2016 10:30:00', 'end': '13-Feb-2017 12:30:00', 'flag_value': 3, 'reason': 'condensation inside te sensor dome BF3/BF5H- deinstalled 3'},
            {'start': '18-Oct-2017 10:00:00', 'end': '10-Apr-2018 16:00:00', 'flag_value': 3, 'reason': 'condensation inside te sensor dome BF3/BF5H- deinstalled 4'},
			{'start': '12-Aug-2019', 'end': 'now', 'flag_value': 3, 'reason': 'BF5 sensor deinstalled for good'},		
        ],
        'PPFD_IN_1_1_2':[
            {'start': '25-Apr-2013 17:30:00', 'end': '13-Jan-2014 10:30:00', 'flag_value': 2, 'reason': 'incorrect SKP215 readings?'}
            {'start': '14-Nov-2022', 'end': 'now', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'PPFD_OUT_1_1_1':[
            {'start': '25-Apr-2013 17:30:00', 'end': '13-Jan-2014 10:30:00', 'flag_value': 2, 'reason': 'incorrect SKP215 readings?'}
            {'start': '14-Nov-2022', 'end': 'now', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'SW_IN_1_1_1':[
            {'start': '14-Nov-2022', 'end': 'now', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'SW_OUT_1_1_1':[
            {'start': '14-Nov-2022', 'end': 'now', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'LW_IN_1_1_1':[
            {'start': '14-Nov-2022', 'end': 'now', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'LW_OUT_1_1_1':[
            {'start': '14-Nov-2022', 'end': 'now', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
	#Air temperature and humidity
        'TA_1_1_1':[
            {'start': '25-Apr-2013 17:30:00', 'end': '17-May-2013 12:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '14-Nov-2022', 'end': 'now', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'RH_1_1_1':[
            {'start': '25-Apr-2013 17:30:00', 'end': '17-May-2013 12:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '14-Nov-2022', 'end': 'now', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'TA_1_2_1':[
            {'start': '25-Apr-2013 17:30:00', 'end': '04-Apr-2014 11:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '14-Nov-2022', 'end': 'now', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'RH_1_2_1':[
            {'start': '25-Apr-2013 17:30:00', 'end': '04-Apr-2014 11:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '14-Nov-2022', 'end': 'now', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ]
    },
    # Tlen2 "old" tower added by Klaudia- 19.07.2025
    'TL2_QF': {
     '*': [
            {'start': '13-Apr-2018 21:30:00', 'end': '10-May-2018 16:30:00', 'flag_value': 3, 'reason': 'datalogger power source mulfuntion'},
            {'start': '19-Jul-2018 21:00:00', 'end': '27-Jul-2018 19:30:00', 'flag_value': 3, 'reason': 'power cut- damage'},
            {'start': '30-Dec-2020 01:30:00', 'end': '28-Apr-2021 13:30:00', 'flag_value': 3, 'reason': 'datalogger mulfuntion'},
        ],
    # Soil heat flux plates
        'G_1_1_1': [
            {'start': '25-Oct-2016 16:00:00', 'end': 'now', 'flag_value': 2, 'reason': 'Sensor damaged'}
        ],
       'G_1_4_1': [
            {'start': '14-Dec-2016 06:30:00', 'end': 'now', 'flag_value': 2, 'reason': 'Sensor damaged'}
        ],
    # Soil temp. 
        'TS_2_2_1': [
            {'start': '14-Dec-2016 06:30:00', 'end': 'now', 'flag_value': 2, 'reason': 'Sensor damaged'}
        ],            
	# Radiation fluxes  
        'PPFD_IN_1_1_1':[
            {'start': '10-Jul-2014 9:30:00', 'end': '29-Aug-2014 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '25-Jul-2018 10:00:00', 'end': '04-Sep-2018 15:30:00', 'flag_value': 3, 'reason': 'gap in data'},
			{'start': '05-Sep-2018 05:30:00', 'end': 'now', 'flag_value': 3, 'reason': 'SKL sensors deinstalled for good'},		
			{'start': '29-Jul-2019 19:30:00', 'end': 'now', 'flag_value': 3, 'reason': 'SKL sensors deinstalled for good???'},		
        ],
        'PPFD_OUT_1_1_1':[
            {'start': '10-Jul-2014 9:30:00', 'end': '29-Aug-2014 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '25-Jul-2018 10:00:00', 'end': '04-Sep-2018 15:30:00', 'flag_value': 3, 'reason': 'gap in data'},
			{'start': '05-Sep-2018 05:30:00', 'end': 'now', 'flag_value': 3, 'reason': 'SKL sensors deinstalled for good'},		
			{'start': '29-Jul-2019 19:30:00', 'end': 'now', 'flag_value': 3, 'reason': 'SKL sensors deinstalled for good???'},		
        ],
        'SW_IN_1_1_1':[
            {'start': '10-Jul-2014 9:30:00', 'end': '29-Aug-2014 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet?? corrected after that?'},
            {'start': '18-Mar-2015 09:30:00', 'end': '19-Mar-2015 00:30:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'}
            {'start': '24-Jul-2018 09:00:00', 'end': '09-Sep-2018 15:30:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'}
            {'start': '13-May-2015 03:30:00', 'end': '15-May-2015 21:00:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'}
            {'start': '13-May-2019 04:00:00', 'end': 'now', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'SW_OUT_1_1_1':[
            {'start': '10-Jul-2014 9:30:00', 'end': '29-Aug-2014 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet?? corrected after that?'},
            {'start': '18-Mar-2015 09:30:00', 'end': '19-Mar-2015 00:30:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'}
            {'start': '24-Jul-2018 09:00:00', 'end': '09-Sep-2018 15:30:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'}
            {'start': '13-May-2015 03:30:00', 'end': '15-May-2015 21:00:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'}
            {'start': '13-May-2019 04:00:00', 'end': 'now', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'LW_IN_1_1_1':[
            {'start': '10-Jul-2014 9:30:00', 'end': '29-Aug-2014 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet?? corrected after that?'},
            {'start': '18-Mar-2015 09:30:00', 'end': '19-Mar-2015 00:30:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'}
            {'start': '24-Jul-2018 09:00:00', 'end': '09-Sep-2018 15:30:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'}
            {'start': '13-May-2015 03:30:00', 'end': '15-May-2015 21:00:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'}
            {'start': '13-May-2019 04:00:00', 'end': 'now', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
        'LW_OUT_1_1_1':[
            {'start': '10-Jul-2014 9:30:00', 'end': '29-Aug-2014 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet?? corrected after that?'},
            {'start': '18-Mar-2015 09:30:00', 'end': '19-Mar-2015 00:30:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'}
            {'start': '24-Jul-2018 09:00:00', 'end': '09-Sep-2018 15:30:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'}
            {'start': '13-May-2015 03:30:00', 'end': '15-May-2015 21:00:00', 'flag_value': 2, 'reason': 'pyrranopyrgeometr meas. malf.'}
            {'start': '13-May-2019 04:00:00', 'end': 'now', 'flag_value': 3, 'reason': 'EC measurememet terminated, mast demounted, only soil sensors and rain gauges left'},
        ],
	#Air temperature and humidity
        'TA_1_1_1':[
            {'start': '10-Jul-2014 9:30:00', 'end': '29-Aug-2014 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '18-Jul-2018 13:30:00', 'end': '04-Sep-2018 16:30:00', 'flag_value': 3, 'reason': 'Sensor mulfuntion'},
        ],
        'RH_1_1_1':[
            {'start': '10-Jul-2014 9:30:00', 'end': '29-Aug-2014 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '18-Jul-2018 13:30:00', 'end': '04-Sep-2018 16:30:00', 'flag_value': 3, 'reason': 'Sensor mulfuntion'},
        ],
        'TA_1_2_1':[
            {'start': '10-Jul-2014 9:30:00', 'end': '29-Aug-2014 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '18-Jul-2018 13:30:00', 'end': '04-Sep-2018 16:30:00', 'flag_value': 3, 'reason': 'Sensor mulfuntion'},
        ],
        'RH_1_2_1':[
            {'start': '10-Jul-2014 9:30:00', 'end': '29-Aug-2014 13:00:00', 'flag_value': 3, 'reason': 'sensor not installed yet'},
            {'start': '18-Jul-2018 13:30:00', 'end': '04-Sep-2018 16:30:00', 'flag_value': 3, 'reason': 'Sensor mulfuntion'},
        ],
    }
    # ... i tak dalej dla innych stacji
}

# 8. SŁOWNIK MAPOWANIA GRUP NA REGUŁY FLAGOWANIA
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
    'ME_TOP_MET_30min_MET_30min': 'ME_TOP_QF', 
    'ME_TOP_MET_1min': 'ME_TOP_QF', 
    'ME_Rain_top': 'ME_TOP_QF', 
    # Tlen1
    # Tlen2
    # Sarbia
}

# 9. SŁOWNIK REGUŁ ZMIANY NAZW KOLUMN
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
        'LWOUT_1_2_1': 'LW_OUT_1_1_1', # CNR4
        'Rn_1_2_1': 'RN_1_1_1', # CNR4
        'PPFD_BC_IN_1_1_1': 'PPFD_BC_IN_1_1_1', # 
        'PPFD_BC_IN_1_1_2': 'PPFD_BC_IN_1_1_2', # 
        'TA_2_2_1': 'TA_1_1_2', # CNR4
        'P_1_1_1': 'P_1_2_1', # Dolny
        'P_2_1_1': 'P_1_1_1', # Górny
        # SWC, G i TS zdefiniowane poprawnie w loggerze - bez mapowania nazw
    },
    'SARBIA_MAP': {
        'PPFDR_1_1_1': 'PPFD_OUT_1_1_1', # APOGE
        'PPFD_1_1_1': 'PPFD_IN_1_1_1', # APOGE
        'SWIN_1_1_1': 'SW_IN_1_1_1', # CNR4
        'RG_1_1_1': 'SW_IN_1_1_2', # zika
        'SWOUT_1_1_1': 'SW_OUT_1_1_1', # CNR4
        'LWin_1_1_1': 'LW_IN_1_1_1', # CNR4
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
        'LWout_1_1_1_Avg': 'LW_OUT_1_1_1' # NR01
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
        'T107_1_1_1_Avg': 'TS_1_1_1' # soil profile 1 – 2cm depth
        'T107_1_2_1_Avg': 'TS_1_2_1'  # soil profile 1 – 5cm depth
        'T107_1_3_1_Avg': 'TS_1_3_1'   # soil profile 1 – 10cm depth
        'T107_1_4_1_Avg': 'TS_1_4_1'  # soil profile 1 – 30cm depth
        'T107_1_5_1_Avg': 'TS_1_5_1'  # soil profile 1 – 50cm depth
        # profile 2
        'T107_2_1_1_Avg': 'TS_2_1_1'  # soil profile 2 – 2cm depth
        'T107_2_2_1_Avg': 'TS_2_2_1'  # soil profile 2 – 5cm depth
        'T107_2_3_1_Avg': 'TS_2_3_1'  # soil profile 2 – 10cm depth
        'T107_2_4_1_Avg': 'TS_2_4_1'  # soil profile 2 – 30cm depth
        'T107_2_5_1_Avg': 'TS_2_5_1'  # soil profile 2 – 500cm depth
        # Precipitation measurements- forest floor – Tipping Rain gauges  A-ster TPG
        'P_rain_1_1_1_Tot': 'P_1_1_1', #Rain gauge 1 
        'P_rain_1_2_1_Tot''P_1_1_1', #Rain gauge 1
        # Air temperature and humidity- HMP 155, Vaisala and NR01
        'Ta_1_1_1_Avg': 'TA_1_1_1',  # HMP 155 air temp. at 2m above ground
        'RH_1_1_1_Avg': 'RH_1_1_1',  # HMP 155 air temp. at 2m above ground
        'Ta_1_2_1_Avg': 'TA_1_2_1',  # HMP 155 air temp. at 30cm above ground
        'RH_1_2_1_Avg': 'RH_1_2_1',  # HMP 155 air temp. at 30cm above ground
        #'Tsurf_Avg' 
        #'NR01TC_Avg' 
        #'NR01TK_Avg’
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
        'LWout_1_1_1_Avg': 'LW_OUT_1_1_1' # NR01
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
        'Ts_1_1_1_Avg': 'TS_1_1_1' # soil profile 1 – 2cm depth
        'Ts_1_2_1_Avg': 'TS_1_2_1'  # soil profile 1 – 5cm depth
        'Ts_1_3_1_Avg': 'TS_1_3_1'   # soil profile 1 – 10cm depth
        'Ts_2_4_1_Avg': 'TS_1_4_1'  # soil profile 1 – 30cm depth
        'Ts_2_5_1_Avg': 'TS_1_5_1'  # soil profile 1 – 50cm depth
        # profile 2
        'Ts_2_1_1_Avg': 'TS_2_1_1'  # soil profile 2 – 2cm depth
        'Ts_2_2_1_Avg': 'TS_2_2_1'  # soil profile 2 – 5cm depth
        'Ts_2_3_1_Avg': 'TS_2_3_1'  # soil profile 2 – 10cm depth
        'Ts_2_4_1_Avg': 'TS_2_4_1'  # soil profile 2 – 30cm depth
        'Ts_2_5_1_Avg': 'TS_2_5_1'  # soil profile 2 – 500cm depth
        # Precipitation measurements- forest floor – Tipping Rain gauges  A-ster TPG
        'P_rain_1_1_1_Tot': 'P_1_1_1', #Rain gauge 1 
        'P_rain_1_2_1_Tot''P_1_1_1', #Rain gauge 1
        # Air temperature and humidity- HMP 155, Vaisala and NR01
        'Ta_1_1_1_Avg': 'TA_1_1_1',  # HMP 155 air temp. at 2m above ground
        'RH_1_1_1_Avg': 'RH_1_1_1',  # HMP 155 air temp. at 2m above ground
        'Ta_1_2_1_Avg': 'TA_1_2_1',  # HMP 155 air temp. at 30cm above ground
        'RH_1_2_1_Avg': 'RH_1_2_1',  # HMP 155 air temp. at 30cm above ground
        #'NetRad_1_1_1_Avg' 
        #'NetRad_1_1_2_Avg' 
        #'NR01TC_Avg'
        #'Tsurf_Avg'
        #'Tsky_Avg' 
        #'NR01TC_Avg' 
        #'NR01TK_Avg’
        }
}

# 10. SŁOWNIK MAPOWANIA GRUP NA REGUŁY ZMIANY NAZW
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
    'ME_TOP_MET_1min': 'MEZYK_MAP',
    'ME_DOWN_MET_1min': 'MEZYK_MAP',
    'ME_Rain_down': 'MEZYK_MAP',
    'ME_Rain_top': 'MEZYK_MAP',
    # ----- SARBIA -----
    'SA_MET_30min': 'SARBIA_MAP',
    'SA_MET_1min': 'SARBIA_MAP',
    # ----- TLEN1 -----
    # Tlen1 old- added by Klaudia, 18.07.2025
    'TL1_RAD_30min' : 'TLEN1_MAP',
    'TL1_RAD_1min' : 'TLEN1_MAP',
    # ----- TLEN2 -----
    # Tlen2 "old" tower added by Klaudia- 19.07.2025
    'TL2_MET_1m' : 'TLEN2_MAP',
    'TL2_MET_30m': 'TLEN2_MAP',
    # ... i tak dalej
}

# 11. SŁOWNIK DOPUSZCZALNYCH ZAKRESÓW WARTOŚCI
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