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
        {"start": "2018-06-05 09:29:00", "end": "2018-06-07 13:02:00", "offset_hours": -1.18 },
        {"start": "2018-10-03 00:00:00", "end": "2018-12-22 00:00:00", "offset_hours": 0.5 },
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
        {'start': '2024-11-10 03:20:00', 'end': '2024-11-10 03:27:00', "offset_hours": 1507.45 },
        {'start': '2024-11-19 13:41:00', 'end': '2024-11-20 17:01:00', "offset_hours": 50.72 },
        # {'start': '2024-12-10T04:09:00', 'end': '2024-12-10T05:08:00', "offset_hours": 9.42 },
        # {'start': '2025-01-08 02:47:00', 'end': '2025-01-08 16:08:00', "offset_hours": 41.12 },
        {'start': '2025-05-15 05:26:00', 'end': '2025-05-20 08:53:00', "offset_hours": 28.38 },
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