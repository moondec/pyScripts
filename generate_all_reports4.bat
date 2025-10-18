@ECHO OFF
CHCP 65001 > NUL
TITLE Generator Raportow PDF v4

:: =================================================================================
:: Plik wsadowy do seryjnego generowania raportow wizualizacyjnych.
:: Wersja 4.0 - Dostosowany do odczytu z rozproszonych baz danych.
:: =================================================================================

:: --- SEKCJA KONFIGURACJI ---

:: 1. Sciezka do skryptu Python
SET "PYTHON_SCRIPT=C:\Users\marek.urbaniak\DokumentsMacBookPro\pyScripts\view_splitSQ.py"

:: 2. Główny katalog, w którym znajdują się dane (zgodnie z runSplit4.bat)
set "BASE_SITES_PATH=D:\sites"

:: 3. Lista prefiksow stacji do przetworzenia
::    (Musi byc zgodna ze stacjami przetworzonymi przez runSplit4.bat)
SET STATIONS=SA TL1 TL1a TL2 ME

:: 4. Lista zmiennych do zwizualizowania (uzyj wzorcow z '*' na koncu)
SET "VARS_TO_PLOT="SW_IN_*" "PPFD_IN_*" "PPFD_BC_IN_*" "TA_*" "TS_*""

:: --- SEKCJA WYKONAWCZA ---

SETLOCAL EnableDelayedExpansion

ECHO.
ECHO Rozpoczynanie procesu generowania raportow v4...
ECHO =================================================

:: Petla przechodzaca przez kazdy prefiks stacji z listy STATIONS
FOR %%s IN (%STATIONS%) DO (
    ECHO.
    ECHO [---] Rozpoczynam przetwarzanie stacji: %%s [---]

    :: Dynamiczne definiowanie ścieżek na podstawie ID stacji
    SET "STATION_ID=%%s"
    SET "OUTPUT_DIR="
    SET "DB_PATH="

    if "!STATION_ID!"=="SA" (
        SET "OUTPUT_DIR=%BASE_SITES_PATH%\SA\met_data_SA"
        SET "DB_PATH=!OUTPUT_DIR!\met_data_SA.db"
    )
    if "!STATION_ID!"=="TL1" ( 
        SET "OUTPUT_DIR=%BASE_SITES_PATH%\TR\met_data_TL1"
        SET "DB_PATH=!OUTPUT_DIR!\met_data_TL1.db"
    )
    if "!STATION_ID!"=="TL1a" ( 
        SET "OUTPUT_DIR=%BASE_SITES_PATH%\TR\met_data_TL1"
        SET "DB_PATH=!OUTPUT_DIR!\met_data_TL1.db"
    )
    if "!STATION_ID!"=="TL2" ( 
        SET "OUTPUT_DIR=%BASE_SITES_PATH%\TR2\met_data_TL2"
        SET "DB_PATH=!OUTPUT_DIR!\met_data_TL2.db"
    )
    if "!STATION_ID!"=="ME" ( 
        SET "OUTPUT_DIR=%BASE_SITES_PATH%\ME\met_data_ME"
        SET "DB_PATH=!OUTPUT_DIR!\met_data_ME.db"
    )

    :: Sprawdzenie, czy zdefiniowano ścieżki dla danej stacji
    IF NOT DEFINED DB_PATH (
        ECHO [!!!] Ostrzezenie: Brak konfiguracji sciezek dla stacji '!STATION_ID!'. Pomijam.
    ) ELSE (
        :: Sprawdzenie, czy baza danych istnieje
        IF NOT EXIST "!DB_PATH!" (
            ECHO [!!!] Blad: Baza danych dla stacji '!STATION_ID!' nie zostala znaleziona w '!DB_PATH!'. Pomijam.
        ) ELSE (
            ECHO Baza danych: !DB_PATH!
            ECHO Katalog wyjsciowy: !OUTPUT_DIR!

            :: Uruchomienie skryptu Pythona z odpowiednimi argumentami
            ECHO Uruchamiam skrypt dla grup pasujacych do wzorca "!STATION_ID!_*"...
            ECHO Wybrane zmienne: %VARS_TO_PLOT%
            
            python %PYTHON_SCRIPT% --db_path "!DB_PATH!" --output_dir "!OUTPUT_DIR!" --groups "!STATION_ID!_*" --vars %VARS_TO_PLOT%
            
            ECHO [OK] Zakonczono przetwarzanie stacji: !STATION_ID!
        )
    )
)

ECHO.
ECHO =================================================
ECHO Wszystkie stacje zostaly przetworzone.
ECHO.
PAUSE
