@ECHO OFF
CHCP 65001 > NUL
TITLE PDF Report Generator v4

:: =================================================================================
:: Batch file for serial generation of visualization reports.
:: Version 4.0 - Adapted for reading from distributed databases.
:: =================================================================================

:: --- CONFIGURATION SECTION ---

:: 1. Path to Python script
SET "PYTHON_SCRIPT=C:\Users\marek.urbaniak\DokumentsMacBookPro\pyScripts\view_splitSQ.py"

:: 2. Main directory where the data is located (according to runSplit4.bat)
set "BASE_SITES_PATH=D:\sites"

:: 3. List of station prefixes to process
::    (Must be consistent with the stations processed by runSplit4.bat)
@REM SET STATIONS=SA TL1 TL1a TL2 ME
SET STATIONS=TL1 TL1a TL2

:: 4. List of variables to visualize (use patterns with '*' at the end)
SET "VARS_TO_PLOT="SW_IN_*" "PPFD_IN_*" "PPFD_BC_IN_*" "TA_*" "TS_*""

:: --- EXECUTION SECTION ---

SETLOCAL EnableDelayedExpansion

ECHO.
ECHO Starting the report generation process v4...
ECHO =================================================

:: Loop through each station prefix from the STATIONS list
FOR %%s IN (%STATIONS%) DO (
    ECHO.
    ECHO [---] Starting processing for station: %%s [---]

    :: Dynamic definition of paths based on station ID
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

    :: Check if paths are defined for the given station
    IF NOT DEFINED DB_PATH (
        ECHO [!!!] Warning: No path configuration for station '!STATION_ID!'. Skipping.
    ) ELSE (
        :: Check if the database exists
        IF NOT EXIST "!DB_PATH!" (
            ECHO [!!!] Error: Database for station '!STATION_ID!' not found in '!DB_PATH!'. Skipping.
        ) ELSE (
            ECHO Database: !DB_PATH!
            ECHO Output directory: !OUTPUT_DIR!

            :: Running the Python script with the appropriate arguments
            ECHO Running script for groups matching the pattern "!STATION_ID!_*"...
            ECHO Selected variables: %VARS_TO_PLOT%
            
            python %PYTHON_SCRIPT% --db_path "!DB_PATH!" --output_dir "!OUTPUT_DIR!" --groups "!STATION_ID!_*" --vars %VARS_TO_PLOT%
            
            ECHO [OK] Finished processing station: !STATION_ID!
        )
    )
)

ECHO.
ECHO =================================================
ECHO All stations have been processed.
ECHO.
