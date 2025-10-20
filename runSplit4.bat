@echo off
setlocal enabledelayedexpansion

:: ============================================================================
:: Skrypt wsadowy (.bat) do uruchamiania przetwarzania danych dla wielu stacji
:: Wersja 4.1 - Poprawki w obsłudze ścieżek i przekazywaniu argumentów
:: ============================================================================

echo Rozpoczynam przetwarzanie wsadowe...

:: --- SEKCJA KONFIGURACJI ---

:: 1. Lista stacji do przetworzenia (oddzielone spacjami).
::    Dostepne opcje: TU SA RZ ME_TOP ME_DOWN TL1 TL1a TL1anew TL2 BR
@REM set "STATIONS=SA TL1 TL1a TL2 ME"
set "STATIONS=TL1 TL1a TL1anew TL2"

:: 2. Pełna ścieżka do Twojego skryptu Pythona
set "PYTHON_SCRIPT_PATH=C:\Users\marek.urbaniak\DokumentsMacBookPro\pyScripts\unified_script.py"

:: 3. Główny katalog, w którym znajdują się dane wejściowe i gdzie będą tworzone wyniki
set "BASE_SITES_PATH=D:\sites"

:: 4. Definicje grup file_id dla poszczególnych zadań
set "TUCZNO_FIDS=TU_PROF_1s TU_SOIL_5s TU_MET_5s TU_SOIL2_30s TU_EC_30m TU_MET_30m TU_WXT_30m TU_Bole_30m TU_PROF_30m TU_SOIL_30m TU_GARDEN_30m TU_GARDEN_10m TU_STUDNIA_1_10m TU_PROF_2m TU_RAD_1"
set "SARBIA_FIDS=SA_MET_30min SA_MET_1min"
set "RZECIN_FIDS=RZ_CSI_30 RZ_CSI_MET2_30 RZ_SKYE_30 RZ_PROF_2m RZ_METHAN_1 RZ_Parsival RZ_NR_lite RZ_SWD RZ_WET_30s_RTRH RZ_WET_30s RZ_WET_30m"
set "MEZYK_TOP_FIDS=ME_TOP_MET_1min ME_TOP_MET_30min ME_Rain_top"
set "MEZYK_DOWN_FIDS=ME_DOWN_MET_1min ME_DOWN_MET_30min ME_Rain_down ME_CalPlates"
set "TLEN1_FIDS=TL1_MET_30 TL1_RAD_30 TL1_SOIL_30 TL1_RAD_1"
set "TLEN1a_FIDS=TL1a_MET_30_dT TL1a_Rain_down_dT TL1a_MET_1_dT TL1a_CalPlates_dT"
set "TLEN1anew_FIDS=TL1a_MET_30_csi TL1a_MET_1_csi"
set "TLEN2_FIDS=TL2_CalPlates_dT TL2_MET_1_csi TL2_MET_1_dT TL2_MET_1m TL2_MET_30_csi TL2_MET_30_dT TL2_MET_30m"
set "BRODY_FIDS=BR_CSI_30 BR_SpectralData_1m BR_Spec_Veg_Ind_30m"

:: --- KONIEC KONFIGURACJI ---


:: --- GŁÓWNA PĘTLA PRZETWARZANIA ---
for %%S in (%STATIONS%) do (
    echo.
    echo #####################################################################
    echo ### Przetwarzanie dla stacji: %%S ###
    echo #####################################################################

    set "STATION_ID=%%S"

    if "!STATION_ID!"=="SA" (
        set "INPUT_PATH=%BASE_SITES_PATH%\SA\csi_SA\Campbell"
        set "OUTPUT_PATH=%BASE_SITES_PATH%\SA\met_data_SA"
        set "DB_PATH=!OUTPUT_PATH!\met_data_SA.db"
        call :process_station SA "!SARBIA_FIDS!"
    )

    if "!STATION_ID!"=="TL1" (
        set "INPUT_PATH=%BASE_SITES_PATH%\TR\csi_TR\Campbell"
        set "OUTPUT_PATH=%BASE_SITES_PATH%\TR\met_data_TL1"
        set "DB_PATH=!OUTPUT_PATH!\met_data_TL1.db"
        call :process_station TL1 "!TLEN1_FIDS!"
    )
    
    if "!STATION_ID!"=="TL1a" (
        set "INPUT_PATH=%BASE_SITES_PATH%\TR\dt_TR\ThermoFisher"
        set "OUTPUT_PATH=%BASE_SITES_PATH%\TR\met_data_TL1"
        set "DB_PATH=!OUTPUT_PATH!\met_data_TL1.db"
        call :process_station TL1a "!TLEN1a_FIDS!"
    )

    if "!STATION_ID!"=="TL1anew" (
        set "INPUT_PATH=%BASE_SITES_PATH%\TR\dT_TR\Campbell"
        set "OUTPUT_PATH=%BASE_SITES_PATH%\TR\met_data_TL1"
        set "DB_PATH=%OUTPUT_PATH%\met_data_TL1.db"
        call :process_station TL1anew "!TLEN1anew_FIDS!"
    )

    if "!STATION_ID!"=="TL2" (
        set "OUTPUT_PATH=%BASE_SITES_PATH%\TR2\met_data_TL2"
        set "DB_PATH=!OUTPUT_PATH!\met_data_TL2.db"
        
        echo. & echo --- Przetwarzam dane CSI dla TL2 ---
        set "INPUT_PATH=%BASE_SITES_PATH%\TR2\csi_TR2\Campbell"
        call :process_station TL2_csi "!TLEN2_FIDS!"

        echo. & echo --- Przetwarzam dane dT dla TL2 ---
        set "INPUT_PATH=%BASE_SITES_PATH%\TR2\dT_TR2\ThermoFisher"
        call :process_station TL2_dT "!TLEN2_FIDS!"
    )

    if "!STATION_ID!"=="ME" (
        set "MEZYK_BASE_INPUT=%BASE_SITES_PATH%\ME\dT_ME\ThermoFisher"
        set "OUTPUT_PATH=%BASE_SITES_PATH%\ME\met_data_ME"
        set "DB_PATH=!OUTPUT_PATH!\met_data_ME.db"

        if not exist "!OUTPUT_PATH!\" (
            echo Tworze katalog wyjsciowy: !OUTPUT_PATH!
            mkdir "!OUTPUT_PATH!"
        )

        :: --- MEZYK TOP ---
        set "TOP_DIRS_LIST="
        for /D %%D in ("!MEZYK_BASE_INPUT!\*") do (
            if exist "%%~D\DT85W_top\" (
                set "TOP_DIRS_LIST=!TOP_DIRS_LIST! "%%~fD\DT85W_top""
            )
        )
        if defined TOP_DIRS_LIST (
            for %%F in (%MEZYK_TOP_FIDS%) do (
                echo. & echo --- Przetwarzam grupe Mezyk-TOP: %%F ---
                python "%PYTHON_SCRIPT_PATH%" -i !TOP_DIRS_LIST! -o "!OUTPUT_PATH!" -fid "%%F" --log-level DEBUG --db-path "!DB_PATH!" --output-format both
            )
        ) else (
            echo Ostrzezenie: Nie znaleziono katalogow dla loggera Mezyk-TOP.
        )

        :: --- MEZYK DOWN ---
        set "DOWN_DIRS_LIST="
        for /D %%D in ("!MEZYK_BASE_INPUT!\*") do (
            if exist "%%~D\DT85_down\" (
                set "DOWN_DIRS_LIST=!DOWN_DIRS_LIST! "%%~fD\DT85_down""
            )
        )
        if defined DOWN_DIRS_LIST (
            for %%F in (%MEZYK_DOWN_FIDS%) do (
                echo. & echo --- Przetwarzam grupe Mezyk-DOWN: %%F ---
                python "%PYTHON_SCRIPT_PATH%" -i !DOWN_DIRS_LIST! -o "!OUTPUT_PATH!" -fid "%%F" --log-level DEBUG --db-path "!DB_PATH!" --output-format both
            )
        ) else (
            echo Ostrzezenie: Nie znaleziono katalogow dla loggera Mezyk-DOWN.
        )
    )
)

goto:end

:: --- PROCEDURA PRZETWARZANIA DLA STANDARDOWEJ STACJI ---
:process_station
    set "local_station_name=%1"
    set "local_fids=%~2"

    if not exist "!INPUT_PATH!\" (
        echo Ostrzezenie: Katalog dla stacji !local_station_name! nie istnieje: !INPUT_PATH!. Pomijam.
        goto:eof
    )

    if not exist "!OUTPUT_PATH!\" (
        echo Tworze katalog wyjsciowy: !OUTPUT_PATH!
        mkdir "!OUTPUT_PATH!"
    )

    for %%F in (%local_fids%) do (
        echo. & echo --- Przetwarzam grupe: %%F ---
        python "!PYTHON_SCRIPT_PATH!" -i "!INPUT_PATH!" -o "!OUTPUT_PATH!" -fid "%%F" --log-level DEBUG --db-path "!DB_PATH!" --output-format both --no-cache --overwrite
    )
goto:eof


:end
echo.
echo =======================================================
echo Wszystkie zdefiniowane zadania zostaly zakonczone.
echo =======================================================
@REM pause