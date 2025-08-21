@echo off
setlocal enabledelayedexpansion

:: ============================================================================
:: Skrypt wsadowy (.bat) do uruchamiania przetwarzania danych dla wielu stacji
:: Wersja ostateczna - uproszczona i odporna na błędy
:: ============================================================================

echo Rozpoczynam przetwarzanie wsadowe...

:: --- SEKCJA KONFIGURACJI ---
:: Wszystkie zmienne, które mogą wymagać modyfikacji, są w jednym miejscu.

:: 1. Pełna ścieżka do Twojego skryptu Pythona
set "PYTHON_SCRIPT_PATH=C:\Users\marek.urbaniak\Documents\pyScripts\unified_script.py"

:: 2. Główny katalog, w którym będą tworzone wszystkie wyniki
set "BASE_OUTPUT_PATH=E:\test_split"
set "DB_OUTPUT_PATH=E:\test_split\metLab.db"

:: 3. Definicje grup file_id dla poszczególnych zadań
REM set "TUCZNO_FIDS= TU_SOIL2_30s TU_EC_30m TU_MET_30m TU_WXT_30m TU_Bole_30m TU_PROF_30m TU_SOIL_30m TU_GARDEN_30m TU_GARDEN_10m TU_STUDNIA_1_10m TU_PROF_2m TU_RAD_1"
set "TUCZNO_FIDS= TU_PROF_1s TU_SOIL_5s TU_MET_5s TU_SOIL2_30s TU_EC_30m TU_MET_30m TU_WXT_30m TU_Bole_30m TU_PROF_30m TU_SOIL_30m TU_GARDEN_30m TU_GARDEN_10m TU_STUDNIA_1_10m TU_PROF_2m TU_RAD_1"
set "SARBIA_FIDS=SA_MET_30min SA_MET_1min"
set "RZECIN_FIDS=RZ_CSI_30 RZ_CSI_MET2_30 RZ_SKYE_30 RZ_PROF_2m RZ_METHAN_1 RZ_Parsival RZ_NR_lite RZ_SWD RZ_WET_30s_RTRH RZ_WET_30s RZ_WET_30m"
set "MEZYK_TOP_FIDS=ME_TOP_MET_1min ME_TOP_MET_30min ME_Rain_top"
set "MEZYK_DOWN_FIDS=ME_DOWN_MET_1min"
REM set "MEZYK_DOWN_FIDS=ME_DOWN_MET_1min ME_DOWN_MET_30min ME_Rain_down ME_CalPlates"
set "TLEN1_FIDS=TL1_MET_30 TL1_RAD_30 TL1_SOIL_30 TL1_RAD_1"
set "TLEN1a_FIDS=TL1a_MET_30_dT TL1a_Rain_down_dT TL1a_MET_1_dT TL1a_CalPlates_dT"
set "TLEN1anew_FIDS=TL1a_MET_30_csi TL1a_MET_1_csi"
set "TLEN2_FIDS=TL2_CalPlates_dT TL2_MET_1_csi TL2_MET_1_dT TL2_MET_1m TL2_MET_30_csi TL2_MET_30_dT TL2_MET_30m"
set "BRODY_FIDS=BR_CSI_30 BR_SpectralData_1m BR_Spec_Veg_Ind_30m"
:: --- KONIEC KONFIGURACJI ---


REM :: --- GŁÓWNE WYKONANIE SKRYPTU ---

REM :: #####################################################################
REM echo.
REM echo ### Przetwarzanie dla stacji: TUCZNO ###
REM echo #####################################################################
REM set "INPUT_PATH=D:\sites\TU\csi_TU\Campbell"
REM set "OUTPUT_PATH=%BASE_OUTPUT_PATH%\TU"
REM if not exist "%INPUT_PATH%\" ( echo Ostrzezenie: Katalog dla stacji TUCZNO nie istnieje. Pomijam. ) else (
    REM for %%F in (%TUCZNO_FIDS%) do (
        REM echo. & echo --- Przetwarzam grupe: %%F ---
        REM python "%PYTHON_SCRIPT_PATH%" -i "%INPUT_PATH%" -o "%OUTPUT_PATH%" -fid "%%F" --log-level DEBUG  --db-path "%DB_OUTPUT_PATH%" --output-format both
    REM )
REM )

REM :: #####################################################################
REM echo.
REM echo ### Przetwarzanie dla stacji: SARBIA ###
REM echo #####################################################################
REM set "INPUT_PATH=D:\sites\SA\csi_SA\Campbell"
REM set "OUTPUT_PATH=%BASE_OUTPUT_PATH%\SA"
REM if not exist "%INPUT_PATH%\" ( echo Ostrzezenie: Katalog dla stacji SARBIA nie istnieje. Pomijam. ) else (
    REM for %%F in (%SARBIA_FIDS%) do (
        REM echo. & echo --- Przetwarzam grupe: %%F ---
        REM python "%PYTHON_SCRIPT_PATH%" -i "%INPUT_PATH%" -o "%OUTPUT_PATH%" -fid "%%F" --log-level DEBUG  --db-path "%DB_OUTPUT_PATH%" --output-format both
    REM )
REM )

REM REM :: #####################################################################
REM echo.
REM echo ### Przetwarzanie dla stacji: RZECIN ###
REM echo #####################################################################
REM set "INPUT_PATH=D:\sites\RZ\csi_RZ\Campbell"
REM set "OUTPUT_PATH=%BASE_OUTPUT_PATH%\RZ"
REM if not exist "%INPUT_PATH%\" ( echo Ostrzezenie: Katalog dla stacji RZECIN nie istnieje. Pomijam. ) else (
    REM for %%F in (%RZECIN_FIDS%) do (
        REM echo. & echo --- Przetwarzam grupe: %%F ---
        REM python "%PYTHON_SCRIPT_PATH%" -i "%INPUT_PATH%" -o "%OUTPUT_PATH%" -fid "%%F" --log-level DEBUG  --db-path "%DB_OUTPUT_PATH%" --output-format both
    REM )
REM )

REM :: #####################################################################
REM echo.
REM echo ### Przetwarzanie dla stacji: TLEN1 ###
REM echo #####################################################################
REM set "INPUT_PATH=D:\sites\TR\csi_TR\Campbell"
REM set "OUTPUT_PATH=%BASE_OUTPUT_PATH%\TL1"
REM if not exist "%INPUT_PATH%\" ( echo Ostrzezenie: Katalog dla stacji TLEN1 nie istnieje. Pomijam. ) else (
    REM for %%F in (%TLEN1_FIDS%) do (
        REM echo. & echo --- Przetwarzam grupe: %%F ---
        REM python "%PYTHON_SCRIPT_PATH%" -i "%INPUT_PATH%" -o "%OUTPUT_PATH%" -fid "%%F" --log-level DEBUG   --db-path "%DB_OUTPUT_PATH%" --output-format both
    REM )
REM )

REM :: #####################################################################
REM echo.
REM echo ### Przetwarzanie dla stacji: TLEN1a (nowe dane) ###
REM echo #####################################################################
REM set "INPUT_PATH=D:\sites\TR\dt_TR\Campbell"
REM set "OUTPUT_PATH=%BASE_OUTPUT_PATH%\TL1"
REM if not exist "%INPUT_PATH%\" ( echo Ostrzezenie: Katalog dla stacji TLEN1a CSI nie istnieje. Pomijam. ) else (
    REM for %%F in (%TLEN1anew_FIDS%) do (
        REM echo. & echo --- Przetwarzam grupe: %%F ---
        REM python "%PYTHON_SCRIPT_PATH%" -i "%INPUT_PATH%" -o "%OUTPUT_PATH%" -fid "%%F" --log-level DEBUG   --db-path "%DB_OUTPUT_PATH%" --output-format both
    REM )
REM )

REM :: #####################################################################
REM echo.
REM echo ### Przetwarzanie dla stacji: TLEN1a (ThermoFisher) ###
REM echo #####################################################################
REM set "INPUT_PATH=D:\sites\TR\dt_TR\ThermoFisher"
REM set "OUTPUT_PATH=%BASE_OUTPUT_PATH%\TL1"
REM if not exist "%INPUT_PATH%\" ( echo Ostrzezenie: Katalog dla stacji TLEN1a ThermoFisher  nie istnieje. Pomijam. ) else (
    REM for %%F in (%TLEN1a_FIDS%) do (
        REM echo. & echo --- Przetwarzam grupe: %%F ---
        REM python "%PYTHON_SCRIPT_PATH%" -i "%INPUT_PATH%" -o "%OUTPUT_PATH%" -fid "%%F" --log-level DEBUG   --db-path "%DB_OUTPUT_PATH%" --output-format both
    REM )
REM )

REM :: #####################################################################
REM echo.
REM echo ### Przetwarzanie dla stacji: TLEN2 csi ###
REM echo #####################################################################
REM set "INPUT_PATH=D:\sites\TR2\csi_TR2\Campbell"
REM set "OUTPUT_PATH=%BASE_OUTPUT_PATH%\TL2"
REM if not exist "%INPUT_PATH%\" ( echo Ostrzezenie: Katalog dla stacji TLEN2 nie istnieje. Pomijam. ) else (
    REM for %%F in (%TLEN2_FIDS%) do (
        REM echo. & echo --- Przetwarzam grupe: %%F ---
        REM python "%PYTHON_SCRIPT_PATH%" -i "%INPUT_PATH%" -o "%OUTPUT_PATH%" -fid "%%F" --log-level DEBUG  --db-path "%DB_OUTPUT_PATH%" --output-format both
    REM )
REM )

REM :: #####################################################################
REM echo.
REM echo ### Przetwarzanie dla stacji: TLEN2 dT ###
REM echo #####################################################################
REM set "INPUT_PATH=D:\sites\TR2\dT_TR2\ThermoFisher"
REM set "OUTPUT_PATH=%BASE_OUTPUT_PATH%\TL2"
REM if not exist "%INPUT_PATH%\" ( echo Ostrzezenie: Katalog dla stacji TLEN2 nie istnieje. Pomijam. ) else (
    REM for %%F in (%TLEN2_FIDS%) do (
        REM echo. & echo --- Przetwarzam grupe: %%F ---
        REM python "%PYTHON_SCRIPT_PATH%" -i "%INPUT_PATH%" -o "%OUTPUT_PATH%" -fid "%%F" --log-level DEBUG  --db-path "%DB_OUTPUT_PATH%" --output-format both
    REM )
REM )

REM :: #####################################################################
REM echo.
REM echo ### Przetwarzanie dla stacji: MEZYK (struktura specjalna) ###
REM echo #####################################################################
set "MEZYK_BASE_INPUT=D:\sites\ME\dT_ME\ThermoFisher"

REM :: --- MEZYK TOP ---
REM set "TOP_DIRS_LIST="
REM for /D %%D in ("%MEZYK_BASE_INPUT%\*") do (
    REM if exist "%%~D\DT85W_top\" (
        REM set "TOP_DIRS_LIST=!TOP_DIRS_LIST! "%%~fD\DT85W_top""
    REM )
REM )
REM if defined TOP_DIRS_LIST (
    REM for %%F in (%MEZYK_TOP_FIDS%) do (
        REM echo. & echo --- Przetwarzam grupe Mezyk-TOP: %%F ---
        REM python "%PYTHON_SCRIPT_PATH%" -i %TOP_DIRS_LIST% -o "%BASE_OUTPUT_PATH%\ME" -fid "%%F" --log-level DEBUG  --db-path "%DB_OUTPUT_PATH%" --output-format both
    REM )
REM ) else (
    REM echo Ostrzezenie: Nie znaleziono katalogow dla loggera Mezyk-TOP.
REM )

:: --- MEZYK DOWN ---
set "DOWN_DIRS_LIST="
for /D %%D in ("%MEZYK_BASE_INPUT%\*") do (
    if exist "%%~D\DT85_down\" (
        set "DOWN_DIRS_LIST=!DOWN_DIRS_LIST! "%%~fD\DT85_down""
    )
)
if defined DOWN_DIRS_LIST (
    for %%F in (%MEZYK_DOWN_FIDS%) do (
        echo. & echo --- Przetwarzam grupe Mezyk-DOWN: %%F ---
        python "%PYTHON_SCRIPT_PATH%" -i %DOWN_DIRS_LIST% -o "%BASE_OUTPUT_PATH%\ME" -fid "%%F" --log-level DEBUG  --db-path "%DB_OUTPUT_PATH%" --output-format both
    )
) else (
    echo Ostrzezenie: Nie znaleziono katalogow dla loggera Mezyk-DOWN.
)


echo.
echo =======================================================
echo Wszystkie zdefiniowane zadania zostaly zakonczone.
echo =======================================================
pause