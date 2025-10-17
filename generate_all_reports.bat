@ECHO OFF
CHCP 65001 > NUL
TITLE Generator Raportow PDF

:: =================================================================================
:: Plik wsadowy do seryjnego generowania raportow wizualizacyjnych.
::
:: Uruchamia skrypt view_splitSQ.py dla kazdej zdefiniowanej stacji,
:: tworzac dla niej osobny folder wyjsciowy i generujac wykresy
:: dla wybranych, kluczowych zmiennych meteorologicznych.
:: =================================================================================

:: --- SEKCJA KONFIGURACJI ---

:: 1. Sciezka do skryptu Python
SET "PYTHON_SCRIPT=C:\Users\marek.urbaniak\DokumentsMacBookPro\pyScripts\view_splitSQ.py"

:: 2. Sciezki do bazy danych i glownego folderu wyjsciowego (pobrane z oryginalnego skryptu)
SET "DB_PATH=E:\test_split\metLab.db"
SET "BASE_OUTPUT_DIR=E:\pdfy\view_splitSQ"

:: 3. Lista prefiksow stacji do przetworzenia (zgodnie z config.py)
::    (Mozesz dowolnie modyfikowac te liste, np. SET STATIONS=TU ME RZ)
REM SET STATIONS=TU ME TL1 TL1a TL2 RZ BR
SET STATIONS=SA TL1 TL1a TL2 ME

:: 4. Lista zmiennych do zwizualizowania (uzyj wzorcow z '*' na koncu)
::    Wybrano podstawowe zmienne: Temperatura, Promieniowanie krotkofalowe, PAR, Opad.
:: SET "VARS_TO_PLOT="TA_*" "SW_IN_*" "PPFD_IN_*" "P_*""
SET "VARS_TO_PLOT="SW_IN_*" "PPFD_IN_*" "PPFD_BC_IN_*" "TA_*" "TS_*""
REM SET "VARS_TO_PLOT="SW_*" "PPFD*" "PPFD_BC_IN_*" "Sw_*""

:: --- SEKCJA WYKONAWCZA ---

ECHO.
ECHO Rozpoczynanie procesu generowania raportow...
ECHO Baza danych: %DB_PATH%
ECHO =================================================

:: Petla przechodzaca przez kazdy prefiks stacji z listy STATIONS
FOR %%s IN (%STATIONS%) DO (
    ECHO.
    ECHO [---] Rozpoczynam przetwarzanie stacji: %%s [---]

    :: Definiowanie i tworzenie dedykowanego folderu dla raportow danej stacji
    SET "STATION_OUTPUT_DIR=%BASE_OUTPUT_DIR%\%%s"
    IF NOT EXIST "%STATION_OUTPUT_DIR%" (
        ECHO Tworze folder wyjsciowy: %STATION_OUTPUT_DIR%
        MKDIR "%STATION_OUTPUT_DIR%"
    ) ELSE (
        ECHO Folder wyjsciowy juz istnieje.
    )

    :: Uruchomienie skryptu Pythona z odpowiednimi argumentami
    ECHO Uruchamiam skrypt dla grup pasujacych do wzorca "%%s_*"...
    ECHO Wybrane zmienne: %VARS_TO_PLOT%
    
    python %PYTHON_SCRIPT% --db_path "%DB_PATH%" --output_dir "%STATION_OUTPUT_DIR%" --groups "%%s_*" --vars %VARS_TO_PLOT%
    
    ECHO [OK] Zakonczono przetwarzanie stacji: %%s
)

ECHO.
ECHO =================================================
ECHO Wszystkie stacje zostaly przetworzone.
ECHO.
PAUSE