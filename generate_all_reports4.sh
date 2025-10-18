#!/bin/bash

# =================================================================================
# Plik wsadowy do seryjnego generowania raportow wizualizacyjnych.
# Wersja 4.0 - Dostosowany do odczytu z rozproszonych baz danych.
# =================================================================================

echo "Generator Raportow PDF v4"

# --- SEKCJA KONFIGURACJI ---

# 1. Sciezka do skryptu Python
PYTHON_SCRIPT="./view_splitSQ.py"

# 2. Główny katalog, w którym znajdują się dane (zgodnie z runSplit4.sh)
#    UWAGA: Zmień tę ścieżkę na odpowiednią dla Twojego systemu!
BASE_SITES_PATH="/Volumes/Promise Pegasus/sites"

# 3. Lista prefiksow stacji do przetworzenia
#    (Musi byc zgodna ze stacjami przetworzonymi przez runSplit4.sh)
STATIONS=("SA" "TL1" "TL1a" "TL2" "ME")

# 4. Lista zmiennych do zwizualizowania (uzyj wzorcow z '*' na koncu)
VARS_TO_PLOT='"SW_IN_*" "PPFD_IN_*" "PPFD_BC_IN_*" "TA_*" "TS_*"'

# --- SEKCJA WYKONAWCZA ---

echo
echo "Rozpoczynanie procesu generowania raportow v4..."
echo "================================================="

# Petla przechodzaca przez kazdy prefiks stacji z listy STATIONS
for STATION_ID in "${STATIONS[@]}"; do
    echo
    echo "[---] Rozpoczynam przetwarzanie stacji: $STATION_ID [---]"

    # Dynamiczne definiowanie ścieżek na podstawie ID stacji
    OUTPUT_DIR=""
    DB_PATH=""

    case $STATION_ID in
        SA)
            OUTPUT_DIR="$BASE_SITES_PATH/SA/met_data_SA"
            DB_PATH="$OUTPUT_DIR/met_data_SA.db"
            ;;
        TL1 | TL1a)
            OUTPUT_DIR="$BASE_SITES_PATH/TR/met_data_TL1"
            DB_PATH="$OUTPUT_DIR/met_data_TL1.db"
            ;;
        TL2)
            OUTPUT_DIR="$BASE_SITES_PATH/TR2/met_data_TL2"
            DB_PATH="$OUTPUT_DIR/met_data_TL2.db"
            ;;
        ME)
            OUTPUT_DIR="$BASE_SITES_PATH/ME/met_data_ME"
            DB_PATH="$OUTPUT_DIR/met_data_ME.db"
            ;;
        *)
            echo "[!!!] Ostrzezenie: Brak konfiguracji sciezek dla stacji '$STATION_ID'. Pomijam."
            continue
            ;;
    esac

    # Sprawdzenie, czy baza danych istnieje
    if [ ! -f "$DB_PATH" ]; then
        echo "[!!!] Blad: Baza danych dla stacji '$STATION_ID' nie zostala znaleziona w '$DB_PATH'. Pomijam."
    else
        echo "Baza danych: $DB_PATH"
        echo "Katalog wyjsciowy: $OUTPUT_DIR"

        # Uruchomienie skryptu Pythona z odpowiednimi argumentami
        echo "Uruchamiam skrypt dla grup pasujacych do wzorca \"${STATION_ID}_*\"..."
        echo "Wybrane zmienne: $VARS_TO_PLOT"
        
        # Używamy eval, aby poprawnie zinterpretować zmienne z gwiazdkami
        eval "python $PYTHON_SCRIPT --db_path \"$DB_PATH\" --output_dir \"$OUTPUT_DIR\" --groups \"${STATION_ID}_*\" --vars $VARS_TO_PLOT"
        
        echo "[OK] Zakonczono przetwarzanie stacji: $STATION_ID"
    fi
done

echo
echo "================================================="
echo "Wszystkie stacje zostaly przetworzone."
echo
