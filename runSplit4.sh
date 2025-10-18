#!/bin/bash

# ============================================================================
# Skrypt (.sh) do uruchamiania przetwarzania danych dla wielu stacji
# Wersja 4.0 - Dynamiczne ścieżki, dedykowane bazy danych i selektywne uruchamianie
# ============================================================================

echo "Rozpoczynam przetwarzanie wsadowe..."

# --- SEKCJA KONFIGURACJI ---

# 1. Lista stacji do przetworzenia (oddzielone spacjami w nawiasach).
#    Dostępne opcje: TU SA RZ ME_TOP ME_DOWN TL1 TL1a TL1anew TL2 BR
STATIONS=("SA" "TL1" "TL1a" "TL2" "ME")

# 2. Ścieżka do Twojego skryptu Pythona (może być względna lub bezwzględna)
PYTHON_SCRIPT_PATH="./unified_script.py"

# 3. Główny katalog, w którym znajdują się dane wejściowe i gdzie będą tworzone wyniki
#    UWAGA: Zmień tę ścieżkę na odpowiednią dla Twojego systemu!
BASE_SITES_PATH="/Volumes/Promise Pegasus/sites"

# 4. Definicje grup file_id dla poszczególnych zadań
TUCZNO_FIDS="TU_PROF_1s TU_SOIL_5s TU_MET_5s TU_SOIL2_30s TU_EC_30m TU_MET_30m TU_WXT_30m TU_Bole_30m TU_PROF_30m TU_SOIL_30m TU_GARDEN_30m TU_GARDEN_10m TU_STUDNIA_1_10m TU_PROF_2m TU_RAD_1"
SARBIA_FIDS="SA_MET_30min SA_MET_1min"
RZECIN_FIDS="RZ_CSI_30 RZ_CSI_MET2_30 RZ_SKYE_30 RZ_PROF_2m RZ_METHAN_1 RZ_Parsival RZ_NR_lite RZ_SWD RZ_WET_30s_RTRH RZ_WET_30s RZ_WET_30m"
MEZYK_TOP_FIDS="ME_TOP_MET_1min ME_TOP_MET_30min ME_Rain_top"
MEZYK_DOWN_FIDS="ME_DOWN_MET_1min ME_DOWN_MET_30min ME_Rain_down ME_CalPlates"
TLEN1_FIDS="TL1_MET_30 TL1_RAD_30 TL1_SOIL_30 TL1_RAD_1"
TLEN1a_FIDS="TL1a_MET_30_dT TL1a_Rain_down_dT TL1a_MET_1_dT TL1a_CalPlates_dT"
TLEN1anew_FIDS="TL1a_MET_30_csi TL1a_MET_1_csi"
TLEN2_FIDS="TL2_CalPlates_dT TL2_MET_1_csi TL2_MET_1_dT TL2_MET_1m TL2_MET_30_csi TL2_MET_30_dT TL2_MET_30m"
BRODY_FIDS="BR_CSI_30 BR_SpectralData_1m BR_Spec_Veg_Ind_30m"

# --- KONIEC KONFIGURACJI ---

# --- FUNKCJA PRZETWARZANIA DLA STANDARDOWEJ STACJI ---
process_station() {
    local station_name="$1"
    local fids="$2"
    local input_path="$3"
    local output_path="$4"
    local db_path="$5"

    if [ ! -d "$input_path" ]; then
        echo "Ostrzezenie: Katalog dla stacji $station_name nie istnieje: $input_path. Pomijam."
        return
    fi

    if [ ! -d "$output_path" ]; then
        echo "Tworze katalog wyjsciowy: $output_path"
        mkdir -p "$output_path"
    fi

    for F in $fids; do
        echo
        echo "--- Przetwarzam grupe: $F ---"
        python "$PYTHON_SCRIPT_PATH" -i "$input_path" -o "$output_path" -fid "$F" --log-level DEBUG --db-path "$db_path" --output-format both
    done
}

# --- GŁÓWNA PĘTLA PRZETWARZANIA ---
for STATION_ID in "${STATIONS[@]}"; do
    echo
    echo "#####################################################################"
    echo "### Przetwarzanie dla stacji: $STATION_ID ###"
    echo "#####################################################################"

    case $STATION_ID in
        TU)
            INPUT_PATH="$BASE_SITES_PATH/TU/csi_TU/Campbell"
            OUTPUT_PATH="$BASE_SITES_PATH/TU/met_data_TU"
            DB_PATH="$OUTPUT_PATH/met_data_TU.db"
            process_station "TU" "$TUCZNO_FIDS" "$INPUT_PATH" "$OUTPUT_PATH" "$DB_PATH"
            ;;
        SA)
            INPUT_PATH="$BASE_SITES_PATH/SA/csi_SA/Campbell"
            OUTPUT_PATH="$BASE_SITES_PATH/SA/met_data_SA"
            DB_PATH="$OUTPUT_PATH/met_data_SA.db"
            process_station "SA" "$SARBIA_FIDS" "$INPUT_PATH" "$OUTPUT_PATH" "$DB_PATH"
            ;;
        TL1)
            INPUT_PATH="$BASE_SITES_PATH/TR/csi_TR/Campbell"
            OUTPUT_PATH="$BASE_SITES_PATH/TR/met_data_TL1"
            DB_PATH="$OUTPUT_PATH/met_data_TL1.db"
            process_station "TL1" "$TLEN1_FIDS" "$INPUT_PATH" "$OUTPUT_PATH" "$DB_PATH"
            ;;
        TL1a)
            INPUT_PATH="$BASE_SITES_PATH/TR/dt_TR/ThermoFisher"
            OUTPUT_PATH="$BASE_SITES_PATH/TR/met_data_TL1" # Wspólny output dla TL1
            DB_PATH="$OUTPUT_PATH/met_data_TL1.db"
            process_station "TL1a" "$TLEN1a_FIDS" "$INPUT_PATH" "$OUTPUT_PATH" "$DB_PATH"
            ;;
        TL1anew)
            INPUT_PATH="$BASE_SITES_PATH/TR/dT_TR/Campbell"
            OUTPUT_PATH="$BASE_SITES_PATH/TR/met_data_TL1" # Wspólny output dla TL1
            DB_PATH="$OUTPUT_PATH/met_data_TL1.db"
            process_station "TL1anew" "$TLEN1anew_FIDS" "$INPUT_PATH" "$OUTPUT_PATH" "$DB_PATH"
            ;;
        TL2)
            OUTPUT_PATH="$BASE_SITES_PATH/TR2/met_data_TL2"
            DB_PATH="$OUTPUT_PATH/met_data_TL2.db"
            
            echo
            echo "--- Przetwarzam dane CSI dla TL2 ---"
            INPUT_PATH_CSI="$BASE_SITES_PATH/TR2/csi_TR2/Campbell"
            process_station "TL2_csi" "$TLEN2_FIDS" "$INPUT_PATH_CSI" "$OUTPUT_PATH" "$DB_PATH"
            
            echo
            echo "--- Przetwarzam dane dT dla TL2 ---"
            INPUT_PATH_DT="$BASE_SITES_PATH/TR2/dT_TR2/ThermoFisher"
            process_station "TL2_dT" "$TLEN2_FIDS" "$INPUT_PATH_DT" "$OUTPUT_PATH" "$DB_PATH"
            ;;
        ME)
            MEZYK_BASE_INPUT="$BASE_SITES_PATH/ME/dT_ME/ThermoFisher"
            OUTPUT_PATH="$BASE_SITES_PATH/ME/met_data_ME"
            DB_PATH="$OUTPUT_PATH/met_data_ME.db"

            if [ ! -d "$OUTPUT_PATH" ]; then
                echo "Tworze katalog wyjsciowy: $OUTPUT_PATH"
                mkdir -p "$OUTPUT_PATH"
            fi

            # --- MEZYK TOP ---
            TOP_DIRS_LIST=()
            for D in "$MEZYK_BASE_INPUT"/*; do
                if [ -d "$D/DT85W_top" ]; then
                    TOP_DIRS_LIST+=("$D/DT85W_top")
                fi
            done
            
            if [ ${#TOP_DIRS_LIST[@]} -gt 0 ]; then
                for F in $MEZYK_TOP_FIDS; do
                    echo
                    echo "--- Przetwarzam grupe Mezyk-TOP: $F ---"
                    python "$PYTHON_SCRIPT_PATH" -i "${TOP_DIRS_LIST[@]}" -o "$OUTPUT_PATH" -fid "$F" --log-level DEBUG --db-path "$DB_PATH" --output-format both
                done
            else
                echo "Ostrzezenie: Nie znaleziono katalogow dla loggera Mezyk-TOP."
            fi

            # --- MEZYK DOWN ---
            DOWN_DIRS_LIST=()
            for D in "$MEZYK_BASE_INPUT"/*; do
                if [ -d "$D/DT85_down" ]; then
                    DOWN_DIRS_LIST+=("$D/DT85_down")
                fi
            done

            if [ ${#DOWN_DIRS_LIST[@]} -gt 0 ]; then
                for F in $MEZYK_DOWN_FIDS; do
                    echo
                    echo "--- Przetwarzam grupe Mezyk-DOWN: $F ---"
                    python "$PYTHON_SCRIPT_PATH" -i "${DOWN_DIRS_LIST[@]}" -o "$OUTPUT_PATH" -fid "$F" --log-level DEBUG --db-path "$DB_PATH" --output-format both
                done
            else
                echo "Ostrzezenie: Nie znaleziono katalogow dla loggera Mezyk-DOWN."
            fi
            ;;
        *)
            echo "Ostrzezenie: Nieznana stacja '$STATION_ID' na liście STATIONS. Pomijam."
            ;;
    esac
done

echo
echo "======================================================"
echo "Wszystkie zdefiniowane zadania zostaly zakonczone."
echo "======================================================"
