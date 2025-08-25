#!/bin/bash

# ============================================================================
# ZOPTYMALIZOWANY Skrypt do wsadowego uruchamiania przetwarzania danych
# dla wielu stacji pomiarowych.
# ============================================================================

# --- SEKCJA GŁÓWNEJ KONFIGURACJI ---

# 1. Pełna ścieżka do Twojego skryptu Pythona
PYTHON_SCRIPT_PATH="/Users/marekurbaniak/Documents/pyScript/split2.py"

# 2. Główny katalog, w którym będą tworzone wszystkie wyniki
BASE_OUTPUT_PATH="/Volumes/Promise Pegasus/mybkp/test_split"


# --- FUNKCJA POMOCNICZA ---
# Ta jedna funkcja zastępuje cały powtarzający się kod.
# Argumenty: 1. Nazwa stacji (dla logów), 2. Ścieżka wejściowa, 3. Ścieżka wyjściowa, 4. Lista FIDów (przekazana jako jeden string)
uruchom_przetwarzanie_dla_stacji() {
    local station_name="$1"
    local input_path="$2"
    local output_path="$3"
    # Odczytaj wszystkie pozostałe argumenty jako listę FIDów
    local fids_to_process=("${@:4}")

    echo -e "\n#####################################################################"
    echo "### Rozpoczynam przetwarzanie dla stacji: $station_name"
    echo "#####################################################################"

    # Sprawdź, czy katalog wejściowy istnieje
    if [ ! -d "$input_path" ]; then
        echo "Ostrzeżenie: Katalog wejściowy dla stacji '$station_name' nie istnieje: $input_path. Pomijam."
        return
    fi
    
    # Pętla po zdefiniowanych grupach file_id dla danej stacji
    for fid in "${fids_to_process[@]}"; do
        echo "-------------------------------------------------------"
        echo "-> Przetwarzam grupę: $fid"
        
        # Wywołaj skrypt Pythona
        python3 "$PYTHON_SCRIPT_PATH" \
            -i "$input_path" \
            -o "$output_path" \
            -fid "$fid"
            # Możesz tu dodać inne flagi, np. -ow, jeśli chcesz nadpisać wszystko
            -ow
        
        echo "-> Zakończono przetwarzanie dla grupy: $fid"
    done
}

# --- GŁÓWNE WYKONANIE SKRYPTU ---
# Teraz wystarczy wywołać funkcję pomocniczą dla każdej stacji,
# podając odpowiednie parametry.

# -- Przetwarzanie dla stacji TUCZNO --
uruchom_przetwarzanie_dla_stacji \
    "Tuczno" \
    "/Volumes/Promise Pegasus/sites/TU/csi_TU/Campbell" \
    "$BASE_OUTPUT_PATH/TU" \
    "TU_MET30min" "TU_MET10min" "TU_MET2min" "TU_MET1min" "TU_MET30sec" "TU_MET5sec" "TU_MET1sec"

# -- Przetwarzanie dla stacji SARBIA --
uruchom_przetwarzanie_dla_stacji \
    "Sarbia" \
    "/Volumes/Promise Pegasus/sites/SA/csi_SA/Campbell" \
    "$BASE_OUTPUT_PATH/SA" \
    "SA_MET30min" "SA_MET1min"

# -- Przetwarzanie dla stacji RZECIN --
uruchom_przetwarzanie_dla_stacji \
    "Rzecin" \
    "/Volumes/Promise Pegasus/sites/RZ/csi_RZ/Campbell" \
    "$BASE_OUTPUT_PATH/RZ" \
    "RZ_MET30min" "RZ_MET1min" "RZ_MET30sec"

# -- Przetwarzanie dla stacji Tlen1 --
uruchom_przetwarzanie_dla_stacji \
    "Tlen1" \
    "/Volumes/Promise Pegasus/sites/TR/csi_TR/Campbell" \
    "$BASE_OUTPUT_PATH/TL1" \
    "TL1_MET30min" "TL1_MET1min"
    
# -- Przetwarzanie dla stacji Tlen1a --
uruchom_przetwarzanie_dla_stacji \
    "Tlen1a" \
    "/Volumes/Promise Pegasus/sites/TR/dt_TR/ThermoFisher" \
    "$BASE_OUTPUT_PATH/TL1a" \
    "TL1a_MET30min" "TL1a_PREC30min" "TL1a_MET1min" "TL1a_CAL1min"
    
uruchom_przetwarzanie_dla_stacji \
    "Tlen1a" \
    "/Volumes/Promise Pegasus/sites/TR/dt_TR/Campbell" \
    "$BASE_OUTPUT_PATH/TL1a" \
    "TL1a_MET30min" "TL1a_MET1min"
    
# -- Przetwarzanie dla stacji Tlen2 --
uruchom_przetwarzanie_dla_stacji \
    "Tlen2" \
    "/Volumes/Promise Pegasus/sites/RZ/dt_TR2/ThermoFisher" \
    "$BASE_OUTPUT_PATH/TL2" \
    "TL2_MET30min" "TL2_MET1min" "TL2_CAL1min"
    
# -- Przetwarzanie dla stacji MEZYK (wymaga zebrania wielu katalogów) --
# Dla Mezyka logika jest bardziej złożona, więc zostawiamy ją w osobnym bloku
echo -e "\n#####################################################################"
echo "### Rozpoczynam przetwarzanie dla stacji: Mezyk (struktura specjalna)"
echo "#####################################################################"

# Definicje grup dla Mezyka
ME_TOP_FIDS=("ME_TOP_MET1min" "ME_TOP_MET30min" "ME_PREC_T_30min")
ME_DOWN_FIDS=("ME_DOWN_MET1min" "ME_DOWN_MET30min" "ME_PREC_D_30min" "ME_CAL1min")
BASE_INPUT_PATH="/Volumes/Promise Pegasus/sites"
# Zbuduj listę katalogów dla loggera TOP
TOP_DIRS_LIST=()
for current_dir in "$BASE_INPUT_PATH"/ME/dT_ME/ThermoFisher/*/; do
    echo "${current_dir}"
    if [ -d "$current_dir/DT85W_top/" ]; then TOP_DIRS_LIST+=("$current_dir/DT85W_top/"); fi
done

# Zbuduj listę katalogów dla loggera DOWN
DOWN_DIRS_LIST=()
for current_dir in "$BASE_INPUT_PATH"/ME/dT_ME/ThermoFisher/*/; do
    if [ -d "$current_dir/DT85_down/" ]; then DOWN_DIRS_LIST+=("$current_dir/DT85_down/"); fi
done

# Przetwarzanie grup dla TOP
if [ ${#TOP_DIRS_LIST[@]} -gt 0 ]; then
    for fid in "${ME_TOP_FIDS[@]}"; do
        echo "-------------------------------------------------------"
        echo "-> Przetwarzam grupę Mezyk-TOP: $fid"
        python3 "$PYTHON_SCRIPT_PATH" -i "${TOP_DIRS_LIST[@]}" -o "$BASE_OUTPUT_PATH/ME_TOP" -fid "$fid"
    done
else
    echo "Ostrzeżenie: Nie znaleziono katalogów dla loggera Mezyk-TOP."
fi

# Przetwarzanie grup dla DOWN
if [ ${#DOWN_DIRS_LIST[@]} -gt 0 ]; then
    for fid in "${ME_DOWN_FIDS[@]}"; do
        echo "-------------------------------------------------------"
        echo "-> Przetwarzam grupę Mezyk-DOWN: $fid"
        python3 "$PYTHON_SCRIPT_PATH" -i "${DOWN_DIRS_LIST[@]}" -o "$BASE_OUTPUT_PATH/ME_DOWN" -fid "$fid"
    done
else
    echo "Ostrzeżenie: Nie znaleziono katalogów dla loggera Mezyk-DOWN."
fi

echo -e "\n======================================================="
echo "Wszystkie zdefiniowane zadania zostały zakończone."
echo "======================================================="